//! Stable-reference projection for derived consumers. The projector is
//! deterministic and never signs, fetches, or mutates canonical data.

use std::collections::HashSet;

use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct StableAssetRefs {
    pub object_keys: HashSet<String>,
    pub pdf_doc_ids: HashSet<String>,
    /// Set when an internal board-storage URL has no stable object-key/doc-id
    /// sibling. Destructive GC must fail closed for this projection.
    pub has_unstable_internal_url: bool,
}

/// Extracts canonical internal asset references used by previews and retention.
pub fn project_stable_refs(state: &Value, board_id: Uuid) -> StableAssetRefs {
    let mut result = StableAssetRefs::default();
    walk(state, board_id, &mut result);
    result
}

fn walk(value: &Value, board_id: Uuid, result: &mut StableAssetRefs) {
    match value {
        Value::Object(map) => {
            let has_object_key = map.iter().any(|(key, value)| {
                normalized(key).contains("objectkey")
                    && value.as_str().is_some_and(|value| !value.is_empty())
            });
            let is_pdf = map.get("type").and_then(Value::as_str) == Some("pdf.page");
            let pdf_doc = if is_pdf {
                map.get("docId")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
            } else {
                None
            };
            if let Some(doc_id) = pdf_doc {
                result.pdf_doc_ids.insert(doc_id.to_string());
            }

            for (key, child) in map {
                let norm = normalized(key);
                if norm.contains("objectkey") {
                    if let Some(object_key) = child.as_str().filter(|value| !value.is_empty()) {
                        result.object_keys.insert(object_key.to_string());
                    }
                } else if norm.contains("url") {
                    if let Some(url) = child.as_str() {
                        let board_marker = format!("/boards/{board_id}/");
                        if url.contains(&board_marker) && !has_object_key && pdf_doc.is_none() {
                            result.has_unstable_internal_url = true;
                        }
                    }
                }
                walk(child, board_id, result);
            }
        }
        Value::Array(items) => {
            for child in items {
                walk(child, board_id, result);
            }
        }
        _ => {}
    }
}

fn normalized(key: &str) -> String {
    key.chars()
        .filter(|character| *character != '_')
        .flat_map(char::to_lowercase)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_stable_refs_and_blocks_only_unresolved_internal_urls() {
        let board = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let state = json!({"entities": [{"id": 1, "components": [
            {"type": "image", "object_key": "boards/x/image.png", "url": format!("/boards/{board}/image.png")},
            {"type": "pdf.page", "docId": "pdf-a", "svgUrl": format!("/boards/{board}/pdf/pdf-a/page-0.svg")},
            {"type": "image", "url": "https://cdn.example/external.png"}
        ]}]});
        let refs = project_stable_refs(&state, board);
        assert_eq!(
            refs.object_keys,
            HashSet::from(["boards/x/image.png".to_string()])
        );
        assert_eq!(refs.pdf_doc_ids, HashSet::from(["pdf-a".to_string()]));
        assert!(!refs.has_unstable_internal_url);

        let unresolved = json!({"entities": [{"id": 1, "components": [
            {"type": "image", "url": format!("https://storage/boards/{board}/lost.png")}
        ]}]});
        assert!(project_stable_refs(&unresolved, board).has_unstable_internal_url);
    }
}
