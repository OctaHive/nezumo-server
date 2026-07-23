//! Independent backend projection and validation for structural text.

use super::mapping::{any_to_json, json_to_any};
use super::ModelError;
use serde_json::{Map as JsonMap, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use yrs::types::Attrs;
use yrs::{
    Any, Map, MapPrelim, MapRef, Out, ReadTxn, Text, TransactionMut, Xml, XmlElementPrelim,
    XmlFragment, XmlFragmentPrelim, XmlFragmentRef, XmlOut, XmlTextPrelim,
};

/// Component key used for the canonical rich-text payload.
pub const COMPONENT: &str = "text.content";
/// On-document representation marker for structural collaborative text.
pub const MODE: &str = "crdt_text";
/// Structural-text representation version stored in each component instance.
const CONTENT_VERSION: f64 = 1.0;
/// Maximum accepted nesting depth for list items.
const MAX_INDENT: u8 = 8;
const BLOCK_TAG: &str = "block";
const ATTR_KIND: &str = "kind";
const ATTR_INDENT: &str = "indent";
const ATTR_EXTRA: &str = "octaboard-extra";
const ATTR_GROUP_EXTRA: &str = "octaboard-group-extra";

/// Supported block kinds in the canonical structural-text document.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Paragraph,
    Bullet,
    Numbered,
}

impl Kind {
    fn as_attr(self) -> &'static str {
        match self {
            Self::Paragraph => "paragraph",
            Self::Bullet => "bullet",
            Self::Numbered => "numbered",
        }
    }
}

/// A contiguous text segment with one stable set of inline formatting marks.
struct Run {
    text: String,
    marks: BTreeMap<String, Value>,
}

/// Normalized block used while projecting Yrs XML into the snapshot schema.
///
/// `extra` belongs to the individual block or list item. `group_extra` belongs
/// to the surrounding list and is emitted once for adjacent items of one kind.
struct Block {
    kind: Kind,
    indent: u8,
    runs: Vec<Run>,
    extra: JsonMap<String, Value>,
    group_extra: JsonMap<String, Value>,
}

impl Block {
    fn empty_paragraph() -> Self {
        Self {
            kind: Kind::Paragraph,
            indent: 0,
            runs: vec![Run {
                text: String::new(),
                marks: BTreeMap::new(),
            }],
            extra: JsonMap::new(),
            group_extra: JsonMap::new(),
        }
    }
}

/// Parses the stable snapshot representation into canonical structural blocks.
fn parse_blocks(value: Option<&Value>) -> Result<Vec<Block>, ModelError> {
    let mut blocks = Vec::new();
    for raw in value.and_then(Value::as_array).into_iter().flatten() {
        let object = raw
            .as_object()
            .ok_or(ModelError::BadTextShape("text block must be an object"))?;
        match object
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("paragraph")
        {
            "paragraph" => {
                let mut extra = object.clone();
                extra.remove("type");
                extra.remove("spans");
                extra.remove("text");
                blocks.push(Block {
                    kind: Kind::Paragraph,
                    indent: 0,
                    runs: parse_runs(object)?,
                    extra,
                    group_extra: JsonMap::new(),
                });
            }
            list_type @ ("bulletList" | "numberedList") => {
                let kind = if list_type == "bulletList" {
                    Kind::Bullet
                } else {
                    Kind::Numbered
                };
                let mut group_extra = object.clone();
                group_extra.remove("type");
                group_extra.remove("items");
                let items = object
                    .get("items")
                    .and_then(Value::as_array)
                    .ok_or(ModelError::BadTextShape("list missing items"))?;
                for (index, item) in items.iter().enumerate() {
                    let item = item
                        .as_object()
                        .ok_or(ModelError::BadTextShape("list item must be an object"))?;
                    let indent = item.get("indentLevel").and_then(Value::as_u64).unwrap_or(0);
                    if indent > MAX_INDENT as u64 {
                        return Err(ModelError::TextLimitExceeded("indent"));
                    }
                    let mut extra = item.clone();
                    extra.remove("spans");
                    extra.remove("text");
                    extra.remove("indentLevel");
                    blocks.push(Block {
                        kind,
                        indent: indent as u8,
                        runs: parse_runs(item)?,
                        extra,
                        group_extra: if index == 0 {
                            group_extra.clone()
                        } else {
                            JsonMap::new()
                        },
                    });
                }
            }
            other => return Err(ModelError::UnknownTextNode(other.to_string())),
        }
    }
    if blocks.is_empty() {
        blocks.push(Block::empty_paragraph());
    }
    Ok(blocks)
}

/// Parses inline spans while preserving unknown mark keys.
fn parse_runs(object: &JsonMap<String, Value>) -> Result<Vec<Run>, ModelError> {
    let mut runs = Vec::new();
    if let Some(spans) = object.get("spans").and_then(Value::as_array) {
        for span in spans {
            let span = span
                .as_object()
                .ok_or(ModelError::BadTextShape("span must be an object"))?;
            runs.push(Run {
                text: span
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                marks: span
                    .iter()
                    .filter(|(key, _)| key.as_str() != "text")
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            });
        }
    } else if let Some(text) = object.get("text").and_then(Value::as_str) {
        runs.push(Run {
            text: text.to_string(),
            marks: BTreeMap::new(),
        });
    }
    if runs.is_empty() {
        runs.push(Run {
            text: String::new(),
            marks: BTreeMap::new(),
        });
    }
    Ok(runs)
}

fn marks_to_attrs(marks: &BTreeMap<String, Value>) -> Result<Attrs, ModelError> {
    let mut attributes = Attrs::new();
    for (key, value) in marks {
        if !matches!(value, Value::Bool(false) | Value::Null) {
            attributes.insert(Arc::from(key.as_str()), json_to_any(value)?);
        }
    }
    Ok(attributes)
}

fn encode_json_attr(value: &JsonMap<String, Value>) -> Result<Option<String>, ModelError> {
    if value.is_empty() {
        Ok(None)
    } else {
        serde_json::to_string(value)
            .map(Some)
            .map_err(|_| ModelError::BadTextShape("cannot encode extra text attributes"))
    }
}

/// Writes a canonical structural-text component directly from snapshot JSON.
pub(crate) fn write_instance(
    txn: &mut TransactionMut,
    inst: &MapRef,
    object: &JsonMap<String, Value>,
) -> Result<(), ModelError> {
    inst.insert(txn, "alive", true);
    inst.insert(txn, "mode", MODE);
    inst.insert(txn, "content_version", CONTENT_VERSION);
    let fields: MapRef = inst.insert(txn, "fields", MapPrelim::default());
    for (key, value) in object {
        if key != "type" && key != "blocks" {
            fields.insert(txn, key.clone(), json_to_any(value)?);
        }
    }
    let content: XmlFragmentRef = inst.insert(txn, "content", XmlFragmentPrelim::default());
    for block in parse_blocks(object.get("blocks"))? {
        let element = content.insert(txn, content.len(txn), XmlElementPrelim::empty(BLOCK_TAG));
        element.insert_attribute(txn, ATTR_KIND, block.kind.as_attr());
        if block.indent != 0 {
            element.insert_attribute(txn, ATTR_INDENT, block.indent.to_string());
        }
        if let Some(extra) = encode_json_attr(&block.extra)? {
            element.insert_attribute(txn, ATTR_EXTRA, extra);
        }
        if let Some(extra) = encode_json_attr(&block.group_extra)? {
            element.insert_attribute(txn, ATTR_GROUP_EXTRA, extra);
        }
        let text = element.push_back(txn, XmlTextPrelim::new(""));
        let mut offset = 0;
        for run in block.runs {
            text.insert_with_attributes(txn, offset, &run.text, marks_to_attrs(&run.marks)?);
            offset += run.text.encode_utf16().count() as u32;
        }
    }
    Ok(())
}

/// Decodes an optional JSON object stored as a string-valued XML attribute.
///
/// A missing or non-string attribute represents an empty object. A present
/// string must contain a valid JSON object.
fn json_attr(value: Option<Out>) -> Result<JsonMap<String, Value>, ModelError> {
    let Some(Out::Any(Any::String(value))) = value else {
        return Ok(JsonMap::new());
    };
    serde_json::from_str(&value).map_err(|_| ModelError::BadTextShape("invalid extra attribute"))
}

/// Extracts a string-valued XML attribute and ignores values of other types.
fn attr_string(value: Option<Out>) -> Option<String> {
    match value {
        Some(Out::Any(Any::String(value))) => Some(value.to_string()),
        _ => None,
    }
}

/// Projects normalized text runs into the stable snapshot span representation.
fn spans(runs: &[Run]) -> Value {
    Value::Array(
        runs.iter()
            .map(|run| {
                let mut span = JsonMap::new();
                span.insert("text".into(), Value::String(run.text.clone()));
                span.extend(run.marks.clone());
                Value::Object(span)
            })
            .collect(),
    )
}

/// Projects normalized blocks into the stable snapshot representation.
///
/// Consecutive list items of the same kind are grouped into one list object;
/// paragraphs remain independent objects.
fn snapshot_blocks(blocks: &[Block]) -> Value {
    let mut result = Vec::new();
    let mut index = 0;
    while index < blocks.len() {
        let block = &blocks[index];
        match block.kind {
            Kind::Paragraph => {
                let mut value = block.extra.clone();
                value.insert("type".into(), Value::String("paragraph".into()));
                value.insert("spans".into(), spans(&block.runs));
                result.push(Value::Object(value));
                index += 1;
            }
            kind @ (Kind::Bullet | Kind::Numbered) => {
                let mut value = block.group_extra.clone();
                value.insert(
                    "type".into(),
                    Value::String(
                        if kind == Kind::Bullet {
                            "bulletList"
                        } else {
                            "numberedList"
                        }
                        .into(),
                    ),
                );
                let mut items = Vec::new();
                while index < blocks.len() && blocks[index].kind == kind {
                    let item = &blocks[index];
                    let mut item_value = item.extra.clone();
                    item_value.insert("spans".into(), spans(&item.runs));
                    if item.indent != 0 {
                        item_value.insert("indentLevel".into(), Value::from(item.indent));
                    }
                    items.push(Value::Object(item_value));
                    index += 1;
                }
                value.insert("items".into(), Value::Array(items));
                result.push(Value::Object(value));
            }
        }
    }
    Value::Array(result)
}

/// Projects one structural text instance back to its stable JSON component shape.
pub fn materialize(txn: &impl ReadTxn, inst: &MapRef) -> Result<Value, ModelError> {
    // Reject unknown representations before interpreting any nested content.
    if !matches!(
        inst.get(txn, "content_version"),
        Some(Out::Any(Any::Number(CONTENT_VERSION)))
    ) {
        return Err(ModelError::BadTextShape("unsupported content version"));
    }
    let Some(Out::YMap(fields)) = inst.get(txn, "fields") else {
        return Err(ModelError::BadTextShape("missing text fields"));
    };
    let Some(Out::YXmlFragment(content)) = inst.get(txn, "content") else {
        return Err(ModelError::BadTextShape("missing text content"));
    };

    // Normalize every XML block and its formatted text runs before constructing
    // the public snapshot shape. Unknown nodes fail closed.
    let mut blocks = Vec::new();
    for child in content.children(txn) {
        let XmlOut::Element(element) = child else {
            return Err(ModelError::UnknownTextNode("non-element block".into()));
        };
        if element.tag().as_ref() != "block" {
            return Err(ModelError::UnknownTextNode(element.tag().to_string()));
        }
        let kind = match attr_string(element.get_attribute(txn, "kind")).as_deref() {
            Some("paragraph") => Kind::Paragraph,
            Some("bullet") => Kind::Bullet,
            Some("numbered") => Kind::Numbered,
            _ => return Err(ModelError::UnknownTextNode("block kind".into())),
        };
        let indent = attr_string(element.get_attribute(txn, "indent"))
            .and_then(|value| value.parse::<u8>().ok())
            .unwrap_or(0);
        if indent > MAX_INDENT {
            return Err(ModelError::TextLimitExceeded("indent"));
        }
        let mut children = element.children(txn);
        let Some(XmlOut::Text(text)) = children.next() else {
            return Err(ModelError::BadTextShape("block missing text"));
        };
        if children.next().is_some() {
            return Err(ModelError::BadTextShape("block has multiple children"));
        }
        let mut runs = Vec::new();
        for diff in text.diff(txn, yrs::types::text::YChange::identity) {
            let Out::Any(Any::String(value)) = diff.insert else {
                return Err(ModelError::UnknownTextNode("inline embed".into()));
            };
            let mut marks = BTreeMap::new();
            if let Some(attributes) = diff.attributes {
                // Null and false remove a Yrs formatting attribute; they are not
                // emitted as active marks in the materialized snapshot.
                for (key, value) in attributes.iter() {
                    if !matches!(value, Any::Null | Any::Bool(false)) {
                        marks.insert(key.to_string(), any_to_json(&value)?);
                    }
                }
            }
            runs.push(Run {
                text: value.to_string(),
                marks,
            });
        }
        if runs.is_empty() {
            // Preserve an explicitly empty block in the snapshot representation.
            runs.push(Run {
                text: String::new(),
                marks: BTreeMap::new(),
            });
        }
        blocks.push(Block {
            kind,
            indent,
            runs,
            extra: json_attr(element.get_attribute(txn, "octaboard-extra"))?,
            group_extra: json_attr(element.get_attribute(txn, "octaboard-group-extra"))?,
        });
    }
    if blocks.is_empty() {
        return Err(ModelError::BadTextShape("content has no blocks"));
    }

    // Scalar component fields live in a Y.Map and are projected alongside the
    // structural block content into one stable component object.
    let mut component = JsonMap::new();
    component.insert("type".into(), Value::String(COMPONENT.into()));
    for (key, value) in fields.iter(txn) {
        let Out::Any(value) = value else {
            return Err(ModelError::BadTextShape("text field is not Any"));
        };
        component.insert(key.to_string(), any_to_json(&value)?);
    }
    component.insert("blocks".into(), snapshot_blocks(&blocks));
    Ok(Value::Object(component))
}
