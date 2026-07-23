//! Stable asset-reference read model. A replacement is atomic and
//! carries the exact canonical generation/barrier from which it was projected.

use std::collections::HashSet;

use uuid::Uuid;

/// Canonical asset-reference projection persisted for one board revision.
pub struct AssetProjection<'a> {
    pub board_id: Uuid,
    pub last_event_seq: i64,
    pub base_generation: i64,
    pub object_keys: &'a HashSet<String>,
    pub pdf_doc_ids: &'a HashSet<String>,
    pub blocker_code: Option<&'a str>,
}

/// Replaces a board's asset projection inside an existing transaction.
pub async fn replace_projection_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    projection: AssetProjection<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM board_yrs_asset_refs WHERE board_id = $1")
        .bind(projection.board_id)
        .execute(&mut **tx)
        .await?;

    if projection.blocker_code.is_none() {
        for (kind, refs) in [
            ("object_key", projection.object_keys),
            ("pdf_doc", projection.pdf_doc_ids),
        ] {
            for stable_ref in refs {
                sqlx::query(
                    "INSERT INTO board_yrs_asset_refs \
                     (board_id, ref_kind, stable_ref, last_event_seq, base_generation) \
                     VALUES ($1, $2, $3, $4, $5)",
                )
                .bind(projection.board_id)
                .bind(kind)
                .bind(stable_ref)
                .bind(projection.last_event_seq)
                .bind(projection.base_generation)
                .execute(&mut **tx)
                .await?;
            }
        }
    }

    let status = if projection.blocker_code.is_some() {
        "blocked"
    } else {
        "ready"
    };
    sqlx::query(
        "INSERT INTO board_yrs_asset_heads \
         (board_id, last_event_seq, base_generation, status, blocker_code, projected_at) \
         VALUES ($1, $2, $3, $4, $5, NOW()) \
         ON CONFLICT (board_id) DO UPDATE SET \
           last_event_seq = EXCLUDED.last_event_seq, \
           base_generation = EXCLUDED.base_generation, \
           status = EXCLUDED.status, blocker_code = EXCLUDED.blocker_code, \
           projected_at = NOW()",
    )
    .bind(projection.board_id)
    .bind(projection.last_event_seq)
    .bind(projection.base_generation)
    .bind(status)
    .bind(projection.blocker_code)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Reads the asset projection only when it matches the current canonical head.
pub async fn read_if_fresh_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    board_id: Uuid,
) -> Result<Option<(HashSet<String>, HashSet<String>)>, sqlx::Error> {
    let fresh: bool = sqlx::query_scalar(
        "SELECT EXISTS ( \
           SELECT 1 FROM board_yrs_asset_heads ah \
           JOIN board_yrs_heads h ON h.board_id = ah.board_id \
           WHERE ah.board_id = $1 AND ah.status = 'ready' \
             AND h.state = 'ready' \
             AND ah.last_event_seq = h.processed_seq \
             AND ah.base_generation = h.base_generation \
         )",
    )
    .bind(board_id)
    .fetch_one(&mut **tx)
    .await?;
    if !fresh {
        return Ok(None);
    }

    let rows = sqlx::query_as::<_, (String, String)>(
        "SELECT ref_kind, stable_ref FROM board_yrs_asset_refs \
         WHERE board_id = $1",
    )
    .bind(board_id)
    .fetch_all(&mut **tx)
    .await?;
    let mut object_keys = HashSet::new();
    let mut pdf_doc_ids = HashSet::new();
    for (kind, stable_ref) in rows {
        match kind.as_str() {
            "object_key" => {
                object_keys.insert(stable_ref);
            }
            "pdf_doc" => {
                pdf_doc_ids.insert(stable_ref);
            }
            _ => return Ok(None),
        }
    }
    Ok(Some((object_keys, pdf_doc_ids)))
}
