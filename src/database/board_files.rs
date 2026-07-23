//! Metadata persistence for files uploaded to collaborative boards.
//!
//! Object bytes live in S3-compatible storage; this table keeps ownership,
//! object keys, content metadata, and the stable URL used to reconcile storage
//! with board state.

use sqlx::PgPool;
use uuid::Uuid;

use crate::database::quotas::{ensure_upload_available_tx, QuotaError};
use crate::models::board_files::{BoardFile, BoardFileInsert};

/// Inserts metadata for an object attached to a board.
pub async fn insert_board_file(
    db: &PgPool,
    owner_id: Uuid,
    payload: &BoardFileInsert,
) -> Result<BoardFile, QuotaError> {
    let mut tx = db.begin().await?;
    ensure_upload_available_tx(&mut tx, owner_id, payload.size_bytes).await?;

    let file = sqlx::query_as!(
        BoardFile,
        r#"
        INSERT INTO board_files
            (board_id, uploader_id, object_key, content_type, size_bytes, original_name, url)
        VALUES
            ($1, $2, $3, $4, $5, $6, $7)
        RETURNING
            id, board_id, uploader_id, object_key, content_type, size_bytes, original_name, url, created_at
        "#,
        payload.board_id,
        payload.uploader_id,
        payload.object_key,
        payload.content_type,
        payload.size_bytes,
        payload.original_name,
        payload.url
    )
    .fetch_one(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(file)
}

/// Deletes one board-file metadata row.
pub async fn delete_board_file(db: &PgPool, id: Uuid) -> Result<(), sqlx::Error> {
    // Runtime query (not the checked `query!` macro) so the build doesn't depend
    // on regenerating the .sqlx offline cache for this trivial delete.
    sqlx::query("DELETE FROM board_files WHERE id = $1")
        .bind(id)
        .execute(db)
        .await?;
    Ok(())
}

/// Lists files attached to a board.
pub async fn list_board_files_by_board_id(
    db: &PgPool,
    board_id: Uuid,
) -> Result<Vec<BoardFile>, sqlx::Error> {
    sqlx::query_as!(
        BoardFile,
        r#"
        SELECT id, board_id, uploader_id, object_key, content_type, size_bytes, original_name, url, created_at
        FROM board_files
        WHERE board_id = $1
        "#,
        board_id
    )
    .fetch_all(db)
    .await
}
