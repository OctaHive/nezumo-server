//! Durable outbox for deleting S3 objects after a board row is removed.

use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct BoardStorageDeletionJob {
    pub board_id: Uuid,
    pub object_prefix: String,
    pub preview_object_key: Option<String>,
}

/// Lists jobs whose grace period elapsed, oldest attempt first. Updating
/// `updated_at` after a failure moves a broken job behind other pending work.
pub async fn list_pending(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<BoardStorageDeletionJob>, sqlx::Error> {
    sqlx::query_as::<_, BoardStorageDeletionJob>(
        r#"
        SELECT board_id, object_prefix, preview_object_key
        FROM board_storage_deletion_jobs
        WHERE cleanup_after <= NOW()
        ORDER BY updated_at ASC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
}

/// Reads one job for an immediate best-effort cleanup attempt.
pub async fn get_by_board_id(
    pool: &PgPool,
    board_id: Uuid,
) -> Result<Option<BoardStorageDeletionJob>, sqlx::Error> {
    sqlx::query_as::<_, BoardStorageDeletionJob>(
        r#"
        SELECT board_id, object_prefix, preview_object_key
        FROM board_storage_deletion_jobs
        WHERE board_id = $1
        "#,
    )
    .bind(board_id)
    .fetch_optional(pool)
    .await
}

/// Removes a completed job. Concurrent completion attempts are harmless.
pub async fn complete(
    pool: &PgPool,
    board_id: Uuid,
    object_prefix: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        DELETE FROM board_storage_deletion_jobs
        WHERE board_id = $1 AND object_prefix = $2
        "#,
    )
    .bind(board_id)
    .bind(object_prefix)
    .execute(pool)
    .await?;
    Ok(())
}

/// Records an S3 failure while preserving the job for a later retry.
pub async fn record_failure(
    pool: &PgPool,
    board_id: Uuid,
    object_prefix: &str,
    error: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE board_storage_deletion_jobs
        SET attempts = attempts + 1,
            last_error = $3,
            updated_at = NOW()
        WHERE board_id = $1 AND object_prefix = $2
        "#,
    )
    .bind(board_id)
    .bind(object_prefix)
    .bind(error)
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use sqlx::postgres::PgPoolOptions;

    use super::*;

    #[tokio::test]
    #[ignore = "requires NEZUMO_TEST_DATABASE_URL pointing to an isolated migrated PostgreSQL database"]
    async fn user_cascade_queues_every_deleted_board_prefix() {
        let database_url = std::env::var("NEZUMO_TEST_DATABASE_URL")
            .expect("NEZUMO_TEST_DATABASE_URL must point to an isolated migrated database");
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .expect("connect test database");

        let user_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let board_ids = [Uuid::new_v4(), Uuid::new_v4()];
        sqlx::query(
            "INSERT INTO users (id, username, email, password_hash) VALUES ($1, $2, $3, 'test')",
        )
        .bind(user_id)
        .bind(format!("storage-delete-{}", user_id.simple()))
        .bind(format!("storage-delete-{user_id}@example.com"))
        .execute(&pool)
        .await
        .expect("insert test user");
        sqlx::query("INSERT INTO projects (id, owner_id, name) VALUES ($1, $2, 'test')")
            .bind(project_id)
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("insert test project");
        for board_id in board_ids {
            sqlx::query(
                "INSERT INTO boards (id, project_id, owner_id, title, visibility)
                 VALUES ($1, $2, $3, 'test', 'private')",
            )
            .bind(board_id)
            .bind(project_id)
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("insert test board");
        }

        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("delete test user");

        let jobs = sqlx::query_as::<_, BoardStorageDeletionJob>(
            "SELECT board_id, object_prefix, preview_object_key
             FROM board_storage_deletion_jobs
             WHERE board_id = ANY($1)
             ORDER BY board_id",
        )
        .bind(&board_ids[..])
        .fetch_all(&pool)
        .await
        .expect("load deletion jobs");
        assert_eq!(jobs.len(), board_ids.len());
        for job in &jobs {
            assert_eq!(job.object_prefix, format!("boards/{}/", job.board_id));
        }

        sqlx::query("DELETE FROM board_storage_deletion_jobs WHERE board_id = ANY($1)")
            .bind(&board_ids[..])
            .execute(&pool)
            .await
            .expect("delete test jobs");
    }
}
