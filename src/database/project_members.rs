//! Project membership and role persistence.
//!
//! Membership writes are upserts, allowing invitations and administration
//! flows to change a user's project role atomically.

use crate::models::projects::ProjectMember;
use sqlx::PgPool;
use uuid::Uuid;

/// Adds or updates a user's project membership role.
pub async fn add_project_member(
    pool: &PgPool,
    project_id: Uuid,
    user_id: Uuid,
    role: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        INSERT INTO project_members (project_id, user_id, role)
        VALUES ($1, $2, $3)
        ON CONFLICT (project_id, user_id)
        DO UPDATE SET role = EXCLUDED.role
        "#,
        project_id,
        user_id,
        role
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Fetches one user's explicit role on a project.
pub async fn get_project_member_role(
    pool: &PgPool,
    project_id: Uuid,
    user_id: Uuid,
) -> Result<Option<String>, sqlx::Error> {
    let row = sqlx::query!(
        r#"
        SELECT role
        FROM project_members
        WHERE project_id = $1 AND user_id = $2
        "#,
        project_id,
        user_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| r.role))
}

/// Lists a project's owner and explicit members with public profile details.
///
/// The owner is synthesized from `projects.owner_id` instead of relying on the
/// redundant owner membership row. This keeps old projects visible even when
/// that row was never created successfully.
pub async fn list_project_members(
    pool: &PgPool,
    project_id: Uuid,
) -> Result<Vec<ProjectMember>, sqlx::Error> {
    sqlx::query_as::<_, ProjectMember>(
        "WITH memberships AS ( \
           SELECT p.id AS project_id, p.owner_id AS user_id, 'owner'::TEXT AS role, \
                  p.created_at, 0 AS sort_order \
           FROM projects p WHERE p.id = $1 \
           UNION ALL \
           SELECT pm.project_id, pm.user_id, pm.role, pm.created_at, 1 AS sort_order \
           FROM project_members pm \
           JOIN projects p ON p.id = pm.project_id \
           WHERE pm.project_id = $1 AND pm.user_id <> p.owner_id \
         ) \
         SELECT m.project_id, m.user_id, m.role, m.created_at, u.username, \
                COALESCE( \
                  NULLIF(BTRIM(CONCAT_WS(' ', u.first_name, u.last_name)), ''), \
                  u.username \
                ) AS display_name, \
                u.profile_picture_url \
         FROM memberships m \
         JOIN users u ON u.id = m.user_id \
         ORDER BY m.sort_order, m.created_at, u.username",
    )
    .bind(project_id)
    .fetch_all(pool)
    .await
}

#[cfg(test)]
mod tests {
    use sqlx::postgres::PgPoolOptions;

    use super::*;

    #[tokio::test]
    #[ignore = "requires NEZUMO_TEST_DATABASE_URL pointing to an isolated migrated PostgreSQL database"]
    async fn lists_owner_and_members_and_exposes_shared_project() {
        let database_url = std::env::var("NEZUMO_TEST_DATABASE_URL")
            .expect("NEZUMO_TEST_DATABASE_URL must point to an isolated migrated database");
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .expect("connect test database");

        let owner_id = Uuid::new_v4();
        let member_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        insert_user(&pool, owner_id, "owner", Some("Project"), Some("Owner")).await;
        insert_user(&pool, member_id, "member", Some("Shared"), Some("Member")).await;
        sqlx::query("INSERT INTO projects (id, owner_id, name) VALUES ($1, $2, 'Shared project')")
            .bind(project_id)
            .bind(owner_id)
            .execute(&pool)
            .await
            .expect("insert project");
        add_project_member(&pool, project_id, member_id, "editor")
            .await
            .expect("insert member");

        let members = list_project_members(&pool, project_id)
            .await
            .expect("list members without redundant owner row");
        assert_eq!(members.len(), 2);
        assert_eq!(members[0].user_id, owner_id);
        assert_eq!(members[0].role, "owner");
        assert_eq!(members[0].display_name, "Project Owner");
        assert_eq!(members[1].user_id, member_id);
        assert_eq!(members[1].role, "editor");
        assert_eq!(members[1].display_name, "Shared Member");

        // A legacy/redundant owner membership must not duplicate the owner.
        add_project_member(&pool, project_id, owner_id, "owner")
            .await
            .expect("insert redundant owner membership");
        assert_eq!(
            list_project_members(&pool, project_id)
                .await
                .expect("list members with redundant owner row")
                .len(),
            2
        );

        let shared = crate::database::projects::list_projects_for_user(&pool, member_id)
            .await
            .expect("list shared projects");
        assert_eq!(shared.len(), 1);
        assert_eq!(shared[0].id, project_id);

        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(owner_id)
            .execute(&pool)
            .await
            .expect("remove fixtures");
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(member_id)
            .execute(&pool)
            .await
            .expect("remove member fixture");
    }

    async fn insert_user(
        pool: &PgPool,
        id: Uuid,
        label: &str,
        first_name: Option<&str>,
        last_name: Option<&str>,
    ) {
        sqlx::query(
            "INSERT INTO users \
             (id, username, email, password_hash, first_name, last_name) \
             VALUES ($1, $2, $3, 'test', $4, $5)",
        )
        .bind(id)
        .bind(format!("{label}-{id}"))
        .bind(format!("{label}-{id}@example.test"))
        .bind(first_name)
        .bind(last_name)
        .execute(pool)
        .await
        .expect("insert user");
    }
}
