//! User/profile queries with avatar URL and online-presence enrichment.

use axum::response::IntoResponse;
use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::Row;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::instrument;
use uuid::Uuid;

use crate::cache::sessions;
use crate::database::users::{
    fetch_active_user_by_field_from_db, fetch_all_users_from_db, search_users as db_search_users,
};
use crate::models::user::{User, UserGetResponse, UserSearchQuery, UserSearchResult};
use crate::routes::AppState;

use crate::storage::presign_url::generate_presigned_url;

// Get all users
#[utoipa::path(
    get,
    path = "/users/all",
    tag = "user",
    security(
        ("jwt_token" = [])
    ),
    responses(
        (status = 200, description = "Successfully fetched all users", body = [UserGetResponse]),
        (status = 401, description = "Unauthorized", body = serde_json::Value),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state))]
pub async fn get_all_users(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match fetch_all_users_from_db(&state.database).await {
        Ok(users) => {
            // Who is connected to a board right now (live heartbeat-backed presence).
            let online_ids = sessions::get_all_online_user_ids(&state.cache)
                .await
                .unwrap_or_default();

            // Last-activity timestamps. Fetched with a runtime (non-macro) query so we
            // don't have to regenerate the offline `.sqlx` cache for `last_active_at`.
            let mut last_active: HashMap<Uuid, DateTime<Utc>> = HashMap::new();
            if let Ok(rows) = sqlx::query("SELECT id, last_active_at FROM users")
                .fetch_all(&state.database)
                .await
            {
                for row in rows {
                    if let (Ok(id), Ok(Some(ts))) = (
                        row.try_get::<Uuid, _>("id"),
                        row.try_get::<Option<DateTime<Utc>>, _>("last_active_at"),
                    ) {
                        last_active.insert(id, ts);
                    }
                }
            }

            // Expose only whether a second factor is configured. The secret
            // itself remains confined to the internal user model.
            let mut totp_enabled: HashMap<Uuid, bool> = HashMap::new();
            if let Ok(rows) =
                sqlx::query("SELECT id, (totp_secret IS NOT NULL) AS totp_enabled FROM users")
                    .fetch_all(&state.database)
                    .await
            {
                for row in rows {
                    if let (Ok(id), Ok(enabled)) = (
                        row.try_get::<Uuid, _>("id"),
                        row.try_get::<bool, _>("totp_enabled"),
                    ) {
                        totp_enabled.insert(id, enabled);
                    }
                }
            }

            // Resource usage is returned with the directory so administrators
            // can understand a tier assignment without exposing internal level
            // numbers. Subqueries avoid multiplying board and file rows.
            let mut quota_usage: HashMap<Uuid, (i64, i64)> = HashMap::new();
            if let Ok(rows) = sqlx::query(
                r#"
                SELECT
                    u.id,
                    (SELECT COUNT(*)::BIGINT FROM boards b WHERE b.owner_id = u.id)
                        AS owned_boards,
                    COALESCE((
                        SELECT SUM(bf.size_bytes)::BIGINT
                        FROM board_files bf
                        JOIN boards b ON b.id = bf.board_id
                        WHERE b.owner_id = u.id
                    ), 0)
                    + COALESCE((
                        SELECT SUM(b.imported_storage_bytes)::BIGINT
                        FROM boards b
                        WHERE b.owner_id = u.id
                    ), 0) AS storage_bytes
                FROM users u
                "#,
            )
            .fetch_all(&state.database)
            .await
            {
                for row in rows {
                    if let (Ok(id), Ok(owned_boards), Ok(storage_bytes)) = (
                        row.try_get::<Uuid, _>("id"),
                        row.try_get::<i64, _>("owned_boards"),
                        row.try_get::<i64, _>("storage_bytes"),
                    ) {
                        quota_usage.insert(id, (owned_boards, storage_bytes));
                    }
                }
            }

            // For each user, add the presigned URL if profile_picture_url is present
            let mut enriched_users = Vec::with_capacity(users.len());
            for user in users {
                let mut user_json =
                    serde_json::to_value(&user).expect("User should serialize to JSON");

                user_json["online"] = json!(online_ids.contains(&user.id));
                user_json["totp_enabled"] =
                    json!(totp_enabled.get(&user.id).copied().unwrap_or(false));
                user_json["last_active_at"] = serde_json::to_value(last_active.get(&user.id))
                    .unwrap_or(serde_json::Value::Null);
                let (owned_boards, storage_bytes) =
                    quota_usage.get(&user.id).copied().unwrap_or_default();
                user_json["owned_boards"] = json!(owned_boards);
                user_json["storage_used_bytes"] = json!(storage_bytes);

                if let Some(ref stored_url) = user.profile_picture_url {
                    let endpoint = &state.storage.endpoint_url;
                    if stored_url.starts_with(endpoint) {
                        let url = stored_url.strip_prefix(endpoint).unwrap_or(stored_url);
                        let url = url.trim_start_matches('/');
                        let mut parts = url.splitn(2, '/');
                        let bucket = parts.next().unwrap_or("");
                        let object_key = parts.next().unwrap_or("");

                        if !bucket.is_empty() && !object_key.is_empty() {
                            if let Ok(presigned_url) =
                                generate_presigned_url(&state.storage, bucket, object_key, 900)
                                    .await
                            {
                                user_json["profile_picture_presigned_url"] = json!(presigned_url);
                            }
                        }
                    }
                }

                enriched_users.push(user_json);
            }

            Ok(Json(enriched_users))
        }
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not fetch the users details." })),
        )),
    }
}

// Get a single user by ID or current user
#[utoipa::path(
    get,
    path = "/users/{id}",
    tag = "user",
    params(
        ("id" = String, Path, description = "User ID or 'current'")
    ),
    responses(
        (status = 200, description = "Successfully fetched user by ID or current user", body = UserGetResponse),
        (status = 400, description = "Invalid UUID format"),
        (status = 401, description = "Unauthorized", body = serde_json::Value),
        (status = 404, description = "User not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state))]
pub async fn get_users_by_id(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
) -> impl IntoResponse {
    let allowed_role_levels = vec![2, 3]; // Add any other role levels that should have access

    // Check if the current user has the required role level to fetch by custom ID
    if id != "current" && !allowed_role_levels.contains(&current_user.role_level) {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "You do not have permission to access this resource." })),
        ));
    }

    let user_id = if id == "current" {
        current_user.id
    } else {
        match Uuid::parse_str(&id) {
            Ok(uuid) => uuid,
            Err(_) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": "Invalid UUID format." })),
                ))
            }
        }
    };

    match fetch_active_user_by_field_from_db(&state.database, "id", &user_id.to_string()).await {
        Ok(Some(user)) => {
            let mut user_json = serde_json::to_value(&user).expect("User should serialize to JSON");

            if let Some(ref stored_url) = user.profile_picture_url {
                let endpoint = &state.storage.endpoint_url;
                if stored_url.starts_with(endpoint) {
                    let url = stored_url.strip_prefix(endpoint).unwrap_or(stored_url);
                    let url = url.trim_start_matches('/');
                    let mut parts = url.splitn(2, '/');
                    let bucket = parts.next().unwrap_or("");
                    let object_key = parts.next().unwrap_or("");

                    if !bucket.is_empty() && !object_key.is_empty() {
                        if let Ok(presigned_url) =
                            generate_presigned_url(&state.storage, bucket, object_key, 900).await
                        {
                            // Insert the presigned URL as a new field
                            user_json["profile_picture_presigned_url"] = json!(presigned_url);
                        }
                    }
                }
            }

            Ok(Json(user_json))
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("User with ID '{}' not found", user_id) })),
        )),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not fetch the users details." })),
        )),
    }
}

#[instrument(skip(state))]
pub async fn search_users(
    State(state): State<Arc<AppState>>,
    Query(params): Query<UserSearchQuery>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(10).min(50);
    if params.q.trim().is_empty() {
        return Ok(Json(Vec::<UserSearchResult>::new()));
    }
    match db_search_users(&state.database, &params.q, limit).await {
        Ok(users) => Ok(Json(users)),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not search users." })),
        )),
    }
}
