//! Board CRUD, access, sharing, uploads, and configuration handlers.

use crate::handlers::board_embed::{embed_grants_view, embed_token_from_headers};
use axum::response::IntoResponse;
use axum::{
    extract::{Extension, Multipart, Path, Query, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::instrument;
use uuid::Uuid;

use chrono::Utc;
use rand::distributions::Alphanumeric;
use rand::Rng;

use crate::core::config::{get_env_u64, get_env_with_default};
use crate::database::board_files::{insert_board_file, list_board_files_by_board_id};
use crate::database::board_invite_links::{
    create_invite_link as db_create_invite_link, delete_invite_link as db_delete_invite_link,
    get_invite_link_by_token, get_invite_link_info, list_invite_links as db_list_invite_links,
};
use crate::database::board_members::{
    add_board_member, get_member_role,
    list_board_members_with_users as db_list_board_members_with_users,
    remove_board_member as db_remove_board_member,
};
use crate::database::boards::{
    create_board as db_create_board, delete_board_by_id, get_board_by_id as fetch_board_by_id,
    list_all_boards as db_list_all_boards, list_boards_by_project,
    list_boards_for_user as db_list_boards_for_user,
    toggle_board_favorite as db_toggle_board_favorite, update_board as db_update_board,
    BoardCreateError,
};
use crate::database::project_members::get_project_member_role;
use crate::database::projects::get_project_by_id;
use crate::database::quotas::ensure_upload_available;
use crate::handlers::quotas::quota_api_error;
use crate::models::board_files::{BoardFile, BoardFileInsert, BoardFileUploadResponse};
use crate::models::board_invite_links::{
    CreateInviteLinkBody, DeleteInviteLinkBody, InviteLinkResponse,
};
use crate::models::boards::{
    AddBoardMemberBody, Board, BoardCreateBody, BoardMember, BoardStateResponse, BoardUpdateBody,
    BoardWithOwner, RemoveBoardMemberBody,
};
use crate::models::user::User;
use crate::routes::AppState;
use crate::storage::delete::delete_from_storage;
use crate::storage::presign_url::generate_presigned_url;
use crate::storage::upload::upload_to_storage;

type BoardApiError = (StatusCode, Json<serde_json::Value>);

async fn validate_upload_quota(
    state: &AppState,
    owner_id: Uuid,
    size: usize,
) -> Result<(), BoardApiError> {
    let size = i64::try_from(size).unwrap_or(i64::MAX);
    ensure_upload_available(&state.database, owner_id, size)
        .await
        .map(|_| ())
        .map_err(quota_api_error)
}

async fn record_uploaded_file(
    state: &AppState,
    owner_id: Uuid,
    bucket: &str,
    payload: &BoardFileInsert,
) -> Result<BoardFile, BoardApiError> {
    match insert_board_file(&state.database, owner_id, payload).await {
        Ok(file) => Ok(file),
        Err(error) => {
            // The definitive transaction-serialized quota check happens while
            // inserting metadata. If it loses a race, remove the uploaded
            // object so rejected bytes do not become orphaned storage.
            if let Err(delete_error) =
                delete_from_storage(&state.storage, bucket, &payload.object_key).await
            {
                tracing::error!(
                    "failed to clean up rejected upload {}: {delete_error}",
                    payload.object_key
                );
            }
            Err(quota_api_error(error))
        }
    }
}

#[utoipa::path(
    get,
    path = "/boards/{id}/state",
    tag = "boards",
    params(("id" = String, Path, description = "Board ID")),
    security(("jwt_token" = [])),
    responses(
        (status = 200, description = "Board state", body = BoardStateResponse),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state))]
pub async fn get_board_state(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    user_ext: Option<Extension<User>>,
) -> impl IntoResponse {
    let board_id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid board id." })),
            ))
        }
    };

    let board = match fetch_board_by_id(&state.database, board_id).await {
        Ok(Some(board)) => board,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Board not found." })),
            ))
        }
        Err(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not fetch board." })),
            ))
        }
    };

    // A valid embed token grants view-only access (the board may be private and
    // the iframe has no session cookie), bypassing the membership checks below.
    let embed_token = embed_token_from_headers(&headers);
    let embed_ok = embed_grants_view(&state.database, board_id, embed_token.as_deref()).await;

    if !embed_ok {
        if let Some(Extension(current_user)) = user_ext {
            if board.link_access == "none" && board.visibility != "public" {
                let is_admin = current_user.role_level >= 2;
                if !is_admin {
                    let role = get_member_role(&state.database, board_id, current_user.id)
                        .await
                        .unwrap_or(None);
                    if role.is_none() {
                        return Err((
                            StatusCode::NOT_FOUND,
                            Json(json!({ "error": "Board not found." })),
                        ));
                    }
                }
            }
        } else if board.visibility != "public" && board.link_access == "none" {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "Authentication required." })),
            ));
        }
    }

    let (mut state_value, last_seq) = state
        .coordinators
        .current_canonical_state(&state.database, board_id)
        .await
        .map_err(|error| {
            tracing::error!(board_id = %board_id, %error, "canonical board state unavailable");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "Canonical board state is unavailable." })),
            )
        })?;

    // The persisted media URLs expire (presign TTL); re-sign from the stored
    // object keys so any consumer of this endpoint gets fetchable URLs.
    crate::storage::presign_url::refresh_state_presigned_urls(&state.storage, &mut state_value)
        .await;

    Ok(Json(BoardStateResponse {
        seq: last_seq,
        state: state_value,
    }))
}

#[utoipa::path(
    post,
    path = "/boards",
    tag = "boards",
    security(("jwt_token" = [])),
    request_body = BoardCreateBody,
    responses(
        (status = 201, description = "Board created", body = Board),
        (status = 400, description = "Invalid payload"),
        (status = 403, description = "Forbidden"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state))]
pub async fn create_board(
    State(state): State<Arc<AppState>>,
    Extension(current_user): Extension<User>,
    Json(body): Json<BoardCreateBody>,
) -> impl IntoResponse {
    let project = match get_project_by_id(&state.database, body.project_id).await {
        Ok(Some(project)) => project,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Project not found." })),
            ))
        }
        Err(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not fetch project." })),
            ))
        }
    };

    if project.owner_id != current_user.id {
        let role = get_project_member_role(&state.database, project.id, current_user.id)
            .await
            .unwrap_or(None);
        if role.as_deref() != Some("owner") && role.as_deref() != Some("editor") {
            return Err((
                StatusCode::FORBIDDEN,
                Json(json!({ "error": "Access denied." })),
            ));
        }
    }

    if body.visibility != "public" && body.visibility != "private" {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Invalid visibility value." })),
        ));
    }

    match db_create_board(&state.database, current_user.id, &body).await {
        Ok(board) => {
            let canonical = match crate::state::canonical_base::from_flat_projection(
                &json!({ "entities": [] }),
                0,
            ) {
                Ok(canonical) => canonical,
                Err(error) => {
                    tracing::error!(
                        "could not build canonical state for board {}: {error}",
                        board.id
                    );
                    let _ = delete_board_by_id(&state.database, board.id).await;
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({ "error": "Could not initialize board state." })),
                    ));
                }
            };
            let seeded = match crate::database::yrs_canonical_bases::upsert_base_cas(
                &state.database,
                board.id,
                &canonical,
                None,
            )
            .await
            {
                Ok(seeded) => seeded,
                Err(error) => {
                    tracing::error!(
                        "could not persist canonical state for board {}: {error}",
                        board.id
                    );
                    let _ = delete_board_by_id(&state.database, board.id).await;
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({ "error": "Could not initialize board state." })),
                    ));
                }
            };
            if !seeded {
                let _ = delete_board_by_id(&state.database, board.id).await;
                return Err((
                    StatusCode::CONFLICT,
                    Json(json!({ "error": "Board state changed during creation." })),
                ));
            }
            let _ = add_board_member(&state.database, board.id, current_user.id, "owner").await;
            Ok((StatusCode::CREATED, Json(board)))
        }
        Err(BoardCreateError::Quota(error)) => Err(quota_api_error(error)),
        Err(BoardCreateError::Database(error)) => {
            tracing::error!("board create database error: {error}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not create board." })),
            ))
        }
    }
}

/// Presign each board's stored preview key into a temporary `preview_url` the
/// frontend can render directly. A failed presign just leaves `preview_url`
/// `None` for that board — listing must still succeed.
async fn attach_preview_urls_with_owner(
    state: &AppState,
    mut boards: Vec<BoardWithOwner>,
) -> Vec<BoardWithOwner> {
    let bucket = get_env_with_default("STORAGE_BUCKET_BOARD_FILES", "board-files");
    let ttl = get_env_u64("STORAGE_PRESIGN_TTL_SECONDS", 3600);
    for board in boards.iter_mut() {
        if let Some(key) = board.preview_object_key.clone() {
            board.preview_url = generate_presigned_url(&state.storage, &bucket, &key, ttl)
                .await
                .ok();
        }
    }
    boards
}

/// Same as [`attach_preview_urls_with_owner`] for the plain [`Board`] DTO.
async fn attach_preview_urls(state: &AppState, mut boards: Vec<Board>) -> Vec<Board> {
    let bucket = get_env_with_default("STORAGE_BUCKET_BOARD_FILES", "board-files");
    let ttl = get_env_u64("STORAGE_PRESIGN_TTL_SECONDS", 3600);
    for board in boards.iter_mut() {
        if let Some(key) = board.preview_object_key.clone() {
            board.preview_url = generate_presigned_url(&state.storage, &bucket, &key, ttl)
                .await
                .ok();
        }
    }
    boards
}

#[instrument(skip(state))]
pub async fn list_all_boards(
    State(state): State<Arc<AppState>>,
    Extension(current_user): Extension<User>,
) -> impl IntoResponse {
    let result = if current_user.role_level >= 2 {
        db_list_all_boards(&state.database).await
    } else {
        db_list_boards_for_user(&state.database, current_user.id).await
    };

    match result {
        Ok(boards) => Ok(Json(attach_preview_urls_with_owner(&state, boards).await)),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not fetch boards." })),
        )),
    }
}

#[utoipa::path(
    get,
    path = "/projects/{project_id}/boards",
    tag = "boards",
    params(("project_id" = String, Path, description = "Project ID")),
    security(("jwt_token" = [])),
    responses(
        (status = 200, description = "Boards list", body = [Board]),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Project not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state))]
pub async fn list_project_boards(
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
    Extension(current_user): Extension<User>,
) -> impl IntoResponse {
    let project_id = match Uuid::parse_str(&project_id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid project id." })),
            ))
        }
    };

    let project = match get_project_by_id(&state.database, project_id).await {
        Ok(Some(project)) => project,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Project not found." })),
            ))
        }
        Err(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not fetch project." })),
            ))
        }
    };

    let is_admin = current_user.role_level >= 2;
    if !is_admin && project.owner_id != current_user.id {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "Access denied." })),
        ));
    }

    match list_boards_by_project(&state.database, project_id).await {
        Ok(boards) => Ok(Json(attach_preview_urls(&state, boards).await)),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not fetch boards." })),
        )),
    }
}

#[utoipa::path(
    get,
    path = "/boards/{id}",
    tag = "boards",
    params(("id" = String, Path, description = "Board ID")),
    security(("jwt_token" = [])),
    responses(
        (status = 200, description = "Board", body = Board),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state))]
pub async fn get_board_by_id(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    user_ext: Option<Extension<User>>,
) -> impl IntoResponse {
    let board_id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid board id." })),
            ))
        }
    };

    let board = match fetch_board_by_id(&state.database, board_id).await {
        Ok(Some(board)) => board,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Board not found." })),
            ))
        }
        Err(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not fetch board." })),
            ))
        }
    };

    // A valid embed token → view-only access for an anonymous iframe viewer.
    let embed_token = embed_token_from_headers(&headers);
    let embed_ok = embed_grants_view(&state.database, board_id, embed_token.as_deref()).await;

    let effective_role = if embed_ok {
        "viewer".to_string()
    } else if let Some(Extension(current_user)) = user_ext {
        // Authenticated user — resolve role via membership
        let user_role = if current_user.role_level >= 2 {
            Some("owner".to_string())
        } else if board.owner_id == current_user.id {
            Some("owner".to_string())
        } else {
            get_member_role(&state.database, board_id, current_user.id)
                .await
                .unwrap_or(None)
        };

        if user_role.is_none() && board.link_access == "none" && board.visibility != "public" {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Board not found." })),
            ));
        }

        user_role.unwrap_or_else(|| {
            if board.link_access != "none" {
                board.link_access.clone()
            } else {
                "viewer".to_string()
            }
        })
    } else {
        // Anonymous user — allow public boards and boards with link access
        if board.visibility != "public" && board.link_access == "none" {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "Authentication required." })),
            ));
        }
        if board.link_access != "none" {
            board.link_access.clone()
        } else {
            "viewer".to_string()
        }
    };

    let mut response = serde_json::to_value(&board).unwrap_or_default();
    if let Some(obj) = response.as_object_mut() {
        obj.insert("user_role".to_string(), json!(effective_role));
    }
    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/boards/{id}/members",
    tag = "boards",
    params(("id" = String, Path, description = "Board ID")),
    security(("jwt_token" = [])),
    responses(
        (status = 200, description = "Board members", body = [BoardMember]),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state))]
pub async fn list_board_members(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
) -> impl IntoResponse {
    let board_id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid board id." })),
            ))
        }
    };

    let is_admin = current_user.role_level >= 2;
    if !is_admin {
        let role = get_member_role(&state.database, board_id, current_user.id)
            .await
            .unwrap_or(None);
        if role.is_none() {
            return Err((
                StatusCode::FORBIDDEN,
                Json(json!({ "error": "Access denied." })),
            ));
        }
    }

    match db_list_board_members_with_users(&state.database, board_id).await {
        Ok(mut members) => {
            // `profile_picture_url` is stored as an S3 endpoint URL; presign it
            // in place so the client can actually load the avatar (mirrors the
            // get_users handler — otherwise the raw key isn't fetchable and the
            // UI falls back to initials).
            let endpoint = state.storage.endpoint_url.clone();
            for member in members.iter_mut() {
                if let Some(stored) = member.profile_picture_url.clone() {
                    if let Some(rest) = stored.strip_prefix(&endpoint) {
                        let rest = rest.trim_start_matches('/');
                        let mut parts = rest.splitn(2, '/');
                        let bucket = parts.next().unwrap_or("");
                        let object_key = parts.next().unwrap_or("");
                        if !bucket.is_empty() && !object_key.is_empty() {
                            // 1h TTL — clients re-fetch and re-push to the renderer
                            // (no long-lived URL is persisted anywhere).
                            if let Ok(presigned) =
                                generate_presigned_url(&state.storage, bucket, object_key, 3600)
                                    .await
                            {
                                member.profile_picture_url = Some(presigned);
                            }
                        }
                    }
                }
            }
            Ok(Json(members))
        }
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not fetch members." })),
        )),
    }
}

#[instrument(skip(state))]
pub async fn add_board_member_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
    Json(body): Json<AddBoardMemberBody>,
) -> impl IntoResponse {
    let board_id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid board id." })),
            ))
        }
    };

    let is_admin = current_user.role_level >= 2;
    if !is_admin {
        let role = get_member_role(&state.database, board_id, current_user.id)
            .await
            .unwrap_or(None);
        if role.as_deref() != Some("owner") {
            return Err((
                StatusCode::FORBIDDEN,
                Json(json!({ "error": "Only the owner can manage members." })),
            ));
        }
    }

    let valid_roles = ["viewer", "editor", "owner"];
    if !valid_roles.contains(&body.role.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Invalid role. Must be viewer, editor, or owner." })),
        ));
    }

    // Only the board creator can change the role of another owner
    if !is_admin {
        let target_role = get_member_role(&state.database, board_id, body.user_id)
            .await
            .unwrap_or(None);
        if target_role.as_deref() == Some("owner") {
            let board = match fetch_board_by_id(&state.database, board_id).await {
                Ok(Some(board)) => board,
                _ => {
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({ "error": "Could not fetch board." })),
                    ))
                }
            };
            if board.owner_id != current_user.id {
                return Err((
                    StatusCode::FORBIDDEN,
                    Json(json!({ "error": "Only the board creator can change an owner's role." })),
                ));
            }
        }
    }

    match add_board_member(&state.database, board_id, body.user_id, &body.role).await {
        Ok(_) => Ok(Json(json!({ "success": true }))),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not add member." })),
        )),
    }
}

#[instrument(skip(state))]
pub async fn remove_board_member_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
    Json(body): Json<RemoveBoardMemberBody>,
) -> impl IntoResponse {
    let board_id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid board id." })),
            ))
        }
    };

    let is_self = body.user_id == current_user.id;
    let role = get_member_role(&state.database, board_id, current_user.id)
        .await
        .unwrap_or(None);

    if is_self {
        // Owner cannot leave their own board
        if role.as_deref() == Some("owner") {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Cannot leave a board you own." })),
            ));
        }
        // Any member can remove themselves
        if role.is_none() {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "You are not a member of this board." })),
            ));
        }
    } else {
        // Only owner or admin can remove others
        let is_admin = current_user.role_level >= 2;
        if !is_admin && role.as_deref() != Some("owner") {
            return Err((
                StatusCode::FORBIDDEN,
                Json(json!({ "error": "Only the owner can remove members." })),
            ));
        }

        // Only the board creator can remove another owner
        if !is_admin {
            let target_role = get_member_role(&state.database, board_id, body.user_id)
                .await
                .unwrap_or(None);
            if target_role.as_deref() == Some("owner") {
                let board = match fetch_board_by_id(&state.database, board_id).await {
                    Ok(Some(board)) => board,
                    _ => {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(json!({ "error": "Could not fetch board." })),
                        ))
                    }
                };
                if board.owner_id != current_user.id {
                    return Err((
                        StatusCode::FORBIDDEN,
                        Json(json!({ "error": "Only the board creator can remove an owner." })),
                    ));
                }
            }
        }
    }

    match db_remove_board_member(&state.database, board_id, body.user_id).await {
        Ok(_) => Ok(Json(json!({ "success": true }))),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not remove member." })),
        )),
    }
}

#[utoipa::path(
    post,
    path = "/boards/{id}/images",
    tag = "boards",
    params(("id" = String, Path, description = "Board ID")),
    security(("jwt_token" = [])),
    responses(
        (status = 200, description = "Image uploaded", body = BoardFileUploadResponse),
        (status = 400, description = "Invalid payload"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Board not found"),
        (status = 415, description = "Unsupported media type"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state, multipart))]
pub async fn upload_board_image(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
    mut multipart: Multipart,
) -> Result<Json<BoardFileUploadResponse>, (StatusCode, Json<serde_json::Value>)> {
    let board_id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid board id." })),
            ))
        }
    };

    let board = match fetch_board_by_id(&state.database, board_id).await {
        Ok(Some(board)) => board,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Board not found." })),
            ))
        }
        Err(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not fetch board." })),
            ))
        }
    };

    let is_admin = current_user.role_level >= 2;
    if !is_admin {
        let role = get_member_role(&state.database, board_id, current_user.id)
            .await
            .unwrap_or(None);
        if role.as_deref() != Some("owner")
            && role.as_deref() != Some("editor")
            && board.owner_id != current_user.id
        {
            return Err((
                StatusCode::FORBIDDEN,
                Json(json!({ "error": "Access denied." })),
            ));
        }
    }

    let bucket = get_env_with_default("STORAGE_BUCKET_BOARD_FILES", "board-files");

    while let Some(field) = multipart.next_field().await.map_err(|e| {
        tracing::error!("Multipart error: {e}");
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Invalid file data" })),
        )
    })? {
        if field.name() != Some("image") {
            continue;
        }

        let content_type = field.content_type().unwrap_or("").to_string();
        let ext = match content_type.as_str() {
            "image/png" => "png",
            "image/jpeg" => "jpg",
            "image/jpg" => "jpg",
            "image/gif" => "gif",
            _ => {
                return Err((
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    Json(json!({ "error": "Only PNG, JPEG and GIF formats allowed" })),
                ))
            }
        };

        let original_name = field.file_name().map(|name| name.to_string());

        let data = field.bytes().await.map_err(|e| {
            tracing::error!("File read error: {e}");
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Failed to read file" })),
            )
        })?;

        validate_upload_quota(&state, board.owner_id, data.len()).await?;

        let file_id = Uuid::new_v4();
        let object_key = format!("boards/{}/{}.{}", board_id, file_id, ext);

        let url = upload_to_storage(&state.storage, &bucket, &object_key, &data)
            .await
            .map_err(|e| {
                tracing::error!("Upload error: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "Upload failed" })),
                )
            })?;

        let payload = BoardFileInsert {
            board_id,
            uploader_id: Some(current_user.id),
            object_key: object_key.clone(),
            content_type: content_type.clone(),
            size_bytes: data.len() as i64,
            original_name,
            url: url.clone(),
        };

        let file = record_uploaded_file(&state, board.owner_id, &bucket, &payload).await?;

        let mut response: BoardFileUploadResponse = file.into();
        let expires_in = get_env_u64("STORAGE_PRESIGN_TTL_SECONDS", 3600);
        match generate_presigned_url(&state.storage, &bucket, &object_key, expires_in).await {
            Ok(url) => {
                response.presigned_url = Some(url);
            }
            Err(err) => {
                tracing::error!("Presign error: {err}");
            }
        }

        return Ok(Json(response));
    }

    Err((
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": "No image uploaded" })),
    ))
}

/// Upload an audio file (mp3/wav/ogg/m4a/etc.) to a board. Stored in S3 and
/// recorded in `board_files`, exactly like `upload_board_image`. Duration is
/// computed client-side, so it is not needed here.
#[instrument(skip(state, current_user, multipart))]
pub async fn upload_board_audio(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
    mut multipart: Multipart,
) -> Result<Json<BoardFileUploadResponse>, (StatusCode, Json<serde_json::Value>)> {
    let board_id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid board id." })),
            ))
        }
    };

    let board = match fetch_board_by_id(&state.database, board_id).await {
        Ok(Some(board)) => board,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Board not found." })),
            ))
        }
        Err(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not fetch board." })),
            ))
        }
    };

    let is_admin = current_user.role_level >= 2;
    if !is_admin {
        let role = get_member_role(&state.database, board_id, current_user.id)
            .await
            .unwrap_or(None);
        if role.as_deref() != Some("owner")
            && role.as_deref() != Some("editor")
            && board.owner_id != current_user.id
        {
            return Err((
                StatusCode::FORBIDDEN,
                Json(json!({ "error": "Access denied." })),
            ));
        }
    }

    let bucket = get_env_with_default("STORAGE_BUCKET_BOARD_FILES", "board-files");

    while let Some(field) = multipart.next_field().await.map_err(|e| {
        tracing::error!("Multipart error: {e}");
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Invalid file data" })),
        )
    })? {
        if field.name() != Some("audio") {
            continue;
        }

        let content_type = field.content_type().unwrap_or("").to_string();
        let ext = match content_type.as_str() {
            "audio/mpeg" | "audio/mp3" => "mp3",
            "audio/wav" | "audio/x-wav" | "audio/wave" => "wav",
            "audio/ogg" | "audio/oga" => "ogg",
            "audio/mp4" | "audio/x-m4a" | "audio/aac" => "m4a",
            "audio/webm" => "webm",
            "audio/flac" | "audio/x-flac" => "flac",
            _ => {
                return Err((
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    Json(json!({ "error": "Only audio files are allowed" })),
                ))
            }
        };

        let original_name = field.file_name().map(|name| name.to_string());

        let data = field.bytes().await.map_err(|e| {
            tracing::error!("File read error: {e}");
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Failed to read file" })),
            )
        })?;

        validate_upload_quota(&state, board.owner_id, data.len()).await?;

        let file_id = Uuid::new_v4();
        let object_key = format!("boards/{}/{}.{}", board_id, file_id, ext);

        let url = upload_to_storage(&state.storage, &bucket, &object_key, &data)
            .await
            .map_err(|e| {
                tracing::error!("Upload error: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "Upload failed" })),
                )
            })?;

        let payload = BoardFileInsert {
            board_id,
            uploader_id: Some(current_user.id),
            object_key: object_key.clone(),
            content_type: content_type.clone(),
            size_bytes: data.len() as i64,
            original_name,
            url: url.clone(),
        };

        let file = record_uploaded_file(&state, board.owner_id, &bucket, &payload).await?;

        let mut response: BoardFileUploadResponse = file.into();
        let expires_in = get_env_u64("STORAGE_PRESIGN_TTL_SECONDS", 3600);
        match generate_presigned_url(&state.storage, &bucket, &object_key, expires_in).await {
            Ok(url) => {
                response.presigned_url = Some(url);
            }
            Err(err) => {
                tracing::error!("Presign error: {err}");
            }
        }

        return Ok(Json(response));
    }

    Err((
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": "No audio uploaded" })),
    ))
}

/// Upload a video file for a board. Mirrors [`upload_board_audio`]: stores the
/// raw video in the `board_files` bucket and returns a presigned URL. The poster
/// (first frame) is extracted client-side and uploaded separately via the image
/// endpoint, so only the video file is handled here.
#[instrument(skip(state, current_user, multipart))]
pub async fn upload_board_video(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
    mut multipart: Multipart,
) -> Result<Json<BoardFileUploadResponse>, (StatusCode, Json<serde_json::Value>)> {
    let board_id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid board id." })),
            ))
        }
    };

    let board = match fetch_board_by_id(&state.database, board_id).await {
        Ok(Some(board)) => board,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Board not found." })),
            ))
        }
        Err(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not fetch board." })),
            ))
        }
    };

    let is_admin = current_user.role_level >= 2;
    if !is_admin {
        let role = get_member_role(&state.database, board_id, current_user.id)
            .await
            .unwrap_or(None);
        if role.as_deref() != Some("owner")
            && role.as_deref() != Some("editor")
            && board.owner_id != current_user.id
        {
            return Err((
                StatusCode::FORBIDDEN,
                Json(json!({ "error": "Access denied." })),
            ));
        }
    }

    let bucket = get_env_with_default("STORAGE_BUCKET_BOARD_FILES", "board-files");

    while let Some(field) = multipart.next_field().await.map_err(|e| {
        tracing::error!("Multipart error: {e}");
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Invalid file data" })),
        )
    })? {
        if field.name() != Some("video") {
            continue;
        }

        let content_type = field.content_type().unwrap_or("").to_string();
        let ext = match content_type.as_str() {
            "video/mp4" | "video/x-m4v" => "mp4",
            "video/webm" => "webm",
            "video/quicktime" => "mov",
            "video/ogg" => "ogv",
            _ => {
                return Err((
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    Json(json!({ "error": "Only video files are allowed" })),
                ))
            }
        };

        let original_name = field.file_name().map(|name| name.to_string());

        let data = field.bytes().await.map_err(|e| {
            tracing::error!("File read error: {e}");
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Failed to read file" })),
            )
        })?;

        validate_upload_quota(&state, board.owner_id, data.len()).await?;

        let file_id = Uuid::new_v4();
        let object_key = format!("boards/{}/{}.{}", board_id, file_id, ext);

        let url = upload_to_storage(&state.storage, &bucket, &object_key, &data)
            .await
            .map_err(|e| {
                tracing::error!("Upload error: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "Upload failed" })),
                )
            })?;

        let payload = BoardFileInsert {
            board_id,
            uploader_id: Some(current_user.id),
            object_key: object_key.clone(),
            content_type: content_type.clone(),
            size_bytes: data.len() as i64,
            original_name,
            url: url.clone(),
        };

        let file = record_uploaded_file(&state, board.owner_id, &bucket, &payload).await?;

        let mut response: BoardFileUploadResponse = file.into();
        let expires_in = get_env_u64("STORAGE_PRESIGN_TTL_SECONDS", 3600);
        match generate_presigned_url(&state.storage, &bucket, &object_key, expires_in).await {
            Ok(url) => {
                response.presigned_url = Some(url);
            }
            Err(err) => {
                tracing::error!("Presign error: {err}");
            }
        }

        return Ok(Json(response));
    }

    Err((
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": "No video uploaded" })),
    ))
}

// --- PDF import ----------------------------------------------------------
// A PDF is converted page-by-page to a glyph-outline SVG via MuPDF
// (`mutool convert -O text=path`). Each page SVG is stored, and a stable page
// URL template is returned so the client can flip pages / explode and have it
// survive reloads (the SVG is served back by `get_pdf_page`).

/// Parse width/height (px) from an SVG header; falls back to the viewBox size.
fn parse_svg_dims(svg: &[u8]) -> Option<(f32, f32)> {
    let head = std::str::from_utf8(&svg[..svg.len().min(1024)]).ok()?;
    let attr = |name: &str| -> Option<f32> {
        let key = format!("{}=\"", name);
        let start = head.find(&key)? + key.len();
        let rest = &head[start..];
        let end = rest.find('"')?;
        rest[..end]
            .chars()
            .take_while(|c| c.is_ascii_digit() || *c == '.')
            .collect::<String>()
            .parse::<f32>()
            .ok()
    };
    if let (Some(w), Some(h)) = (attr("width"), attr("height")) {
        if w > 0.0 && h > 0.0 {
            return Some((w, h));
        }
    }
    let key = "viewBox=\"";
    let start = head.find(key)? + key.len();
    let rest = &head[start..];
    let end = rest.find('"')?;
    let nums: Vec<f32> = rest[..end]
        .split(|c: char| c == ',' || c.is_whitespace())
        .filter_map(|t| t.parse::<f32>().ok())
        .collect();
    if nums.len() == 4 && nums[2] > 0.0 && nums[3] > 0.0 {
        Some((nums[2], nums[3]))
    } else {
        None
    }
}

async fn ensure_board_edit_access(
    state: &Arc<AppState>,
    board_id: Uuid,
    current_user: &User,
) -> Result<Board, (StatusCode, Json<serde_json::Value>)> {
    let board = match fetch_board_by_id(&state.database, board_id).await {
        Ok(Some(board)) => board,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Board not found." })),
            ))
        }
        Err(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not fetch board." })),
            ))
        }
    };
    let is_admin = current_user.role_level >= 2;
    if !is_admin {
        let role = get_member_role(&state.database, board_id, current_user.id)
            .await
            .unwrap_or(None);
        if role.as_deref() != Some("owner")
            && role.as_deref() != Some("editor")
            && board.owner_id != current_user.id
        {
            return Err((
                StatusCode::FORBIDDEN,
                Json(json!({ "error": "Access denied." })),
            ));
        }
    }
    Ok(board)
}

#[instrument(skip(state, multipart))]
pub async fn upload_board_pdf(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
    mut multipart: Multipart,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let board_id = Uuid::parse_str(&id).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Invalid board id." })),
        )
    })?;
    let board = ensure_board_edit_access(&state, board_id, &current_user).await?;

    let mut pdf_bytes: Option<Vec<u8>> = None;
    let mut pdf_name: Option<String> = None;
    while let Some(field) = multipart.next_field().await.map_err(|e| {
        tracing::error!("Multipart error: {e}");
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Invalid file data" })),
        )
    })? {
        if field.name() == Some("pdf") {
            pdf_name = field.file_name().map(|name| name.to_string());
            let data = field.bytes().await.map_err(|e| {
                tracing::error!("PDF read error: {e}");
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": "Failed to read file" })),
                )
            })?;
            pdf_bytes = Some(data.to_vec());
            break;
        }
    }
    let pdf_bytes = pdf_bytes.ok_or((
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": "No pdf uploaded" })),
    ))?;
    validate_upload_quota(&state, board.owner_id, pdf_bytes.len()).await?;
    if !pdf_bytes.starts_with(b"%PDF") {
        return Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Json(json!({ "error": "Not a PDF" })),
        ));
    }

    let doc_id = Uuid::new_v4();
    let work_dir = std::env::temp_dir().join(format!("octapdf-{doc_id}"));
    let server_error = |msg: &str| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": msg.to_string() })),
        )
    };
    std::fs::create_dir_all(&work_dir).map_err(|e| {
        tracing::error!("tmp dir error: {e}");
        server_error("Could not prepare conversion")
    })?;
    let pdf_path = work_dir.join("in.pdf");
    std::fs::write(&pdf_path, &pdf_bytes).map_err(|e| {
        tracing::error!("tmp write error: {e}");
        server_error("Could not prepare conversion")
    })?;
    let out_pattern = work_dir.join("page-%d.svg");

    let mutool = get_env_with_default("MUTOOL_BIN", "mutool");
    let output = tokio::process::Command::new(&mutool)
        .arg("convert")
        .arg("-O")
        .arg("text=path")
        .arg("-o")
        .arg(&out_pattern)
        .arg(&pdf_path)
        .output()
        .await
        .map_err(|e| {
            tracing::error!("mutool spawn error: {e}");
            server_error("PDF conversion tool unavailable")
        })?;
    if !output.status.success() {
        tracing::error!("mutool failed: {}", String::from_utf8_lossy(&output.stderr));
        let _ = std::fs::remove_dir_all(&work_dir);
        return Err(server_error("PDF conversion failed"));
    }

    let bucket = get_env_with_default("STORAGE_BUCKET_BOARD_FILES", "board-files");
    let mut page_count: u32 = 0;
    let (mut page_w, mut page_h) = (595.5f32, 842.25f32);
    // mutool emits 1-based page-1.svg, page-2.svg, ...; store as 0-based.
    let mut n = 1u32;
    loop {
        let page_path = work_dir.join(format!("page-{n}.svg"));
        let svg = match std::fs::read(&page_path) {
            Ok(bytes) => bytes,
            Err(_) => break,
        };
        if n == 1 {
            if let Some((w, h)) = parse_svg_dims(&svg) {
                page_w = w;
                page_h = h;
            }
        }
        let object_key = format!("boards/{}/pdf/{}/page-{}.svg", board_id, doc_id, n - 1);
        upload_to_storage(&state.storage, &bucket, &object_key, &svg)
            .await
            .map_err(|e| {
                tracing::error!("page upload error: {e}");
                server_error("Failed to store page")
            })?;
        page_count += 1;
        n += 1;
    }
    let _ = std::fs::remove_dir_all(&work_dir);

    if page_count == 0 {
        return Err(server_error("PDF produced no pages"));
    }

    // Store the original PDF so it can be downloaded later (the pages above are
    // SVG renders, not the source file). Deterministic key keyed by doc_id.
    let source_name = pdf_name
        .filter(|n| !n.trim().is_empty())
        .unwrap_or_else(|| "document.pdf".to_string());
    let source_object_key = format!("boards/{}/pdf/{}/original.pdf", board_id, doc_id);
    let source_url = upload_to_storage(&state.storage, &bucket, &source_object_key, &pdf_bytes)
        .await
        .map_err(|e| {
            tracing::error!("Original PDF upload error: {e}");
            server_error("Failed to store original PDF")
        })?;
    let source_insert = BoardFileInsert {
        board_id,
        uploader_id: Some(current_user.id),
        object_key: source_object_key.clone(),
        content_type: "application/pdf".to_string(),
        size_bytes: pdf_bytes.len() as i64,
        original_name: Some(source_name.clone()),
        url: source_url,
    };
    record_uploaded_file(&state, board.owner_id, &bucket, &source_insert).await?;

    Ok(Json(json!({
        "docId": doc_id.to_string(),
        "pageCount": page_count,
        "pageWidth": page_w,
        "pageHeight": page_h,
        "pageSvgUrlTemplate": format!("/boards/{}/pdf/{}/page/{{n}}", board_id, doc_id),
        "sourceObjectKey": source_object_key,
        "sourceName": source_name,
    })))
}

#[instrument(skip(state))]
pub async fn get_pdf_page(
    State(state): State<Arc<AppState>>,
    Path((id, doc_id, page)): Path<(String, String, String)>,
) -> Result<axum::response::Response, (StatusCode, Json<serde_json::Value>)> {
    let board_id = Uuid::parse_str(&id).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Invalid board id." })),
        )
    })?;
    // Validate doc id + page number to avoid arbitrary key access.
    Uuid::parse_str(&doc_id).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Invalid document id." })),
        )
    })?;
    let page_n: u32 = page.trim_end_matches(".svg").parse().map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Invalid page." })),
        )
    })?;

    let bucket = get_env_with_default("STORAGE_BUCKET_BOARD_FILES", "board-files");
    let object_key = format!("boards/{}/pdf/{}/page-{}.svg", board_id, doc_id, page_n);
    let obj = state
        .storage
        .client
        .get_object()
        .bucket(&bucket)
        .key(&object_key)
        .send()
        .await
        .map_err(|e| {
            tracing::error!("page fetch error: {e}");
            (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Page not found." })),
            )
        })?;
    let bytes = obj
        .body
        .collect()
        .await
        .map_err(|e| {
            tracing::error!("page body error: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to read page." })),
            )
        })?
        .into_bytes();

    Ok((
        [
            (axum::http::header::CONTENT_TYPE, "image/svg+xml"),
            (axum::http::header::CACHE_CONTROL, "public, max-age=600"),
        ],
        bytes,
    )
        .into_response())
}

#[utoipa::path(
    post,
    path = "/boards/{id}/files",
    tag = "boards",
    params(("id" = String, Path, description = "Board ID")),
    security(("jwt_token" = [])),
    responses(
        (status = 200, description = "File uploaded", body = BoardFileUploadResponse),
        (status = 400, description = "Invalid payload"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Board not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state, multipart))]
pub async fn upload_board_file(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
    mut multipart: Multipart,
) -> Result<Json<BoardFileUploadResponse>, (StatusCode, Json<serde_json::Value>)> {
    let board_id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid board id." })),
            ))
        }
    };

    let board = match fetch_board_by_id(&state.database, board_id).await {
        Ok(Some(board)) => board,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Board not found." })),
            ))
        }
        Err(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not fetch board." })),
            ))
        }
    };

    let is_admin = current_user.role_level >= 2;
    if !is_admin {
        let role = get_member_role(&state.database, board_id, current_user.id)
            .await
            .unwrap_or(None);
        if role.as_deref() != Some("owner")
            && role.as_deref() != Some("editor")
            && board.owner_id != current_user.id
        {
            return Err((
                StatusCode::FORBIDDEN,
                Json(json!({ "error": "Access denied." })),
            ));
        }
    }

    let bucket = get_env_with_default("STORAGE_BUCKET_BOARD_FILES", "board-files");

    while let Some(field) = multipart.next_field().await.map_err(|e| {
        tracing::error!("Multipart error: {e}");
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Invalid file data" })),
        )
    })? {
        if field.name() != Some("file") {
            continue;
        }

        let content_type = field.content_type().unwrap_or("").to_string();
        let original_name = field.file_name().map(|name| name.to_string());
        let ext = original_name
            .as_deref()
            .and_then(|name| std::path::Path::new(name).extension())
            .and_then(|value| value.to_str())
            .map(|value| value.to_lowercase())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "bin".to_string());

        let data = field.bytes().await.map_err(|e| {
            tracing::error!("File read error: {e}");
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Failed to read file" })),
            )
        })?;

        validate_upload_quota(&state, board.owner_id, data.len()).await?;

        let file_id = Uuid::new_v4();
        let object_key = format!("boards/{}/{}.{}", board_id, file_id, ext);

        let url = upload_to_storage(&state.storage, &bucket, &object_key, &data)
            .await
            .map_err(|e| {
                tracing::error!("Upload error: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "Upload failed" })),
                )
            })?;

        let payload = BoardFileInsert {
            board_id,
            uploader_id: Some(current_user.id),
            object_key: object_key.clone(),
            content_type: content_type.clone(),
            size_bytes: data.len() as i64,
            original_name,
            url: url.clone(),
        };

        let file = record_uploaded_file(&state, board.owner_id, &bucket, &payload).await?;

        let mut response: BoardFileUploadResponse = file.into();
        let expires_in = get_env_u64("STORAGE_PRESIGN_TTL_SECONDS", 3600);
        match generate_presigned_url(&state.storage, &bucket, &object_key, expires_in).await {
            Ok(url) => {
                response.presigned_url = Some(url);
            }
            Err(err) => {
                tracing::error!("Presign error: {err}");
            }
        }

        return Ok(Json(response));
    }

    Err((
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": "No file uploaded" })),
    ))
}

#[utoipa::path(
    delete,
    path = "/boards/{id}",
    tag = "boards",
    params(("id" = String, Path, description = "Board ID")),
    security(("jwt_token" = [])),
    responses(
        (status = 204, description = "Board deleted"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Board not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state))]
pub async fn delete_board(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
) -> impl IntoResponse {
    let board_id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid board id." })),
            ))
        }
    };

    let board = match fetch_board_by_id(&state.database, board_id).await {
        Ok(Some(board)) => board,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Board not found." })),
            ))
        }
        Err(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not fetch board." })),
            ))
        }
    };

    let is_admin = current_user.role_level >= 2;
    if !is_admin && board.owner_id != current_user.id {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "Access denied." })),
        ));
    }

    let bucket = get_env_with_default("STORAGE_BUCKET_BOARD_FILES", "board-files");
    match list_board_files_by_board_id(&state.database, board_id).await {
        Ok(files) => {
            for file in files {
                if let Err(err) =
                    delete_from_storage(&state.storage, &bucket, &file.object_key).await
                {
                    tracing::error!("Failed to delete file {}: {err}", file.object_key);
                }
            }
        }
        Err(err) => {
            tracing::error!("Failed to list board files: {err}");
        }
    }

    // Remove all converted PDF pages (not tracked in board_files).
    crate::storage::delete::gc_orphaned_pdf_docs(
        &state.storage,
        board_id,
        &std::collections::HashSet::new(),
        0,
    )
    .await;

    // Remove the server-rendered preview thumbnail (also not in board_files).
    let preview_key = format!("boards/{}/preview.png", board_id);
    if let Err(err) = delete_from_storage(&state.storage, &bucket, &preview_key).await {
        tracing::debug!("preview delete for board {board_id} (may not exist): {err}");
    }

    match delete_board_by_id(&state.database, board_id).await {
        Ok(_) => Ok(StatusCode::NO_CONTENT),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not delete board." })),
        )),
    }
}

#[utoipa::path(
    patch,
    path = "/boards/{id}",
    tag = "boards",
    params(("id" = String, Path, description = "Board ID")),
    security(("jwt_token" = [])),
    request_body = BoardUpdateBody,
    responses(
        (status = 200, description = "Board updated", body = Board),
        (status = 400, description = "Invalid payload"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state))]
pub async fn update_board(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
    Json(body): Json<BoardUpdateBody>,
) -> impl IntoResponse {
    let board_id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid board id." })),
            ))
        }
    };

    let board = match fetch_board_by_id(&state.database, board_id).await {
        Ok(Some(b)) => b,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Board not found." })),
            ))
        }
        Err(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not fetch board." })),
            ))
        }
    };

    if board.owner_id != current_user.id && current_user.role_level < 2 {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "Only the owner can update a board." })),
        ));
    }

    if let Some(ref v) = body.visibility {
        if v != "public" && v != "private" {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(
                    json!({ "error": "Invalid visibility value. Must be 'public' or 'private'." }),
                ),
            ));
        }
    }

    if let Some(ref la) = body.link_access {
        if la != "none" && la != "viewer" && la != "editor" {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(
                    json!({ "error": "Invalid link_access value. Must be 'none', 'viewer', or 'editor'." }),
                ),
            ));
        }
    }

    match db_update_board(
        &state.database,
        board_id,
        body.title.as_deref(),
        body.visibility.as_deref(),
        body.link_access.as_deref(),
    )
    .await
    {
        Ok(updated) => Ok(Json(updated)),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not update board." })),
        )),
    }
}

#[utoipa::path(
    post,
    path = "/boards/{id}/favorite",
    tag = "boards",
    params(("id" = String, Path, description = "Board ID")),
    security(("jwt_token" = [])),
    responses(
        (status = 200, description = "Favorite toggled", body = Board),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state))]
pub async fn toggle_board_favorite(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
) -> impl IntoResponse {
    let board_id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid board id." })),
            ))
        }
    };

    let board = match fetch_board_by_id(&state.database, board_id).await {
        Ok(Some(b)) => b,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Board not found." })),
            ))
        }
        Err(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not fetch board." })),
            ))
        }
    };

    if board.owner_id != current_user.id && current_user.role_level < 2 {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "Only the owner can favorite a board." })),
        ));
    }

    match db_toggle_board_favorite(&state.database, board_id).await {
        Ok(updated) => Ok(Json(updated)),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not toggle favorite." })),
        )),
    }
}

#[instrument(skip(state))]
pub async fn presign_board_file(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    user_ext: Option<Extension<User>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let board_id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid board id." })),
            ))
        }
    };

    let key = match params.get("key") {
        Some(k) if !k.trim().is_empty() => k.clone(),
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Missing key parameter." })),
            ))
        }
    };

    // Security: key must belong to this board
    let expected_prefix = format!("boards/{}/", board_id);
    if !key.starts_with(&expected_prefix) {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "Key does not belong to this board." })),
        ));
    }

    let board = match fetch_board_by_id(&state.database, board_id).await {
        Ok(Some(b)) => b,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Board not found." })),
            ))
        }
        Err(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not fetch board." })),
            ))
        }
    };

    // A valid embed token grants view-only access to this board's media (so a
    // private board renders its images/video/audio inside an iframe).
    let embed_token = embed_token_from_headers(&headers);
    let embed_ok = embed_grants_view(&state.database, board_id, embed_token.as_deref()).await;

    if !embed_ok && board.link_access == "none" && board.visibility != "public" {
        let current_user = match user_ext.as_ref() {
            Some(Extension(u)) => u,
            None => {
                return Err((
                    StatusCode::UNAUTHORIZED,
                    Json(json!({ "error": "Authentication required." })),
                ))
            }
        };
        let is_admin = current_user.role_level >= 2;
        let has_access = is_admin || {
            let role = get_member_role(&state.database, board_id, current_user.id)
                .await
                .unwrap_or(None);
            role.is_some() || board.owner_id == current_user.id
        };
        if !has_access {
            return Err((
                StatusCode::FORBIDDEN,
                Json(json!({ "error": "Access denied." })),
            ));
        }
    }

    let bucket = get_env_with_default("STORAGE_BUCKET_BOARD_FILES", "board-files");
    let expires_in = get_env_u64("STORAGE_PRESIGN_TTL_SECONDS", 3600);

    match generate_presigned_url(&state.storage, &bucket, &key, expires_in).await {
        Ok(url) => Ok(Json(json!({ "url": url }))),
        Err(err) => {
            tracing::error!("Presign error: {err}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to generate presigned URL." })),
            ))
        }
    }
}

// ── Invite link handlers ──────────────────────────────────────────────

#[instrument(skip(state))]
pub async fn create_invite_link_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
    Json(body): Json<CreateInviteLinkBody>,
) -> impl IntoResponse {
    let board_id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid board id." })),
            ))
        }
    };

    let is_admin = current_user.role_level >= 2;
    if !is_admin {
        let role = get_member_role(&state.database, board_id, current_user.id)
            .await
            .unwrap_or(None);
        if role.as_deref() != Some("owner") {
            return Err((
                StatusCode::FORBIDDEN,
                Json(json!({ "error": "Only the owner can manage invite links." })),
            ));
        }
    }

    let valid_roles = ["viewer", "editor"];
    if !valid_roles.contains(&body.role.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Invalid role. Must be viewer or editor." })),
        ));
    }

    let token: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(48)
        .map(char::from)
        .collect();

    match db_create_invite_link(
        &state.database,
        board_id,
        &token,
        &body.role,
        current_user.id,
        None,
    )
    .await
    {
        Ok(link) => {
            let response = InviteLinkResponse {
                id: link.id,
                token: link.token.clone(),
                role: link.role,
                expires_at: link.expires_at,
                created_at: link.created_at,
                url: format!("/invite/{}", link.token),
            };
            Ok((StatusCode::CREATED, Json(response)))
        }
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not create invite link." })),
        )),
    }
}

#[instrument(skip(state))]
pub async fn list_invite_links_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
) -> impl IntoResponse {
    let board_id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid board id." })),
            ))
        }
    };

    let is_admin = current_user.role_level >= 2;
    if !is_admin {
        let role = get_member_role(&state.database, board_id, current_user.id)
            .await
            .unwrap_or(None);
        if role.as_deref() != Some("owner") {
            return Err((
                StatusCode::FORBIDDEN,
                Json(json!({ "error": "Only the owner can view invite links." })),
            ));
        }
    }

    match db_list_invite_links(&state.database, board_id).await {
        Ok(links) => {
            let responses: Vec<InviteLinkResponse> = links
                .into_iter()
                .map(|link| InviteLinkResponse {
                    id: link.id,
                    token: link.token.clone(),
                    role: link.role,
                    expires_at: link.expires_at,
                    created_at: link.created_at,
                    url: format!("/invite/{}", link.token),
                })
                .collect();
            Ok(Json(responses))
        }
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not fetch invite links." })),
        )),
    }
}

#[instrument(skip(state))]
pub async fn delete_invite_link_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
    Json(body): Json<DeleteInviteLinkBody>,
) -> impl IntoResponse {
    let board_id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid board id." })),
            ))
        }
    };

    let is_admin = current_user.role_level >= 2;
    if !is_admin {
        let role = get_member_role(&state.database, board_id, current_user.id)
            .await
            .unwrap_or(None);
        if role.as_deref() != Some("owner") {
            return Err((
                StatusCode::FORBIDDEN,
                Json(json!({ "error": "Only the owner can delete invite links." })),
            ));
        }
    }

    match db_delete_invite_link(&state.database, body.id, board_id).await {
        Ok(_) => Ok(Json(json!({ "success": true }))),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not delete invite link." })),
        )),
    }
}

#[instrument(skip(state))]
pub async fn get_invite_info_handler(
    State(state): State<Arc<AppState>>,
    Path(token): Path<String>,
    _user_ext: Option<Extension<User>>,
) -> impl IntoResponse {
    match get_invite_link_info(&state.database, &token).await {
        Ok(Some(info)) => {
            if let Some(expires_at) = info.expires_at {
                if expires_at < Utc::now() {
                    return Err((
                        StatusCode::GONE,
                        Json(json!({ "error": "Invite link has expired." })),
                    ));
                }
            }
            Ok(Json(json!({
                "board_id": info.board_id,
                "board_title": info.board_title,
                "role": info.role,
                "creator_username": info.creator_username,
            })))
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Invite link not found." })),
        )),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not fetch invite info." })),
        )),
    }
}

#[instrument(skip(state))]
pub async fn accept_invite_handler(
    State(state): State<Arc<AppState>>,
    Path(token): Path<String>,
    Extension(current_user): Extension<User>,
) -> impl IntoResponse {
    let link = match get_invite_link_by_token(&state.database, &token).await {
        Ok(Some(link)) => link,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Invite link not found." })),
            ))
        }
        Err(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not fetch invite link." })),
            ))
        }
    };

    if let Some(expires_at) = link.expires_at {
        if expires_at < Utc::now() {
            return Err((
                StatusCode::GONE,
                Json(json!({ "error": "Invite link has expired." })),
            ));
        }
    }

    match add_board_member(&state.database, link.board_id, current_user.id, &link.role).await {
        Ok(_) => Ok(Json(json!({ "board_id": link.board_id }))),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not accept invite." })),
        )),
    }
}
