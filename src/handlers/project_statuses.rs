//! Handlers for the per-project task-card status dictionary.

use axum::response::IntoResponse;
use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    Json,
};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

use crate::database::project_members::get_project_member_role;
use crate::database::project_statuses::{
    create_status, delete_status, list_or_seed_statuses, update_status,
};
use crate::database::projects::get_project_by_id as fetch_project_by_id;
use crate::models::project_statuses::{ProjectStatusCreateBody, ProjectStatusUpdateBody};
use crate::models::user::User;
use crate::routes::AppState;

type HandlerError = (StatusCode, Json<serde_json::Value>);

/// Parse + authorize: the caller must own the project, be a member, or be admin.
async fn authorize_project(state: &AppState, id: &str, user: &User) -> Result<Uuid, HandlerError> {
    let project_id = Uuid::parse_str(id).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Invalid project id." })),
        )
    })?;
    let project = fetch_project_by_id(&state.database, project_id)
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not fetch project." })),
            )
        })?
        .ok_or((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Project not found." })),
        ))?;

    let is_admin = user.role_level >= 2;
    if !is_admin && project.owner_id != user.id {
        let role = get_project_member_role(&state.database, project_id, user.id)
            .await
            .unwrap_or(None);
        if role.is_none() {
            return Err((
                StatusCode::FORBIDDEN,
                Json(json!({ "error": "Access denied." })),
            ));
        }
    }
    Ok(project_id)
}

/// `GET /projects/{id}/statuses` — list (seeding the preset defaults on first read).
pub async fn list_project_statuses(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
) -> impl IntoResponse {
    let project_id = match authorize_project(&state, &id, &current_user).await {
        Ok(id) => id,
        Err(e) => return Err(e),
    };
    match list_or_seed_statuses(&state.database, project_id).await {
        Ok(statuses) => Ok(Json(statuses)),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not fetch statuses." })),
        )),
    }
}

/// `POST /projects/{id}/statuses` — add a status to the dictionary.
pub async fn create_project_status(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
    Json(body): Json<ProjectStatusCreateBody>,
) -> impl IntoResponse {
    let project_id = match authorize_project(&state, &id, &current_user).await {
        Ok(id) => id,
        Err(e) => return Err(e),
    };
    let label = body.label.trim();
    let color = body.color.trim();
    if label.is_empty() || color.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Label and color are required." })),
        ));
    }
    match create_status(&state.database, project_id, label, color).await {
        Ok(status) => Ok((StatusCode::CREATED, Json(status))),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not create status." })),
        )),
    }
}

/// `PATCH /projects/{id}/statuses/{status_id}` — rename / recolor a status.
pub async fn update_project_status(
    State(state): State<Arc<AppState>>,
    Path((id, status_id)): Path<(String, String)>,
    Extension(current_user): Extension<User>,
    Json(body): Json<ProjectStatusUpdateBody>,
) -> impl IntoResponse {
    let project_id = match authorize_project(&state, &id, &current_user).await {
        Ok(id) => id,
        Err(e) => return Err(e),
    };
    let status_id = match Uuid::parse_str(&status_id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid status id." })),
            ))
        }
    };
    let label = body
        .label
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let color = body
        .color
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    if label.is_none() && color.is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Nothing to update." })),
        ));
    }
    match update_status(&state.database, project_id, status_id, label, color).await {
        Ok(Some(status)) => Ok(Json(status)),
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Status not found." })),
        )),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not update status." })),
        )),
    }
}

/// `DELETE /projects/{id}/statuses/{status_id}` — remove a status.
pub async fn delete_project_status(
    State(state): State<Arc<AppState>>,
    Path((id, status_id)): Path<(String, String)>,
    Extension(current_user): Extension<User>,
) -> impl IntoResponse {
    let project_id = match authorize_project(&state, &id, &current_user).await {
        Ok(id) => id,
        Err(e) => return Err(e),
    };
    let status_id = match Uuid::parse_str(&status_id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid status id." })),
            ))
        }
    };
    match delete_status(&state.database, project_id, status_id).await {
        Ok(()) => Ok(StatusCode::NO_CONTENT),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not delete status." })),
        )),
    }
}
