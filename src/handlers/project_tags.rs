//! Handlers for the per-project tag dictionary.

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
use crate::database::project_tags::{create_tag, delete_tag, list_tags, update_tag};
use crate::database::projects::get_project_by_id as fetch_project_by_id;
use crate::models::project_tags::{ProjectTagCreateBody, ProjectTagUpdateBody};
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

/// `GET /projects/{id}/tags` — list the project's tags.
pub async fn list_project_tags(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
) -> impl IntoResponse {
    let project_id = match authorize_project(&state, &id, &current_user).await {
        Ok(id) => id,
        Err(e) => return Err(e),
    };
    match list_tags(&state.database, project_id).await {
        Ok(tags) => Ok(Json(tags)),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not fetch tags." })),
        )),
    }
}

/// `POST /projects/{id}/tags` — add a tag to the dictionary.
pub async fn create_project_tag(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
    Json(body): Json<ProjectTagCreateBody>,
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
    match create_tag(&state.database, project_id, label, color).await {
        Ok(tag) => Ok((StatusCode::CREATED, Json(tag))),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not create tag." })),
        )),
    }
}

/// `PATCH /projects/{id}/tags/{tag_id}` — rename / recolor a tag.
pub async fn update_project_tag(
    State(state): State<Arc<AppState>>,
    Path((id, tag_id)): Path<(String, String)>,
    Extension(current_user): Extension<User>,
    Json(body): Json<ProjectTagUpdateBody>,
) -> impl IntoResponse {
    let project_id = match authorize_project(&state, &id, &current_user).await {
        Ok(id) => id,
        Err(e) => return Err(e),
    };
    let tag_id = match Uuid::parse_str(&tag_id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid tag id." })),
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
    match update_tag(&state.database, project_id, tag_id, label, color).await {
        Ok(Some(tag)) => Ok(Json(tag)),
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Tag not found." })),
        )),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not update tag." })),
        )),
    }
}

/// `DELETE /projects/{id}/tags/{tag_id}` — remove a tag.
pub async fn delete_project_tag(
    State(state): State<Arc<AppState>>,
    Path((id, tag_id)): Path<(String, String)>,
    Extension(current_user): Extension<User>,
) -> impl IntoResponse {
    let project_id = match authorize_project(&state, &id, &current_user).await {
        Ok(id) => id,
        Err(e) => return Err(e),
    };
    let tag_id = match Uuid::parse_str(&tag_id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid tag id." })),
            ))
        }
    };
    match delete_tag(&state.database, project_id, tag_id).await {
        Ok(()) => Ok(StatusCode::NO_CONTENT),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not delete tag." })),
        )),
    }
}
