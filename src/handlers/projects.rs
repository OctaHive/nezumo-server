//! Project CRUD, favorites, membership listing, and access-control handlers.

use axum::response::IntoResponse;
use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    Json,
};
use serde_json::json;
use std::sync::Arc;
use tracing::instrument;
use uuid::Uuid;

use crate::database::project_members::{
    add_project_member, get_project_member_role, list_project_members as db_list_project_members,
};
use crate::database::projects::{
    create_project as db_create_project, delete_project as db_delete_project,
    get_project_by_id as fetch_project_by_id, list_all_projects, list_projects_for_user,
    rename_project as db_rename_project, toggle_project_favorite as db_toggle_project_favorite,
};
use crate::models::projects::{Project, ProjectCreateBody, ProjectUpdateBody};
use crate::models::user::User;
use crate::routes::AppState;

#[utoipa::path(
    get,
    path = "/projects/{id}/members",
    tag = "projects",
    params(("id" = String, Path, description = "Project ID")),
    security(("jwt_token" = [])),
    responses(
        (status = 200, description = "Project members", body = [crate::models::projects::ProjectMember]),
        (status = 400, description = "Invalid project ID"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Project not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state))]
pub async fn list_project_members(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
) -> impl IntoResponse {
    let project_id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid project id." })),
            ))
        }
    };

    let project = match fetch_project_by_id(&state.database, project_id).await {
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

    if current_user.role_level < 2 && project.owner_id != current_user.id {
        match get_project_member_role(&state.database, project_id, current_user.id).await {
            Ok(Some(_)) => {}
            Ok(None) => {
                return Err((
                    StatusCode::FORBIDDEN,
                    Json(json!({ "error": "Access denied." })),
                ))
            }
            Err(_) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "Could not verify project access." })),
                ))
            }
        }
    }

    match db_list_project_members(&state.database, project_id).await {
        Ok(mut members) => {
            for member in &mut members {
                if let Some(stored) = member.profile_picture_url.clone() {
                    if let Some(presigned) =
                        crate::storage::presign_url::presign_stored_url(&state.storage, &stored)
                            .await
                    {
                        member.profile_picture_url = Some(presigned);
                    }
                }
            }
            Ok(Json(members))
        }
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not fetch project members." })),
        )),
    }
}

#[utoipa::path(
    post,
    path = "/projects",
    tag = "projects",
    security(("jwt_token" = [])),
    request_body = ProjectCreateBody,
    responses(
        (status = 201, description = "Project created", body = Project),
        (status = 400, description = "Invalid payload"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state))]
pub async fn create_project(
    State(state): State<Arc<AppState>>,
    Extension(current_user): Extension<User>,
    Json(body): Json<ProjectCreateBody>,
) -> impl IntoResponse {
    match db_create_project(&state.database, current_user.id, &body).await {
        Ok(project) => {
            let _ = add_project_member(&state.database, project.id, current_user.id, "owner").await;
            Ok((StatusCode::CREATED, Json(project)))
        }
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not create project." })),
        )),
    }
}

#[utoipa::path(
    get,
    path = "/projects",
    tag = "projects",
    security(("jwt_token" = [])),
    responses(
        (status = 200, description = "Projects list", body = [Project]),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state))]
pub async fn list_projects(
    State(state): State<Arc<AppState>>,
    Extension(current_user): Extension<User>,
) -> impl IntoResponse {
    let is_admin = current_user.role_level >= 2;
    let result = if is_admin {
        list_all_projects(&state.database).await
    } else {
        list_projects_for_user(&state.database, current_user.id).await
    };

    match result {
        Ok(projects) => Ok(Json(projects)),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not fetch projects." })),
        )),
    }
}

#[utoipa::path(
    get,
    path = "/projects/{id}",
    tag = "projects",
    params(("id" = String, Path, description = "Project ID")),
    security(("jwt_token" = [])),
    responses(
        (status = 200, description = "Project", body = Project),
        (status = 404, description = "Not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state))]
pub async fn get_project_by_id(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
) -> impl IntoResponse {
    let project_id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid project id." })),
            ))
        }
    };

    match fetch_project_by_id(&state.database, project_id).await {
        Ok(Some(project)) => {
            let is_admin = current_user.role_level >= 2;
            if !is_admin && project.owner_id != current_user.id {
                let role = crate::database::project_members::get_project_member_role(
                    &state.database,
                    project_id,
                    current_user.id,
                )
                .await
                .unwrap_or(None);
                if role.is_none() {
                    return Err((
                        StatusCode::FORBIDDEN,
                        Json(json!({ "error": "Access denied." })),
                    ));
                }
            }
            Ok(Json(project))
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Project not found." })),
        )),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not fetch project." })),
        )),
    }
}

#[utoipa::path(
    patch,
    path = "/projects/{id}",
    tag = "projects",
    params(("id" = String, Path, description = "Project ID")),
    security(("jwt_token" = [])),
    request_body = ProjectUpdateBody,
    responses(
        (status = 200, description = "Project renamed", body = Project),
        (status = 400, description = "Invalid payload"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state))]
pub async fn rename_project(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
    Json(body): Json<ProjectUpdateBody>,
) -> impl IntoResponse {
    let project_id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid project id." })),
            ))
        }
    };

    let project = match fetch_project_by_id(&state.database, project_id).await {
        Ok(Some(p)) => p,
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
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "Only the owner can rename a project." })),
        ));
    }

    match db_rename_project(&state.database, project_id, &body.name).await {
        Ok(updated) => Ok(Json(updated)),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not rename project." })),
        )),
    }
}

#[utoipa::path(
    delete,
    path = "/projects/{id}",
    tag = "projects",
    params(("id" = String, Path, description = "Project ID")),
    security(("jwt_token" = [])),
    responses(
        (status = 204, description = "Project deleted"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state))]
pub async fn delete_project(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
) -> impl IntoResponse {
    let project_id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid project id." })),
            ))
        }
    };

    let project = match fetch_project_by_id(&state.database, project_id).await {
        Ok(Some(p)) => p,
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
            Json(json!({ "error": "Only the owner can delete a project." })),
        ));
    }

    match db_delete_project(&state.database, project_id).await {
        Ok(()) => Ok(StatusCode::NO_CONTENT),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not delete project." })),
        )),
    }
}

#[utoipa::path(
    post,
    path = "/projects/{id}/favorite",
    tag = "projects",
    params(("id" = String, Path, description = "Project ID")),
    security(("jwt_token" = [])),
    responses(
        (status = 200, description = "Favorite toggled", body = Project),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state))]
pub async fn toggle_favorite(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
) -> impl IntoResponse {
    let project_id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid project id." })),
            ))
        }
    };

    let project = match fetch_project_by_id(&state.database, project_id).await {
        Ok(Some(p)) => p,
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
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "Only the owner can favorite a project." })),
        ));
    }

    match db_toggle_project_favorite(&state.database, project_id).await {
        Ok(updated) => Ok(Json(updated)),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not toggle favorite." })),
        )),
    }
}
