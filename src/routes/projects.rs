//! Project CRUD, membership, status, tag, and project-board routes.

use axum::Router;
use std::sync::Arc;

use crate::handlers::boards::list_project_boards;
use crate::handlers::project_statuses::{
    create_project_status, delete_project_status, list_project_statuses, update_project_status,
};
use crate::handlers::project_tags::{
    create_project_tag, delete_project_tag, list_project_tags, update_project_tag,
};
use crate::handlers::projects::{
    create_project, delete_project, get_project_by_id, list_project_members, list_projects,
    rename_project, toggle_favorite,
};
use crate::routes::AppState;
use crate::wrappers::authentication_route_builder::AuthenticatedRouteBuilder;

/// Builds all project CRUD, membership, tag, and status routes.
pub fn create_project_routes(state: Arc<AppState>) -> Router<Arc<AppState>> {
    AuthenticatedRouteBuilder::new(state)
        .get("/", list_projects, vec![1, 2])
        .post("/", create_project, vec![1, 2])
        .get("/{id}", get_project_by_id, vec![1, 2])
        .patch("/{id}", rename_project, vec![1, 2])
        .delete("/{id}", delete_project, vec![1, 2])
        .post("/{id}/favorite", toggle_favorite, vec![1, 2])
        .get("/{id}/members", list_project_members, vec![1, 2])
        .get("/{project_id}/boards", list_project_boards, vec![1, 2])
        .get("/{id}/statuses", list_project_statuses, vec![1, 2])
        .post("/{id}/statuses", create_project_status, vec![1, 2])
        .patch(
            "/{id}/statuses/{status_id}",
            update_project_status,
            vec![1, 2],
        )
        .delete(
            "/{id}/statuses/{status_id}",
            delete_project_status,
            vec![1, 2],
        )
        .get("/{id}/tags", list_project_tags, vec![1, 2])
        .post("/{id}/tags", create_project_tag, vec![1, 2])
        .patch("/{id}/tags/{tag_id}", update_project_tag, vec![1, 2])
        .delete("/{id}/tags/{tag_id}", delete_project_tag, vec![1, 2])
        .build()
}
