//! Board CRUD, sharing, membership, uploads, import/export, and embed routes.
//!
//! This module also applies endpoint-specific request body limits for large
//! board files and media uploads.

use axum::extract::DefaultBodyLimit;
use axum::Router;
use std::sync::Arc;

use crate::handlers::board_embed::{
    create_embed_token_handler, delete_embed_token_handler, get_embed_token_handler,
};
use crate::handlers::board_view::{get_board_view_handler, put_board_view_handler};
use crate::handlers::boards::{
    accept_invite_handler, add_board_member_handler, create_board, create_invite_link_handler,
    delete_board, delete_invite_link_handler, get_board_by_id, get_board_state,
    get_invite_info_handler, get_pdf_page, list_all_boards, list_board_members,
    list_invite_links_handler, presign_board_file, remove_board_member_handler,
    toggle_board_favorite, update_board, upload_board_audio, upload_board_file, upload_board_image,
    upload_board_pdf, upload_board_video,
};
use crate::handlers::export::{export_board, export_board_image};
use crate::handlers::import::import_board;
use crate::handlers::voting::{
    cast_vote_handler, end_voting_handler, finish_voting_handler, get_voting_handler,
    set_participation_handler, start_voting_handler,
};
use crate::routes::AppState;
use crate::wrappers::authentication_route_builder::AuthenticatedRouteBuilder;

/// Builds board CRUD, asset, membership, import, export, and embed routes.
pub fn create_board_routes(state: Arc<AppState>) -> Router<Arc<AppState>> {
    AuthenticatedRouteBuilder::new(state)
        .get("/all", list_all_boards, vec![1, 2])
        .post("/", create_board, vec![1, 2])
        .post("/import", import_board, vec![1, 2])
        .maybe_authenticated_get("/{id}", get_board_by_id)
        .maybe_authenticated_get("/{id}/state", get_board_state)
        .get("/{id}/export", export_board, vec![1, 2])
        .get("/{id}/export/image", export_board_image, vec![1, 2])
        .patch("/{id}", update_board, vec![1, 2])
        .delete("/{id}", delete_board, vec![1, 2])
        .post("/{id}/favorite", toggle_board_favorite, vec![1, 2])
        .maybe_authenticated_get("/{id}/view", get_board_view_handler)
        .maybe_authenticated_post("/{id}/view", put_board_view_handler)
        .maybe_authenticated_get("/{id}/voting", get_voting_handler)
        .post("/{id}/voting", start_voting_handler, vec![1, 2])
        .post(
            "/{id}/voting/participation",
            set_participation_handler,
            vec![1, 2],
        )
        .post("/{id}/voting/vote", cast_vote_handler, vec![1, 2])
        .post("/{id}/voting/finish", finish_voting_handler, vec![1, 2])
        .post("/{id}/voting/end", end_voting_handler, vec![1, 2])
        .post("/{id}/embed", create_embed_token_handler, vec![1, 2])
        .get("/{id}/embed", get_embed_token_handler, vec![1, 2])
        .delete("/{id}/embed", delete_embed_token_handler, vec![1, 2])
        .get("/{id}/members", list_board_members, vec![1, 2])
        .post("/{id}/members", add_board_member_handler, vec![1, 2])
        .delete("/{id}/members", remove_board_member_handler, vec![1, 2])
        .post("/{id}/images", upload_board_image, vec![1, 2])
        .post("/{id}/audio", upload_board_audio, vec![1, 2])
        .post("/{id}/video", upload_board_video, vec![1, 2])
        .post("/{id}/files", upload_board_file, vec![1, 2])
        .post("/{id}/pdf", upload_board_pdf, vec![1, 2])
        .maybe_authenticated_get("/{id}/pdf/{doc_id}/page/{page}", get_pdf_page)
        .maybe_authenticated_get("/{id}/files/presign", presign_board_file)
        .get("/{id}/invites", list_invite_links_handler, vec![1, 2])
        .post("/{id}/invites", create_invite_link_handler, vec![1, 2])
        .delete("/{id}/invites", delete_invite_link_handler, vec![1, 2])
        .maybe_authenticated_get("/invites/{token}", get_invite_info_handler)
        .post("/invites/{token}/accept", accept_invite_handler, vec![1, 2])
        .build()
        // The highest configured tier allows a 500 MiB file. Keep a small
        // multipart-envelope allowance; the handler still applies the exact
        // per-tier limit to the uploaded field.
        .layer(DefaultBodyLimit::max(510 * 1024 * 1024))
}
