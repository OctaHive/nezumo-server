//! PostgreSQL persistence layer.
//!
//! Each submodule owns SQL for one domain and accepts a shared `PgPool` or an
//! explicit transaction. HTTP concerns and authorization decisions belong in
//! handlers; these modules focus on typed database operations.

pub mod apikeys;
pub mod board_embed_tokens;
pub mod board_files;
pub mod board_invite_links;
pub mod board_members;
pub mod board_view;
pub mod boards;
pub mod connect;
pub mod events;
pub mod login_challenges;
pub mod oauth_accounts;
pub mod project_members;
pub mod project_statuses;
pub mod project_tags;
pub mod projects;
pub mod quotas;
pub mod server_settings;
pub mod snapshots;
pub mod totp_enrollments;
pub mod usage;
pub mod users;
pub mod voting;
pub mod yrs_assets;
pub mod yrs_canonical_bases;
pub mod yrs_heads;
pub mod yrs_snapshots;
pub mod yrs_updates;
