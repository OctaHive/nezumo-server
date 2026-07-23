//! Serializable API contracts and SQLx row models grouped by domain.
//!
//! Types in this layer define transport and persistence shapes; handlers own
//! authorization and orchestration, while database modules own SQL operations.

/// Module for API key related models.
pub mod apikey;
/// Module for authentication related models.
pub mod auth;
/// Module for board file related models.
pub mod board_files;
/// Module for board invite link related models.
pub mod board_invite_links;
/// Module for board related models.
pub mod boards;
/// Module for documentation related models.
pub mod documentation;
/// Module for errors.
pub mod error;
/// Module for event related models.
pub mod events;
/// Module for the health endpoint related models.
pub mod health;
/// Module for project task-card status dictionary models.
pub mod project_statuses;
/// Module for project tag dictionary models.
pub mod project_tags;
/// Module for project related models.
pub mod projects;
/// Module for userrole related models.
pub mod role;
/// Module for the health endpoint related models.
pub mod usage;
/// Module for user related models.
pub mod user;
