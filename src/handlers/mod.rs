//! HTTP and WebSocket request handlers.
//!
//! Handler modules translate transport input into validated domain operations,
//! enforce authorization, call persistence and external services, and map
//! results into API responses. Route paths are assembled in `crate::routes`.

pub mod board_embed;
pub mod board_view;
pub mod boards;
pub mod delete_apikeys;
pub mod delete_users;
pub mod events;
pub mod export;
pub mod favicon;
pub mod get_apikeys;
pub mod get_health;
pub mod get_referencedata;
pub mod get_usage;
pub mod get_users;
pub mod homepage;
pub mod import;
pub mod login;
pub mod logout;
pub mod oauth;
pub mod patch_users;
pub mod post_apikeys;
pub mod post_users;
pub mod preferences;
pub mod project_statuses;
pub mod project_tags;
pub mod projects;
pub mod protected;
pub mod quotas;
pub mod realtime;
pub mod rotate_apikeys;
pub mod server_settings;
pub mod support;
pub mod totp;
pub mod voting;
