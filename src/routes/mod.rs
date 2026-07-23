//! Top-level Axum router, shared application state, and OpenAPI assembly.
//!
//! Domain route modules are combined beneath `/api/v1`; global tracing and the
//! fallback error handler are attached after the versioned router is built.

pub mod apikey;
pub mod auth;
pub mod boards;
pub mod events;
pub mod health;
pub mod homepage;
pub mod link;
pub mod projects;
pub mod realtime;
pub mod referencedata;
pub mod server_settings;
pub mod support;
pub mod usage;
pub mod user;

use axum::Router;
use tower_http::trace::TraceLayer;
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};
use utoipa_swagger_ui::SwaggerUi;

// Application state structure
use crate::mail::MailerState; // SmtpTransport for sending emails
use crate::storage::StorageState; // S3 client for file storage
use deadpool_redis::Pool as RedisPool; // Redis connection pool
use sqlx::PgPool;
use std::sync::Arc; // For thread-safe reference counting

pub mod handlers {
    pub use crate::handlers::*;
}

pub mod models {
    pub use crate::models::*;
}

pub mod database {
    pub use crate::database::*;
}

use crate::utils::global_error_handler::global_error_handler; // Global error handler

use self::{
    apikey::create_apikey_routes,
    auth::create_auth_routes,
    boards::create_board_routes,
    events::create_event_routes,
    health::create_health_route,
    homepage::create_homepage_route,
    link::create_link_routes,
    projects::create_project_routes,
    realtime::create_realtime_routes,
    referencedata::create_referencedata_routes,
    server_settings::create_server_settings_routes,
    support::create_support_routes,
    usage::create_usage_routes,
    user::{create_user_root_routes, create_user_routes},
};

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct AppState {
    pub database: PgPool,
    pub storage: StorageState,
    pub cache: RedisPool,
    pub mail: MailerState,
    pub realtime: crate::realtime::RealtimeHub,
    /// Resident canonical-coordinator registry. Boards activate lazily.
    pub coordinators: crate::state::coordinator_registry::CoordinatorRegistry,
    /// Cross-instance durable canonical fan-out. Redis is only a wake-up
    /// transport; PostgreSQL remains the replay source.
    pub yrs_fanout: crate::state::yrs_fanout::CanonicalFanout,
}

#[allow(dead_code)] // Not sure why, but rust-analyzer is complaining about this. While Utoipa uses it.
struct SecurityAddon;
impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "jwt_token",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("JWT")
                    .description(Some("Use JWT token obtained from /login endpoint."))
                    .build(),
            ),
        );
    }
}

// Define the OpenAPI documentation structure
#[derive(OpenApi)]
#[openapi(
    info(
        title = "Nezumo",
        description = "The Nezumo API",
        version = "1.0.0",
        contact(
            url = "https://github.com/OctaHive/nezumo-server"
        ),
        license(
            name = "AGPL-3.0-only",
            url = "https://www.gnu.org/licenses/agpl-3.0.html"
        )
    ),
    paths(
        handlers::get_users::get_all_users,
        handlers::get_users::get_users_by_id,
        handlers::get_apikeys::get_all_apikeys,
        handlers::get_apikeys::get_apikeys_by_id,
        handlers::get_usage::get_usage_last_day,
        handlers::get_usage::get_usage_last_week,
        handlers::quotas::get_tiers,
        handlers::quotas::get_current_quota,
        handlers::get_health::get_health,
        handlers::get_referencedata::get_referencedata,
        handlers::post_users::post_user,
        handlers::post_users::post_user_register_verify,
        handlers::post_users::post_user_register,
        handlers::post_users::post_user_register_complete,
        handlers::post_users::post_user_password_reset_verify,
        handlers::post_users::post_user_password_reset,
        handlers::post_users::post_user_profilepicture,
        handlers::patch_users::patch_user_profile,
        handlers::patch_users::change_password,
        handlers::patch_users::activate_user,
        handlers::patch_users::deactivate_user,
        handlers::preferences::get_color_preferences,
        handlers::preferences::update_color_preferences_handler,
        handlers::totp::get_totp_status,
        handlers::totp::setup_totp,
        handlers::totp::confirm_totp,
        handlers::totp::disable_totp,
        handlers::totp::reset_user_totp,
        handlers::post_apikeys::post_apikey,
        handlers::rotate_apikeys::rotate_apikey,
        handlers::delete_users::delete_user_by_id,
        handlers::delete_apikeys::delete_apikey_by_id,
        handlers::protected::protected,
        handlers::login::login,
        handlers::login::login_totp,
        handlers::logout::logout,
        handlers::oauth::oauth_google,
        handlers::oauth::oauth_google_callback,
        handlers::projects::create_project,
        handlers::projects::list_projects,
        handlers::projects::get_project_by_id,
        handlers::projects::list_project_members,
        handlers::boards::create_board,
        handlers::boards::list_project_boards,
        handlers::boards::get_board_by_id,
        handlers::boards::list_board_members,
        handlers::boards::upload_board_image,
        handlers::boards::delete_board,
        handlers::events::post_commit_event,
        handlers::events::get_events_since,
        handlers::events::get_latest_snapshot,
        handlers::events::post_snapshot,
    ),
    components(
        schemas(
            models::apikey::ApiKey,
            models::apikey::ApiKeyInsertBody,
            models::apikey::ApiKeyInsertResponse,
            models::apikey::ApiKeyResponse,
            models::apikey::ApiKeyByIDResponse,
            models::apikey::ApiKeyGetActiveForUserResponse,
            models::apikey::ApiKeyByUserIDResponse,
            models::apikey::ApiKeyNewBody,
            models::apikey::ApiKeyRotateResponse,
            models::apikey::ApiKeyRotateResponseInfo,
            models::apikey::ApiKeyRotateBody,
            models::auth::Claims,
            models::auth::LoginData,
            models::auth::LoginTotpData,
            models::auth::LoginChallengeResponse,
            models::auth::TotpStatusResponse,
            models::auth::TotpSetupResponse,
            models::auth::TotpConfirmData,
            models::auth::TotpDisableData,
            models::documentation::SuccessResponse,
            models::documentation::ErrorResponse,
            models::health::HealthResponse,
            models::health::CpuUsage,
            models::health::DatabaseStatus,
            models::health::DiskUsage,
            models::health::MemoryStatus,
            models::role::Role,
            models::usage::UsageResponseLastDay,
            models::usage::UsageResponseLastWeek,
            models::user::User,
            models::user::UserGetResponse,
            models::user::UserInsertBody,
            models::user::UserInsertResponse,
            models::user::UserUpdateBody,
            models::user::UserUpdateResponse,
            models::user::UserStatusResponse,
            models::user::ColorPreferences,
            models::user::UserRegisterEmailVerifyBody,
            models::user::UserRegisterBody,
            models::user::UserRegisterCompleteBody,
            models::user::UserRegisterVerifyResponse,
            models::user::UserChangePasswordBody,
            models::user::UserPasswordResetCode,
            models::user::UserPasswordResetConfirmBody,
            models::user::UserPasswordResetRequestBody,
            models::projects::Project,
            models::projects::ProjectCreateBody,
            models::projects::ProjectMember,
            models::boards::Board,
            models::boards::BoardCreateBody,
            models::boards::BoardMember,
            models::boards::BoardStateResponse,
            models::board_files::BoardFileUploadResponse,
            database::quotas::TierQuota,
            database::quotas::QuotaUsage,
            handlers::quotas::CurrentQuotaResponse,
            models::events::CommitEventBody,
            models::events::CommitEventResponse,
            models::events::EventRecord,
            models::events::SnapshotRecord,
            models::events::SnapshotCreateBody
        )
    ),
    tags(
        (name = "user", description = "User related endpoints."),
        (name = "apikey", description = "API key related endpoints."),
        (name = "usage", description = "Usage related endpoints."),
        (name = "health", description = "Health check endpoint."),
        (name = "projects", description = "Project related endpoints."),
        (name = "boards", description = "Board related endpoints."),
        (name = "events", description = "Board event endpoints."),
    )
)]
struct ApiDoc;

/// Function to create and configure all routes
pub fn create_routes(state: Arc<AppState>) -> Router<()> {
    // Create OpenAPI specification
    let openapi = ApiDoc::openapi();

    // Create Swagger UI
    let swagger_ui = SwaggerUi::new("/docs").url("/openapi.json", openapi.clone());

    let api_router = Router::new()
        .merge(create_homepage_route(state.clone()))
        .merge(create_link_routes(state.clone()))
        .merge(create_auth_routes(state.clone()))
        .merge(create_user_root_routes(state.clone()))
        .merge(swagger_ui)
        .merge(create_referencedata_routes(state.clone()))
        .nest("/users", create_user_routes(state.clone()))
        .merge(create_server_settings_routes(state.clone()))
        .nest("/apikeys", create_apikey_routes(state.clone()))
        .nest("/usage", create_usage_routes(state.clone()))
        .nest("/projects", create_project_routes(state.clone()))
        .nest("/boards", create_board_routes(state.clone()))
        .merge(create_event_routes(state.clone()))
        .merge(create_realtime_routes(state.clone()))
        .merge(create_support_routes(state.clone()))
        .merge(create_health_route(state.clone()))
        .with_state(state);

    // Combine all routes and add middleware
    Router::new()
        .nest("/api/v1", api_router)
        .layer(TraceLayer::new_for_http())
        .fallback(global_error_handler)
}
