//! User persistence, profile, registration, credential, and preference models.
//!
//! Internal models may carry password hashes or TOTP secrets; public response
//! conversions must continue to exclude sensitive credential fields.

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use utoipa::ToSchema;
use uuid::Uuid;
use validator::Validate;

use crate::utils::validate::{
    validate_birthday, validate_country_code, validate_language_code, validate_password,
    validate_username,
};

/// Database model (SQLx compatible)
#[derive(Debug, FromRow)]
pub struct UserRow {
    pub id: Uuid,
    pub username: String,
    pub email: String,
    pub role_level: i32,
    pub tier_level: i32,
    pub status: String,
    pub creation_date: Option<NaiveDate>,
    pub profile_picture_url: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub country_code: Option<String>,
    pub language_code: Option<String>,
    pub birthday: Option<NaiveDate>,
    pub description: Option<String>,
    pub password_hash: String,
    pub totp_secret: Option<String>,
}

/// Pending registration record (email verification before account creation)
#[derive(Debug, FromRow)]
pub struct PendingRegistrationRow {
    #[allow(dead_code)]
    pub id: Uuid,
    #[allow(dead_code)]
    pub email: String,
    pub verification_code: String,
    pub verification_expires_at: DateTime<Utc>,
    pub verified_at: Option<DateTime<Utc>>,
    pub completion_token: Option<String>,
    pub completion_expires_at: Option<DateTime<Utc>>,
    #[allow(dead_code)]
    pub created_at: DateTime<Utc>,
}

/// Internal domain model (non-SQLx)
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct User {
    pub id: Uuid,
    pub username: String,
    pub email: String,
    pub role_level: i32,
    pub tier_level: i32,
    pub status: String,
    pub creation_date: Option<NaiveDate>,
    pub profile_picture_url: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub country_code: Option<String>,
    pub language_code: Option<String>,
    pub birthday: Option<NaiveDate>,
    pub description: Option<String>,
    #[serde(skip)]
    pub password_hash: String,
    #[serde(skip)]
    pub totp_secret: Option<String>,
}

/// Public user response
#[derive(Debug, Serialize, Clone, ToSchema)]
pub struct UserGetResponse {
    pub id: Uuid,
    pub username: String,
    pub email: String,
    pub role_level: i32,
    pub tier_level: i32,
    pub status: String,
    pub creation_date: Option<NaiveDate>,
    pub profile_picture_url: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub country_code: Option<String>,
    pub language_code: Option<String>,
    pub birthday: Option<NaiveDate>,
    pub description: Option<String>,
}

/// Request body for user creation
#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct UserInsertBody {
    #[validate(length(min = 3, max = 50), custom(function = "validate_username"))]
    pub username: String,

    #[validate(email)]
    pub email: String,

    #[validate(custom(function = "validate_password"))]
    pub password: String,

    pub totp: Option<bool>,

    #[validate(length(min = 1, max = 50))]
    pub first_name: Option<String>,

    #[validate(length(min = 1, max = 50))]
    pub last_name: Option<String>,

    #[validate(length(equal = 2), custom(function = "validate_country_code"))]
    pub country_code: Option<String>,

    #[validate(length(min = 2, max = 5), custom(function = "validate_language_code"))]
    pub language_code: Option<String>,

    #[validate(custom(function = "validate_birthday"))]
    pub birthday: Option<NaiveDate>,

    #[validate(length(max = 1000))]
    pub description: Option<String>,

    #[validate(url)]
    pub profile_picture_url: Option<String>,
}

/// Response for user creation
#[derive(Debug, Serialize, ToSchema)]
pub struct UserInsertResponse {
    pub id: Uuid,
    pub username: String,
    pub email: String,
    pub role_level: i32,
    pub tier_level: i32,
    pub creation_date: NaiveDateTime,
    pub profile_picture_url: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub country_code: Option<String>,
    pub language_code: Option<String>,
    pub birthday: Option<NaiveDate>,
    pub description: Option<String>,
    pub totp_secret: Option<String>,
}

/// Per-user color picker preferences: recently used colors and saved custom
/// colors. Stored as a JSON string in `users.color_preferences`.
#[derive(Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct ColorPreferences {
    #[serde(default)]
    pub recent: Vec<String>,
    #[serde(default)]
    pub custom: Vec<String>,
}

/// Request body for user updates
#[derive(Debug, Deserialize, Serialize, Validate, ToSchema)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub struct UserUpdateBody {
    #[validate(length(min = 3, max = 30), custom(function = "validate_username"))]
    pub username: Option<String>,

    #[validate(length(min = 1, max = 50))]
    pub first_name: Option<String>,

    #[validate(length(min = 1, max = 50))]
    pub last_name: Option<String>,

    #[validate(custom(function = "validate_country_code"), length(equal = 2))]
    pub country_code: Option<Option<String>>,

    #[validate(length(min = 2, max = 5), custom(function = "validate_language_code"))]
    pub language_code: Option<String>,

    #[validate(custom(function = "validate_birthday"))]
    pub birthday: Option<Option<NaiveDate>>,

    #[validate(length(max = 1000))]
    pub description: Option<String>,

    pub role_level: Option<i32>, // Added the role_level field to the update body

    pub tier_level: Option<i32>, // Added the role_level field to the update body

    #[validate(length(min = 4, max = 20))]
    pub status: Option<String>,
}

/// Response for user updates
#[derive(Debug, Serialize, ToSchema)]
pub struct UserUpdateResponse {
    pub success: bool,
}

/// Result of an administrator changing an account's login status.
#[derive(Debug, Serialize, ToSchema)]
pub struct UserStatusResponse {
    pub user_id: Uuid,
    pub status: String,
}

/// Profile picture upload handling
#[allow(dead_code)]
#[derive(Debug, Deserialize, ToSchema)]
pub struct UserProfilePictureUploadBody {
    #[serde(rename = "profile_picture")]
    pub profile_picture: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct UserProfilePictureUploadResponse {
    pub url: String,
}

// Conversion implementations
impl From<UserRow> for User {
    fn from(row: UserRow) -> Self {
        Self {
            id: row.id,
            username: row.username,
            email: row.email,
            role_level: row.role_level,
            tier_level: row.tier_level,
            status: row.status,
            creation_date: row.creation_date,
            profile_picture_url: row.profile_picture_url,
            first_name: row.first_name,
            last_name: row.last_name,
            country_code: row.country_code,
            language_code: row.language_code,
            birthday: row.birthday,
            description: row.description,
            password_hash: row.password_hash,
            totp_secret: row.totp_secret,
        }
    }
}

impl From<User> for UserGetResponse {
    fn from(user: User) -> Self {
        Self {
            id: user.id,
            username: user.username,
            email: user.email,
            role_level: user.role_level,
            tier_level: user.tier_level,
            status: user.status,
            creation_date: user.creation_date,
            profile_picture_url: user.profile_picture_url,
            first_name: user.first_name,
            last_name: user.last_name,
            country_code: user.country_code,
            language_code: user.language_code,
            birthday: user.birthday,
            description: user.description,
        }
    }
}

impl From<User> for UserInsertResponse {
    fn from(user: User) -> Self {
        Self {
            id: user.id,
            username: user.username,
            email: user.email,
            role_level: user.role_level,
            tier_level: user.tier_level,
            creation_date: user
                .creation_date
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .expect("Invalid creation date"),
            profile_picture_url: user.profile_picture_url,
            first_name: user.first_name,
            last_name: user.last_name,
            country_code: user.country_code,
            language_code: user.language_code,
            birthday: user.birthday,
            description: user.description,
            totp_secret: user.totp_secret,
        }
    }
}

// Additional conversions for handler convenience
impl From<UserRow> for UserGetResponse {
    fn from(row: UserRow) -> Self {
        UserGetResponse {
            id: row.id,
            username: row.username,
            email: row.email,
            role_level: row.role_level,
            tier_level: row.tier_level,
            status: row.status,
            creation_date: row.creation_date,
            profile_picture_url: row.profile_picture_url,
            first_name: row.first_name,
            last_name: row.last_name,
            country_code: row.country_code,
            language_code: row.language_code,
            birthday: row.birthday,
            description: row.description,
        }
    }
}

impl From<UserRow> for UserInsertResponse {
    fn from(row: UserRow) -> Self {
        UserInsertResponse::from(User::from(row))
    }
}

/// Request body for authenticated password change.
/// `current_password` is required for regular users but optional for OAuth-only users.
#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct UserChangePasswordBody {
    pub current_password: Option<String>,

    #[validate(length(min = 8, message = "New password must be at least 8 characters"))]
    pub new_password: String,
}

#[derive(Deserialize, ToSchema)]
#[allow(dead_code)]
pub struct UserPasswordResetRequestBody {
    pub email: String,
}

#[derive(Deserialize, ToSchema, Validate)]
#[allow(dead_code)]
pub struct UserPasswordResetConfirmBody {
    #[validate(email)]
    pub email: String,
    pub code: String,
    pub new_password: String,
}

#[derive(Debug, Clone, ToSchema)]
#[allow(dead_code)]
pub struct UserPasswordResetCode {
    pub user_id: Uuid,
    pub code: String,
    pub expires_at: NaiveDateTime,
}

/// Data sent by the client to register a new user
#[derive(Debug, Deserialize, Validate, ToSchema)]
#[allow(dead_code)]
pub struct UserRegisterBody {
    #[validate(email)]
    pub email: String,
}

#[derive(Deserialize, Validate, ToSchema)]
#[allow(dead_code)]
pub struct UserRegisterEmailVerifyBody {
    #[validate(email)]
    pub email: String,
    #[validate(length(min = 6, max = 6))]
    pub code: String,
}

#[derive(Serialize, ToSchema)]
pub struct UserRegisterVerifyResponse {
    pub completion_token: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, FromRow, ToSchema)]
pub struct UserSearchResult {
    pub id: Uuid,
    pub username: String,
    pub email: String,
    pub profile_picture_url: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UserSearchQuery {
    pub q: String,
    pub limit: Option<i64>,
}

#[derive(Deserialize, Validate, ToSchema)]
#[allow(dead_code)]
pub struct UserRegisterCompleteBody {
    #[validate(email)]
    pub email: String,
    pub completion_token: String,

    #[validate(length(min = 3, max = 50), custom(function = "validate_username"))]
    pub username: String,

    #[validate(custom(function = "validate_password"))]
    pub password: String,

    pub totp: Option<bool>,
}
