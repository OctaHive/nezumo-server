//! Password hashing, JWT, API-key, TOTP, and cookie/header token helpers.

use argon2::{
    password_hash::{Error, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2, Params, Version,
};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{Duration, Utc};
use jsonwebtoken::{
    decode, encode, errors::ErrorKind, DecodingKey, EncodingKey, Header, TokenData, Validation,
};
use lazy_static::lazy_static;
use rand::{rngs::OsRng, Rng};
use tokio::task;
use totp_rs::{Algorithm, Secret, TOTP};
use tracing::{error, instrument, warn};
use url::Url;

use crate::core::config::{get_env, get_env_with_default};
use crate::models::auth::{AuthError, Claims};

lazy_static! {
    static ref SECRET_KEY: String = get_env("JWT_SECRET_KEY");
}

// Password hashing and verification
#[instrument(skip(password, hash))]
pub async fn verify_hash(password: &str, hash: &str) -> Result<bool, Error> {
    let password_owned = password.to_string();
    let hash_owned = hash.to_string();

    task::spawn_blocking(move || {
        let parsed_hash = PasswordHash::new(&hash_owned)?;
        match Argon2::default().verify_password(password_owned.as_bytes(), &parsed_hash) {
            Ok(()) => Ok(true),
            Err(Error::Password) => Ok(false),
            Err(error) => Err(error),
        }
    })
    .await
    .map_err(|_| Error::Crypto)?
}

#[instrument(skip(password))]
pub fn hash_password(password: &str) -> Result<String, argon2::password_hash::Error> {
    // Generate random salt
    let salt = SaltString::generate(&mut OsRng);

    // Configure Argon2id with recommended parameters
    let argon2 = Argon2::new(
        argon2::Algorithm::Argon2id, // Explicitly use Argon2id variant
        Version::V0x13,              // Latest version
        Params::new(
            // OWASP-recommended parameters
            15360, // 15 MiB memory cost
            2,     // 2 iterations
            1,     // 1 parallelism
            None,  // Default output length
        )?,
    );

    // Hash password with configured parameters
    let password_hash = argon2
        .hash_password(password.as_bytes(), &salt)?
        .to_string();
    Ok(password_hash)
}

// JWT encoding and decoding
#[instrument(skip(email))]
pub fn encode_jwt(email: String) -> Result<String, StatusCode> {
    // Get the current time and expiration time
    let now = Utc::now();
    let expire = Duration::hours(24);
    let exp: usize = (now + expire).timestamp() as usize;
    let iat: usize = now.timestamp() as usize;

    // Get issuer and audience from environment variables
    let issuer = get_env("JWT_ISSUER"); // Fetching the issuer from environment variables
    let audience = get_env("JWT_AUDIENCE"); // Fetching the audience from environment variables

    // Create claims using the fetched issuer and audience
    let claim = Claims {
        sub: email,
        iat,
        exp,
        iss: issuer,   // Set the issuer from the environment
        aud: audience, // Set the audience from the environment
    };

    // Sign the token using the secret key and the default algorithm (HS256)
    encode(
        &Header::default(),
        &claim,
        &EncodingKey::from_secret(SECRET_KEY.as_ref()),
    )
    .map_err(|e| {
        error!("Failed to encode JWT: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

#[instrument(skip(jwt))]
pub fn decode_jwt(jwt: String) -> Result<TokenData<Claims>, AuthError> {
    let secret_key = get_env("JWT_SECRET_KEY");

    // Get issuer and audience from environment variables
    let issuer = get_env("JWT_ISSUER"); // Fetching the issuer from environment variables
    let audience = get_env("JWT_AUDIENCE"); // Fetching the audience from environment variables

    // Configure validation
    let mut validation = Validation::default();

    // Optional: enforce audience and issuer using the environment variables
    validation.set_issuer(&[issuer]);
    validation.set_audience(&[audience]);

    // Add a small leeway to account for clock skew (e.g., 5 minutes)
    validation.leeway = 300; // Allow up to 5 minutes of clock skew

    // Decode and validate token
    match decode::<Claims>(
        &jwt,
        &DecodingKey::from_secret(secret_key.as_ref()),
        &validation,
    ) {
        Ok(token_data) => Ok(token_data),
        Err(err) => {
            let message = match err.kind() {
                ErrorKind::InvalidToken => "Invalid token format.",
                ErrorKind::InvalidSignature => "Invalid token signature.",
                ErrorKind::ExpiredSignature => "Token has expired.",
                ErrorKind::InvalidIssuer => "Invalid token issuer.",
                ErrorKind::InvalidAudience => "Invalid token audience.",
                _ => "Failed to decode token.",
            };

            warn!("JWT decode error: {:?}", err);

            Err(AuthError {
                message: message.to_string(),
                status_code: StatusCode::UNAUTHORIZED,
            })
        }
    }
}

// Token extraction
pub fn extract_bearer_token(header: &str) -> Result<&str, AuthError> {
    let parts: Vec<&str> = header.splitn(2, ' ').collect();
    if parts.len() != 2 || parts[0] != "Bearer" {
        return Err(AuthError {
            message: "Authorization header must be in Bearer format.".to_string(),
            status_code: StatusCode::UNAUTHORIZED,
        });
    }
    Ok(parts[1])
}

/// Extracts a bearer token from the `Authorization` request header.
pub fn extract_token_from_header(req: &Request<Body>) -> Option<String> {
    let header = req.headers().get(axum::http::header::AUTHORIZATION);
    header
        .and_then(|header| header.to_str().ok())
        .and_then(|auth_header| extract_bearer_token(auth_header).ok())
        .map(str::to_owned)
}

/// Extracts the configured authentication token from the `Cookie` header.
pub fn extract_token_from_cookie(req: &Request<Body>) -> Option<String> {
    let cookie_name = get_env_with_default("JWT_COOKIE_NAME", "auth_token");
    req.headers()
        .get(axum::http::header::COOKIE)
        .and_then(|header| header.to_str().ok())
        .and_then(|cookie_str| {
            cookie_str.split(';').find_map(|cookie| {
                let (name, value) = cookie.trim().split_once('=')?;
                (name == cookie_name).then(|| value.to_owned())
            })
        })
}

// TOTP and API key generation
#[instrument]
pub fn generate_totp_secret() -> String {
    Secret::generate_secret().to_encoded().to_string()
}

/// Builds the standard provisioning URI consumed by authenticator apps.
pub fn totp_provisioning_uri(account: &str, secret: &str) -> Result<String, String> {
    let normalized = secret.trim().to_ascii_uppercase();
    let decoded = Secret::Encoded(normalized.clone())
        .to_bytes()
        .map_err(|error| format!("invalid Base32 TOTP secret: {error}"))?;
    if decoded.len() != 20 {
        return Err("TOTP provisioning secrets must contain 160 bits".to_string());
    }

    let mut uri = Url::parse("otpauth://totp/")
        .map_err(|error| format!("could not construct TOTP provisioning URI: {error}"))?;
    uri.path_segments_mut()
        .map_err(|_| "could not construct TOTP provisioning URI path".to_string())?
        .push(&format!("Nezumo:{account}"));
    uri.query_pairs_mut()
        .append_pair("secret", &normalized)
        .append_pair("issuer", "Nezumo")
        .append_pair("algorithm", "SHA1")
        .append_pair("digits", "6")
        .append_pair("period", "30");
    Ok(uri.into())
}

/// Verifies a TOTP code against the stored secret.
///
/// New secrets use the interoperable RFC 6238 profile expected by common
/// authenticator applications: a Base32-encoded 160-bit secret, SHA-1,
/// six digits, a 30-second period, and one period of clock skew. Eight-digit
/// numeric values are recognized as legacy secrets generated by older Nezumo
/// versions and retain their historical SHA-512 profile until re-enrollment.
pub fn verify_totp_code(secret: &str, code: &str) -> Result<bool, String> {
    let legacy = is_legacy_totp_secret(secret);
    let expected_digits = if legacy { 8 } else { 6 };
    if code.len() != expected_digits || !code.bytes().all(|byte| byte.is_ascii_digit()) {
        return Ok(false);
    }

    let (algorithm, bytes) = if legacy {
        (Algorithm::SHA512, secret.as_bytes().to_vec())
    } else {
        let normalized = secret.trim().to_ascii_uppercase();
        let bytes = Secret::Encoded(normalized)
            .to_bytes()
            .map_err(|error| format!("invalid Base32 TOTP secret: {error}"))?;
        (Algorithm::SHA1, bytes)
    };

    let totp = if legacy {
        // Older versions stored only the generated eight-digit code. That is
        // below RFC 4226's minimum secret size, so the checked constructor
        // rejects it. Keep this path solely to avoid locking affected users
        // out during migration; never use it for new enrollments.
        TOTP::new_unchecked(algorithm, expected_digits, 1, 30, bytes)
    } else {
        TOTP::new(algorithm, expected_digits, 1, 30, bytes)
            .map_err(|error| format!("invalid TOTP configuration: {error}"))?
    };
    totp.check_current(code)
        .map_err(|error| format!("could not read system time for TOTP verification: {error}"))
}

fn is_legacy_totp_secret(secret: &str) -> bool {
    secret.len() == 8 && secret.bytes().all(|byte| byte.is_ascii_digit())
}

#[instrument]
pub fn generate_api_key() -> String {
    // Use OsRng for cryptographically secure random number generation
    let mut rng = OsRng;
    (0..5)
        .map(|_| {
            (0..8)
                .map(|_| format!("{:02x}", rng.gen::<u8>()))
                .collect::<String>()
        })
        .collect::<Vec<String>>()
        .join("-")
}

// Asynchronous password and API key verification
#[instrument(skip(password, hash))]
pub async fn verify_password(password: String, hash: String) -> Result<bool, Error> {
    verify_hash(&password, &hash).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn password_verification_is_bound_to_the_expected_hash() {
        let first_hash = hash_password("First-password-1!").unwrap();
        let second_hash = hash_password("Second-password-2!").unwrap();

        assert!(verify_hash("First-password-1!", &first_hash).await.unwrap());
        assert!(!verify_hash("First-password-1!", &second_hash)
            .await
            .unwrap());
    }

    #[test]
    fn generated_totp_secret_is_base32_with_160_bits_of_entropy() {
        let secret = generate_totp_secret();
        let decoded = Secret::Encoded(secret.clone()).to_bytes().unwrap();

        assert_eq!(decoded.len(), 20);
        assert_eq!(secret.len(), 32);
        assert!(secret
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit()));
    }

    #[test]
    fn verifies_codes_for_new_totp_secrets() {
        let secret = generate_totp_secret();
        let bytes = Secret::Encoded(secret.clone()).to_bytes().unwrap();
        let totp = TOTP::new(Algorithm::SHA1, 6, 1, 30, bytes).unwrap();
        let code = totp.generate_current().unwrap();

        assert!(verify_totp_code(&secret, &code).unwrap());
        assert!(!verify_totp_code(&secret, "12345678").unwrap());
    }

    #[test]
    fn builds_an_authenticator_provisioning_uri() {
        let secret = generate_totp_secret();
        let uri = totp_provisioning_uri("user+test@example.com", &secret).unwrap();
        let parsed = Url::parse(&uri).unwrap();
        let params: std::collections::HashMap<_, _> = parsed.query_pairs().into_owned().collect();

        assert_eq!(parsed.scheme(), "otpauth");
        assert_eq!(parsed.host_str(), Some("totp"));
        assert_eq!(parsed.path(), "/Nezumo:user+test@example.com");
        assert_eq!(params.get("secret"), Some(&secret));
        assert_eq!(params.get("issuer").map(String::as_str), Some("Nezumo"));
        assert_eq!(params.get("algorithm").map(String::as_str), Some("SHA1"));
        assert_eq!(params.get("digits").map(String::as_str), Some("6"));
        assert_eq!(params.get("period").map(String::as_str), Some("30"));
    }

    #[test]
    fn retains_legacy_totp_verification_during_migration() {
        let secret = "12345678";
        let totp = TOTP::new_unchecked(Algorithm::SHA512, 8, 1, 30, secret.as_bytes().to_vec());
        let code = totp.generate_current().unwrap();

        assert!(verify_totp_code(secret, &code).unwrap());
    }
}
