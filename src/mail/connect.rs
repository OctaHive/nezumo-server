//! SMTP client construction and startup connectivity validation.

use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use thiserror::Error;

use crate::core::config::{get_env, get_env_with_default};
use crate::mail::MailerState;

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum SmtpError {
    #[error("❌  Environment error: {0}")]
    EnvError(String),
    #[error("❌  Mail connection error: {0}")]
    ConnectionError(String),
    #[error("❌  Mail operation error: {0}")]
    OperationError(String),
}

/// Creates the SMTP transport and validates it with a startup connection test.
pub async fn connect_to_mail() -> Result<MailerState, SmtpError> {
    let smtp_server = get_env("MAIL_SERVER");
    let smtp_port = get_env_with_default("MAIL_PORT", "587");
    let smtp_user = get_env("MAIL_USER");
    let smtp_pass = get_env("MAIL_PASS");
    let mail_from = get_env_with_default("MAIL_FROM", "");
    let mail_security = get_env_with_default("MAIL_SECURITY", "starttls");

    if mail_from.trim().is_empty() {
        return Err(SmtpError::EnvError(
            "MAIL_FROM is required and must be a valid email address.".into(),
        ));
    }

    let port = smtp_port.parse().unwrap_or(587);
    let creds = if smtp_user.trim().is_empty() && smtp_pass.trim().is_empty() {
        None
    } else {
        Some(Credentials::new(smtp_user.clone(), smtp_pass))
    };

    let mailer = match mail_security.trim().to_lowercase().as_str() {
        "none" => {
            let builder =
                AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&smtp_server).port(port);
            let builder = if let Some(creds) = creds {
                builder.credentials(creds)
            } else {
                builder
            };
            builder.build()
        }
        "starttls" => {
            let builder = AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&smtp_server)
                .map_err(|e| SmtpError::ConnectionError(e.to_string()))?
                .port(port);
            let builder = if let Some(creds) = creds {
                builder.credentials(creds)
            } else {
                builder
            };
            builder.build()
        }
        "implicit" => {
            let builder = AsyncSmtpTransport::<Tokio1Executor>::relay(&smtp_server)
                .map_err(|e| SmtpError::ConnectionError(e.to_string()))?
                .port(port);
            let builder = if let Some(creds) = creds {
                builder.credentials(creds)
            } else {
                builder
            };
            builder.build()
        }
        _ => {
            return Err(SmtpError::EnvError(
                "MAIL_SECURITY must be one of: none, starttls, implicit.".into(),
            ));
        }
    };

    // Send a test email to the `MAIL_USER` address
    let test_email = Message::builder()
        .from(mail_from.parse::<lettre::message::Mailbox>().map_err(|e| SmtpError::OperationError(e.to_string()))?)
        .to(mail_from.parse::<lettre::message::Mailbox>().map_err(|e| SmtpError::OperationError(e.to_string()))?)
        .subject("SMTP Test")
        .body("This is a test email sent from the Nezumo SMTP connection validation. Nezumo will sent a verification mail during each startup to test if email is working properly. You can ignore this mail.".to_string())
        .map_err(|e| SmtpError::OperationError(e.to_string()))?;

    // Send the test email and validate the connection
    match mailer.send(test_email).await {
        Ok(_) => Ok(MailerState {
            mailer,
            from: mail_from,
        }),
        Err(e) => Err(SmtpError::OperationError(format!(
            "Failed to send test email: {}",
            e
        ))),
    }
}
