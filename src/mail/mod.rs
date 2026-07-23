//! SMTP connection state and outbound message helpers.

pub mod connect;
pub mod send;

use lettre::{AsyncSmtpTransport, Tokio1Executor};

/// Cloneable SMTP transport and configured sender address.
#[derive(Clone, Debug)]
pub struct MailerState {
    pub mailer: AsyncSmtpTransport<Tokio1Executor>,
    pub from: String, // "from" address for outbound mail
}
