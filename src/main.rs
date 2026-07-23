//! Nezumo backend process entry point and allocator/runtime initialization.

// jemalloc replaces the default glibc malloc on non-MSVC targets. glibc malloc
// held freed pages after our load-test bursts, leaving RSS pinned ~2.3 GB at
// idle; jemalloc's background thread (enabled at startup in `main`) purges dirty
// pages back to the OS on a decay schedule, so RSS tracks the live working set.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[allow(dead_code)]
// Core modules for the configuration, TLS setup, and server creation
mod core;
use core::{config, server};

// Other modules for database, routes, models, and middlewares
mod cache;
mod database;
mod handlers;
mod jobs;
mod mail;
mod middlewares;
mod models;
mod realtime;
mod referencedata;
mod routes;
mod state;
mod storage;
mod utils;
mod wrappers;

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::signal;

use tracing::error;
use tracing_subscriber;

use axum_server::tls_rustls::RustlsConfig;

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("❌ Failed to install Ctrl+C handler.");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    println!("\n⏳  Shutdown signal received, starting graceful shutdown.");
}

fn display_additional_info(protocol: &str, ip: IpAddr, port: u16) {
    println!("\n📖  Explore the API using Swagger ({protocol}://{ip}:{port}/docs)\n    or import the OpenAPI spec ({protocol}://{ip}:{port}/openapi.json).");
    println!("\n🩺  Ensure your Docker setup is reliable,\n    by pointing its healthcheck to {protocol}://{ip}:{port}/health");
    println!("\nPress [CTRL] + [C] to gracefully shutdown.");
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok(); // Load environment variables from a .env file

    tracing_subscriber::fmt::init(); // Initialize the logging system

    // Turn on jemalloc's background purge thread. It's OFF by default, which means
    // an idle process would never hand freed pages back to the OS; with it on,
    // dirty pages decay back on a timer so RSS follows the live working set
    // instead of the load-burst high-water mark.
    #[cfg(not(target_env = "msvc"))]
    match tikv_jemalloc_ctl::background_thread::write(true) {
        Ok(()) => tracing::info!("jemalloc background_thread enabled"),
        Err(e) => tracing::warn!("jemalloc background_thread enable failed: {e}"),
    }

    println!(
        "{}",
        r#"
        nnnn  nnnnnnnn        eeeeeeeeeeee    zzzzzzzzzzzzzzzzzuuuuuu    uuuuuu     mmmmmmm    mmmmmmm      ooooooooooo
        n:::nn::::::::nn    ee::::::::::::ee  z:::::::::::::::zu::::u    u::::u   mm:::::::m  m:::::::mm  oo:::::::::::oo
        n::::::::::::::nn  e::::::eeeee:::::eez::::::::::::::z u::::u    u::::u  m::::::::::mm::::::::::mo:::::::::::::::o
        nn:::::::::::::::ne::::::e     e:::::ezzzzzzzz::::::z  u::::u    u::::u  m::::::::::::::::::::::mo:::::ooooo:::::o
          n:::::nnnn:::::ne:::::::eeeee::::::e      z::::::z   u::::u    u::::u  m:::::mmm::::::mmm:::::mo::::o     o::::o
          n::::n    n::::ne:::::::::::::::::e      z::::::z    u::::u    u::::u  m::::m   m::::m   m::::mo::::o     o::::o
          n::::n    n::::ne::::::eeeeeeeeeee      z::::::z     u::::u    u::::u  m::::m   m::::m   m::::mo::::o     o::::o
          n::::n    n::::ne:::::::e              z::::::z      u:::::uuuu:::::u  m::::m   m::::m   m::::mo::::o     o::::o
          n::::n    n::::ne::::::::e            z::::::zzzzzzzzu:::::::::::::::uum::::m   m::::m   m::::mo:::::ooooo:::::o
          n::::n    n::::n e::::::::eeeeeeee   z::::::::::::::z u:::::::::::::::um::::m   m::::m   m::::mo:::::::::::::::o
          n::::n    n::::n  ee:::::::::::::e  z:::::::::::::::z  uu::::::::uu:::um::::m   m::::m   m::::m oo:::::::::::oo
          nnnnnn    nnnnnn    eeeeeeeeeeeeee  zzzzzzzzzzzzzzzzz    uuuuuuuu  uuuummmmmm   mmmmmm   mmmmmm   ooooooooooo
              - GitHub: https://github.com/OctaHive/nezumo-server
              - Version: 1.0
    "#
    );

    println!("🦖  Starting Nezumo...");

    let ip: IpAddr = config::get_env_with_default("SERVER_IP", "127.0.0.1")
        .parse()
        .expect("❌  Invalid IP address format.");
    let port: u16 = config::get_env_u16("SERVER_PORT", 3000);
    let addr = SocketAddr::new(ip, port);
    let app = server::create_server().await;

    let is_https = config::get_env_bool("SERVER_HTTPS_ENABLED", false);
    let is_http2 = config::get_env_bool("SERVER_HTTPS_HTTP2_ENABLED", false);
    let protocol = if is_https { "https" } else { "http" };

    if is_https {
        // HTTPS

        // Ensure that the crypto provider is initialized before using rustls
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .unwrap_or_else(|e| {
                error!("❌ Crypto provider initialization failed: {:?}", e);
                std::process::exit(1);
            });

        // Get certificate and key file paths from environment variables
        let cert_path = config::get_env("SERVER_HTTPS_CERT_FILE_PATH");
        let key_path = config::get_env("SERVER_HTTPS_KEY_FILE_PATH");

        // Set up Rustls config with HTTP/2 support
        let (certs, key) = {
            // Load certificate chain
            let certs = tokio::fs::read(&cert_path).await.unwrap_or_else(|e| {
                error!("❌  Failed to read certificate file: {}", e);
                std::process::exit(1);
            });

            // Load private key
            let key = tokio::fs::read(&key_path).await.unwrap_or_else(|e| {
                error!("❌  Failed to read key file: {}", e);
                std::process::exit(1);
            });

            // Parse certificates and private key
            let certs = rustls_pemfile::certs(&mut &*certs)
                .collect::<Result<Vec<_>, _>>()
                .unwrap_or_else(|e| {
                    error!("❌  Failed to parse certificates: {}", e);
                    std::process::exit(1);
                });

            // Try PKCS#8 first, then fall back to PKCS#1 (RSA)
            let key = {
                let pkcs8_keys: Vec<_> = rustls_pemfile::pkcs8_private_keys(&mut &*key)
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap_or_default();

                if let Some(k) = pkcs8_keys.into_iter().next() {
                    rustls::pki_types::PrivateKeyDer::Pkcs8(k)
                } else {
                    let rsa_keys: Vec<_> = rustls_pemfile::rsa_private_keys(&mut &*key)
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap_or_else(|e| {
                            error!(
                                "❌  Failed to parse private key (tried PKCS#8 and PKCS#1): {}",
                                e
                            );
                            std::process::exit(1);
                        });
                    let k = rsa_keys.into_iter().next().unwrap_or_else(|| {
                        error!("❌  No private key found in file (tried PKCS#8 and PKCS#1)");
                        std::process::exit(1);
                    });
                    rustls::pki_types::PrivateKeyDer::Pkcs1(k)
                }
            };

            (certs, key)
        };

        let mut config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap_or_else(|e| {
                error!("❌  Failed to build TLS configuration: {}", e);
                std::process::exit(1);
            });

        if is_http2 {
            config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        }

        let rustls_config = RustlsConfig::from_config(Arc::new(config));

        println!("🔒  Server started with HTTPS at: {protocol}://{ip}:{port}");

        display_additional_info(protocol, ip, port);

        // Create the server future but don't await it yet
        let server = axum_server::bind_rustls(addr, rustls_config).serve(app.into_make_service());

        tokio::select! {
            result = server => {
                if let Err(e) = result {
                    error!("❌  Server failed to start with HTTPS: {}", e);
                }
            },
            _ = shutdown_signal() => {},
        }
    } else {
        // HTTP

        println!(
            "🔓  Server started with HTTP at: {}://{}:{}",
            protocol, ip, port
        );

        display_additional_info(protocol, ip, port);

        // Create the server future but don't await it yet
        let server = axum_server::bind(addr).serve(app.into_make_service());

        tokio::select! {
            result = server => {
                if let Err(e) = result {
                    error!("❌  Server failed to start with HTTP: {}", e);
                }
            },
            _ = shutdown_signal() => {},
        }
    }
    println!("\n✔️   Server has shut down gracefully.");
}
