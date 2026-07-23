//! Server-side board preview generation.
//!
//! Renders a board snapshot to a thumbnail image via a long-lived
//! `nezumo-render --serve` daemon (native wgpu offscreen renderer), uploads
//! the result to S3, and records the object key on the board. Decoupled from the
//! API server process on purpose — the renderer needs a GPU (or a software
//! Vulkan/GL stack like lavapipe), so it runs as a separate executable that can
//! live in its own GPU-equipped container. The daemon (see
//! [`super::preview_service`]) loads its GPU + fonts/atlases once and renders
//! every board cheaply thereafter.
//!
//! A periodic job refreshes stale thumbnails from the authoritative canonical
//! state; import handlers can also enqueue an immediate first render. Every
//! failure here is non-fatal and only logged — a missing renderer or GPU must
//! never break board persistence.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::OnceCell;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, warn};
use uuid::Uuid;

use crate::core::config::{get_env_u64, get_env_with_default};
use crate::jobs::preview_service::PreviewService;
use crate::routes::AppState;
use crate::storage::presign_url::{refresh_pdf_page_presigned_urls, refresh_state_presigned_urls};
use crate::storage::upload::upload_to_storage;

/// Default minimum time between thumbnail renders for one board.
const DEFAULT_PREVIEW_INTERVAL_SECS: u64 = 3600;

/// Process-wide render daemon, shared by throttled thumbnail previews AND
/// on-demand full-resolution exports. A single warm `nezumo-render --serve`
/// process: its GPU context and cached fonts/atlases are reused across every
/// render, so an export never re-fetches assets. The per-render deadline and
/// `max_px` are supplied per request. Started on first use from env config.
static RENDER_SERVICE: OnceCell<PreviewService> = OnceCell::const_new();

async fn render_service() -> &'static PreviewService {
    RENDER_SERVICE
        .get_or_init(|| async {
            let bin = get_env_with_default("PREVIEW_RENDERER_BIN", "nezumo-render");
            let asset_base = get_env_with_default("ASSET_BASE_URL", "");
            let max_px = get_env_u64("PREVIEW_MAX_PX", 512) as u32;
            PreviewService::start(bin, asset_base, max_px)
        })
        .await
}

/// Per-render deadline for a background thumbnail preview (settles fast).
const PREVIEW_RENDER_TIMEOUT: Duration = Duration::from_secs(60);

/// Starts periodic thumbnail refreshes independently of canonical checkpoint
/// compaction. A board is selected only when its canonical state changed after
/// the last successful preview and the per-board throttle has elapsed.
pub fn start_preview_job(state: Arc<AppState>, scan_interval_secs: u64, batch_limit: i64) {
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(scan_interval_secs.max(5)));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            if let Err(error) = run_preview_cycle(&state, batch_limit.clamp(1, 100)).await {
                warn!("preview scan failed: {error}");
            }
        }
    });
}

async fn run_preview_cycle(state: &Arc<AppState>, batch_limit: i64) -> Result<(), sqlx::Error> {
    let interval_secs = preview_interval_secs();
    let board_ids = list_previews_due(&state.database, interval_secs, batch_limit).await?;
    for board_id in board_ids {
        // Recheck after selection so another replica's successful render does
        // not immediately trigger a duplicate render on this process.
        if !is_preview_due(&state.database, board_id).await {
            continue;
        }
        let state_value = match state
            .coordinators
            .current_canonical_state(&state.database, board_id)
            .await
        {
            Ok((state_value, _)) => state_value,
            Err(error) => {
                warn!("preview state load failed for board {board_id}: {error}");
                continue;
            }
        };
        generate_and_store(state.clone(), board_id, state_value).await;
    }
    Ok(())
}

fn preview_interval_secs() -> i64 {
    get_env_u64("PREVIEW_INTERVAL_SECS", DEFAULT_PREVIEW_INTERVAL_SECS).min(i64::MAX as u64) as i64
}

/// Boards without a preview are immediately due. Existing previews are due only
/// when canonical content changed after the last render and the throttle has
/// elapsed. A future timestamp is treated as due to recover from clock drift.
const PREVIEW_DUE_FILTER: &str = r#"
    c.abandoned_at IS NULL
    AND (h.board_id IS NULL OR h.state <> 'quarantined')
    AND (
        b.preview_generated_at IS NULL
        OR b.preview_generated_at > NOW()
        OR (
            GREATEST(c.updated_at, COALESCE(h.updated_at, c.updated_at))
                > b.preview_generated_at
            AND b.preview_generated_at
                <= NOW() - ($1 * INTERVAL '1 second')
        )
    )
"#;

async fn list_previews_due(
    pool: &sqlx::PgPool,
    interval_secs: i64,
    limit: i64,
) -> Result<Vec<Uuid>, sqlx::Error> {
    let query = format!(
        "SELECT b.id \
         FROM boards b \
         JOIN board_yrs_canonical_bases c ON c.board_id = b.id \
         LEFT JOIN board_yrs_heads h ON h.board_id = b.id \
         WHERE {PREVIEW_DUE_FILTER} \
         ORDER BY b.preview_generated_at ASC NULLS FIRST, \
                  GREATEST(c.updated_at, COALESCE(h.updated_at, c.updated_at)) ASC \
         LIMIT $2"
    );
    sqlx::query_scalar::<_, Uuid>(&query)
        .bind(interval_secs)
        .bind(limit)
        .fetch_all(pool)
        .await
}

/// Returns whether a board thumbnail should be regenerated now.
pub async fn is_preview_due(pool: &sqlx::PgPool, board_id: Uuid) -> bool {
    let query = format!(
        "SELECT EXISTS ( \
             SELECT 1 \
             FROM boards b \
             JOIN board_yrs_canonical_bases c ON c.board_id = b.id \
             LEFT JOIN board_yrs_heads h ON h.board_id = b.id \
             WHERE {PREVIEW_DUE_FILTER} AND b.id = $2 \
         )"
    );
    match sqlx::query_scalar::<_, bool>(&query)
        .bind(preview_interval_secs())
        .bind(board_id)
        .fetch_one(pool)
        .await
    {
        Ok(due) => due,
        Err(error) => {
            warn!("preview due-check failed for board {board_id}: {error}");
            false
        }
    }
}

/// Hard ceiling on an export's longest side, matching the renderer's `MAX_SIDE`
/// (16384). Requests are clamped to this so a runaway resolution can't OOM.
pub const EXPORT_MAX_PX_CEILING: u32 = 16384;

/// Default longest side for an export when the request omits `max_px`.
/// A 16K RGBA readback needs roughly one GiB before encoding and can stall a
/// software Vulkan adapter, so the ordinary path defaults to 4K while keeping
/// the explicit 16K option available.
pub fn export_default_max_px() -> u32 {
    get_env_u64("EXPORT_DEFAULT_MAX_PX", 4096).clamp(1, EXPORT_MAX_PX_CEILING as u64) as u32
}

/// Makes every asset URL in a transient renderer snapshot independently
/// fetchable. Ordinary media carries object-key siblings; PDF pages use their
/// deterministic board/doc/page storage key instead.
async fn refresh_render_asset_urls(
    state: &Arc<AppState>,
    board_id: Uuid,
    state_value: &mut serde_json::Value,
) {
    refresh_state_presigned_urls(&state.storage, state_value).await;
    refresh_pdf_page_presigned_urls(&state.storage, board_id, state_value).await;
}

/// Render a board's latest saved state to an encoded image ("png" | "jpeg") at up
/// to `max_px` longest side, on the dedicated export daemon. Media URLs in the
/// stored snapshot have long-expired presigned TTLs, so re-presign first (same as
/// the preview path). Returns the encoded image bytes.
pub async fn render_board_image(
    state: &Arc<AppState>,
    board_id: Uuid,
    max_px: u32,
    format: &str,
) -> Result<Vec<u8>, String> {
    // Current state = latest snapshot + events since it (what a client sees),
    // NOT the last periodic snapshot alone — otherwise a download loses edits
    // made in the minutes since the last snapshot.
    let (mut state_value, _) = state
        .coordinators
        .current_canonical_state(&state.database, board_id)
        .await
        .map_err(|e| format!("compute canonical current state: {e}"))?;
    refresh_render_asset_urls(state, board_id, &mut state_value).await;
    let max_px = max_px.clamp(1, EXPORT_MAX_PX_CEILING);
    // Full-res exports on software wgpu can take minutes → generous per-render
    // deadline (env EXPORT_RENDER_TIMEOUT_SECS, default 300s).
    let timeout = Duration::from_secs(get_env_u64("EXPORT_RENDER_TIMEOUT_SECS", 300));
    render_service()
        .await
        .render_format(state_value, max_px, format, timeout)
        .await
}

/// Render a board's latest saved state to a self-contained SVG string.
///
/// Unlike the raster path (a warm `--serve` daemon), SVG is produced by a
/// one-shot `nezumo-render --svg` process: it builds the world, runs the
/// systems, walks the ECS and emits vector geometry + editable text with
/// `@font-face`-embedded fonts. `ASSET_BASE_URL` (arg 3) lets text lay out with
/// real font metrics during the settle pass; `RENDER_FONTS_DIR` (arg 5, a local
/// dir of `{variant}.woff2/.ttf/.otf` + `fonts.json`) supplies the embed bytes —
/// omitted ⇒ family-name fallback. Media URLs are re-presigned first, same as the
/// raster path. Returns the SVG document text.
pub async fn render_board_svg(state: &Arc<AppState>, board_id: Uuid) -> Result<String, String> {
    // Current state = latest snapshot + events since it (matches the raster path
    // and the live client), so an SVG export never loses recent edits.
    let (mut state_value, _) = state
        .coordinators
        .current_canonical_state(&state.database, board_id)
        .await
        .map_err(|e| format!("compute canonical current state: {e}"))?;
    refresh_render_asset_urls(state, board_id, &mut state_value).await;

    let bin = get_env_with_default("PREVIEW_RENDERER_BIN", "nezumo-render");
    let asset_base = get_env_with_default("ASSET_BASE_URL", "");
    let fonts_dir = get_env_with_default("RENDER_FONTS_DIR", "");
    let timeout = Duration::from_secs(get_env_u64("EXPORT_RENDER_TIMEOUT_SECS", 300));

    // The one-shot CLI reads the snapshot from a file and writes SVG to stdout.
    let tmp = std::env::temp_dir().join(format!("nezumo-svg-{}.json", Uuid::new_v4()));
    let json = serde_json::to_vec(&state_value).map_err(|e| format!("serialize state: {e}"))?;
    tokio::fs::write(&tmp, &json)
        .await
        .map_err(|e| format!("write temp snapshot: {e}"))?;

    let mut cmd = tokio::process::Command::new(&bin);
    cmd.arg("--svg").arg(&tmp).arg(&asset_base).arg("-");
    if !fonts_dir.is_empty() {
        cmd.arg(&fonts_dir);
    }
    cmd.env("RUST_LOG", "warn");
    // A timed-out SVG/PDF export must not leave an orphaned renderer consuming
    // CPU/GPU and blocking later exports from the same service cgroup.
    cmd.kill_on_drop(true);

    let result = tokio::time::timeout(timeout, cmd.output()).await;
    let _ = tokio::fs::remove_file(&tmp).await;

    let output = result
        .map_err(|_| "svg render timed out".to_string())?
        .map_err(|e| format!("spawn {bin}: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "renderer exited {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    String::from_utf8(output.stdout).map_err(|e| format!("svg not utf8: {e}"))
}

/// Render a board to a self-contained **vector PDF**.
///
/// Reuses the SVG export (current state, embedded fonts + inlined images) and
/// converts SVG → PDF with the pure-Rust `svg2pdf` — no extra sidecar, no system
/// libraries. Text is emitted as vector outlines (visually exact, not
/// selectable). `usvg` lays text out from its own font database (it can't use the
/// SVG's embedded subset-woff2 `@font-face`), so we load real OTF/TTF into it — see
/// [`svg_to_pdf`]. The convert is CPU-bound, so it runs on a blocking thread.
pub async fn render_board_pdf(state: &Arc<AppState>, board_id: Uuid) -> Result<Vec<u8>, String> {
    let svg = render_board_svg(state, board_id).await?;
    if svg.trim().is_empty() {
        return Ok(Vec::new());
    }
    tokio::task::spawn_blocking(move || svg_to_pdf(&svg))
        .await
        .map_err(|e| format!("pdf convert task: {e}"))?
}

/// Directory of real OTF/TTF font files (+ `fonts.json`) that `svg_to_pdf` loads
/// into usvg. usvg can't decode the SVG's embedded subset-woff2 `@font-face`, so
/// it needs actual font files here or text falls back to a serif.
///
/// `RENDER_FONTS_DIR` (the same dir the SVG embed uses) overrides; otherwise the
/// complete set shipped as `./fonts` next to the binary (`backend/fonts`, all 11
/// families as OTF/TTF). To render every family, `RENDER_FONTS_DIR` must itself
/// hold the full OTF/TTF set — so if unsure, leave it empty and rely on `./fonts`.
fn pdf_fonts_dir() -> String {
    let configured = get_env_with_default("RENDER_FONTS_DIR", "");
    if !configured.is_empty() && std::path::Path::new(&configured).is_dir() {
        return configured;
    }
    "fonts".to_string()
}

/// Convert a self-contained SVG document to PDF bytes via `svg2pdf`.
///
/// usvg ignores the SVG's embedded `@font-face` (our subset woff2 don't decode
/// through its shaper), so it shapes text purely from its `fontdb`. Two things are
/// needed for text to survive the SVG→PDF conversion:
///  1. real OTF/TTF loaded into `fontdb` (from [`pdf_fonts_dir`]); and
///  2. the SVG's `font-family` values — which are lowercase Nezumo font *ids*
///     (`roboto`, `noto-sans`) — rewritten to the OTF's real family *name*
///     (`Roboto`, `Noto Sans`), because fontdb's family query is case-sensitive
///     and id≠name. The id→name map comes from `fonts.json` in the fonts dir.
/// Without both, usvg drops the text or falls back to a serif system font.
fn svg_to_pdf(svg: &str) -> Result<Vec<u8>, String> {
    let fonts_dir = pdf_fonts_dir();
    let svg = inline_svg_pages(svg);
    let svg = rewrite_font_families(&svg, &fonts_dir);

    let mut options = svg2pdf::usvg::Options::default();
    let loaded = {
        let db = options.fontdb_mut();
        db.load_fonts_dir(&fonts_dir);
        db.load_system_fonts(); // last-resort fallback for anything unmapped
        db.len()
    };
    if loaded == 0 {
        warn!("pdf export: no fonts loaded (dir={fonts_dir}) — text may render as fallback");
    }
    let tree =
        svg2pdf::usvg::Tree::from_str(&svg, &options).map_err(|e| format!("usvg parse: {e}"))?;
    svg2pdf::to_pdf(
        &tree,
        svg2pdf::ConversionOptions::default(),
        svg2pdf::PageOptions::default(),
    )
    .map_err(|e| format!("svg2pdf convert: {e:?}"))
}

/// Flatten embedded PDF pages so their raster content survives SVG→PDF.
///
/// The SVG export inlines each PDF page as `<image href="data:image/svg+xml…">`
/// (a nested SVG: vector glyph outlines PLUS embedded raster `<image>` — scanned
/// figures, logos). usvg renders the nested SVG's *vector* content but **drops
/// raster `<image>` nested inside a `data:image/svg+xml`** (the outer vector text
/// shows, the inner bitmaps vanish). Replacing that `<image>` with an inline
/// `<svg>` element carrying the same x/y/width/height (and the page's `viewBox`)
/// makes usvg treat the page as a real sub-document, so its rasters render too.
/// Only the PDF-page `data:image/svg+xml` images are touched — raster `<image>`
/// (png/jpeg) already render at the top level and are left alone.
fn inline_svg_pages(svg: &str) -> String {
    use base64::Engine;
    // Matches emit_pdf's exact output: xlink:href, then x/y/width/height,
    // preserveAspectRatio, self-closing. Anchored to `data:image/svg+xml` so
    // raster images (png/jpeg) never match.
    static IMG_RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let img_re = IMG_RE.get_or_init(|| {
        regex::Regex::new(
            r#"<image\s+xlink:href="data:image/svg\+xml;base64,([A-Za-z0-9+/=]+)"\s+x="([^"]*)"\s+y="([^"]*)"\s+width="([^"]*)"\s+height="([^"]*)"\s+preserveAspectRatio="[^"]*"\s*/>"#,
        )
        .expect("valid image regex")
    });
    static VB_RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let vb_re = VB_RE.get_or_init(|| regex::Regex::new(r#"viewBox="([^"]*)""#).unwrap());
    static WH_RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let wh_re = WH_RE.get_or_init(|| regex::Regex::new(r#"(?:width|height)="([0-9.]+)"#).unwrap());
    static ROOT_RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let root_re = ROOT_RE.get_or_init(|| regex::Regex::new(r"(?s)^.*?<svg\b[^>]*>").unwrap());

    let mut count = 0usize;
    let out = img_re.replace_all(svg, |c: &regex::Captures| {
        let decoded = match base64::engine::general_purpose::STANDARD.decode(&c[1]) {
            Ok(b) => b,
            Err(_) => return c[0].to_string(), // leave malformed data URIs untouched
        };
        let inner = String::from_utf8_lossy(&decoded);
        // The nested `<svg>` needs a viewBox to scale into the target rect. Prefer
        // the page's own; else synthesise one from its width/height numbers.
        let view_box = vb_re
            .captures(&inner)
            .map(|m| m[1].to_string())
            .unwrap_or_else(|| {
                let mut nums = wh_re
                    .captures_iter(&inner)
                    .filter_map(|m| m[1].parse::<f64>().ok());
                let w = nums.next().unwrap_or(0.0);
                let h = nums.next().unwrap_or(0.0);
                format!("0 0 {w} {h}")
            });
        // Drop the page's own root `<svg …>` tag, keep its children (+ closing tag).
        let body = root_re.replace(&inner, "");
        count += 1;
        format!(
            r#"<svg x="{}" y="{}" width="{}" height="{}" viewBox="{}" preserveAspectRatio="none" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">{}"#,
            &c[2], &c[3], &c[4], &c[5], view_box, body
        )
    });
    if count > 0 {
        info!("pdf export: inlined {count} PDF page(s) for raster fidelity");
    }
    out.into_owned()
}

/// Rewrite the SVG's `font-family` values from Nezumo font *ids* (lowercase,
/// e.g. `roboto`, `noto-sans`) to the OTF's real family *name* (`Roboto`,
/// `Noto Sans`) so usvg's case-sensitive fontdb query matches a loaded face. The
/// id→name map is read from `{fonts_dir}/fonts.json`; if that's unreadable we fall
/// back to a small built-in map. Rewrites both the `<text font-family="id">`
/// attribute and the `@font-face { font-family:'id' }` CSS forms. Ids that are
/// substrings of others (`roboto` ⊂ `roboto-condensed`) don't collide because the
/// closing quote bounds each match.
fn rewrite_font_families(svg: &str, fonts_dir: &str) -> String {
    let map = font_id_name_map(fonts_dir);
    let mut out = svg.to_string();
    for (id, name) in &map {
        if id == name {
            continue;
        }
        for quote in ['"', '\''] {
            for prefix in ["font-family=", "font-family:"] {
                let from = format!("{prefix}{quote}{id}{quote}");
                if out.contains(&from) {
                    let to = format!("{prefix}{quote}{name}{quote}");
                    out = out.replace(&from, &to);
                }
            }
        }
    }
    out
}

/// Build the Nezumo font-id → family-name map from `{fonts_dir}/fonts.json`
/// (`[{ "id": "noto-sans", "name": "Noto Sans", .. }]`). Falls back to the core
/// families if the file is missing/malformed, so PDF text still resolves.
fn font_id_name_map(fonts_dir: &str) -> std::collections::HashMap<String, String> {
    let path = std::path::Path::new(fonts_dir).join("fonts.json");
    if let Ok(text) = std::fs::read_to_string(&path) {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
            if let Some(arr) = json["fonts"].as_array() {
                let map: std::collections::HashMap<String, String> = arr
                    .iter()
                    .filter_map(|f| {
                        Some((
                            f["id"].as_str()?.to_string(),
                            f["name"].as_str()?.to_string(),
                        ))
                    })
                    .collect();
                if !map.is_empty() {
                    return map;
                }
            }
        }
    }
    warn!("pdf export: fonts.json not found in {fonts_dir}; using built-in font map");
    [
        ("roboto", "Roboto"),
        ("noto-sans", "Noto Sans"),
        ("times", "Times New Roman"),
        ("source-code-pro", "Source Code Pro"),
        ("roboto-condensed", "Roboto Condensed"),
        ("inter", "Inter"),
        ("open-sans", "Open Sans"),
        ("montserrat", "Montserrat"),
        ("oswald", "Oswald"),
        ("shantell-sans", "Shantell Sans"),
        ("comic-sans-ms", "Comic Sans MS"),
    ]
    .iter()
    .map(|(id, name)| (id.to_string(), name.to_string()))
    .collect()
}

/// Render `state_value` to a thumbnail via the preview daemon, upload it to S3,
/// and record the object key. Safe for the periodic job or a detached import
/// task; all errors are logged and swallowed.
pub async fn generate_and_store(
    state: Arc<AppState>,
    board_id: Uuid,
    mut state_value: serde_json::Value,
) {
    // The snapshot bakes in presigned media URLs whose TTL (default 1h) has long
    // expired by the time this preview runs, so the renderer's fetches fail.
    // Re-presign from the stored object keys before rendering — the server-side
    // counterpart of the frontend's `refreshPresignedUrls`.
    refresh_render_asset_urls(&state, board_id, &mut state_value).await;

    let max_px = get_env_u64("PREVIEW_MAX_PX", 512) as u32;
    let png = match render_service()
        .await
        .render(state_value, max_px, PREVIEW_RENDER_TIMEOUT)
        .await
    {
        Ok(png) if !png.is_empty() => png,
        Ok(_) => {
            warn!("preview render produced no bytes for board {}", board_id);
            return;
        }
        Err(err) => {
            warn!("preview render failed for board {}: {}", board_id, err);
            return;
        }
    };

    let bucket = get_env_with_default("STORAGE_BUCKET_BOARD_FILES", "board-files");
    let object_key = format!("boards/{}/preview.png", board_id);
    if let Err(err) = upload_to_storage(&state.storage, &bucket, &object_key, &png).await {
        warn!("preview upload failed for board {}: {}", board_id, err);
        return;
    }

    let updated = sqlx::query(
        "UPDATE boards SET preview_object_key = $1, preview_generated_at = NOW() WHERE id = $2",
    )
    .bind(&object_key)
    .bind(board_id)
    .execute(&state.database)
    .await;
    match updated {
        Ok(_) => info!(
            "preview generated for board {} ({} bytes)",
            board_id,
            png.len()
        ),
        Err(err) => warn!("preview db update failed for board {}: {:?}", board_id, err),
    }
}
