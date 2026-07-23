//! Long-lived board-preview renderer client.
//!
//! Owns a single persistent `nezumo-render --serve` child process (the native
//! wgpu renderer) and feeds it board snapshots one at a time. The daemon builds
//! its GPU context + plugin set and loads fonts/atlases ONCE, then renders every
//! subsequent board cheaply — so we avoid paying process start + GPU init + asset
//! load on every preview (which made one-shot rendering slow).
//!
//! Design: an mpsc actor. Callers (the snapshot job, possibly several at once)
//! send a [`Job`] and await a oneshot reply; the actor serializes them onto the
//! single daemon (which renders one board at a time anyway) and respawns the
//! child if it dies. Communication with the child is line-delimited JSON over
//! stdin/stdout with snapshot/PNG bodies passed via temp files (see
//! `nezumo-render`'s `--serve` mode).

use std::process::Stdio;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

/// Time to wait for the daemon's readiness handshake after spawn — GPU init +
/// the first asset fetch on the software renderer can be slow. The per-render
/// deadline is supplied per request instead (previews are fast, full-res exports
/// can take minutes).
const READY_TIMEOUT: Duration = Duration::from_secs(120);

/// Handle to the preview renderer actor. Cheap to clone-share via a global.
pub struct PreviewService {
    tx: mpsc::Sender<Job>,
}

struct Job {
    snapshot: Value,
    max_px: u32,
    /// Encoded output format: "png" or "jpeg".
    format: String,
    /// Per-render deadline (fast for previews, generous for full-res exports).
    timeout: Duration,
    reply: oneshot::Sender<Result<Vec<u8>, String>>,
}

impl PreviewService {
    /// Start the actor. The daemon child is spawned lazily on the first job and
    /// then kept warm (its GPU context + cached fonts/atlases are reused across
    /// every preview and export). The per-render deadline is supplied per request.
    pub fn start(bin: String, asset_base: String, default_max_px: u32) -> Self {
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(actor(bin, asset_base, default_max_px, rx));
        Self { tx }
    }

    /// Render one board snapshot to PNG bytes. Serialized with all other callers.
    pub async fn render(
        &self,
        snapshot: Value,
        max_px: u32,
        timeout: Duration,
    ) -> Result<Vec<u8>, String> {
        self.render_format(snapshot, max_px, "png", timeout).await
    }

    /// Render one board snapshot to encoded image bytes in `format` ("png" |
    /// "jpeg"). Serialized with all other callers on this service's daemon.
    /// `timeout` is the per-render deadline (short for previews, long for exports).
    pub async fn render_format(
        &self,
        snapshot: Value,
        max_px: u32,
        format: &str,
        timeout: Duration,
    ) -> Result<Vec<u8>, String> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Job {
                snapshot,
                max_px,
                format: format.to_string(),
                timeout,
                reply,
            })
            .await
            .map_err(|_| "preview service stopped".to_string())?;
        rx.await
            .map_err(|_| "preview worker dropped the request".to_string())?
    }
}

/// The actor loop: owns the daemon, processes jobs serially, respawns on death.
async fn actor(bin: String, asset_base: String, default_max_px: u32, mut rx: mpsc::Receiver<Job>) {
    let mut daemon: Option<Daemon> = None;
    let mut counter: u64 = 0;

    while let Some(job) = rx.recv().await {
        counter += 1;

        if daemon.is_none() {
            match Daemon::spawn(&bin, &asset_base, default_max_px).await {
                Ok(d) => {
                    info!("render daemon started ({bin})");
                    daemon = Some(d);
                }
                Err(e) => {
                    let _ = job.reply.send(Err(format!("spawn render daemon: {e}")));
                    continue;
                }
            }
        }

        let d = daemon.as_mut().expect("daemon present");
        let started = std::time::Instant::now();
        info!(
            "render job {counter} started (format={}, max_px={}, timeout_s={})",
            job.format,
            job.max_px,
            job.timeout.as_secs()
        );
        let result = d
            .render(counter, &job.snapshot, job.max_px, &job.format, job.timeout)
            .await;

        // A failed exchange likely means the child died or desynced — drop it so
        // the next job respawns a fresh daemon.
        match &result {
            Ok(bytes) => info!(
                "render job {counter} finished (elapsed_ms={}, bytes={})",
                started.elapsed().as_millis(),
                bytes.len()
            ),
            Err(error) => {
                warn!(
                    "render job {counter} failed after {} ms; daemon will respawn: {error}",
                    started.elapsed().as_millis()
                );
                daemon = None;
            }
        }

        let _ = job.reply.send(result);
    }
}

/// One running `nezumo-render --serve` child + its pipes.
struct Daemon {
    _child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl Daemon {
    async fn spawn(bin: &str, asset_base: &str, max_px: u32) -> Result<Self, String> {
        // The daemon shares our systemd-service cgroup, so a runaway render
        // (pathological board, huge world) can push the cgroup over its memory
        // limit. The kernel OOM-killer then picks a victim by oom_score (~RSS),
        // which can be the MAIN server (large: DB pools, caches) rather than the
        // daemon — and systemd restarts the whole unit. Force the daemon to be
        // the FIRST OOM victim (`oom_score_adj = 1000`): the kernel always kills
        // it instead of the server, freeing the memory, and the actor respawns
        // it. So a render blowup can never take the main process down with it.
        let mut child = {
            #[cfg(target_os = "linux")]
            {
                use std::os::unix::process::CommandExt;
                let mut std_cmd = std::process::Command::new(bin);
                std_cmd
                    .arg("--serve")
                    .arg(asset_base)
                    .arg(max_px.to_string())
                    .env("RUST_LOG", "warn")
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::inherit()); // daemon logs flow to our stderr
                                               // Runs in the forked child before exec: mark it the preferred
                                               // OOM victim. Best-effort — failures here must not abort spawn.
                unsafe {
                    std_cmd.pre_exec(|| {
                        // Kernel sends SIGKILL to this child the instant the parent
                        // (the server) dies — however it dies (crash, SIGKILL, or a
                        // systemd cgroup-kill that fails with "Invalid argument", as
                        // seen in the logs). Without this, a hard-killed server
                        // orphans the long-lived `--serve` daemon and it lingers
                        // holding ~0.5 GB; across restarts these accumulate.
                        libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL as libc::c_ulong);
                        // Mark the daemon the preferred OOM victim so a runaway
                        // render never takes the main server down. Best-effort.
                        use std::io::Write;
                        if let Ok(mut f) = std::fs::OpenOptions::new()
                            .write(true)
                            .open("/proc/self/oom_score_adj")
                        {
                            let _ = f.write_all(b"1000");
                        }
                        Ok(())
                    });
                }
                let mut cmd = Command::from(std_cmd);
                cmd.kill_on_drop(true);
                cmd.spawn().map_err(|e| format!("{e}"))?
            }
            #[cfg(not(target_os = "linux"))]
            {
                Command::new(bin)
                    .arg("--serve")
                    .arg(asset_base)
                    .arg(max_px.to_string())
                    .env("RUST_LOG", "warn")
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::inherit())
                    .kill_on_drop(true)
                    .spawn()
                    .map_err(|e| format!("{e}"))?
            }
        };

        let stdin = child.stdin.take().ok_or("daemon stdin unavailable")?;
        let stdout = BufReader::new(child.stdout.take().ok_or("daemon stdout unavailable")?);
        let mut daemon = Daemon {
            _child: child,
            stdin,
            stdout,
        };

        // First line is the readiness handshake (emitted after GPU init).
        let line = daemon.read_line(READY_TIMEOUT).await?;
        let v: Value =
            serde_json::from_str(line.trim()).map_err(|e| format!("ready line parse: {e}"))?;
        if v.get("ready").and_then(Value::as_bool) != Some(true) {
            return Err(format!("unexpected daemon init line: {}", line.trim()));
        }
        Ok(daemon)
    }

    async fn render(
        &mut self,
        id: u64,
        snapshot: &Value,
        max_px: u32,
        format: &str,
        timeout: Duration,
    ) -> Result<Vec<u8>, String> {
        let dir = std::env::temp_dir();
        let ext = if format.eq_ignore_ascii_case("jpeg") || format.eq_ignore_ascii_case("jpg") {
            "jpg"
        } else {
            "png"
        };
        let in_path = dir.join(format!("nezumo-preview-{id}.json"));
        let out_path = dir.join(format!("nezumo-preview-{id}.{ext}"));

        let body = serde_json::to_vec(snapshot).map_err(|e| format!("serialize snapshot: {e}"))?;
        tokio::fs::write(&in_path, &body)
            .await
            .map_err(|e| format!("write snapshot temp: {e}"))?;

        let req = json!({
            "in": in_path.to_string_lossy(),
            "out": out_path.to_string_lossy(),
            "max_px": max_px,
            "format": if ext == "jpg" { "jpeg" } else { "png" },
        });
        let send = async {
            self.stdin
                .write_all(format!("{req}\n").as_bytes())
                .await
                .map_err(|e| format!("write request: {e}"))?;
            self.stdin
                .flush()
                .await
                .map_err(|e| format!("flush request: {e}"))
        };
        if let Err(e) = send.await {
            let _ = tokio::fs::remove_file(&in_path).await;
            return Err(e);
        }

        let line = self.read_line(timeout).await;
        let _ = tokio::fs::remove_file(&in_path).await;
        let line = line?;

        let resp: Value =
            serde_json::from_str(line.trim()).map_err(|e| format!("response parse: {e}"))?;
        if resp.get("ok").and_then(Value::as_bool) == Some(true) {
            let bytes = tokio::fs::read(&out_path)
                .await
                .map_err(|e| format!("read rendered png: {e}"))?;
            let _ = tokio::fs::remove_file(&out_path).await;
            Ok(bytes)
        } else {
            let _ = tokio::fs::remove_file(&out_path).await;
            Err(resp
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("unknown render error")
                .to_string())
        }
    }

    /// Read one line from the daemon, bounded by `timeout`. EOF (the daemon died)
    /// and timeout both surface as errors so the actor respawns.
    async fn read_line(&mut self, timeout: Duration) -> Result<String, String> {
        let mut line = String::new();
        match tokio::time::timeout(timeout, self.stdout.read_line(&mut line)).await {
            Ok(Ok(0)) => Err("daemon closed stdout (process died)".to_string()),
            Ok(Ok(_)) => Ok(line),
            Ok(Err(e)) => Err(format!("read daemon stdout: {e}")),
            Err(_) => Err("daemon render timed out".to_string()),
        }
    }
}
