//! Backend-side client for the independent isolated validator pool.
//!
//! This is an independent validation boundary: before the coordinator commits a
//! canonical `yupdate`, it ships the exact `{base_update, base_state_vector,
//! candidate}` to a POOL of `nezumo-render --validate` subprocess workers,
//! which re-decode/project via the RENDERER-owned `yrs_map` (a second, decoupled
//! implementation) and answer accept/reject. A reject → retryable `nack`, and
//! neither the event nor the Yrs update is committed.
//!
//! The backend depends only on the ability to spawn the renderer
//! binary and speak a length-prefixed BINARY protocol over its stdin/stdout — it
//! never links the renderer or client crates. The two Yrs mappings meet only
//! through this wire protocol and their shared conformance fixtures.
//!
//! Protocol (mirrors `crates/native-render/src/main.rs`, all ints big-endian):
//!   frame        := u32 body_len, body
//!   request body := u8 ver(=2), u64 seq, u64 base_seq, u64 base_generation,
//!                   u64 writer_client_id, blob base_update, blob base_sv,
//!                   blob candidate, blob expected_projection_json
//!   blob         := u32 len, bytes
//!   ready reply  := u8 ver(=2), u8 tag(=0)
//!   result reply := u8 ver(=2), u8 tag(=1), u64 seq, u8 ok, blob error_utf8

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::core::config::get_env_with_default;

/// Binary protocol version understood by both backend and renderer.
const PROTO_VERSION: u8 = 2;
/// Maximum accepted frame body, protecting the server from unbounded allocation.
const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
/// Maximum time allowed for a newly spawned renderer to announce readiness.
const READY_TIMEOUT: Duration = Duration::from_secs(30);

/// Exact input required by the validator. A state vector alone is insufficient;
/// the full base update is required to apply the
/// incremental candidate.
#[derive(Debug, Clone)]
pub struct ValidateRequest {
    /// Correlation sequence echoed by the renderer in its response.
    pub seq: u64,
    /// Durable event sequence represented by `base_update`.
    pub base_seq: u64,
    /// Writer lineage that owns the supplied base and candidate.
    pub base_generation: u64,
    /// The originating writer's client id (actor context; band-checked/logged).
    pub writer_client_id: u64,
    /// Full canonical document update used as the validation starting point.
    pub base_update: Vec<u8>,
    /// State vector paired with `base_update` for integrity verification.
    pub base_state_vector: Vec<u8>,
    /// Incremental client-authored Yrs update being considered for commit.
    pub candidate: Vec<u8>,
    /// Canonical JSON projection expected after applying the paired payload.
    /// The isolated renderer normalizes both sides before comparing them.
    pub expected_projection: Vec<u8>,
}

/// One queued validation request and the channel used to return its result.
struct Job {
    /// Request transferred to an available renderer worker.
    req: ValidateRequest,
    /// Single-use response channel owned by the waiting coordinator call.
    reply: oneshot::Sender<Result<(), String>>,
}

/// A pool of persistent validator workers sharing one job queue. Cloneable
/// handle (just the sender).
#[derive(Clone)]
pub struct ValidatorPool {
    /// Bounded shared queue; backpressure prevents unlimited pending validations.
    tx: mpsc::Sender<Job>,
}

impl ValidatorPool {
    /// Spawn `workers` persistent validator worker tasks. `bin` is the renderer
    /// executable (`PREVIEW_RENDERER_BIN`), `per_request_timeout` bounds a single
    /// validation (a worker that overruns is killed and respawned).
    pub fn spawn(bin: String, workers: usize, per_request_timeout: Duration) -> Self {
        let (tx, rx) = mpsc::channel::<Job>(256);
        let rx = Arc::new(Mutex::new(rx));
        let workers = workers.max(1);
        for i in 0..workers {
            let rx = rx.clone();
            let bin = bin.clone();
            tokio::spawn(worker_loop(i, bin, rx, per_request_timeout));
        }
        Self { tx }
    }

    /// Builds a pool from `YRS_VALIDATOR_WORKERS`,
    /// `YRS_VALIDATOR_TIMEOUT_MS`, and `PREVIEW_RENDERER_BIN`.
    pub fn from_env() -> Self {
        let bin = get_env_with_default("PREVIEW_RENDERER_BIN", "nezumo-render");
        let workers = get_env_with_default("YRS_VALIDATOR_WORKERS", "2")
            .parse::<usize>()
            .unwrap_or(2);
        let timeout_ms = get_env_with_default("YRS_VALIDATOR_TIMEOUT_MS", "5000")
            .parse::<u64>()
            .unwrap_or(5000);
        Self::spawn(bin, workers, Duration::from_millis(timeout_ms))
    }

    /// Validate one candidate. `Ok(())` = accepted; `Err(reason)` = rejected or
    /// worker error — both are retryable `nack`s at the coordinator (neither the
    /// event nor the Yrs update is committed).
    pub async fn validate(&self, req: ValidateRequest) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Job { req, reply })
            .await
            .map_err(|_| "validator pool closed".to_string())?;
        rx.await
            .map_err(|_| "validator worker dropped reply".to_string())?
    }
}

/// One worker: owns a child (respawned lazily after crash/timeout), pulls jobs
/// off the shared queue, and validates each.
async fn worker_loop(
    id: usize,
    bin: String,
    rx: Arc<Mutex<mpsc::Receiver<Job>>>,
    per_request_timeout: Duration,
) {
    let mut worker: Option<Worker> = None;
    loop {
        // One lock hold per job; released while validating so peers can dequeue.
        let job = {
            let mut guard = rx.lock().await;
            match guard.recv().await {
                Some(job) => job,
                None => break, // pool dropped → shut down
            }
        };

        // Ensure a live worker (lazy spawn / respawn after a prior kill).
        if worker.is_none() {
            match Worker::spawn(&bin).await {
                Ok(w) => worker = Some(w),
                Err(e) => {
                    let _ = job.reply.send(Err(format!("validator[{id}] spawn: {e}")));
                    continue;
                }
            }
        }
        let w = worker.as_mut().unwrap();

        match tokio::time::timeout(per_request_timeout, w.validate(&job.req)).await {
            Ok(Ok(result)) => {
                let _ = job.reply.send(result);
            }
            Ok(Err(io_err)) => {
                // Protocol/IO failure → the child is suspect; drop it so the next
                // job respawns a clean one.
                tracing::warn!("validator[{id}] io error, respawning: {io_err}");
                worker = None;
                let _ = job.reply.send(Err(format!("validator io error: {io_err}")));
            }
            Err(_) => {
                tracing::warn!(
                    "validator[{id}] timed out after {:?}, killing worker",
                    per_request_timeout
                );
                worker = None; // Child dropped → kill_on_drop reaps it.
                let _ = job.reply.send(Err("validator timeout".to_string()));
            }
        }
    }
}

/// One running `nezumo-render --validate` child and its protocol pipes.
struct Worker {
    /// Owned process handle; `kill_on_drop` terminates a failed or timed-out worker.
    _child: Child,
    /// Framed requests are written to the renderer through this pipe.
    stdin: ChildStdin,
    /// Buffered response pipe used for the ready handshake and validation replies.
    stdout: BufReader<ChildStdout>,
}

impl Worker {
    /// Starts a renderer worker and verifies its protocol-version handshake.
    async fn spawn(bin: &str) -> Result<Self, String> {
        let mut child = spawn_child(bin)?;
        let stdin = child.stdin.take().ok_or("validator stdin unavailable")?;
        let stdout = BufReader::new(child.stdout.take().ok_or("validator stdout unavailable")?);
        let mut w = Worker {
            _child: child,
            stdin,
            stdout,
        };
        // Handshake: a ready frame [ver, 0].
        let ready = tokio::time::timeout(READY_TIMEOUT, read_frame(&mut w.stdout))
            .await
            .map_err(|_| "validator ready timeout".to_string())?
            .map_err(|e| format!("validator ready read: {e}"))?;
        if ready.as_slice() != [PROTO_VERSION, 0] {
            return Err(format!("unexpected validator handshake: {ready:?}"));
        }
        Ok(w)
    }

    /// Send one request frame and read its result frame. An `Err` here is an
    /// IO/protocol failure (worker must be respawned); an in-band reject arrives
    /// as `Ok(Err(reason))`.
    async fn validate(
        &mut self,
        req: &ValidateRequest,
    ) -> Result<Result<(), String>, std::io::Error> {
        write_frame(&mut self.stdin, &encode_request(req)).await?;
        let body = read_frame(&mut self.stdout).await?;
        parse_result(req.seq, &body)
    }
}

/// Spawn the child with the same OOM/parent-death hardening the preview daemon
/// uses on Linux, so a runaway validation can never take the main server down.
fn spawn_child(bin: &str) -> Result<Child, String> {
    use std::process::Stdio;
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt;
        let mut std_cmd = std::process::Command::new(bin);
        std_cmd
            .arg("--validate")
            .env("RUST_LOG", "warn")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        unsafe {
            std_cmd.pre_exec(|| {
                libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL as libc::c_ulong);
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
        cmd.spawn().map_err(|e| format!("{e}"))
    }
    #[cfg(not(target_os = "linux"))]
    {
        Command::new(bin)
            .arg("--validate")
            .env("RUST_LOG", "warn")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| format!("{e}"))
    }
}

/// Encodes one validation request body in the renderer's versioned wire format.
fn encode_request(req: &ValidateRequest) -> Vec<u8> {
    let mut body = Vec::with_capacity(
        1 + 8 * 4
            + 16
            + req.base_update.len()
            + req.base_state_vector.len()
            + req.candidate.len()
            + req.expected_projection.len(),
    );
    body.push(PROTO_VERSION);
    body.extend_from_slice(&req.seq.to_be_bytes());
    body.extend_from_slice(&req.base_seq.to_be_bytes());
    body.extend_from_slice(&req.base_generation.to_be_bytes());
    body.extend_from_slice(&req.writer_client_id.to_be_bytes());
    push_blob(&mut body, &req.base_update);
    push_blob(&mut body, &req.base_state_vector);
    push_blob(&mut body, &req.candidate);
    push_blob(&mut body, &req.expected_projection);
    body
}

/// Appends a big-endian length-prefixed byte string to a request body.
fn push_blob(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

/// Parse a result reply body → `Ok(())` accept / `Err(reason)` reject. `seq`
/// must match the request (guards against a desynced pipe).
fn parse_result(expected_seq: u64, body: &[u8]) -> Result<Result<(), String>, std::io::Error> {
    let bad = |m: &str| std::io::Error::new(std::io::ErrorKind::InvalidData, m.to_string());
    if body.len() < 15 {
        return Err(bad("result frame too short"));
    }
    if body[0] != PROTO_VERSION {
        return Err(bad("result protocol version mismatch"));
    }
    if body[1] != 1 {
        return Err(bad("expected result tag"));
    }
    let seq = u64::from_be_bytes(body[2..10].try_into().unwrap());
    if seq != expected_seq {
        return Err(bad("result seq mismatch (pipe desync)"));
    }
    let ok = body[10] == 1;
    let err_len = u32::from_be_bytes(body[11..15].try_into().unwrap()) as usize;
    if body.len() < 15 + err_len {
        return Err(bad("result error blob truncated"));
    }
    if ok {
        Ok(Ok(()))
    } else {
        let reason = String::from_utf8_lossy(&body[15..15 + err_len]).into_owned();
        Ok(Err(reason))
    }
}

/// Writes and flushes one length-delimited protocol frame.
async fn write_frame<W: AsyncWriteExt + Unpin>(w: &mut W, body: &[u8]) -> std::io::Result<()> {
    w.write_all(&(body.len() as u32).to_be_bytes()).await?;
    w.write_all(body).await?;
    w.flush().await
}

/// Reads one bounded length-delimited frame, rejecting oversized bodies before allocation.
async fn read_frame<R: AsyncReadExt + Unpin>(r: &mut R) -> std::io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("frame too large: {len}"),
        ));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}
