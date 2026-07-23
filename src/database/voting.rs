//! Board voting persistence (Miro-style). Server-authoritative: the tally and
//! participation live in Postgres so they survive reload/reconnect and the
//! server enforces the per-participant vote budget. Runtime queries (no
//! compile-time `query!` macro) so the crate builds without a live DB.

use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use uuid::Uuid;

/// One voting session row (candidate_ids kept as the raw JSON-array string).
#[derive(Debug, Clone)]
pub struct VotingSessionRow {
    pub id: Uuid,
    pub board_id: Uuid,
    pub created_by: Uuid,
    pub candidate_ids: String,
    pub votes_per_participant: i32,
    pub anonymous: bool,
    pub status: String,
    pub ends_at: chrono::DateTime<chrono::Utc>,
    pub rev: i64,
}

const SESSION_COLS: &str = "id, board_id, created_by, candidate_ids, votes_per_participant, \
                            anonymous, status, ends_at, rev";

fn row_to_session(r: &PgRow) -> VotingSessionRow {
    VotingSessionRow {
        id: r.get("id"),
        board_id: r.get("board_id"),
        created_by: r.get("created_by"),
        candidate_ids: r.get("candidate_ids"),
        votes_per_participant: r.get("votes_per_participant"),
        anonymous: r.get("anonymous"),
        status: r.get("status"),
        ends_at: r.get("ends_at"),
        rev: r.get("rev"),
    }
}

/// Create a fresh active session, ending any prior active one for the board
/// first (one active per board), and auto-join the creator. `minutes` sets the
/// deadline (`ends_at = now() + minutes`). `candidate_ids` is a JSON-encoded
/// array of synced id strings.
pub async fn create_session(
    pool: &PgPool,
    board_id: Uuid,
    created_by: Uuid,
    candidate_ids: &str,
    votes_per_participant: i32,
    anonymous: bool,
    minutes: i32,
) -> Result<VotingSessionRow, sqlx::Error> {
    // Replace any existing active session (the partial unique index allows only
    // one active per board).
    sqlx::query(
        "UPDATE board_voting_sessions SET status = 'ended', rev = rev + 1 \
         WHERE board_id = $1 AND status = 'active'",
    )
    .bind(board_id)
    .execute(pool)
    .await?;

    let id = Uuid::new_v4();
    let row = sqlx::query(&format!(
        "INSERT INTO board_voting_sessions \
           (id, board_id, created_by, candidate_ids, votes_per_participant, anonymous, status, ends_at, rev) \
         VALUES ($1, $2, $3, $4, $5, $6, 'active', now() + make_interval(mins => $7), 1) \
         RETURNING {SESSION_COLS}"
    ))
    .bind(id)
    .bind(board_id)
    .bind(created_by)
    .bind(candidate_ids)
    .bind(votes_per_participant)
    .bind(anonymous)
    .bind(minutes)
    .fetch_one(pool)
    .await?;

    set_participation(pool, id, created_by, "joined").await?;
    Ok(row_to_session(&row))
}

/// The board's current active session, if any.
pub async fn active_session(
    pool: &PgPool,
    board_id: Uuid,
) -> Result<Option<VotingSessionRow>, sqlx::Error> {
    let row = sqlx::query(&format!(
        "SELECT {SESSION_COLS} FROM board_voting_sessions \
         WHERE board_id = $1 AND status = 'active' \
         ORDER BY created_at DESC LIMIT 1"
    ))
    .bind(board_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.as_ref().map(row_to_session))
}

/// The board's most recent session regardless of status (so clients can still
/// render results after it has ended). Clients dismiss ended results locally.
pub async fn latest_session(
    pool: &PgPool,
    board_id: Uuid,
) -> Result<Option<VotingSessionRow>, sqlx::Error> {
    let row = sqlx::query(&format!(
        "SELECT {SESSION_COLS} FROM board_voting_sessions \
         WHERE board_id = $1 ORDER BY created_at DESC LIMIT 1"
    ))
    .bind(board_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.as_ref().map(row_to_session))
}

/// Upsert a participant's join/decline status.
pub async fn set_participation(
    pool: &PgPool,
    session_id: Uuid,
    user_id: Uuid,
    status: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO board_voting_participants (id, session_id, user_id, status) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (session_id, user_id) DO UPDATE SET status = EXCLUDED.status",
    )
    .bind(Uuid::new_v4())
    .bind(session_id)
    .bind(user_id)
    .bind(status)
    .execute(pool)
    .await?;
    Ok(())
}

/// Whether a user has explicitly joined this session.
pub async fn is_joined(
    pool: &PgPool,
    session_id: Uuid,
    user_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let row = sqlx::query(
        "SELECT status FROM board_voting_participants WHERE session_id = $1 AND user_id = $2",
    )
    .bind(session_id)
    .bind(user_id)
    .fetch_optional(pool)
    .await?;
    Ok(row
        .map(|r| r.get::<String, _>("status") == "joined")
        .unwrap_or(false))
}

/// One participant with their running vote count.
#[derive(Debug, Clone)]
pub struct ParticipantRow {
    pub user_id: Uuid,
    pub status: String,
    pub votes_used: i64,
    /// Explicitly finished voting early (without spending every vote).
    pub finished: bool,
}

/// All participants of a session with each one's votes-used, join order.
pub async fn participants(
    pool: &PgPool,
    session_id: Uuid,
) -> Result<Vec<ParticipantRow>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT p.user_id AS user_id, p.status AS status, p.finished AS finished, \
                COALESCE(v.cnt, 0) AS votes_used \
         FROM board_voting_participants p \
         LEFT JOIN ( \
            SELECT voter_id, COUNT(*) AS cnt FROM board_votes WHERE session_id = $1 GROUP BY voter_id \
         ) v ON v.voter_id = p.user_id \
         WHERE p.session_id = $1 \
         ORDER BY p.created_at ASC",
    )
    .bind(session_id)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .iter()
        .map(|r| ParticipantRow {
            user_id: r.get("user_id"),
            status: r.get("status"),
            votes_used: r.get("votes_used"),
            finished: r.get("finished"),
        })
        .collect())
}

/// Mark a joined participant as finished (submitted early). No-op if they never
/// joined this session.
pub async fn set_finished(
    pool: &PgPool,
    session_id: Uuid,
    user_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE board_voting_participants SET finished = TRUE \
         WHERE session_id = $1 AND user_id = $2",
    )
    .bind(session_id)
    .bind(user_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// How many votes a given voter has already cast in this session.
pub async fn voter_used(
    pool: &PgPool,
    session_id: Uuid,
    voter_id: Uuid,
) -> Result<i64, sqlx::Error> {
    let row = sqlx::query(
        "SELECT COUNT(*) AS cnt FROM board_votes WHERE session_id = $1 AND voter_id = $2",
    )
    .bind(session_id)
    .bind(voter_id)
    .fetch_one(pool)
    .await?;
    Ok(row.get::<i64, _>("cnt"))
}

/// Insert one vote (multiple per candidate allowed; the per-participant cap is
/// enforced by the caller via [`voter_used`]).
pub async fn insert_vote(
    pool: &PgPool,
    session_id: Uuid,
    voter_id: Uuid,
    candidate_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO board_votes (id, session_id, voter_id, candidate_id) VALUES ($1, $2, $3, $4)",
    )
    .bind(Uuid::new_v4())
    .bind(session_id)
    .bind(voter_id)
    .bind(candidate_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Per-candidate vote counts `[(candidate_id, count)]`.
pub async fn tally(pool: &PgPool, session_id: Uuid) -> Result<Vec<(String, i64)>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT candidate_id, COUNT(*) AS cnt FROM board_votes \
         WHERE session_id = $1 GROUP BY candidate_id",
    )
    .bind(session_id)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .iter()
        .map(|r| (r.get::<String, _>("candidate_id"), r.get::<i64, _>("cnt")))
        .collect())
}

/// Per-candidate voter ids `[(candidate_id, voter_id)]` — only surfaced to
/// clients when the session is NOT anonymous.
pub async fn voters(pool: &PgPool, session_id: Uuid) -> Result<Vec<(String, Uuid)>, sqlx::Error> {
    let rows = sqlx::query("SELECT candidate_id, voter_id FROM board_votes WHERE session_id = $1")
        .bind(session_id)
        .fetch_all(pool)
        .await?;
    Ok(rows
        .iter()
        .map(|r| {
            (
                r.get::<String, _>("candidate_id"),
                r.get::<Uuid, _>("voter_id"),
            )
        })
        .collect())
}

/// Bump the session revision so a subsequent broadcast supersedes prior state
/// under the client's LWW (`rev` monotonic). Returns the new rev.
pub async fn bump_rev(pool: &PgPool, session_id: Uuid) -> Result<i64, sqlx::Error> {
    let row =
        sqlx::query("UPDATE board_voting_sessions SET rev = rev + 1 WHERE id = $1 RETURNING rev")
            .bind(session_id)
            .fetch_one(pool)
            .await?;
    Ok(row.get::<i64, _>("rev"))
}

/// Mark a session ended (bumping rev). Returns the updated row.
pub async fn end_session(
    pool: &PgPool,
    session_id: Uuid,
) -> Result<Option<VotingSessionRow>, sqlx::Error> {
    let row = sqlx::query(&format!(
        "UPDATE board_voting_sessions SET status = 'ended', rev = rev + 1 \
         WHERE id = $1 RETURNING {SESSION_COLS}"
    ))
    .bind(session_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.as_ref().map(row_to_session))
}
