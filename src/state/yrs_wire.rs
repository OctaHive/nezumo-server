//! Versioned binary envelope shared by REST catch-up and binary WebSockets.
//!
//! A stream is zero or more records. Each record is independently framed:
//! `u32 frame_len`, then `u8 version`, `i64 seq`, `i32 schema_version`,
//! `u32 client_event_id_len`, `u32 yupdate_len`, followed by the UTF-8 client
//! event id and the raw Yrs update. Integers are big-endian.

use crate::database::yrs_updates::YrsUpdateRow;

pub const YRS_UPDATE_WIRE_VERSION: u8 = 1;
/// Media type used by the length-delimited canonical update stream endpoint.
pub const YRS_UPDATES_CONTENT_TYPE: &str = "application/vnd.octaboard.yrs-updates-v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct YrsUpdateEnvelope {
    pub seq: i64,
    pub client_event_id: String,
    pub schema_version: i32,
    pub yupdate: Vec<u8>,
}

impl From<&YrsUpdateRow> for YrsUpdateEnvelope {
    fn from(row: &YrsUpdateRow) -> Self {
        Self {
            seq: row.seq,
            client_event_id: row.client_event_id.clone(),
            schema_version: row.schema_version,
            yupdate: row.yupdate.clone(),
        }
    }
}

/// Encodes ordered database rows into the binary update-stream wire format.
pub fn encode_update_stream(rows: &[YrsUpdateRow]) -> Result<Vec<u8>, &'static str> {
    let mut out = Vec::new();
    for row in rows {
        encode_envelope(&YrsUpdateEnvelope::from(row), &mut out)?;
    }
    Ok(out)
}

/// Encodes one canonical update envelope for streaming or tests.
pub fn encode_envelope(
    envelope: &YrsUpdateEnvelope,
    out: &mut Vec<u8>,
) -> Result<(), &'static str> {
    let client_id = envelope.client_event_id.as_bytes();
    let client_len = u32::try_from(client_id.len()).map_err(|_| "client_event_id too large")?;
    let update_len = u32::try_from(envelope.yupdate.len()).map_err(|_| "yupdate too large")?;
    let body_len = 1usize
        .checked_add(8 + 4 + 4 + 4)
        .and_then(|n| n.checked_add(client_id.len()))
        .and_then(|n| n.checked_add(envelope.yupdate.len()))
        .ok_or("frame length overflow")?;
    let body_len = u32::try_from(body_len).map_err(|_| "frame too large")?;

    out.extend_from_slice(&body_len.to_be_bytes());
    out.push(YRS_UPDATE_WIRE_VERSION);
    out.extend_from_slice(&envelope.seq.to_be_bytes());
    out.extend_from_slice(&envelope.schema_version.to_be_bytes());
    out.extend_from_slice(&client_len.to_be_bytes());
    out.extend_from_slice(&update_len.to_be_bytes());
    out.extend_from_slice(client_id);
    out.extend_from_slice(&envelope.yupdate);
    Ok(())
}

/// Decodes a length-delimited stream of canonical Yrs updates.
///
/// Rejects truncation, unknown versions, invalid UTF-8, and trailing bytes
/// inside a record instead of accepting ambiguous input.
#[cfg(test)]
pub fn decode_update_stream(mut bytes: &[u8]) -> Result<Vec<YrsUpdateEnvelope>, String> {
    let mut decoded = Vec::new();
    while !bytes.is_empty() {
        if bytes.len() < 4 {
            return Err("truncated frame length".into());
        }
        let frame_len = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
        bytes = &bytes[4..];
        if bytes.len() < frame_len {
            return Err("truncated frame body".into());
        }
        let (frame, rest) = bytes.split_at(frame_len);
        bytes = rest;
        decoded.push(decode_envelope(frame)?);
    }
    Ok(decoded)
}

/// Decodes one frame body after its outer `u32` length prefix has been removed.
///
/// Validates the wire version, fixed-width fields, declared variable lengths,
/// and UTF-8 event identifier before copying the raw Yrs update into the result.
#[cfg(test)]
fn decode_envelope(frame: &[u8]) -> Result<YrsUpdateEnvelope, String> {
    const FIXED: usize = 1 + 8 + 4 + 4 + 4;
    if frame.len() < FIXED {
        return Err("update frame too short".into());
    }
    if frame[0] != YRS_UPDATE_WIRE_VERSION {
        return Err(format!("unsupported update wire version {}", frame[0]));
    }
    let seq = i64::from_be_bytes(frame[1..9].try_into().unwrap());
    let schema_version = i32::from_be_bytes(frame[9..13].try_into().unwrap());
    let client_len = u32::from_be_bytes(frame[13..17].try_into().unwrap()) as usize;
    let update_len = u32::from_be_bytes(frame[17..21].try_into().unwrap()) as usize;
    let expected = FIXED
        .checked_add(client_len)
        .and_then(|n| n.checked_add(update_len))
        .ok_or_else(|| "update frame length overflow".to_string())?;
    if frame.len() != expected {
        return Err("update frame length mismatch".into());
    }
    let client_end = FIXED + client_len;
    let client_event_id = std::str::from_utf8(&frame[FIXED..client_end])
        .map_err(|_| "client_event_id is not UTF-8")?
        .to_owned();
    Ok(YrsUpdateEnvelope {
        seq,
        client_event_id,
        schema_version,
        yupdate: frame[client_end..].to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_stream_round_trip_and_order() {
        let input = [
            YrsUpdateEnvelope {
                seq: 41,
                client_event_id: "event-жва".into(),
                schema_version: 1,
                yupdate: vec![0, 1, 255],
            },
            YrsUpdateEnvelope {
                seq: 44,
                client_event_id: "event-b".into(),
                schema_version: 2,
                yupdate: vec![],
            },
        ];
        let mut wire = Vec::new();
        for envelope in &input {
            encode_envelope(envelope, &mut wire).unwrap();
        }
        assert_eq!(decode_update_stream(&wire).unwrap(), input);
    }

    #[test]
    fn decoder_rejects_truncation_and_unknown_version() {
        let envelope = YrsUpdateEnvelope {
            seq: 1,
            client_event_id: "e".into(),
            schema_version: 1,
            yupdate: vec![7],
        };
        let mut wire = Vec::new();
        encode_envelope(&envelope, &mut wire).unwrap();
        assert!(decode_update_stream(&wire[..wire.len() - 1]).is_err());
        wire[4] = 99;
        assert!(decode_update_stream(&wire).is_err());
    }
}
