//! Bounded local Unix-domain desired-target control plane.

use super::demand::{Demand, DemandChange, DemandError, DemandLimits, TargetRef};
use crate::peer::{own_euid, peer_euid};
use serde::{Deserialize, Serialize};
use std::{os::fd::AsFd, sync::Arc, time::Instant};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::{Semaphore, mpsc, oneshot, watch},
    time::{Duration, interval, timeout},
};

const MAX_SESSIONS: usize = 128;
const MAX_LINE: usize = 64 * 1024;
const INBOX: usize = 64;
const IO_TIMEOUT: Duration = Duration::from_secs(5);
const EXPIRE_TICK: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub enum ControlError {
    Io,
    Busy,
    Stopped,
}
enum LineError {
    Closed,
    TooLong,
    Io,
}
#[derive(Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Add { targets: Vec<TargetRef> },
    Remove { targets: Vec<TargetRef> },
    Replace { targets: Vec<TargetRef> },
    Lease { targets: Vec<TargetRef> },
    Release { targets: Vec<TargetRef> },
    Renew,
    Status,
}
#[derive(Serialize)]
struct Response {
    ok: bool,
    code: &'static str,
    revision: u64,
    desired: usize,
    changed: bool,
    ready: bool,
}
enum Operation {
    Request(Request),
    Disconnect,
}
struct Message {
    session: u64,
    operation: Operation,
    reply: oneshot::Sender<Response>,
}

/// Serves bounded NDJSON ownership commands until `stop` is set.
pub async fn serve(
    listener: UnixListener,
    initial: Vec<TargetRef>,
    desired: watch::Sender<Vec<TargetRef>>,
    mut stop: watch::Receiver<bool>,
) -> Result<(), ControlError> {
    let (tx, mut rx) = mpsc::channel::<Message>(INBOX);
    let mut demand = Demand::new(DemandLimits {
        max_sessions: MAX_SESSIONS,
        ..DemandLimits::default()
    });
    demand.replace_pins(initial).map_err(|_| ControlError::Io)?;
    let _ = desired.send(demand.desired());
    let mut revision = 1u64;
    let mut next_session = 1u64;
    let permits = Arc::new(Semaphore::new(MAX_SESSIONS));
    let mut expiry = interval(EXPIRE_TICK);
    loop {
        tokio::select! {
            changed = stop.changed() => { if changed.is_err() || *stop.borrow() { return Ok(()); } }
            _ = expiry.tick() => {
                let change = demand.expire(Instant::now());
                if !change.added.is_empty() || !change.removed.is_empty() { revision = revision.checked_add(1).ok_or(ControlError::Stopped)?; let _ = desired.send(demand.desired()); }
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted.map_err(|_| ControlError::Io)?;
                let Ok(permit) = permits.clone().try_acquire_owned() else { continue; };
                if peer_euid(stream.as_fd()).ok() != Some(own_euid()) { drop(permit); continue; }
                let id = next_session; next_session = next_session.checked_add(1).ok_or(ControlError::Stopped)?;
                tokio::spawn(session(stream, id, tx.clone(), permit, stop.clone()));
            }
            message = rx.recv() => {
                let Some(message) = message else { return Err(ControlError::Stopped) };
                let before = demand.desired();
                let answer = match message.operation {
                    Operation::Disconnect => { let change = demand.disconnect(message.session); response(&demand, revision, change, "ok") }
                    Operation::Request(request) => match apply(&mut demand, message.session, request) {
                        Ok(change) => response(&demand, revision, change, "ok"),
                        Err(_) => Response { ok:false, code:"invalid", revision, desired:before.len(), changed:false, ready:false },
                    }
                };
                if answer.changed { revision = revision.checked_add(1).ok_or(ControlError::Stopped)?; let _ = desired.send(demand.desired()); }
                let _ = message.reply.send(Response { revision, ..answer });
            }
        }
    }
}

fn apply(demand: &mut Demand, session: u64, request: Request) -> Result<DemandChange, DemandError> {
    let now = Instant::now();
    match request {
        Request::Add { targets } => demand.add_pins(targets),
        Request::Remove { targets } => demand.remove_pins(targets),
        Request::Replace { targets } => demand.replace_pins(targets),
        Request::Lease { targets } => demand.lease(session, targets, now),
        Request::Release { targets } => demand.release(session, targets, now),
        Request::Renew => demand.renew(session, now),
        Request::Status => Ok(DemandChange::default()),
    }
}
fn response(demand: &Demand, revision: u64, change: DemandChange, code: &'static str) -> Response {
    Response {
        ok: true,
        code,
        revision,
        desired: demand.desired().len(),
        changed: !change.added.is_empty() || !change.removed.is_empty(),
        ready: false,
    }
}

async fn session(
    stream: UnixStream,
    session: u64,
    tx: mpsc::Sender<Message>,
    _permit: tokio::sync::OwnedSemaphorePermit,
    mut stop: watch::Receiver<bool>,
) {
    let (read, mut write) = stream.into_split();
    let mut lines = BufReader::new(read);
    let mut first = true;
    loop {
        let next = if first {
            timeout(IO_TIMEOUT, bounded_line(&mut lines))
                .await
                .ok()
                .and_then(Result::ok)
        } else {
            tokio::select! { changed = stop.changed() => { if changed.is_err() || *stop.borrow() { None } else { continue } }, line = bounded_line(&mut lines) => line.ok() }
        };
        first = false;
        let line = match next {
            Some(line) => line,
            _ => break,
        };
        let request = serde_json::from_str(&line);
        let (reply_tx, reply_rx) = oneshot::channel();
        let response = match request {
            Ok(request) => {
                if tx
                    .try_send(Message {
                        session,
                        operation: Operation::Request(request),
                        reply: reply_tx,
                    })
                    .is_err()
                {
                    Response {
                        ok: false,
                        code: "busy",
                        revision: 0,
                        desired: 0,
                        changed: false,
                        ready: false,
                    }
                } else {
                    reply_rx.await.unwrap_or(Response {
                        ok: false,
                        code: "stopped",
                        revision: 0,
                        desired: 0,
                        changed: false,
                        ready: false,
                    })
                }
            }
            Err(_) => Response {
                ok: false,
                code: "invalid",
                revision: 0,
                desired: 0,
                changed: false,
                ready: false,
            },
        };
        if !matches!(
            timeout(
                IO_TIMEOUT,
                write.write_all(
                    format!("{}\n", serde_json::to_string(&response).unwrap()).as_bytes()
                ),
            )
            .await,
            Ok(Ok(()))
        ) {
            break;
        }
    }
    let (reply, _) = oneshot::channel();
    let _ = tx
        .send(Message {
            session,
            operation: Operation::Disconnect,
            reply,
        })
        .await;
}

async fn bounded_line<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
) -> Result<String, LineError> {
    let mut line = Vec::new();
    loop {
        let buffer = reader.fill_buf().await.map_err(|_| LineError::Io)?;
        if buffer.is_empty() {
            return if line.is_empty() {
                Err(LineError::Closed)
            } else {
                Err(LineError::TooLong)
            };
        }
        let take = buffer
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|index| index + 1)
            .unwrap_or(buffer.len());
        if line
            .len()
            .checked_add(take)
            .filter(|size| *size <= MAX_LINE)
            .is_none()
        {
            return Err(LineError::TooLong);
        }
        line.extend_from_slice(&buffer[..take]);
        reader.consume(take);
        if line.last() == Some(&b'\n') {
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return String::from_utf8(line).map_err(|_| LineError::Io);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn target() -> TargetRef {
        TargetRef {
            venue: "limitless".into(),
            market: "m".into(),
            asset: None,
            amm: false,
        }
    }

    #[test]
    fn requests_reject_unknown_fields_and_never_accept_market_payload() {
        assert!(serde_json::from_str::<Request>(r#"{"command":"lease","targets":[{"venue":"limitless","market":"m","asset":null,"amm":false}],"payload":{"price":1}}"#).is_err());
        assert!(serde_json::from_str::<Request>(r#"{"command":"lease","targets":[{"venue":"limitless","market":"m","asset":null,"amm":false}]}"#).is_ok());
    }

    #[test]
    fn duplicate_lease_does_not_change_aggregate_desired_set() {
        let mut demand = Demand::new(DemandLimits::default());
        assert!(
            apply(
                &mut demand,
                1,
                Request::Lease {
                    targets: vec![target()]
                }
            )
            .unwrap()
            .added
            .len()
                == 1
        );
        let change = apply(
            &mut demand,
            2,
            Request::Lease {
                targets: vec![target()],
            },
        )
        .unwrap();
        assert!(change.added.is_empty() && change.removed.is_empty());
    }

    #[tokio::test]
    async fn oversized_unterminated_line_is_rejected_without_waiting_for_eof() {
        let (mut writer, reader) = UnixStream::pair().unwrap();
        let write = tokio::spawn(async move { writer.write_all(&vec![b'x'; MAX_LINE + 1]).await });
        let mut reader = BufReader::new(reader);
        assert!(matches!(
            timeout(Duration::from_secs(1), bounded_line(&mut reader)).await,
            Ok(Err(LineError::TooLong))
        ));
        write.await.unwrap().unwrap();
    }

    fn socket_path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "pm-ws-control-{}-{}",
            std::process::id(),
            Instant::now().elapsed().as_nanos()
        ))
    }

    async fn lease(stream: &mut UnixStream) {
        stream.write_all(br#"{"command":"lease","targets":[{"venue":"limitless","market":"m","asset":null,"amm":false}]}"#).await.unwrap();
        stream.write_all(b"\n").await.unwrap();
        let mut response = [0; 256];
        assert!(stream.read(&mut response).await.unwrap() > 0);
    }

    #[tokio::test]
    async fn independent_leases_disconnect_individually_and_stop_closes_quiet_clients() {
        let path = socket_path();
        let listener = UnixListener::bind(&path).unwrap();
        let (wanted_tx, mut wanted_rx) = watch::channel(Vec::new());
        let (stop_tx, stop_rx) = watch::channel(false);
        let server = tokio::spawn(serve(listener, Vec::new(), wanted_tx, stop_rx));
        let mut first = UnixStream::connect(&path).await.unwrap();
        let mut second = UnixStream::connect(&path).await.unwrap();
        lease(&mut first).await;
        wanted_rx.changed().await.unwrap();
        assert_eq!(wanted_rx.borrow().len(), 1);
        lease(&mut second).await;
        assert!(
            timeout(Duration::from_millis(30), wanted_rx.changed())
                .await
                .is_err()
        );
        drop(first);
        assert!(
            timeout(Duration::from_millis(30), wanted_rx.changed())
                .await
                .is_err()
        );
        drop(second);
        wanted_rx.changed().await.unwrap();
        assert!(wanted_rx.borrow().is_empty());
        let quiet = UnixStream::connect(&path).await.unwrap();
        stop_tx.send(true).unwrap();
        assert!(matches!(
            timeout(Duration::from_secs(1), server).await,
            Ok(Ok(Ok(())))
        ));
        drop(quiet);
        let _ = std::fs::remove_file(path);
    }
}
