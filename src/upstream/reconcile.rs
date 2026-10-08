//! Desired-set reconciliation for one native upstream peer.

use std::time::Duration;

use tokio::{
    sync::watch,
    time::{Instant, sleep, timeout},
};

use super::{ConnectionContext, NativeTarget, PeerConfig, PeerEnd, peer_state};

const STOP_DRAIN: Duration = Duration::from_secs(6);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

enum Active {
    Replace,
    Retry(PeerEnd),
    Stop,
    Finished,
}

/// Keeps one peer aligned with the latest desired native target set.
pub(super) async fn run_peer(
    mut config: PeerConfig,
    context: ConnectionContext,
    mut desired: watch::Receiver<Vec<NativeTarget>>,
    mut stop: watch::Receiver<bool>,
) -> Result<(), PeerEnd> {
    let mut current = Vec::new();
    let mut backoff = Duration::from_secs(1);
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        let next = desired.borrow().clone();
        if next == current {
            if current.is_empty() {
                if wait_change(&mut desired, &mut stop).await {
                    return Ok(());
                }
                continue;
            }
        } else {
            current = next;
            if current.is_empty() && !advance(&mut config, &context, &current) {
                context.fault("generation_overflow");
                return Err(PeerEnd::AdmissionFailed);
            }
            backoff = Duration::from_secs(1);
        }
        if current.is_empty() {
            continue;
        }
        if !advance(&mut config, &context, &current) {
            context.fault("generation_overflow");
            return Err(PeerEnd::AdmissionFailed);
        }
        if crate::etiquette::admit_attempt(Instant::now(), 280).is_err() {
            context.fault("attempt_budget");
            if wait_backoff(backoff, &mut desired, &mut stop).await {
                return Ok(());
            }
            backoff = (backoff * 2).min(BACKOFF_MAX);
            continue;
        }
        let (retire, peer_stop) = watch::channel(false);
        let peer = peer_context(&context, config.generation);
        let result = run_transport(config.clone(), peer, peer_stop);
        tokio::pin!(result);
        let active = loop {
            tokio::select! {
                result = &mut result => break match result { Ok(()) | Err(PeerEnd::Stopped) => Active::Finished, Err(reason) => Active::Retry(reason) },
                changed = desired.changed() => {
                    if changed.is_err() || *stop.borrow() { break Active::Stop; }
                    if *desired.borrow() != current { break Active::Replace; }
                },
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() { break Active::Stop; }
                },
            }
        };
        match active {
            Active::Finished => {
                fence(&context, config.generation, true);
                return Ok(());
            }
            Active::Stop => {
                let _ = retire.send(true);
                fence(&context, config.generation, true);
                if timeout(STOP_DRAIN, &mut result).await.is_err() {
                    context.fault("retire_drain_timeout");
                    return Err(PeerEnd::ReadFailed);
                }
                return Ok(());
            }
            Active::Replace => {
                let _ = retire.send(true);
                fence(&context, config.generation, false);
                if timeout(STOP_DRAIN, &mut result).await.is_err() {
                    context.fault("retire_drain_timeout");
                    return Err(PeerEnd::ReadFailed);
                }
            }
            Active::Retry(PeerEnd::AdmissionFailed) => {
                fence(&context, config.generation, false);
                context.fault("admission_fenced");
                return Err(PeerEnd::AdmissionFailed);
            }
            Active::Retry(reason) => {
                context.fault(reason_name(reason));
                fence(&context, config.generation, false);
                if wait_backoff(backoff, &mut desired, &mut stop).await {
                    return Ok(());
                }
                backoff = (backoff * 2).min(BACKOFF_MAX);
            }
        }
    }
}

async fn run_transport(
    config: PeerConfig,
    context: ConnectionContext,
    stop: watch::Receiver<bool>,
) -> Result<(), PeerEnd> {
    match config.venue {
        crate::native::NativeVenue::Limitless => {
            crate::limitless::upstream::run(config, context, stop).await
        }
        crate::native::NativeVenue::Polymarket => {
            crate::polymarket::upstream::run(config, context, stop).await
        }
    }
}

fn advance(config: &mut PeerConfig, context: &ConnectionContext, targets: &[NativeTarget]) -> bool {
    config.generation = match config.generation.checked_add(1) {
        Some(value) => value,
        None => return false,
    };
    config.targets = targets.to_vec();
    let mut state = context.state.borrow_mut();
    state.peers[context.index] = peer_state(targets);
    state.peers[context.index].generation = config.generation;
    state.gate.set_generation(config.generation);
    true
}

fn peer_context(context: &ConnectionContext, generation: u64) -> ConnectionContext {
    ConnectionContext {
        state: context.state.clone(),
        window: context.window.clone(),
        index: context.index,
        slot: context.slot,
        generation,
        source_name: context.source_name.clone(),
        cpu_timing: context.cpu_timing,
        tape: context.tape.clone(),
    }
}

fn fence(context: &ConnectionContext, generation: u64, retain_coverage: bool) {
    let mut state = context.state.borrow_mut();
    let peer = &mut state.peers[context.index];
    if peer.generation == generation {
        peer.connected = false;
        peer.accepting = false;
        if !retain_coverage {
            peer.subscription_sent = false;
            peer.last_heartbeat_ns = None;
            peer.partial_ack = false;
            peer.coverage
                .values_mut()
                .for_each(|covered| *covered = false);
        }
    }
}

async fn wait_change(
    desired: &mut watch::Receiver<Vec<NativeTarget>>,
    stop: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! { changed = desired.changed() => changed.is_err(), changed = stop.changed() => changed.is_err() || *stop.borrow() }
}

async fn wait_backoff(
    delay: Duration,
    desired: &mut watch::Receiver<Vec<NativeTarget>>,
    stop: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! { _ = sleep(delay) => false, changed = desired.changed() => changed.is_err(), changed = stop.changed() => changed.is_err() || *stop.borrow() }
}

fn reason_name(reason: PeerEnd) -> &'static str {
    match reason {
        PeerEnd::ConnectFailed => "connect_failed",
        PeerEnd::ConnectTimedOut => "connect_timeout",
        PeerEnd::WriteFailed => "write_failed",
        PeerEnd::HeartbeatTimeout => "heartbeat_timeout",
        PeerEnd::SocketClosed => "socket_closed",
        PeerEnd::ReadFailed => "read_failed",
        PeerEnd::DecodeFailed => "decode_failed",
        PeerEnd::RoutingFailed => "routing_failed",
        PeerEnd::ControlOverload => "control_overload",
        PeerEnd::VenueRejected => "venue_rejected",
        PeerEnd::AdmissionFailed | PeerEnd::Stopped => "stopped",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        native::{NativeBatch, NativeVenue},
        upstream::test_context,
    };
    use futures_util::StreamExt;
    use std::sync::Arc;
    use tokio::{net::TcpListener, sync::oneshot};
    use tokio_tungstenite::accept_async;

    fn target(asset: &str) -> NativeTarget {
        NativeTarget {
            market: Arc::from("m"),
            asset: Some(Arc::from(asset)),
            amm: false,
        }
    }
    fn config(endpoint: String, targets: Vec<NativeTarget>) -> PeerConfig {
        PeerConfig {
            venue: NativeVenue::Polymarket,
            endpoint,
            slot: 0,
            generation: 1,
            targets,
            lifecycle: true,
            min_command_interval: Duration::ZERO,
            tolerate_partial_ack: false,
        }
    }

    #[tokio::test]
    async fn desired_noop_replacement_removal_and_stop_are_generation_fenced() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let (accepted, mut seen) = tokio::sync::mpsc::channel(4);
        let (release, released) = oneshot::channel();
        tokio::spawn(async move {
            for index in 0..3 {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(tcp).await.unwrap();
                let _ = socket.next().await;
                accepted.send(()).await.unwrap();
                if index != 0 {
                    let _ = socket.next().await;
                }
            }
            let _ = released.await;
        });
        let first = vec![target("a")];
        let (wanted, desired) = watch::channel(first.clone());
        let (stop_tx, stop) = watch::channel(false);
        let context = test_context(first.clone());
        let controller = async {
            seen.recv().await.unwrap();
            seen.recv().await.unwrap();
            assert!(!context.state.borrow().faults.is_empty());
            wanted.send(first.clone()).unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(80), seen.recv())
                    .await
                    .is_err()
            );
            stop_tx.send(false).unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(80), seen.recv())
                    .await
                    .is_err()
            );
            wanted.send(vec![target("b")]).unwrap();
            tokio::time::timeout(Duration::from_secs(2), seen.recv())
                .await
                .unwrap()
                .unwrap();
            wanted.send(Vec::new()).unwrap();
            tokio::time::sleep(Duration::from_millis(30)).await;
            assert!(
                context.state.borrow().peers[context.index]
                    .coverage
                    .is_empty()
            );
            stop_tx.send(true).unwrap();
        };
        let (result, ()) = tokio::join!(
            run_peer(
                config(endpoint, first.clone()),
                context.clone(),
                desired,
                stop
            ),
            controller
        );
        assert_eq!(result, Ok(()));
        let _ = release.send(());
    }

    #[test]
    fn retired_context_cannot_admit_after_generation_fence() {
        let targets = vec![target("a")];
        let context = test_context(targets.clone());
        let mut config = config("ws://unused".into(), targets);
        let assigned = config.targets.clone();
        assert!(advance(&mut config, &context, &assigned));
        let old = peer_context(&context, config.generation);
        fence(&context, config.generation, false);
        let received = old.stamp();
        let source = old.source(received);
        assert_eq!(
            old.admit(
                NativeBatch {
                    source,
                    input_bytes: 0,
                    events: Vec::new()
                },
                received,
                old.stamp(),
                crate::upstream::StageStamp {
                    wall_ns: old.now_ns(),
                    cpu_ns: None
                },
            ),
            Err(PeerEnd::AdmissionFailed)
        );
    }
}
