//! Static stream assignment and guarded control-socket ownership.

use super::{NativeTarget, StreamPlan, demand::TargetRef};
use crate::native::NativeVenue;
use std::{
    collections::BTreeMap,
    fs,
    os::unix::{
        fs::{FileTypeExt, MetadataExt, PermissionsExt},
        net::UnixListener as StdUnixListener,
    },
    path::{Path, PathBuf},
    sync::Arc,
};

/// Markets whose initial shard this router remembers, one per frozen selection market at the
/// selection bound.
const INITIAL_MARKET_CAPACITY: usize = 4_096;

/// Routes static and newly desired targets without moving an initially assigned market.
pub(super) struct AssignmentRouter {
    known: BTreeMap<(u8, String), usize>,
    initial: Vec<TargetRef>,
    shards: BTreeMap<u8, Vec<usize>>,
}

impl AssignmentRouter {
    /// Records at most [`INITIAL_MARKET_CAPACITY`] initial markets; a market beyond that is not
    /// recorded and is routed by hash like any unknown market. A recorded market retains its
    /// recorded shard forever.
    pub(super) fn new(plans: &[StreamPlan]) -> Self {
        let mut known = BTreeMap::new();
        let mut initial = Vec::new();
        let mut shards: BTreeMap<u8, Vec<usize>> = BTreeMap::new();
        for plan in plans {
            let indexes = shards.entry(code(plan.venue)).or_default();
            if !indexes.contains(&plan.index) {
                indexes.push(plan.index);
            }
            for target in &plan.targets {
                let venue = code(plan.venue);
                let key = (venue, target.market.to_string());
                if known.len() < INITIAL_MARKET_CAPACITY {
                    known.entry(key).or_insert(plan.index);
                }
                initial.push(TargetRef {
                    venue: venue_name(plan.venue).into(),
                    market: target.market.to_string(),
                    asset: target.asset.as_ref().map(ToString::to_string),
                    amm: target.amm,
                });
            }
        }
        initial.sort();
        initial.dedup();
        Self {
            known,
            initial,
            shards,
        }
    }
    /// Returns deterministic, sorted, deduplicated targets for one shard. Unknown markets hash by venue and market.
    pub(super) fn desired(
        &self,
        venue: NativeVenue,
        index: usize,
        targets: &[TargetRef],
    ) -> Vec<NativeTarget> {
        let mut result = Vec::new();
        for target in targets {
            if target.venue != venue_name(venue) {
                continue;
            }
            let shard = self.shard_for(venue, &target.market);
            if shard == index {
                result.push(NativeTarget {
                    market: Arc::from(target.market.as_str()),
                    asset: target.asset.as_ref().map(|asset| Arc::from(asset.as_str())),
                    amm: target.amm,
                });
            }
        }
        result.sort();
        result.dedup();
        result
    }
    /// Returns the deduplicated initial target set in deterministic order.
    pub(super) fn initial(&self) -> Vec<TargetRef> {
        self.initial.clone()
    }
    /// Returns a fixed assignment or selects exactly one configured shard for a new market.
    pub(super) fn shard_for(&self, venue: NativeVenue, market: &str) -> usize {
        self.known
            .get(&(code(venue), market.into()))
            .copied()
            .unwrap_or_else(|| {
                self.shards
                    .get(&code(venue))
                    .map_or(0, |shards| shards[hash(venue, market) % shards.len()])
            })
    }
}
fn code(venue: NativeVenue) -> u8 {
    match venue {
        NativeVenue::Limitless => 1,
        NativeVenue::Polymarket => 2,
    }
}
fn venue_name(venue: NativeVenue) -> &'static str {
    match venue {
        NativeVenue::Limitless => "limitless",
        NativeVenue::Polymarket => "polymarket",
    }
}
fn hash(venue: NativeVenue, market: &str) -> usize {
    let mut state = u64::from(code(venue));
    for byte in market.bytes() {
        state = state
            .wrapping_mul(0x100000001b3)
            .wrapping_add(u64::from(byte));
    }
    state as usize
}

/// Guard for a newly created Unix control socket.
///
/// Binding requires a non-existent path below an existing absolute parent owned by this euid
/// and not writable by group or others. Drop unlinks only when the path still names the socket
/// recorded at bind time, so replacing it cannot cause deletion of another process's file.
pub(super) struct ControlSocket {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl ControlSocket {
    /// Binds `path` with mode 0600 after enforcing the documented parent and path preconditions.
    pub(super) fn bind(path: &Path) -> Result<(Self, tokio::net::UnixListener), String> {
        if fs::symlink_metadata(path).is_ok() {
            return Err(format!("control socket {} already exists", path.display()));
        }
        let parent = path
            .parent()
            .ok_or_else(|| "control socket has no parent".to_owned())?;
        let parent = fs::canonicalize(parent)
            .map_err(|error| format!("control parent {}: {error}", parent.display()))?;
        if !parent.is_absolute() {
            return Err("control parent is not absolute".into());
        }
        let name = path
            .file_name()
            .ok_or_else(|| "control socket has no file name".to_owned())?;
        let absolute = parent.join(name);
        let metadata = fs::metadata(&parent)
            .map_err(|error| format!("control parent {}: {error}", parent.display()))?;
        if metadata.uid() != crate::peer::own_euid() || metadata.mode() & 0o022 != 0 {
            return Err("control parent must be owned by euid and not group/world writable".into());
        }
        let listener = StdUnixListener::bind(&absolute)
            .map_err(|error| format!("control socket {}: {error}", absolute.display()))?;
        let bound = fs::symlink_metadata(&absolute)
            .map_err(|error| format!("control socket stat: {error}"))?;
        let guard = Self {
            path: absolute,
            device: bound.dev(),
            inode: bound.ino(),
        };
        fs::set_permissions(&guard.path, fs::Permissions::from_mode(0o600))
            .map_err(|error| format!("control socket permissions: {error}"))?;
        listener
            .set_nonblocking(true)
            .map_err(|error| format!("control socket nonblocking: {error}"))?;
        let listener = tokio::net::UnixListener::from_std(listener)
            .map_err(|error| format!("control socket tokio: {error}"))?;
        Ok((guard, listener))
    }
}
impl Drop for ControlSocket {
    fn drop(&mut self) {
        if fs::symlink_metadata(&self.path).is_ok_and(|meta| {
            meta.file_type().is_socket() && meta.dev() == self.device && meta.ino() == self.inode
        }) {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    fn dir() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "pmws-routing-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        path
    }
    #[tokio::test(flavor = "current_thread")]
    async fn bind_refuses_existing_and_only_unlinks_its_inode() {
        let dir = dir();
        let path = dir.join("control.sock");
        fs::write(&path, b"x").unwrap();
        assert!(ControlSocket::bind(&path).is_err());
        fs::remove_file(&path).unwrap();
        let (guard, _) = ControlSocket::bind(&path).unwrap();
        fs::remove_file(&path).unwrap();
        fs::write(&path, b"replacement").unwrap();
        drop(guard);
        assert_eq!(fs::read(&path).unwrap(), b"replacement");
        fs::remove_file(&path).unwrap();
        fs::remove_dir(&dir).unwrap();
    }
}
