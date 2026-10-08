//! Bounded desired-target ownership for operator pins and client leases.

use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

/// One venue-native subscription target controlled without carrying market data.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetRef {
    /// Lowercase venue name: `limitless` or `polymarket`.
    pub venue: String,
    /// Venue-native market identifier.
    pub market: String,
    /// Venue-native asset identifier when the venue requires one.
    pub asset: Option<String>,
    /// Whether this target requests an AMM feed.
    pub amm: bool,
}

/// Bounds for retained pin and lease ownership. `lease_ttl` uses monotonic [`Instant`] time.
#[derive(Clone, Debug)]
pub struct DemandLimits {
    /// Maximum distinct desired targets across all owners.
    pub max_targets: usize,
    /// Maximum connected lease-owning sessions.
    pub max_sessions: usize,
    /// Maximum distinct targets owned by a session.
    pub max_leases_per_session: usize,
    /// Maximum estimated bytes retained by copied target strings and ownership metadata.
    pub max_bytes: usize,
    /// Time after renewal at which a session's ownership expires.
    pub lease_ttl: Duration,
}

impl Default for DemandLimits {
    fn default() -> Self {
        Self {
            max_targets: 4_096,
            max_sessions: 1_024,
            max_leases_per_session: 1_024,
            max_bytes: 1_048_576,
            lease_ttl: Duration::from_secs(60),
        }
    }
}

/// A sorted, aggregate desired-set transition.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DemandChange {
    /// Targets newly wanted by at least one owner.
    pub added: Vec<TargetRef>,
    /// Targets no longer wanted by any owner.
    pub removed: Vec<TargetRef>,
    /// Requested targets which left the aggregate desired set unchanged.
    pub unchanged: Vec<TargetRef>,
}

/// Rejection reason for a control-plane ownership command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DemandError {
    InvalidTarget,
    Capacity,
    UnknownSession,
    TimeOverflow,
    InvalidLimits,
}

#[derive(Clone)]
struct Lease {
    targets: BTreeSet<TargetRef>,
    expires: Instant,
}

/// Owns bounded operator pins and expiring client leases; it never accepts market payloads.
pub struct Demand {
    limits: DemandLimits,
    invalid_limits: bool,
    pins: BTreeSet<TargetRef>,
    sessions: BTreeMap<u64, Lease>,
}

impl TargetRef {
    /// Validates venue-native target shape and bounded identifier lengths.
    pub fn validate(&self) -> Result<(), DemandError> {
        if self.market.is_empty()
            || self.market.len() > 1_024
            || self
                .asset
                .as_ref()
                .is_some_and(|asset| asset.is_empty() || asset.len() > 1_024)
        {
            return Err(DemandError::InvalidTarget);
        }
        match self.venue.as_str() {
            "limitless" if self.asset.is_none() => Ok(()),
            "polymarket" if self.asset.is_some() && !self.amm => Ok(()),
            _ => Err(DemandError::InvalidTarget),
        }
    }
    fn bytes(&self) -> Option<usize> {
        self.venue
            .len()
            .checked_add(self.market.len())?
            .checked_add(self.asset.as_ref().map_or(0, String::len))?
            .checked_add(64)
    }
}

impl Demand {
    /// Creates an empty bounded target owner.
    pub fn new(limits: DemandLimits) -> Self {
        Self {
            invalid_limits: limits.lease_ttl.is_zero(),
            limits,
            pins: BTreeSet::new(),
            sessions: BTreeMap::new(),
        }
    }
    /// Replaces all operator pins atomically.
    pub fn replace_pins(&mut self, targets: Vec<TargetRef>) -> Result<DemandChange, DemandError> {
        let next = Self::set(targets)?;
        self.commit(next, self.sessions.clone(), None)
    }
    /// Adds operator pins atomically.
    pub fn add_pins(&mut self, targets: Vec<TargetRef>) -> Result<DemandChange, DemandError> {
        let mut next = self.pins.clone();
        next.extend(Self::set(targets)?);
        self.commit(next, self.sessions.clone(), None)
    }
    /// Removes operator pins atomically.
    pub fn remove_pins(&mut self, targets: Vec<TargetRef>) -> Result<DemandChange, DemandError> {
        let requested = Self::set(targets)?;
        let mut next = self.pins.clone();
        for target in &requested {
            next.remove(target);
        }
        self.commit(next, self.sessions.clone(), Some(requested))
    }
    /// Adds targets to a session lease and refreshes its expiry; duplicate targets are idempotent.
    pub fn lease(
        &mut self,
        session: u64,
        targets: Vec<TargetRef>,
        now: Instant,
    ) -> Result<DemandChange, DemandError> {
        let requested = Self::set(targets)?;
        let expiry = now
            .checked_add(self.limits.lease_ttl)
            .ok_or(DemandError::TimeOverflow)?;
        let mut sessions = self.sessions.clone();
        let lease = sessions.entry(session).or_insert_with(|| Lease {
            targets: BTreeSet::new(),
            expires: expiry,
        });
        lease.targets.extend(requested.iter().cloned());
        lease.expires = expiry;
        self.commit(self.pins.clone(), sessions, Some(requested))
    }
    /// Releases only this session's requested target ownership.
    pub fn release(
        &mut self,
        session: u64,
        targets: Vec<TargetRef>,
        now: Instant,
    ) -> Result<DemandChange, DemandError> {
        let requested = Self::set(targets)?;
        let mut sessions = self.sessions.clone();
        let Some(lease) = sessions.get_mut(&session) else {
            return Ok(DemandChange::default());
        };
        for target in &requested {
            lease.targets.remove(target);
        }
        lease.expires = now
            .checked_add(self.limits.lease_ttl)
            .ok_or(DemandError::TimeOverflow)?;
        if lease.targets.is_empty() {
            sessions.remove(&session);
        }
        self.commit(self.pins.clone(), sessions, Some(requested))
    }
    /// Releases all ownership held by one disconnected session.
    pub fn disconnect(&mut self, session: u64) -> DemandChange {
        let mut sessions = self.sessions.clone();
        let requested = sessions
            .remove(&session)
            .map_or_else(BTreeSet::new, |lease| lease.targets);
        self.commit(self.pins.clone(), sessions, Some(requested))
            .unwrap_or_default()
    }
    /// Refreshes a session without changing its target set.
    pub fn renew(&mut self, session: u64, now: Instant) -> Result<DemandChange, DemandError> {
        let mut sessions = self.sessions.clone();
        let lease = sessions
            .get_mut(&session)
            .ok_or(DemandError::UnknownSession)?;
        lease.expires = now
            .checked_add(self.limits.lease_ttl)
            .ok_or(DemandError::TimeOverflow)?;
        self.commit(self.pins.clone(), sessions, Some(BTreeSet::new()))
    }
    /// Expires only leases whose explicit deadline has passed; quiet targets remain desired.
    pub fn expire(&mut self, now: Instant) -> DemandChange {
        let mut sessions = self.sessions.clone();
        let mut requested = BTreeSet::new();
        sessions.retain(|_, lease| {
            if lease.expires <= now {
                requested.extend(lease.targets.iter().cloned());
                false
            } else {
                true
            }
        });
        self.commit(self.pins.clone(), sessions, Some(requested))
            .unwrap_or_default()
    }
    /// Returns the current aggregate desired set in deterministic order.
    pub fn desired(&self) -> Vec<TargetRef> {
        Self::wanted(&self.pins, &self.sessions)
            .into_iter()
            .collect()
    }
    /// Returns `(desired_targets, pinned_targets, live_lease_sessions)`.
    pub fn status_counts(&self) -> (usize, usize, usize) {
        (
            Self::wanted(&self.pins, &self.sessions).len(),
            self.pins.len(),
            self.sessions.len(),
        )
    }
    fn set(targets: Vec<TargetRef>) -> Result<BTreeSet<TargetRef>, DemandError> {
        let mut unique = BTreeSet::new();
        for target in targets {
            target.validate()?;
            unique.insert(target);
        }
        Ok(unique)
    }
    fn wanted(pins: &BTreeSet<TargetRef>, sessions: &BTreeMap<u64, Lease>) -> BTreeSet<TargetRef> {
        let mut wanted = pins.clone();
        for lease in sessions.values() {
            wanted.extend(lease.targets.iter().cloned());
        }
        wanted
    }
    fn within(&self, pins: &BTreeSet<TargetRef>, sessions: &BTreeMap<u64, Lease>) -> bool {
        if sessions.len() > self.limits.max_sessions
            || sessions
                .values()
                .any(|lease| lease.targets.len() > self.limits.max_leases_per_session)
        {
            return false;
        }
        let wanted = Self::wanted(pins, sessions);
        if wanted.len() > self.limits.max_targets {
            return false;
        }
        let mut bytes = 0usize;
        for target in pins
            .iter()
            .chain(sessions.values().flat_map(|lease| lease.targets.iter()))
        {
            let Some(value) = target.bytes() else {
                return false;
            };
            let Some(next) = bytes.checked_add(value) else {
                return false;
            };
            bytes = next;
        }
        bytes <= self.limits.max_bytes
    }
    fn commit(
        &mut self,
        pins: BTreeSet<TargetRef>,
        sessions: BTreeMap<u64, Lease>,
        requested: Option<BTreeSet<TargetRef>>,
    ) -> Result<DemandChange, DemandError> {
        if self.invalid_limits {
            return Err(DemandError::InvalidLimits);
        }
        if !self.within(&pins, &sessions) {
            return Err(DemandError::Capacity);
        }
        let old = Self::wanted(&self.pins, &self.sessions);
        let new = Self::wanted(&pins, &sessions);
        let requested = requested.unwrap_or_else(|| old.union(&new).cloned().collect());
        let change = DemandChange {
            added: new.difference(&old).cloned().collect(),
            removed: old.difference(&new).cloned().collect(),
            unchanged: requested
                .into_iter()
                .filter(|target| old.contains(target) == new.contains(target))
                .collect(),
        };
        self.pins = pins;
        self.sessions = sessions;
        Ok(change)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn target(name: &str) -> TargetRef {
        TargetRef {
            venue: "polymarket".into(),
            market: name.into(),
            asset: Some("a".into()),
            amm: false,
        }
    }
    #[test]
    fn aggregate_ownership_and_idempotence() {
        let now = Instant::now();
        let mut demand = Demand::new(DemandLimits::default());
        assert_eq!(
            demand.lease(1, vec![target("m")], now).unwrap().added,
            vec![target("m")]
        );
        assert!(
            demand
                .lease(2, vec![target("m")], now)
                .unwrap()
                .added
                .is_empty()
        );
        assert!(
            demand
                .release(1, vec![target("m")], now)
                .unwrap()
                .removed
                .is_empty()
        );
        assert_eq!(demand.disconnect(2).removed, vec![target("m")]);
    }
    #[test]
    fn pins_outlive_leases_and_expiry_is_explicit() {
        let now = Instant::now();
        let mut demand = Demand::new(DemandLimits {
            lease_ttl: Duration::from_secs(1),
            ..DemandLimits::default()
        });
        demand.add_pins(vec![target("pin")]).unwrap();
        demand
            .lease(1, vec![target("pin"), target("lease")], now)
            .unwrap();
        assert_eq!(
            demand.expire(now + Duration::from_secs(2)).removed,
            vec![target("lease")]
        );
        assert_eq!(demand.desired(), vec![target("pin")]);
    }
    #[test]
    fn capacity_failure_is_atomic() {
        let mut demand = Demand::new(DemandLimits {
            max_targets: 1,
            ..DemandLimits::default()
        });
        assert!(demand.add_pins(vec![target("a"), target("b")]).is_err());
        assert!(demand.desired().is_empty());
    }
    #[test]
    fn release_is_idempotent_and_bounds_cover_bytes() {
        let now = Instant::now();
        let mut demand = Demand::new(DemandLimits {
            max_bytes: 70,
            ..DemandLimits::default()
        });
        assert!(demand.add_pins(vec![target("a")]).is_err());
        assert!(demand.desired().is_empty());
        assert_eq!(
            demand.release(99, vec![target("a")], now).unwrap(),
            DemandChange::default()
        );
        let mut normal = Demand::new(DemandLimits::default());
        normal
            .lease(1, vec![target("a"), target("b")], now)
            .unwrap();
        assert_eq!(normal.status_counts(), (2, 0, 1));
        assert!(
            normal
                .lease(1, vec![target("a"), target("b")], now)
                .unwrap()
                .added
                .is_empty()
        );
    }
}
