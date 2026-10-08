use crate::{MarketRef, NativeOutcome};
use serde::{Deserialize, Serialize};

pub const MAX_SOURCE_EVIDENCE: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub struct SourceDigest(String);
impl SourceDigest {
    pub fn new(value: impl Into<String>) -> Result<Self, DescriptorError> {
        let value = value.into();
        if value.is_empty() || value.len() > 512 {
            Err(DescriptorError::InvalidSourceEvidence)
        } else {
            Ok(Self(value))
        }
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Representation {
    VenueNative,
    Normalized,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Origin {
    SourceReported,
    NormalizedFromSource,
    LocallyDerived(Derivation),
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Derivation {
    SnapshotDiff,
}
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub struct ConnectionIdentity {
    value: String,
    generation: u64,
}
impl ConnectionIdentity {
    pub fn new(value: impl Into<String>, generation: u64) -> Result<Self, DescriptorError> {
        let value = value.into();
        if value.is_empty() || value.len() > 256 {
            Err(DescriptorError::InvalidSourceEvidence)
        } else {
            Ok(Self { value, generation })
        }
    }
    pub fn value(&self) -> &str {
        &self.value
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct SourceTimestamp(String);
impl SourceTimestamp {
    pub fn new(value: impl Into<String>) -> Result<Self, DescriptorError> {
        let value = value.into();
        if value.is_empty() || value.len() > 256 {
            Err(DescriptorError::InvalidSourceEvidence)
        } else {
            Ok(Self(value))
        }
    }
    pub fn as_lexeme(&self) -> &str {
        &self.0
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct LocalMonotonicTimestamp(u64);
impl LocalMonotonicTimestamp {
    pub fn new(value: u64) -> Self {
        Self(value)
    }
    pub fn value(&self) -> u64 {
        self.0
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum SourceEvidence {
    Sequence(SourceEvidenceValue),
    Version(SourceEvidenceValue),
    Checksum(SourceEvidenceValue),
    Hash(SourceEvidenceValue),
    EventId(SourceEvidenceValue),
    NumericLexeme {
        field: SourceFieldPath,
        lexeme: OriginalDecimalLexeme,
    },
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct SourceFieldPath(String);
impl SourceFieldPath {
    pub fn new(value: impl Into<String>) -> Result<Self, DescriptorError> {
        let value = value.into();
        if value.is_empty() || value.len() > 256 {
            Err(DescriptorError::InvalidSourceEvidence)
        } else {
            Ok(Self(value))
        }
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct OriginalDecimalLexeme(String);
impl OriginalDecimalLexeme {
    pub fn new(value: impl Into<String>) -> Result<Self, DescriptorError> {
        let value = value.into();
        if value.is_empty() || value.len() > 256 {
            Err(DescriptorError::InvalidSourceEvidence)
        } else {
            Ok(Self(value))
        }
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct SourceEvidenceValue(String);
impl SourceEvidenceValue {
    pub fn new(value: impl Into<String>) -> Result<Self, DescriptorError> {
        let value = value.into();
        if value.is_empty() || value.len() > 1024 {
            Err(DescriptorError::InvalidSourceEvidence)
        } else {
            Ok(Self(value))
        }
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl SourceEvidence {
    pub fn sequence(value: impl Into<String>) -> Result<Self, DescriptorError> {
        Ok(Self::Sequence(SourceEvidenceValue::new(value)?))
    }
    pub fn numeric_lexeme(field: SourceFieldPath, lexeme: OriginalDecimalLexeme) -> Self {
        Self::NumericLexeme { field, lexeme }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct SourceEvidenceCapacity(usize);
impl SourceEvidenceCapacity {
    pub fn new(value: usize) -> Result<Self, DescriptorError> {
        if value > MAX_SOURCE_EVIDENCE {
            Err(DescriptorError::CapacityExceeded)
        } else {
            Ok(Self(value))
        }
    }
    pub fn value(self) -> usize {
        self.0
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BoundedSourceEvidence {
    values: Vec<SourceEvidence>,
    capacity: SourceEvidenceCapacity,
}
impl BoundedSourceEvidence {
    pub fn new(
        values: impl IntoIterator<Item = SourceEvidence>,
        capacity: SourceEvidenceCapacity,
    ) -> Result<Self, DescriptorError> {
        let mut collected = Vec::new();
        for value in values {
            if collected.len() == capacity.value() {
                return Err(DescriptorError::CapacityExceeded);
            }
            collected.push(value);
        }
        Ok(Self {
            values: collected,
            capacity,
        })
    }
    pub fn values(&self) -> &[SourceEvidence] {
        &self.values
    }
    pub fn capacity(&self) -> SourceEvidenceCapacity {
        self.capacity
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReplicaRole {
    PublishingPrimary,
    HotStandby,
    Recovery,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Provenance {
    market: MarketRef,
    outcome: Option<NativeOutcome>,
    native_family: String,
    source_timestamp: Option<SourceTimestamp>,
    source_evidence: BoundedSourceEvidence,
    daemon_generation: u64,
    connection: ConnectionIdentity,
    subscription_generation: u64,
    receive_position: u64,
    commit_position: u64,
    local_receive_time: LocalMonotonicTimestamp,
    local_commit_time: LocalMonotonicTimestamp,
    replica: ReplicaRole,
    representation: Representation,
    origin: Origin,
    local_revision: u64,
    continuity_epoch: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProvenanceInput {
    pub market: MarketRef,
    pub outcome: Option<NativeOutcome>,
    pub native_family: String,
    pub source_timestamp: Option<SourceTimestamp>,
    pub source_evidence: BoundedSourceEvidence,
    pub daemon_generation: u64,
    pub connection: ConnectionIdentity,
    pub subscription_generation: u64,
    pub receive_position: u64,
    pub commit_position: u64,
    pub local_receive_time: LocalMonotonicTimestamp,
    pub local_commit_time: LocalMonotonicTimestamp,
    pub replica: ReplicaRole,
    pub representation: Representation,
    pub origin: Origin,
    pub local_revision: u64,
    pub continuity_epoch: u64,
}

impl Provenance {
    pub fn new(input: ProvenanceInput) -> Result<Self, DescriptorError> {
        let native_family = input.native_family;
        if native_family.is_empty()
            || native_family.len() > 256
            || input.source_evidence.values().iter().any(|value| {
                source_evidence_value(value).is_empty() || source_evidence_value(value).len() > 1024
            })
            || input.commit_position < input.receive_position
            || input.local_commit_time < input.local_receive_time
            || !matches!(
                (&input.representation, &input.origin),
                (Representation::VenueNative, Origin::SourceReported)
                    | (Representation::Normalized, Origin::NormalizedFromSource)
                    | (Representation::Normalized, Origin::LocallyDerived(_))
            )
        {
            return Err(DescriptorError::InvalidSourceEvidence);
        }
        Ok(Self {
            market: input.market,
            outcome: input.outcome,
            native_family,
            source_timestamp: input.source_timestamp,
            source_evidence: input.source_evidence,
            daemon_generation: input.daemon_generation,
            connection: input.connection,
            subscription_generation: input.subscription_generation,
            receive_position: input.receive_position,
            commit_position: input.commit_position,
            local_receive_time: input.local_receive_time,
            local_commit_time: input.local_commit_time,
            replica: input.replica,
            representation: input.representation,
            origin: input.origin,
            local_revision: input.local_revision,
            continuity_epoch: input.continuity_epoch,
        })
    }
    pub fn market(&self) -> &MarketRef {
        &self.market
    }
    pub fn outcome(&self) -> Option<&NativeOutcome> {
        self.outcome.as_ref()
    }
    pub fn native_family(&self) -> &str {
        &self.native_family
    }
    pub fn source_timestamp(&self) -> Option<&SourceTimestamp> {
        self.source_timestamp.as_ref()
    }
    pub fn source_evidence(&self) -> &[SourceEvidence] {
        self.source_evidence.values()
    }
    pub fn daemon_generation(&self) -> u64 {
        self.daemon_generation
    }
    pub fn connection(&self) -> &ConnectionIdentity {
        &self.connection
    }
    pub fn subscription_generation(&self) -> u64 {
        self.subscription_generation
    }
    pub fn receive_position(&self) -> u64 {
        self.receive_position
    }
    pub fn commit_position(&self) -> u64 {
        self.commit_position
    }
    pub fn local_receive_time(&self) -> LocalMonotonicTimestamp {
        self.local_receive_time
    }
    pub fn local_commit_time(&self) -> LocalMonotonicTimestamp {
        self.local_commit_time
    }
    pub fn replica(&self) -> &ReplicaRole {
        &self.replica
    }
    pub fn representation(&self) -> &Representation {
        &self.representation
    }
    pub fn origin(&self) -> &Origin {
        &self.origin
    }
    pub fn local_revision(&self) -> u64 {
        self.local_revision
    }
    pub fn continuity_epoch(&self) -> u64 {
        self.continuity_epoch
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescriptorError {
    InvalidSourceEvidence,
    CapacityExceeded,
}
impl std::fmt::Display for DescriptorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid descriptor")
    }
}
impl std::error::Error for DescriptorError {}

impl<'de> Deserialize<'de> for SourceDigest {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}
impl<'de> Deserialize<'de> for ConnectionIdentity {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Wire {
            value: String,
            generation: u64,
        }
        let wire = Wire::deserialize(d)?;
        Self::new(wire.value, wire.generation).map_err(serde::de::Error::custom)
    }
}
impl<'de> Deserialize<'de> for SourceTimestamp {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}
impl<'de> Deserialize<'de> for SourceEvidence {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        enum Wire {
            Sequence(String),
            Version(String),
            Checksum(String),
            Hash(String),
            EventId(String),
            NumericLexeme { field: String, lexeme: String },
        }
        let wire = Wire::deserialize(d)?;
        let value = match wire {
            Wire::Sequence(value) => {
                Self::Sequence(SourceEvidenceValue::new(value).map_err(serde::de::Error::custom)?)
            }
            Wire::Version(value) => {
                Self::Version(SourceEvidenceValue::new(value).map_err(serde::de::Error::custom)?)
            }
            Wire::Checksum(value) => {
                Self::Checksum(SourceEvidenceValue::new(value).map_err(serde::de::Error::custom)?)
            }
            Wire::Hash(value) => {
                Self::Hash(SourceEvidenceValue::new(value).map_err(serde::de::Error::custom)?)
            }
            Wire::EventId(value) => {
                Self::EventId(SourceEvidenceValue::new(value).map_err(serde::de::Error::custom)?)
            }
            Wire::NumericLexeme { field, lexeme } => Self::numeric_lexeme(
                SourceFieldPath::new(field).map_err(serde::de::Error::custom)?,
                OriginalDecimalLexeme::new(lexeme).map_err(serde::de::Error::custom)?,
            ),
        };
        if source_evidence_value(&value).is_empty() || source_evidence_value(&value).len() > 1024 {
            Err(serde::de::Error::custom(
                DescriptorError::InvalidSourceEvidence,
            ))
        } else {
            Ok(value)
        }
    }
}
fn source_evidence_value(value: &SourceEvidence) -> &str {
    match value {
        SourceEvidence::Sequence(value)
        | SourceEvidence::Version(value)
        | SourceEvidence::Checksum(value)
        | SourceEvidence::Hash(value)
        | SourceEvidence::EventId(value) => value.as_str(),
        SourceEvidence::NumericLexeme { lexeme, .. } => lexeme.as_str(),
    }
}
