use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use object_store::{ObjectStore, PutPayload, path::Path};
use serde::{Deserialize, Serialize};

use crate::{
    config::Config,
    error::{Error, Result},
};

// ── LeaderInfo ────────────────────────────────────────────────────────────────

/// Identity and contact information of the current leader, derived from the
/// lockfile. Used by both [`LeaderElection`] observers and [`LeaderClient`].
///
/// [`LeaderElection`]: crate::LeaderElection
/// [`LeaderClient`]: crate::LeaderClient
#[derive(Debug, Clone)]
pub struct LeaderInfo {
    pub node_id: String,
    pub address: String,
    pub term: u64,
    pub metadata: HashMap<String, String>,
}

// ── Lockfile wire format ──────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Lease {
    pub(crate) node_id: String,
    pub(crate) address: String,
    pub(crate) term: u64,
    /// Unix timestamp (ms) at which this lease was written.
    pub(crate) acquired_at_ms: u64,
    /// TTL (ms) stamped by the leader when it wrote the file. Readers use
    /// this to determine liveness without any out-of-band configuration.
    pub(crate) lease_duration_ms: u64,
    pub(crate) metadata: HashMap<String, String>,
}

impl Lease {
    pub(crate) fn is_valid(&self) -> bool {
        now_ms().saturating_sub(self.acquired_at_ms) < self.lease_duration_ms
    }

    pub(crate) fn into_leader_info(self) -> LeaderInfo {
        LeaderInfo {
            node_id: self.node_id,
            address: self.address,
            term: self.term,
            metadata: self.metadata,
        }
    }
}

// ── Shared helpers ────────────────────────────────────────────────────────────

pub(crate) fn make_lease(
    node_id: &str,
    address: &str,
    metadata: &HashMap<String, String>,
    term: u64,
    config: &Config,
) -> Lease {
    Lease {
        node_id: node_id.to_string(),
        address: address.to_string(),
        term,
        acquired_at_ms: now_ms(),
        lease_duration_ms: config.lease_duration.as_millis() as u64,
        metadata: metadata.clone(),
    }
}

pub(crate) fn encode(lease: &Lease) -> Result<PutPayload> {
    Ok(PutPayload::from(Bytes::from(serde_json::to_vec(lease)?)))
}

pub(crate) async fn read_lease(
    store: &dyn ObjectStore,
    path: &Path,
) -> Result<Option<(Lease, String)>> {
    match store.get(path).await {
        Ok(result) => {
            let etag = result.meta.e_tag.clone().ok_or(Error::NoEtag)?;
            let bytes = result.bytes().await.map_err(Error::Store)?;
            Ok(Some((serde_json::from_slice(&bytes)?, etag)))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(Error::Store(e)),
    }
}

pub(crate) fn lock_path(prefix: &str) -> Path {
    Path::from(format!("{prefix}/leader"))
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
