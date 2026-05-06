use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub address: String,
    /// Unix timestamp (ms) of the last successful heartbeat write.
    pub last_heartbeat_ms: u64,
    pub metadata: HashMap<String, String>,
}

impl NodeInfo {
    pub fn is_alive(&self, ttl: Duration) -> bool {
        now_ms().saturating_sub(self.last_heartbeat_ms) < ttl.as_millis() as u64
    }
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
