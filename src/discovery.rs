use crate::{
    config::DiscoveryConfig,
    error::{Error, Result},
    node::{now_ms, NodeInfo},
};
use bytes::Bytes;
use futures::StreamExt;
use object_store::{path::Path, ObjectStore, PutPayload};
use std::{collections::HashMap, sync::Arc};
use tokio::task::JoinHandle;
use tracing::warn;

pub struct DiscoveryService {
    store: Arc<dyn ObjectStore>,
    config: DiscoveryConfig,
    node_id: String,
    heartbeat: JoinHandle<()>,
}

impl DiscoveryService {
    /// Register this node and begin heartbeating until dropped or `deregister` is called.
    pub async fn register(
        store: Arc<dyn ObjectStore>,
        config: DiscoveryConfig,
        node_id: impl Into<String>,
        address: impl Into<String>,
        metadata: HashMap<String, String>,
    ) -> Result<Self> {
        let node_id = node_id.into();
        let address = address.into();

        write_presence(&store, &config, &node_id, &address, &metadata).await?;

        let heartbeat = {
            let (store, config, node_id, address, metadata) = (
                store.clone(),
                config.clone(),
                node_id.clone(),
                address.clone(),
                metadata.clone(),
            );
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(config.heartbeat_interval);
                interval.tick().await; // skip immediate first tick
                loop {
                    interval.tick().await;
                    if let Err(e) =
                        write_presence(&store, &config, &node_id, &address, &metadata).await
                    {
                        warn!(error = %e, node_id, "failed to refresh node presence");
                    }
                }
            })
        };

        Ok(Self { store, config, node_id, heartbeat })
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Return all nodes that have heartbeated within the configured TTL.
    pub async fn list_nodes(&self) -> Result<Vec<NodeInfo>> {
        let prefix = Path::from(format!("{}/nodes", self.config.namespace));
        let ttl = self.config.node_ttl;
        let mut stream = self.store.list(Some(&prefix));
        let mut nodes = Vec::new();

        while let Some(meta) = stream.next().await.transpose().map_err(Error::Store)? {
            match self.store.get(&meta.location).await {
                Ok(result) => match result.bytes().await {
                    Ok(bytes) => match serde_json::from_slice::<NodeInfo>(&bytes) {
                        Ok(info) if info.is_alive(ttl) => nodes.push(info),
                        Ok(_) => {} // expired entry, skip
                        Err(e) => warn!(error = %e, path = ?meta.location, "corrupt node entry"),
                    },
                    Err(e) => warn!(error = %e, "failed to read node entry"),
                },
                // Deleted between list and get — harmless
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(Error::Store(e)),
            }
        }

        Ok(nodes)
    }

    /// Remove this node's presence entry and stop heartbeating.
    pub async fn deregister(self) -> Result<()> {
        self.heartbeat.abort();
        let path = node_path(&self.config, &self.node_id);
        self.store.delete(&path).await.map_err(Error::Store)
    }
}

impl Drop for DiscoveryService {
    fn drop(&mut self) {
        self.heartbeat.abort();
    }
}

async fn write_presence(
    store: &Arc<dyn ObjectStore>,
    config: &DiscoveryConfig,
    node_id: &str,
    address: &str,
    metadata: &HashMap<String, String>,
) -> Result<()> {
    let info = NodeInfo {
        node_id: node_id.to_string(),
        address: address.to_string(),
        last_heartbeat_ms: now_ms(),
        metadata: metadata.clone(),
    };
    let path = node_path(config, node_id);
    let payload = PutPayload::from(Bytes::from(serde_json::to_vec(&info)?));
    store.put(&path, payload).await.map_err(Error::Store)?;
    Ok(())
}

fn node_path(config: &DiscoveryConfig, node_id: &str) -> Path {
    Path::from(format!("{}/nodes/{}", config.namespace, node_id))
}
