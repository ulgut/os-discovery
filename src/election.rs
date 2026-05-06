use std::{collections::HashMap, sync::Arc, time::Duration};

use object_store::{ObjectStore, PutMode, PutOptions, UpdateVersion, path::Path};
use rand::Rng;
use tokio::{sync::watch, task::JoinHandle, time};
use tracing::{debug, info, warn};

use crate::{
    config::Config,
    error::{Error, Result},
    lease::{LeaderInfo, Lease, encode, lock_path, make_lease, read_lease},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    Follower,
    Leader,
}

/// Snapshot of this node's current election state.
#[derive(Debug, Clone)]
pub struct State {
    pub role: Role,
    pub term: u64,
    /// Populated when `role == Follower` and a valid leader is known.
    pub leader: Option<LeaderInfo>,
}

// ── LeaderElection ───────────────────────────────────────────────────────────

/// A participant in leader election backed by a single object-store lockfile.
///
/// The lockfile lives at `{prefix}/leader` within whatever bucket the
/// [`ObjectStore`] is configured with. All nodes in a cluster must use the
/// same `store` + `prefix` pair to participate in the same election.
///
/// ## Protocol
///
/// **Follower** — polls the lockfile every `check_interval`:
/// - File absent or embedded TTL expired → wait a random jitter, then race to
///   write the lockfile via a conditional put.
/// - File present and fresh → sleep and poll again.
///
/// **Leader** — rewrites the lockfile every `heartbeat_interval` via an
/// ETag-conditional put. A lost race means another node usurped leadership;
/// the node steps back to Follower.
pub struct LeaderElection {
    store: Arc<dyn ObjectStore>,
    lock_path: Path,
    state_rx: watch::Receiver<State>,
    task: JoinHandle<()>,
}

impl LeaderElection {
    /// Start participating in the election.
    ///
    /// * `store`    — backend; the bucket is encoded in the store's configuration.
    /// * `prefix`   — path prefix within the bucket; lockfile = `{prefix}/leader`.
    /// * `node_id`  — stable unique identifier for this node.
    /// * `address`  — how clients can reach this node when it is leader.
    /// * `metadata` — arbitrary key/value pairs published alongside the lease.
    pub fn start(
        store: Arc<dyn ObjectStore>,
        prefix: impl AsRef<str>,
        config: Config,
        node_id: impl Into<String>,
        address: impl Into<String>,
        metadata: HashMap<String, String>,
    ) -> Self {
        let lp = lock_path(prefix.as_ref());
        let node_id = node_id.into();
        let address = address.into();

        let (state_tx, state_rx) = watch::channel(State {
            role: Role::Follower,
            term: 0,
            leader: None,
        });

        let task = tokio::spawn(election_loop(
            store.clone(),
            lp.clone(),
            config,
            node_id,
            address,
            metadata,
            state_tx,
        ));

        Self {
            store,
            lock_path: lp,
            state_rx,
            task,
        }
    }

    /// Subscribe to role / leader changes.
    pub fn state(&self) -> watch::Receiver<State> {
        self.state_rx.clone()
    }

    /// Returns `true` if this node currently holds the leader lease.
    pub fn is_leader(&self) -> bool {
        self.state_rx.borrow().role == Role::Leader
    }

    /// Read the lockfile directly. Safe to call from nodes that are not
    /// running an election loop (pure-client observers).
    pub async fn current_leader(&self) -> Result<Option<LeaderInfo>> {
        match read_lease(&*self.store, &self.lock_path).await? {
            Some((lease, _)) if lease.is_valid() => Ok(Some(lease.into_leader_info())),
            _ => Ok(None),
        }
    }

    /// Voluntarily step down by deleting the lockfile.
    ///
    /// Followers detect the missing file on their next poll and immediately
    /// start their jitter timers, so a successor is elected promptly.
    pub async fn resign(&self) -> Result<()> {
        match self.store.delete(&self.lock_path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(Error::Store(e)),
        }
    }
}

impl Drop for LeaderElection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

// ── State machine ────────────────────────────────────────────────────────────

async fn election_loop(
    store: Arc<dyn ObjectStore>,
    lock_path: Path,
    config: Config,
    node_id: String,
    address: String,
    metadata: HashMap<String, String>,
    state_tx: watch::Sender<State>,
) {
    let mut current_etag: Option<String> = None;
    let mut term = 0u64;
    let mut role = Role::Follower;

    loop {
        match role {
            Role::Follower => match read_lease(&*store, &lock_path).await {
                Err(e) => {
                    warn!(error = %e, "failed to read lockfile; will retry");
                    time::sleep(config.check_interval).await;
                }

                Ok(Some((lease, _))) if lease.is_valid() && lease.node_id != node_id => {
                    term = term.max(lease.term);
                    set_state(
                        &state_tx,
                        Role::Follower,
                        term,
                        Some(lease.into_leader_info()),
                    );
                    time::sleep(config.check_interval).await;
                }

                // Our own lease, still valid (crash/restart fast path).
                Ok(Some((lease, etag))) if lease.is_valid() => {
                    term = term.max(lease.term);
                    current_etag = Some(etag);
                    role = Role::Leader;
                    info!(term, node_id, "reclaimed leadership after restart");
                    set_state(&state_tx, Role::Leader, term, None);
                }

                // Absent or expired: random jitter then race.
                Ok(maybe_expired) => {
                    if let Some((expired, _)) = &maybe_expired {
                        // Track the expired term so we never write a regression.
                        term = term.max(expired.term);
                    }
                    set_state(&state_tx, Role::Follower, term, None);
                    time::sleep(random_jitter(&config)).await;

                    let new_term = term + 1;
                    let lease = make_lease(&node_id, &address, &metadata, new_term, &config);
                    let mode = match maybe_expired {
                        None => PutMode::Create,
                        Some((_, old_etag)) => PutMode::Update(UpdateVersion {
                            e_tag: Some(old_etag),
                            version: None,
                        }),
                    };

                    match put_lease(&*store, &lock_path, &lease, mode).await {
                        Ok(new_etag) => {
                            term = new_term;
                            current_etag = Some(new_etag);
                            role = Role::Leader;
                            info!(term, node_id, "became leader");
                            set_state(&state_tx, Role::Leader, term, None);
                        }
                        Err(e) => {
                            debug!(error = %e, "election attempt lost; staying follower");
                        }
                    }
                }
            },

            Role::Leader => {
                time::sleep(config.heartbeat_interval).await;

                let Some(etag) = current_etag.as_deref() else {
                    warn!("leader has no etag; stepping down");
                    current_etag = None;
                    role = Role::Follower;
                    set_state(&state_tx, Role::Follower, term, None);
                    continue;
                };

                let lease = make_lease(&node_id, &address, &metadata, term, &config);
                let mode = PutMode::Update(UpdateVersion {
                    e_tag: Some(etag.to_string()),
                    version: None,
                });

                match put_lease(&*store, &lock_path, &lease, mode).await {
                    Ok(new_etag) => {
                        current_etag = Some(new_etag);
                        debug!(term, node_id, "renewed leader lease");
                    }
                    Err(e) => {
                        warn!(error = %e, term, node_id, "lease renewal failed; stepping down");
                        current_etag = None;
                        role = Role::Follower;
                        set_state(&state_tx, Role::Follower, term, None);
                    }
                }
            }
        }
    }
}

// ── Storage ──────────────────────────────────────────────────────────────────

/// Conditionally write a lease. The mode encodes the precondition:
/// `Create` (file must not exist) or `Update(etag)` (file must match etag).
async fn put_lease(
    store: &dyn ObjectStore,
    path: &Path,
    lease: &Lease,
    mode: PutMode,
) -> Result<String> {
    match store
        .put_opts(
            path,
            encode(lease)?,
            PutOptions {
                mode,
                ..Default::default()
            },
        )
        .await
    {
        Ok(r) => r.e_tag.ok_or(Error::NoEtag),
        Err(object_store::Error::AlreadyExists { .. })
        | Err(object_store::Error::Precondition { .. }) => Err(Error::Conflict),
        Err(e) => Err(Error::Store(e)),
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn random_jitter(config: &Config) -> Duration {
    let min = config.election_timeout_min.as_millis() as u64;
    let max = config.election_timeout_max.as_millis() as u64;
    Duration::from_millis(rand::rng().random_range(min..=max))
}

fn set_state(tx: &watch::Sender<State>, role: Role, term: u64, leader: Option<LeaderInfo>) {
    tx.send_if_modified(|prev| {
        let changed = prev.role != role
            || prev.term != term
            || prev.leader.as_ref().map(|l| &l.node_id) != leader.as_ref().map(|l| &l.node_id);
        if changed {
            *prev = State { role, term, leader };
        }
        changed
    });
}
