use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use object_store::ObjectStore;

use crate::{
    error::{Error, Result},
    lease::{lock_path, read_lease},
};

// ── Transport abstraction ─────────────────────────────────────────────────────

/// Implemented by transport error types to indicate that the cached leader
/// address should be discarded before retrying.
///
/// # Examples
///
/// ```rust,ignore
/// // reqwest (enabled automatically with the `http` feature)
/// // tonic / gRPC:
/// impl ShouldInvalidate for tonic::Status {
///     fn should_invalidate(&self) -> bool {
///         matches!(self.code(), tonic::Code::Unavailable | tonic::Code::Unknown)
///     }
/// }
/// ```
pub trait ShouldInvalidate {
    fn should_invalidate(&self) -> bool;
}

/// Returned by [`LeaderClient::with_leader`]; separates leader-discovery
/// failures from transport-layer failures.
#[derive(Debug)]
pub enum LeaderError<E> {
    /// Could not determine the current leader (lockfile absent, expired, or
    /// unreadable). Inner value is the underlying [`Error`].
    Discovery(Error),
    /// The transport returned an error after the leader address was resolved.
    Transport(E),
}

impl<E: std::fmt::Display> std::fmt::Display for LeaderError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Discovery(e) => write!(f, "leader discovery: {e}"),
            Self::Transport(e) => write!(f, "transport: {e}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for LeaderError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Discovery(e) => Some(e),
            Self::Transport(e) => Some(e),
        }
    }
}

// ── LeaderClient ──────────────────────────────────────────────────────────────

struct CachedLeader {
    address: String,
    valid_until: Instant,
}

/// Transport-agnostic client that resolves the current leader address from
/// the lockfile and caches it with a configurable TTL.
///
/// Use [`with_leader`](Self::with_leader) to dispatch requests; the client
/// handles cache invalidation and a single retry automatically.
///
/// # Transport examples
///
/// ```rust,ignore
/// // HTTP (reqwest) ─────────────────────────────────────────────────────────
/// let resp = client
///     .with_leader(|addr| async move {
///         reqwest::get(format!("http://{addr}/api/v1/status")).await
///     })
///     .await?;
///
/// // gRPC (tonic) ───────────────────────────────────────────────────────────
/// let resp = client
///     .with_leader(|addr| async move {
///         let mut svc = MyServiceClient::connect(format!("http://{addr}")).await?;
///         svc.my_rpc(Request::new(payload)).await
///     })
///     .await?;
/// ```
pub struct LeaderClient {
    store: Arc<dyn ObjectStore>,
    lock_path: object_store::path::Path,
    cached: Mutex<Option<CachedLeader>>,
    cache_ttl: Duration,
}

impl LeaderClient {
    /// Create a client that reads from the same lockfile as [`LeaderElection`].
    ///
    /// * `prefix`    — must match the `prefix` used with [`LeaderElection::start`].
    /// * `cache_ttl` — how long a resolved leader address is reused before
    ///   re-reading the lockfile. Shorter means faster failover detection at
    ///   the cost of more object-store reads.
    ///
    /// [`LeaderElection`]: crate::LeaderElection
    /// [`LeaderElection::start`]: crate::LeaderElection::start
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl AsRef<str>, cache_ttl: Duration) -> Self {
        Self {
            store,
            lock_path: lock_path(prefix.as_ref()),
            cached: Mutex::new(None),
            cache_ttl,
        }
    }

    /// Return the current leader address, using the cache when still valid.
    pub async fn leader_address(&self) -> Result<String> {
        if let Some(addr) = self.cached_address() {
            return Ok(addr);
        }
        self.refresh().await
    }

    /// Discard the cached leader address. The next call to
    /// [`leader_address`](Self::leader_address) or
    /// [`with_leader`](Self::with_leader) will re-read the lockfile.
    ///
    /// Call this when you receive an error that suggests the current leader is
    /// unreachable; `with_leader` does this automatically.
    pub fn invalidate(&self) {
        if let Ok(mut g) = self.cached.lock() {
            *g = None;
        }
    }

    /// Dispatch a request to the current leader.
    ///
    /// 1. Resolve the leader address (from cache or lockfile).
    /// 2. Call `f(address)`.
    /// 3. If the returned error satisfies [`ShouldInvalidate::should_invalidate`],
    ///    invalidate the cache and retry **once** with a fresh address.
    /// 4. Return the result of the second attempt regardless.
    pub async fn with_leader<F, Fut, T, E>(&self, f: F) -> std::result::Result<T, LeaderError<E>>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = std::result::Result<T, E>>,
        E: ShouldInvalidate,
    {
        // First attempt — use cached address if available.
        let address = self
            .leader_address()
            .await
            .map_err(LeaderError::Discovery)?;
        match f(address).await {
            Ok(val) => return Ok(val),
            Err(e) if !e.should_invalidate() => return Err(LeaderError::Transport(e)),
            Err(_) => self.invalidate(),
        }

        // Retry once with a freshly-resolved address.
        let address = self
            .leader_address()
            .await
            .map_err(LeaderError::Discovery)?;
        f(address).await.map_err(LeaderError::Transport)
    }

    fn cached_address(&self) -> Option<String> {
        let g = self.cached.lock().ok()?;
        let c = g.as_ref()?;
        (Instant::now() < c.valid_until).then(|| c.address.clone())
    }

    async fn refresh(&self) -> Result<String> {
        match read_lease(&*self.store, &self.lock_path).await? {
            Some((lease, _)) if lease.is_valid() => {
                let address = lease.address.clone();
                if let Ok(mut g) = self.cached.lock() {
                    *g = Some(CachedLeader {
                        address: address.clone(),
                        valid_until: Instant::now() + self.cache_ttl,
                    });
                }
                Ok(address)
            }
            _ => Err(Error::NoLeader),
        }
    }
}

// ── reqwest integration (feature = "http") ───────────────────────────────────

/// Responses with these status codes indicate the node we reached is not the
/// real leader or is temporarily unavailable — invalidate and retry.
#[cfg(feature = "http")]
const HTTP_LEADER_CACHE_INVALIDATING_STATUSES: &[u16] = &[502, 503, 504];

#[cfg(feature = "http")]
impl ShouldInvalidate for reqwest::Error {
    fn should_invalidate(&self) -> bool {
        if self.is_connect() || self.is_timeout() {
            return true;
        }
        self.status()
            .map(|s| HTTP_LEADER_CACHE_INVALIDATING_STATUSES.contains(&s.as_u16()))
            .unwrap_or(false)
    }
}
