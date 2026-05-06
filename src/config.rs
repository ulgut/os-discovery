use std::time::Duration;

/// Tuning parameters for a [`LeaderElection`] participant.
///
/// [`LeaderElection`]: crate::LeaderElection
#[derive(Debug, Clone)]
pub struct Config {
    /// How long the lockfile lease is valid without renewal.
    /// Must be significantly longer than `heartbeat_interval`.
    pub lease_duration: Duration,

    /// How often the current leader refreshes the lockfile.
    /// Rule of thumb: `lease_duration / 3`.
    pub heartbeat_interval: Duration,

    /// Lower bound randomized election timeout
    pub election_timeout_min: Duration,

    /// Upper bound randomized election timeout
    pub election_timeout_max: Duration,

    /// How often followers poll the lockfile while waiting for a leader.
    pub check_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            lease_duration: Duration::from_secs(15),
            heartbeat_interval: Duration::from_secs(5),
            election_timeout_min: Duration::from_secs(10),
            election_timeout_max: Duration::from_secs(20),
            check_interval: Duration::from_secs(3),
        }
    }
}
