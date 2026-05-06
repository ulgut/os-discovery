//! Two-node demo using the in-memory backend.
//!
//! Run with: cargo run --example demo
use object_store::memory::InMemory;
use os_discovery::{Config, LeaderElection};
use std::{collections::HashMap, sync::Arc, time::Duration};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // Both nodes share one in-memory store — swap for S3/GCS/Azure in prod.
    let store = Arc::new(InMemory::new());

    let config = Config {
        lease_duration: Duration::from_secs(6),
        heartbeat_interval: Duration::from_secs(2),
        election_timeout_min: Duration::from_millis(500),
        election_timeout_max: Duration::from_millis(1500),
        check_interval: Duration::from_millis(500),
    };

    // The bucket is encoded in the ObjectStore; `prefix` scopes the lockfile path.
    let node_a = LeaderElection::start(
        store.clone(),
        "demo",
        config.clone(),
        "node-a",
        "10.0.0.1:8080",
        HashMap::new(),
    );

    let node_b = LeaderElection::start(
        store.clone(),
        "demo",
        config,
        "node-b",
        "10.0.0.2:8080",
        HashMap::new(),
    );

    let mut state_a = node_a.state();
    let mut state_b = node_b.state();

    let deadline = tokio::time::sleep(Duration::from_secs(30));
    tokio::pin!(deadline);

    let mut crashed = false;

    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = state_a.changed() => {
                let s = state_a.borrow_and_update();
                println!(
                    "[node-a] {:?}  term={}  leader={:?}",
                    s.role, s.term, s.leader.as_ref().map(|l| &l.node_id)
                );
            }
            _ = state_b.changed() => {
                let s = state_b.borrow_and_update();
                println!(
                    "[node-b] {:?}  term={}  leader={:?}",
                    s.role, s.term, s.leader.as_ref().map(|l| &l.node_id)
                );
            }
        }

        // Simulate leader failure once: whichever node wins first, resign it
        // so we can watch the other take over.
        if !crashed && node_a.is_leader() {
            crashed = true;
            println!("\n--- node-a won; resigning to simulate failure ---\n");
            node_a.resign().await.unwrap();
        } else if !crashed && node_b.is_leader() {
            crashed = true;
            println!("\n--- node-b won; resigning to simulate failure ---\n");
            node_b.resign().await.unwrap();
        }
    }
}
