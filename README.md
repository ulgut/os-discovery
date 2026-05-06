# os_discovery

A simple leader election and service discovery library/client built on top of modern cloud object stores. A
single lockfile under `{prefix}/leader` is the source of truth; competing
nodes serialize via S3-style conditional writes (`If-None-Match: *` and
`If-Match: <etag>`).

## Why

I wanted to build a simple library that could be used for basic cluster discovery and leader election without the immense hassle of dealing with ZK, etc.

## Backends

Anything that implements `object_store::ObjectStore` and returns ETags:

- Amazon S3 and S3-compatible (MinIO, etc.)
- Google Cloud Storage
- Azure Blob
- In-memory (for tests)

## Election

```rust
use std::{collections::HashMap, sync::Arc};
use object_store::aws::AmazonS3Builder;
use os_discovery::{Config, LeaderElection};

#[tokio::main]
async fn main() {
    let store = Arc::new(
        AmazonS3Builder::from_env()
            .with_bucket_name("my-bucket")
            .build()
            .unwrap(),
    );

    let election = LeaderElection::start(
        store,
        "my-service",        // lockfile path = my-service/leader
        Config::default(),
        "node-1",            // unique node id
        "10.0.0.1:8080",     // how clients reach us
        HashMap::new(),      // optional metadata
    );

    // Synchronous check
    if election.is_leader() {
        println!("term={}", election.state().borrow().term);
    }

    // Subscribe to changes
    let mut state = election.state();
    while state.changed().await.is_ok() {
        let s = state.borrow_and_update();
        println!("role={:?}, leader={:?}", s.role, s.leader);
    }
}
```

## Client

`LeaderClient` reads the lockfile, caches the leader's address for a TTL of
your choice, and auto-invalidates on transport errors that suggest the leader
is gone. The transport is generic — pass a closure that takes an address and
returns a future.

### HTTP (`--features http`)

```rust
use os_discovery::LeaderClient;
use std::{sync::Arc, time::Duration};

let client = LeaderClient::new(store, "my-service", Duration::from_secs(5));

let resp = client
    .with_leader(|addr| async move {
        reqwest::get(format!("http://{addr}/api/v1/status")).await
    })
    .await?;
```

With the `http` feature, `ShouldInvalidate` is implemented for `reqwest::Error`
automatically. Connection errors and 502/503/504 responses trigger one cache
invalidation + retry.

### gRPC

Implement `ShouldInvalidate` for your transport error:

```rust
use os_discovery::ShouldInvalidate;

impl ShouldInvalidate for tonic::Status {
    fn should_invalidate(&self) -> bool {
        matches!(self.code(), tonic::Code::Unavailable | tonic::Code::Unknown)
    }
}

let resp = client
    .with_leader(|addr| async move {
        MyServiceClient::connect(format!("http://{addr}"))
            .await
            .map_err(Into::into)?           // adapt connect error
            .my_rpc(Request::new(payload))
            .await
    })
    .await?;
```

## Protocol

Each follower polls the lockfile every `check_interval`:

| Lockfile state | Action |
|---|---|
| Absent | Wait random jitter, conditional `Create` |
| Present, embedded TTL expired | Wait random jitter, conditional `Update(etag)` |
| Present, fresh | Sleep, poll again |

The leader rewrites the lockfile every `heartbeat_interval` via an
ETag-conditional put. A lost race (another node usurped leadership) demotes the
node back to Follower.

The TTL is stamped *into* the lockfile by the leader. Followers determine
liveness from that field.

## Configuration

```rust
Config {
    lease_duration: Duration::from_secs(15),         // TTL written into the file
    heartbeat_interval: Duration::from_secs(5),      // leader renewal cadence
    election_timeout_min: Duration::from_secs(10),   // jitter floor
    election_timeout_max: Duration::from_secs(20),   // jitter ceiling
    check_interval: Duration::from_secs(3),          // follower poll period
}
```

Rules of thumb:
- `heartbeat_interval ≈ lease_duration / 3` — leader has 3 attempts to renew
  before the lease expires.
- `election_timeout_min > lease_duration` — followers wait at least one full
  lease period before challenging, giving the current leader time to recover
  from a slow heartbeat.