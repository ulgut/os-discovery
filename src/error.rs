use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("object store: {0}")]
    Store(#[from] object_store::Error),

    #[error("serialization: {0}")]
    Json(#[from] serde_json::Error),

    /// Lost a conditional-write race to another node.
    #[error("conditional write conflict")]
    Conflict,

    /// The backend returned no ETag after a write, which is required for
    /// conditional renewal. Use a backend that supports ETags (S3, GCS, Azure Blob).
    #[error("backend returned no ETag (required for conditional writes)")]
    NoEtag,

    /// The lockfile is absent or its embedded TTL has expired; no current leader.
    #[error("no current leader")]
    NoLeader,
}

pub type Result<T> = std::result::Result<T, Error>;
