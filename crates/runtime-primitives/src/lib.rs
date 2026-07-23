//! Shared, bounded runtime primitives for durable documents, file locks, and async execution.

mod async_runtime;
mod document;
mod lock;

pub use async_runtime::{
    AsyncRuntime, AsyncRuntimeConfig, AsyncRuntimeError, MAX_RUNTIME_SHUTDOWN_TIMEOUT,
    MAX_RUNTIME_WORKER_THREADS,
};
pub use document::{
    DocumentError, DocumentFormat, DurableDocument, StagedDocumentWrite, TypedDocument,
};
pub use lock::{ExclusiveFileLock, LockError};
