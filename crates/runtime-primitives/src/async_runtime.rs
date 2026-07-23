use std::{
    fmt::{Display, Formatter},
    future::Future,
    time::Duration,
};
use tokio::runtime::{Builder, Handle, Runtime};
use tokio_util::sync::CancellationToken;

pub const MAX_RUNTIME_WORKER_THREADS: usize = 64;
pub const MAX_RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AsyncRuntimeConfig {
    pub worker_threads: usize,
    pub shutdown_timeout: Duration,
}

impl Default for AsyncRuntimeConfig {
    fn default() -> Self {
        Self {
            worker_threads: std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1)
                .min(8),
            shutdown_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AsyncRuntimeError {
    PolicyInvalid,
    BuildFailed,
}

impl AsyncRuntimeError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::PolicyInvalid => "RUNTIME_ASYNC_POLICY_INVALID",
            Self::BuildFailed => "RUNTIME_ASYNC_BUILD_FAILED",
        }
    }
}

impl Display for AsyncRuntimeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for AsyncRuntimeError {}

pub struct AsyncRuntime {
    runtime: Option<Runtime>,
    cancellation: CancellationToken,
    shutdown_timeout: Duration,
}

impl AsyncRuntime {
    pub fn new() -> Result<Self, AsyncRuntimeError> {
        Self::with_config(AsyncRuntimeConfig::default())
    }

    pub fn with_config(config: AsyncRuntimeConfig) -> Result<Self, AsyncRuntimeError> {
        if config.worker_threads == 0
            || config.worker_threads > MAX_RUNTIME_WORKER_THREADS
            || config.shutdown_timeout > MAX_RUNTIME_SHUTDOWN_TIMEOUT
        {
            return Err(AsyncRuntimeError::PolicyInvalid);
        }
        let mut builder = Builder::new_multi_thread();
        builder
            .worker_threads(config.worker_threads)
            .thread_name("cmclient-async")
            .enable_all();
        let runtime = builder
            .build()
            .map_err(|_| AsyncRuntimeError::BuildFailed)?;
        Ok(Self {
            runtime: Some(runtime),
            cancellation: CancellationToken::new(),
            shutdown_timeout: config.shutdown_timeout,
        })
    }

    pub fn handle(&self) -> &Handle {
        self.runtime().handle()
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub fn request_shutdown(&self) {
        self.cancellation.cancel();
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runtime().block_on(future)
    }

    pub fn shutdown(mut self) {
        self.shutdown_inner();
    }

    fn runtime(&self) -> &Runtime {
        self.runtime
            .as_ref()
            .expect("runtime exists until bounded shutdown begins")
    }

    fn shutdown_inner(&mut self) {
        self.cancellation.cancel();
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(self.shutdown_timeout);
        }
    }
}

impl Drop for AsyncRuntime {
    fn drop(&mut self) {
        self.shutdown_inner();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    #[test]
    fn runtime_executes_time_and_exposes_one_shared_cancellation_root() {
        let runtime = AsyncRuntime::new().unwrap();
        let first = runtime.cancellation_token();
        let second = runtime.cancellation_token();
        let value = runtime.block_on(async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            42
        });
        assert_eq!(value, 42);
        assert!(!first.is_cancelled());
        runtime.request_shutdown();
        runtime.block_on(first.cancelled());
        assert!(second.is_cancelled());
        runtime.shutdown();
    }

    #[test]
    fn runtime_policy_is_bounded_and_errors_are_stable() {
        for config in [
            AsyncRuntimeConfig {
                worker_threads: 0,
                shutdown_timeout: Duration::ZERO,
            },
            AsyncRuntimeConfig {
                worker_threads: MAX_RUNTIME_WORKER_THREADS + 1,
                shutdown_timeout: Duration::ZERO,
            },
            AsyncRuntimeConfig {
                worker_threads: 1,
                shutdown_timeout: MAX_RUNTIME_SHUTDOWN_TIMEOUT + Duration::from_millis(1),
            },
        ] {
            assert!(matches!(
                AsyncRuntime::with_config(config),
                Err(AsyncRuntimeError::PolicyInvalid)
            ));
        }
        for error in [
            AsyncRuntimeError::PolicyInvalid,
            AsyncRuntimeError::BuildFailed,
        ] {
            assert_eq!(error.to_string(), error.code());
        }
    }

    #[test]
    fn runtime_drives_loopback_network_io() {
        let runtime = AsyncRuntime::new().unwrap();
        runtime.block_on(async {
            let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 4];
                stream.read_exact(&mut request).await.unwrap();
                assert_eq!(&request, b"ping");
                stream.write_all(b"pong").await.unwrap();
            });
            let mut client = TcpStream::connect(address).await.unwrap();
            client.write_all(b"ping").await.unwrap();
            let mut response = [0_u8; 4];
            client.read_exact(&mut response).await.unwrap();
            assert_eq!(&response, b"pong");
            server.await.unwrap();
        });
    }

    #[test]
    fn dropping_runtime_cancels_the_shared_root() {
        let runtime = AsyncRuntime::new().unwrap();
        let cancellation = runtime.cancellation_token();
        assert!(!cancellation.is_cancelled());

        drop(runtime);

        assert!(cancellation.is_cancelled());
    }
}
