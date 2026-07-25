//! Process supervision primitives owned by the Rust Agent.

use cmclient_runtime_logging::{
    ChildOutputCapture, RuntimeLogError, StructuredLogSink, WriteHealthSnapshot,
};
use hmac::{Hmac, Mac};
#[cfg(unix)]
use process_wrap::std::ProcessGroup;
use process_wrap::std::{ChildWrapper, CommandWrap};
#[cfg(windows)]
use process_wrap::std::{CommandWrapper, JobObject};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::{
    collections::BTreeMap,
    fmt,
    io::{ErrorKind, Read, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream},
    process::{Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};
use zeroize::Zeroizing;

/// Stable workspace identity for the supervisor boundary.
pub const COMPONENT: &str = "supervisor";
/// Runtime a restarted child must survive before its crash counter is cleared.
pub const DEFAULT_STABLE_WINDOW: Duration = Duration::from_secs(30);
#[cfg(not(test))]
const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(40);
#[cfg(test)]
const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(25);
const SHUTDOWN_COMMAND: &[u8] = b"CMCLIENT_SHUTDOWN\n";
pub const GATEWAY_PRIVATE_FRAME_MAX_BYTES: usize = 16 * 1024;
pub const GATEWAY_CALLMESH_API_KEY_MAX_BYTES: usize = 4096;
pub const DEFAULT_GATEWAY_BOOTSTRAP_DEADLINE: Duration = Duration::from_secs(30);
const GATEWAY_OWNERSHIP_RESPONSE_MAX_BYTES: usize = 4096;
const GATEWAY_OWNERSHIP_PATH: &str = "/_cmclient/bootstrap/ownership";
const GATEWAY_OWNERSHIP_CHALLENGE_HEADER: &str = "x-cmclient-gateway-ownership-challenge";
const GATEWAY_OWNERSHIP_PROOF_HEADER: &str = "x-cmclient-gateway-ownership-proof";
const GATEWAY_OWNERSHIP_PROTOCOL: &str = "cmclient-bootstrap-ownership-v1";
const GATEWAY_OWNERSHIP_DOMAIN: &str = "cmclient.gateway.bootstrap-ownership.v1";
const CALLMESH_API_KEY_ENVIRONMENT_NAME: &str = "CMCLIENT_CALLMESH_API_KEY";
const INHERITED_RUNTIME_ENVIRONMENT_NAMES: [&str; 3] = ["SystemRoot", "WINDIR", "ComSpec"];

#[cfg(windows)]
#[derive(Debug, Clone, Copy)]
struct SpawnFailureCleanup;

#[cfg(windows)]
impl CommandWrapper for SpawnFailureCleanup {
    fn wrap_child(
        &mut self,
        child: Box<dyn ChildWrapper>,
        _core: &CommandWrap,
    ) -> std::io::Result<Box<dyn ChildWrapper>> {
        Ok(Box::new(SpawnFailureCleanupChild { child: Some(child) }))
    }
}

#[cfg(windows)]
#[derive(Debug)]
struct SpawnFailureCleanupChild {
    child: Option<Box<dyn ChildWrapper>>,
}

#[cfg(windows)]
impl ChildWrapper for SpawnFailureCleanupChild {
    fn inner(&self) -> &dyn ChildWrapper {
        self.child
            .as_deref()
            .expect("spawn cleanup child must remain available")
    }

    fn inner_mut(&mut self) -> &mut dyn ChildWrapper {
        self.child
            .as_deref_mut()
            .expect("spawn cleanup child must remain available")
    }

    fn into_inner(mut self: Box<Self>) -> Box<dyn ChildWrapper> {
        self.child
            .take()
            .expect("spawn cleanup child must remain available")
    }
}

#[cfg(windows)]
impl Drop for SpawnFailureCleanupChild {
    fn drop(&mut self) {
        let Some(child) = self.child.as_mut() else {
            return;
        };
        child.stdin().take();
        if child.start_kill().is_ok() {
            let _ = child.wait();
        } else {
            let _ = child.try_wait();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayCommand {
    pub program: String,
    pub arguments: Vec<String>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct GatewayReady {
    pub pid: u32,
    pub address: SocketAddr,
    pub startup_nonce: String,
    pub capability: String,
}

impl fmt::Debug for GatewayReady {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewayReady")
            .field("pid", &self.pid)
            .field("address", &self.address)
            .field("startup_nonce", &"[REDACTED]")
            .field("capability", &"[REDACTED]")
            .finish()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GatewayBootstrapFrame<'a> {
    schema_version: u8,
    #[serde(rename = "type")]
    frame_type: &'static str,
    startup_nonce: String,
    capability: String,
    setup_generation: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    callmesh_api_key: Option<&'a str>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GatewayReadyFrame {
    schema_version: u8,
    #[serde(rename = "type")]
    frame_type: String,
    pid: u32,
    startup_nonce: String,
    host: String,
    port: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackoffPolicy {
    pub initial_delay: Duration,
    pub maximum_delay: Duration,
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(1),
            maximum_delay: Duration::from_secs(30),
        }
    }
}

impl BackoffPolicy {
    pub fn delay_for_attempt(self, attempt: u32) -> Duration {
        let exponent = attempt.saturating_sub(1).min(16);
        self.initial_delay
            .checked_mul(2_u32.pow(exponent))
            .unwrap_or(self.maximum_delay)
            .min(self.maximum_delay)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GatewayStatus {
    Stopped,
    Running { pid: u32 },
    Backoff { attempt: u32, delay: Duration },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisorEvent {
    Started {
        pid: u32,
    },
    Heartbeat {
        pid: u32,
    },
    Exited {
        status: Option<i32>,
        restart_delay: Duration,
    },
    Backoff {
        attempt: u32,
        restart_in: Duration,
    },
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisorError {
    EmptyProgram,
    InvalidTimingPolicy,
    SpawnFailed,
    ProcessIoFailed,
    LoggingFailed(&'static str),
    BootstrapInvalid,
    BootstrapTimeout,
    BootstrapIoFailed,
    BootstrapProbeFailed,
}

impl SupervisorError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::EmptyProgram => "GATEWAY_SUPERVISOR_PROGRAM_EMPTY",
            Self::InvalidTimingPolicy => "GATEWAY_SUPERVISOR_TIMING_POLICY_INVALID",
            Self::SpawnFailed => "GATEWAY_SUPERVISOR_SPAWN_FAILED",
            Self::ProcessIoFailed => "GATEWAY_SUPERVISOR_PROCESS_IO_FAILED",
            Self::LoggingFailed(code) => code,
            Self::BootstrapInvalid => "GATEWAY_SUPERVISOR_BOOTSTRAP_INVALID",
            Self::BootstrapTimeout => "GATEWAY_SUPERVISOR_BOOTSTRAP_TIMEOUT",
            Self::BootstrapIoFailed => "GATEWAY_SUPERVISOR_BOOTSTRAP_IO_FAILED",
            Self::BootstrapProbeFailed => "GATEWAY_SUPERVISOR_BOOTSTRAP_PROBE_FAILED",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GatewayLogHealthUpdate {
    pub capture_error_code: Option<&'static str>,
    pub write_error_code: Option<&'static str>,
    pub write_recovered_code: Option<&'static str>,
}

pub struct GatewaySupervisor {
    command: GatewayCommand,
    backoff_policy: BackoffPolicy,
    child: Option<Box<dyn ChildWrapper>>,
    environment: BTreeMap<String, String>,
    log_sink: Option<StructuredLogSink>,
    output_capture: Option<ChildOutputCapture>,
    bootstrap_reader: Option<thread::JoinHandle<()>>,
    pending_capture_log_error_code: Option<&'static str>,
    pending_write_log_error_code: Option<&'static str>,
    pending_write_recovered_code: Option<&'static str>,
    failed_attempts: u32,
    restart_not_before: Option<Instant>,
    stable_window: Duration,
    started_at: Option<Instant>,
    private_bootstrap: bool,
    setup_generation: u64,
    callmesh_api_key: Option<Zeroizing<String>>,
    bootstrap_deadline: Duration,
    ready: Option<GatewayReady>,
}

impl GatewaySupervisor {
    pub fn new(
        command: GatewayCommand,
        backoff_policy: BackoffPolicy,
    ) -> Result<Self, SupervisorError> {
        Self::new_with_stable_window(command, backoff_policy, DEFAULT_STABLE_WINDOW)
    }

    pub fn new_with_stable_window(
        command: GatewayCommand,
        backoff_policy: BackoffPolicy,
        stable_window: Duration,
    ) -> Result<Self, SupervisorError> {
        if command.program.trim().is_empty() {
            return Err(SupervisorError::EmptyProgram);
        }
        let now = Instant::now();
        if backoff_policy.initial_delay.is_zero()
            || backoff_policy.maximum_delay < backoff_policy.initial_delay
            || stable_window.is_zero()
            || now.checked_add(backoff_policy.maximum_delay).is_none()
            || now.checked_add(stable_window).is_none()
        {
            return Err(SupervisorError::InvalidTimingPolicy);
        }
        Ok(Self {
            command,
            backoff_policy,
            child: None,
            environment: inherited_runtime_environment(),
            log_sink: None,
            output_capture: None,
            bootstrap_reader: None,
            pending_capture_log_error_code: None,
            pending_write_log_error_code: None,
            pending_write_recovered_code: None,
            failed_attempts: 0,
            restart_not_before: None,
            stable_window,
            started_at: None,
            private_bootstrap: false,
            setup_generation: 1,
            callmesh_api_key: None,
            bootstrap_deadline: DEFAULT_GATEWAY_BOOTSTRAP_DEADLINE,
            ready: None,
        })
    }

    pub fn status(&self) -> GatewayStatus {
        match self.child.as_ref() {
            Some(child) => GatewayStatus::Running { pid: child.id() },
            None if self.failed_attempts > 0 => GatewayStatus::Backoff {
                attempt: self.failed_attempts,
                delay: self.backoff_policy.delay_for_attempt(self.failed_attempts),
            },
            None => GatewayStatus::Stopped,
        }
    }

    pub fn set_environment(&mut self, mut environment: BTreeMap<String, String>) {
        environment.remove(CALLMESH_API_KEY_ENVIRONMENT_NAME);
        self.environment.remove(CALLMESH_API_KEY_ENVIRONMENT_NAME);
        self.environment.extend(environment);
    }

    pub fn enable_private_bootstrap(&mut self) -> Result<(), SupervisorError> {
        if self.child.is_some() {
            return Err(SupervisorError::ProcessIoFailed);
        }
        self.private_bootstrap = true;
        Ok(())
    }

    pub fn set_setup_generation(&mut self, generation: u64) -> Result<(), SupervisorError> {
        const MAX_SAFE_JAVASCRIPT_INTEGER: u64 = 9_007_199_254_740_991;
        if self.child.is_some() || generation == 0 || generation > MAX_SAFE_JAVASCRIPT_INTEGER {
            return Err(SupervisorError::BootstrapInvalid);
        }
        self.setup_generation = generation;
        Ok(())
    }

    pub const fn configured_setup_generation(&self) -> u64 {
        self.setup_generation
    }

    pub fn set_callmesh_api_key(&mut self, api_key: &str) -> Result<(), SupervisorError> {
        if !self.private_bootstrap
            || self.child.is_some()
            || api_key.is_empty()
            || api_key.len() > GATEWAY_CALLMESH_API_KEY_MAX_BYTES
            || api_key.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(SupervisorError::BootstrapInvalid);
        }
        self.callmesh_api_key = Some(Zeroizing::new(api_key.to_owned()));
        Ok(())
    }

    pub fn gateway_ready(&self) -> Option<&GatewayReady> {
        self.ready.as_ref()
    }

    pub fn set_log_sink(&mut self, sink: StructuredLogSink) -> Result<(), SupervisorError> {
        if self.child.is_some() || self.output_capture.is_some() {
            return Err(SupervisorError::ProcessIoFailed);
        }
        self.log_sink = Some(sink);
        Ok(())
    }

    pub fn take_log_health_update(&mut self) -> GatewayLogHealthUpdate {
        self.collect_sink_log_health();
        GatewayLogHealthUpdate {
            capture_error_code: self.pending_capture_log_error_code.take(),
            write_error_code: self.pending_write_log_error_code.take(),
            write_recovered_code: self.pending_write_recovered_code.take(),
        }
    }

    pub fn start(&mut self) -> Result<SupervisorEvent, SupervisorError> {
        if let Some(child) = self.child.as_ref() {
            return Ok(SupervisorEvent::Heartbeat { pid: child.id() });
        }
        self.spawn_child()
    }

    /// Advances process supervision using the supervisor's monotonic clock.
    ///
    /// A running child is polled once. An exited child enters backoff, and a
    /// later tick starts it only after the internally tracked deadline. Callers
    /// never supply wall-clock or elapsed-time values.
    pub fn tick(&mut self) -> Result<SupervisorEvent, SupervisorError> {
        if self.child.is_some() {
            return self.poll_heartbeat();
        }
        if self.failed_attempts == 0 {
            return Ok(SupervisorEvent::Stopped);
        }

        let now = Instant::now();
        let restart_not_before = self
            .restart_not_before
            .ok_or(SupervisorError::ProcessIoFailed)?;
        if now < restart_not_before {
            return Ok(SupervisorEvent::Backoff {
                attempt: self.failed_attempts,
                restart_in: restart_not_before.duration_since(now),
            });
        }
        self.spawn_child()
    }

    fn spawn_child(&mut self) -> Result<SupervisorEvent, SupervisorError> {
        let capture_output = self.log_sink.is_some();
        let private_bootstrap = self.private_bootstrap;
        let mut command = Command::new(&self.command.program);
        command
            .args(&self.command.arguments)
            .env_clear()
            .envs(&self.environment)
            .stdin(Stdio::piped())
            .stdout(if capture_output || private_bootstrap {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stderr(if capture_output {
                Stdio::piped()
            } else {
                Stdio::null()
            });
        let mut command = CommandWrap::from(command);
        #[cfg(windows)]
        {
            // JobObject spawns suspended; retain a kill-and-reap owner if wrapping fails.
            command.wrap(SpawnFailureCleanup);
            command.wrap(JobObject);
        }
        #[cfg(unix)]
        command.wrap(ProcessGroup::leader());
        let child = command.spawn();
        let mut child = match child {
            Ok(child) => child,
            Err(_) => {
                self.register_failure(Instant::now());
                return Err(SupervisorError::SpawnFailed);
            }
        };
        let ready = if private_bootstrap {
            let (result, reader) = perform_private_bootstrap(
                &mut child,
                self.bootstrap_deadline,
                self.setup_generation,
                self.callmesh_api_key.as_deref().map(String::as_str),
            );
            match result {
                Ok(ready) => {
                    if join_bootstrap_reader(reader).is_err() {
                        let observed_at = Instant::now();
                        self.reject_spawned_child(child, None, observed_at);
                        return Err(SupervisorError::BootstrapIoFailed);
                    }
                    Some(ready)
                }
                Err(error) => {
                    let observed_at = Instant::now();
                    self.reject_spawned_child(child, reader, observed_at);
                    return Err(error);
                }
            }
        } else {
            None
        };
        if let Err(error) = self.capture_child_output(&mut child, private_bootstrap, ready.as_ref())
        {
            let observed_at = Instant::now();
            if terminate_child(&mut child).is_err() {
                self.child = Some(child);
                self.started_at = Some(observed_at);
            } else {
                self.register_failure(observed_at);
            }
            return Err(error);
        }
        let pid = child.id();
        self.ready = ready;
        self.child = Some(child);
        self.restart_not_before = None;
        self.started_at = Some(Instant::now());
        Ok(SupervisorEvent::Started { pid })
    }

    pub fn poll_heartbeat(&mut self) -> Result<SupervisorEvent, SupervisorError> {
        let Some(child) = self.child.as_mut() else {
            return Ok(SupervisorEvent::Stopped);
        };
        let result = child
            .try_wait()
            .map_err(|_| SupervisorError::ProcessIoFailed)?;
        let observed_at = Instant::now();
        let survived_stable_window = self
            .started_at
            .is_some_and(|started_at| observed_at.duration_since(started_at) >= self.stable_window);
        match result {
            None => {
                let pid = child.id();
                if survived_stable_window {
                    self.failed_attempts = 0;
                    self.restart_not_before = None;
                }
                Ok(SupervisorEvent::Heartbeat { pid })
            }
            Some(status) => {
                self.ready = None;
                let mut child = self.child.take().ok_or(SupervisorError::ProcessIoFailed)?;
                if terminate_observed_exited_tree(&mut child).is_err() {
                    self.child = Some(child);
                    return Err(SupervisorError::ProcessIoFailed);
                }
                self.finish_bootstrap_reader();
                self.finish_output_capture();
                if survived_stable_window {
                    self.failed_attempts = 0;
                    self.restart_not_before = None;
                }
                let restart_delay = self.register_failure(observed_at);
                Ok(SupervisorEvent::Exited {
                    status: status.code(),
                    restart_delay,
                })
            }
        }
    }

    fn register_failure(&mut self, observed_at: Instant) -> Duration {
        self.failed_attempts = self.failed_attempts.saturating_add(1);
        self.started_at = None;
        let delay = self.backoff_policy.delay_for_attempt(self.failed_attempts);
        self.restart_not_before = observed_at.checked_add(delay);
        delay
    }

    pub fn stop(&mut self) -> Result<SupervisorEvent, SupervisorError> {
        let Some(mut child) = self.child.take() else {
            self.finish_output_capture();
            self.reset_tracking();
            return Ok(SupervisorEvent::Stopped);
        };
        if terminate_child(&mut child).is_err() {
            self.child = Some(child);
            return Err(SupervisorError::ProcessIoFailed);
        }
        self.ready = None;
        self.finish_bootstrap_reader();
        self.finish_output_capture();
        self.reset_tracking();
        Ok(SupervisorEvent::Stopped)
    }

    fn capture_child_output(
        &mut self,
        child: &mut Box<dyn ChildWrapper>,
        stdout_reserved: bool,
        ready: Option<&GatewayReady>,
    ) -> Result<(), SupervisorError> {
        let Some(sink) = self.log_sink.clone() else {
            return Ok(());
        };
        let stderr = child.stderr().take().ok_or_else(|| {
            let code = RuntimeLogError::CaptureReadFailed.code();
            self.remember_capture_log_error_code(code);
            SupervisorError::LoggingFailed(code)
        })?;
        let secrets = self.child_output_redaction_secrets(ready);
        let capture = if stdout_reserved {
            sink.capture(std::io::empty(), stderr, secrets)
        } else {
            let stdout = child.stdout().take().ok_or_else(|| {
                let code = RuntimeLogError::CaptureReadFailed.code();
                self.remember_capture_log_error_code(code);
                SupervisorError::LoggingFailed(code)
            })?;
            sink.capture(stdout, stderr, secrets)
        };
        self.output_capture = Some(capture.map_err(|error| {
            let code = error.code();
            self.remember_capture_log_error_code(code);
            SupervisorError::LoggingFailed(code)
        })?);
        Ok(())
    }

    fn child_output_redaction_secrets(&self, ready: Option<&GatewayReady>) -> Vec<String> {
        let mut secrets = sensitive_environment_values(&self.environment);
        if let Some(ready) = ready {
            secrets.push(ready.capability.clone());
            secrets.push(ready.startup_nonce.clone());
        }
        if let Some(api_key) = self.callmesh_api_key.as_deref() {
            secrets.push(api_key.to_owned());
        }
        secrets
    }

    fn finish_output_capture(&mut self) {
        if let Some(capture) = self.output_capture.take() {
            if let Err(error) = capture.finish() {
                self.remember_classified_log_error_code(error.code());
            }
        }
        self.collect_sink_log_health();
    }

    fn finish_bootstrap_reader(&mut self) {
        let _ = join_bootstrap_reader(self.bootstrap_reader.take());
    }

    fn reject_spawned_child(
        &mut self,
        mut child: Box<dyn ChildWrapper>,
        reader: Option<thread::JoinHandle<()>>,
        observed_at: Instant,
    ) {
        self.ready = None;
        if force_terminate_child(&mut child).is_ok() {
            let _ = join_bootstrap_reader(reader);
            self.register_failure(observed_at);
        } else {
            self.child = Some(child);
            self.bootstrap_reader = reader;
            self.started_at = Some(observed_at);
        }
    }

    fn collect_sink_log_health(&mut self) {
        let (capture_error_code, write_health) =
            self.log_sink
                .as_ref()
                .map_or((None, WriteHealthSnapshot::default()), |sink| {
                    (
                        sink.take_capture_error_code(),
                        sink.take_write_health_snapshot(),
                    )
                });
        if let Some(code) = capture_error_code {
            self.remember_capture_log_error_code(code);
        }
        if let Some(code) = write_health.write_error_code {
            self.pending_write_recovered_code = None;
            self.pending_write_log_error_code = Some(code);
        }
        if let Some(code) = write_health.write_recovered_code {
            if self
                .pending_write_log_error_code
                .is_some_and(|pending| pending != code)
            {
                self.pending_write_log_error_code = None;
            }
            self.pending_write_recovered_code = Some(code);
        }
    }

    fn remember_classified_log_error_code(&mut self, code: &'static str) {
        if is_capture_log_error_code(code) {
            self.remember_capture_log_error_code(code);
        } else {
            self.remember_write_log_error_code(code);
        }
    }

    fn remember_capture_log_error_code(&mut self, code: &'static str) {
        if self.pending_capture_log_error_code.is_none() {
            self.pending_capture_log_error_code = Some(code);
        }
    }

    fn remember_write_log_error_code(&mut self, code: &'static str) {
        if self.pending_write_log_error_code.is_none() {
            self.pending_write_log_error_code = Some(code);
        }
    }

    fn reset_tracking(&mut self) {
        self.failed_attempts = 0;
        self.restart_not_before = None;
        self.started_at = None;
        self.ready = None;
    }
}

fn is_capture_log_error_code(code: &str) -> bool {
    matches!(
        code,
        "RUNTIME_LOG_CAPTURE_READ_FAILED"
            | "RUNTIME_LOG_CAPTURE_THREAD_FAILED"
            | "RUNTIME_LOG_QUEUE_FULL"
    )
}

fn perform_private_bootstrap(
    child: &mut Box<dyn ChildWrapper>,
    deadline: Duration,
    setup_generation: u64,
    callmesh_api_key: Option<&str>,
) -> (
    Result<GatewayReady, SupervisorError>,
    Option<thread::JoinHandle<()>>,
) {
    let mut reader = None;
    let result = (|| {
        if deadline.is_zero() || deadline > Duration::from_secs(30) {
            return Err(SupervisorError::BootstrapInvalid);
        }
        let bootstrap_deadline = Instant::now()
            .checked_add(deadline)
            .ok_or(SupervisorError::BootstrapInvalid)?;
        let startup_nonce = uuid::Uuid::new_v4().simple().to_string();
        let capability = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let bootstrap = GatewayBootstrapFrame {
            schema_version: 2,
            frame_type: "gateway.bootstrap",
            startup_nonce: startup_nonce.clone(),
            capability: capability.clone(),
            setup_generation,
            callmesh_api_key,
        };
        let encoded = Zeroizing::new(encode_private_frame(&bootstrap)?);
        child
            .stdin()
            .as_mut()
            .ok_or(SupervisorError::BootstrapIoFailed)?
            .write_all(encoded.as_slice())
            .and_then(|()| {
                child
                    .stdin()
                    .as_mut()
                    .ok_or_else(|| std::io::Error::other("child stdin unavailable"))?
                    .flush()
            })
            .map_err(|_| SupervisorError::BootstrapIoFailed)?;
        let mut stdout = child
            .stdout()
            .take()
            .ok_or(SupervisorError::BootstrapIoFailed)?;
        let (sender, receiver) = mpsc::sync_channel(1);
        reader = Some(
            thread::Builder::new()
                .name(String::from("cmclient-gateway-bootstrap"))
                .spawn(move || {
                    let _ = sender.send(read_private_frame(&mut stdout));
                })
                .map_err(|_| SupervisorError::BootstrapIoFailed)?,
        );
        let bytes = receiver
            .recv_timeout(remaining_until(bootstrap_deadline)?)
            .map_err(|error| match error {
                mpsc::RecvTimeoutError::Timeout => SupervisorError::BootstrapTimeout,
                mpsc::RecvTimeoutError::Disconnected => SupervisorError::BootstrapIoFailed,
            })??;
        let ready = validate_gateway_ready(&bytes, child.id(), startup_nonce, capability)?;
        probe_gateway_ownership(&ready, remaining_until(bootstrap_deadline)?)?;
        Ok(ready)
    })();
    (result, reader)
}

fn validate_gateway_ready(
    bytes: &[u8],
    child_pid: u32,
    startup_nonce: String,
    capability: String,
) -> Result<GatewayReady, SupervisorError> {
    let ready: GatewayReadyFrame =
        serde_json::from_slice(bytes).map_err(|_| SupervisorError::BootstrapInvalid)?;
    if ready.schema_version != 1
        || ready.frame_type != "gateway.ready"
        || ready.pid != child_pid
        || ready.startup_nonce != startup_nonce
        || ready.host != "127.0.0.1"
        || ready.port == 0
    {
        return Err(SupervisorError::BootstrapInvalid);
    }
    Ok(GatewayReady {
        pid: ready.pid,
        address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), ready.port),
        startup_nonce,
        capability,
    })
}

fn probe_gateway_ownership(ready: &GatewayReady, timeout: Duration) -> Result<(), SupervisorError> {
    if timeout.is_zero()
        || ready.capability.len() != 64
        || !ready
            .capability
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(SupervisorError::BootstrapProbeFailed);
    }
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or(SupervisorError::BootstrapTimeout)?;
    let challenge = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let mut stream = TcpStream::connect_timeout(&ready.address, remaining_until(deadline)?)
        .map_err(map_probe_io_error)?;
    stream
        .set_write_timeout(Some(remaining_until(deadline)?))
        .map_err(|_| SupervisorError::BootstrapProbeFailed)?;
    let request = format!(
        "GET {GATEWAY_OWNERSHIP_PATH} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nConnection: Upgrade\r\nUpgrade: {GATEWAY_OWNERSHIP_PROTOCOL}\r\n{GATEWAY_OWNERSHIP_CHALLENGE_HEADER}: {challenge}\r\nContent-Length: 0\r\n\r\n",
        ready.address.port(),
    );
    stream
        .write_all(request.as_bytes())
        .and_then(|()| stream.flush())
        .map_err(map_probe_io_error)?;

    let mut response = Vec::with_capacity(512);
    let mut chunk = [0_u8; 512];
    loop {
        if let Some(proof) = parse_gateway_ownership_response(&response)? {
            let mut mac = Hmac::<Sha256>::new_from_slice(ready.capability.as_bytes())
                .map_err(|_| SupervisorError::BootstrapProbeFailed)?;
            mac.update(gateway_ownership_transcript(ready, &challenge).as_bytes());
            mac.verify_slice(&proof)
                .map_err(|_| SupervisorError::BootstrapProbeFailed)?;
            return Ok(());
        }
        if response.len() == GATEWAY_OWNERSHIP_RESPONSE_MAX_BYTES {
            return Err(SupervisorError::BootstrapProbeFailed);
        }
        stream
            .set_read_timeout(Some(remaining_until(deadline)?))
            .map_err(|_| SupervisorError::BootstrapProbeFailed)?;
        let count = stream.read(&mut chunk).map_err(map_probe_io_error)?;
        if count == 0 {
            return Err(SupervisorError::BootstrapProbeFailed);
        }
        if response.len().saturating_add(count) > GATEWAY_OWNERSHIP_RESPONSE_MAX_BYTES {
            return Err(SupervisorError::BootstrapProbeFailed);
        }
        response.extend_from_slice(&chunk[..count]);
    }
}

fn remaining_until(deadline: Instant) -> Result<Duration, SupervisorError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or(SupervisorError::BootstrapTimeout)
}

fn map_probe_io_error(error: std::io::Error) -> SupervisorError {
    if matches!(error.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock) {
        SupervisorError::BootstrapTimeout
    } else {
        SupervisorError::BootstrapProbeFailed
    }
}

fn gateway_ownership_transcript(ready: &GatewayReady, challenge: &str) -> String {
    format!(
        "{GATEWAY_OWNERSHIP_DOMAIN}\n{}\n{}\n{}\n{}\n{challenge}",
        ready.startup_nonce,
        ready.pid,
        ready.address.ip(),
        ready.address.port()
    )
}

fn parse_gateway_ownership_response(response: &[u8]) -> Result<Option<[u8; 32]>, SupervisorError> {
    let Some(header_end) = response.windows(4).position(|bytes| bytes == b"\r\n\r\n") else {
        return Ok(None);
    };
    if response.len() != header_end + 4 {
        return Err(SupervisorError::BootstrapProbeFailed);
    }
    let header = std::str::from_utf8(&response[..header_end])
        .map_err(|_| SupervisorError::BootstrapProbeFailed)?;
    let mut lines = header.split("\r\n");
    if lines.next() != Some("HTTP/1.1 200 OK") {
        return Err(SupervisorError::BootstrapProbeFailed);
    }

    let mut content_length = None;
    let mut connection = None;
    let mut proof = None;
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .ok_or(SupervisorError::BootstrapProbeFailed)?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            if content_length.replace(value).is_some() {
                return Err(SupervisorError::BootstrapProbeFailed);
            }
        } else if name.eq_ignore_ascii_case("connection") {
            if connection.replace(value).is_some() {
                return Err(SupervisorError::BootstrapProbeFailed);
            }
        } else if name.eq_ignore_ascii_case(GATEWAY_OWNERSHIP_PROOF_HEADER) {
            if proof.replace(decode_ownership_proof(value)?).is_some() {
                return Err(SupervisorError::BootstrapProbeFailed);
            }
        } else {
            return Err(SupervisorError::BootstrapProbeFailed);
        }
    }
    if content_length != Some("0") || connection != Some("close") {
        return Err(SupervisorError::BootstrapProbeFailed);
    }
    proof.map(Some).ok_or(SupervisorError::BootstrapProbeFailed)
}

fn decode_ownership_proof(value: &str) -> Result<[u8; 32], SupervisorError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(SupervisorError::BootstrapProbeFailed);
    }
    let mut proof = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let encoded =
            std::str::from_utf8(pair).map_err(|_| SupervisorError::BootstrapProbeFailed)?;
        proof[index] =
            u8::from_str_radix(encoded, 16).map_err(|_| SupervisorError::BootstrapProbeFailed)?;
    }
    Ok(proof)
}

fn encode_private_frame(value: &impl Serialize) -> Result<Vec<u8>, SupervisorError> {
    let body = serde_json::to_vec(value).map_err(|_| SupervisorError::BootstrapInvalid)?;
    let length = u32::try_from(body.len()).map_err(|_| SupervisorError::BootstrapInvalid)?;
    if body.is_empty() || body.len() > GATEWAY_PRIVATE_FRAME_MAX_BYTES {
        return Err(SupervisorError::BootstrapInvalid);
    }
    let mut frame = Vec::with_capacity(body.len() + 4);
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

fn read_private_frame(reader: &mut impl Read) -> Result<Vec<u8>, SupervisorError> {
    let maximum_frame_bytes = GATEWAY_PRIVATE_FRAME_MAX_BYTES + 4;
    let mut frame = Vec::with_capacity(maximum_frame_bytes + 1);
    let mut chunk = [0_u8; GATEWAY_PRIVATE_FRAME_MAX_BYTES + 5];
    let mut expected_frame_bytes = None;
    loop {
        let count = reader
            .read(&mut chunk)
            .map_err(|_| SupervisorError::BootstrapIoFailed)?;
        if count == 0 {
            return Err(SupervisorError::BootstrapIoFailed);
        }
        if frame.len().saturating_add(count) > maximum_frame_bytes + 1 {
            return Err(SupervisorError::BootstrapInvalid);
        }
        frame.extend_from_slice(&chunk[..count]);
        if expected_frame_bytes.is_none() && frame.len() >= 4 {
            let length = usize::try_from(u32::from_be_bytes(
                frame[..4]
                    .try_into()
                    .map_err(|_| SupervisorError::BootstrapInvalid)?,
            ))
            .map_err(|_| SupervisorError::BootstrapInvalid)?;
            if length == 0 || length > GATEWAY_PRIVATE_FRAME_MAX_BYTES {
                return Err(SupervisorError::BootstrapInvalid);
            }
            expected_frame_bytes = Some(
                length
                    .checked_add(4)
                    .ok_or(SupervisorError::BootstrapInvalid)?,
            );
        }
        let Some(expected_frame_bytes) = expected_frame_bytes else {
            continue;
        };
        if frame.len() > expected_frame_bytes {
            return Err(SupervisorError::BootstrapInvalid);
        }
        if frame.len() == expected_frame_bytes {
            return Ok(frame.split_off(4));
        }
    }
}

fn join_bootstrap_reader(reader: Option<thread::JoinHandle<()>>) -> Result<(), SupervisorError> {
    reader
        .map_or(Ok(()), |reader| reader.join())
        .map_err(|_| SupervisorError::BootstrapIoFailed)
}

impl Drop for GatewaySupervisor {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            if terminate_child(&mut child).is_err() {
                let _ = force_terminate_child(&mut child);
            }
        }
        self.finish_bootstrap_reader();
        self.finish_output_capture();
    }
}

fn terminate_child(child: &mut Box<dyn ChildWrapper>) -> std::io::Result<()> {
    if child.try_wait().is_ok_and(|status| status.is_some()) {
        return terminate_observed_exited_tree(child);
    }
    let graceful_requested = child.stdin().take().is_some_and(|mut input| {
        input
            .write_all(SHUTDOWN_COMMAND)
            .and_then(|()| input.flush())
            .is_ok()
    });
    if graceful_requested {
        let deadline = Instant::now() + GRACEFUL_SHUTDOWN_TIMEOUT;
        loop {
            match child.try_wait() {
                Ok(Some(_)) => return terminate_observed_exited_tree(child),
                Ok(None) => {}
                Err(_) => break,
            }
            if Instant::now() >= deadline {
                break;
            }
            thread::sleep(SHUTDOWN_POLL_INTERVAL);
        }
    }
    force_terminate_child(child)
}

fn terminate_observed_exited_tree(child: &mut Box<dyn ChildWrapper>) -> std::io::Result<()> {
    child.stdin().take();
    match child.start_kill() {
        Ok(()) => Ok(()),
        Err(error) if matches!(error.kind(), ErrorKind::InvalidInput | ErrorKind::NotFound) => {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn force_terminate_child(child: &mut Box<dyn ChildWrapper>) -> std::io::Result<()> {
    child.stdin().take();
    match child.start_kill() {
        Ok(()) => {}
        Err(error) if matches!(error.kind(), ErrorKind::InvalidInput | ErrorKind::NotFound) => {}
        Err(error) => return Err(error),
    }
    child.wait()?;
    Ok(())
}

fn inherited_runtime_environment() -> BTreeMap<String, String> {
    inherited_runtime_environment_from(|name| std::env::var(name).ok())
}

fn inherited_runtime_environment_from(
    mut read: impl FnMut(&str) -> Option<String>,
) -> BTreeMap<String, String> {
    INHERITED_RUNTIME_ENVIRONMENT_NAMES
        .into_iter()
        .filter_map(|name| read(name).map(|value| (String::from(name), value)))
        .collect()
}

fn sensitive_environment_values(environment: &BTreeMap<String, String>) -> Vec<String> {
    environment
        .iter()
        .filter(|(name, value)| !value.is_empty() && is_sensitive_environment_name(name))
        .map(|(_, value)| value.clone())
        .collect()
}

fn is_sensitive_environment_name(name: &str) -> bool {
    let normalized: String = name
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect();
    [
        "apikey",
        "authorization",
        "passcode",
        "password",
        "secret",
        "token",
        "credential",
        "cookie",
        "session",
        "privatekey",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
}

#[cfg(test)]
mod tests {
    #[cfg(windows)]
    use super::SpawnFailureCleanup;
    use super::{
        BackoffPolicy, GATEWAY_CALLMESH_API_KEY_MAX_BYTES, GATEWAY_OWNERSHIP_CHALLENGE_HEADER,
        GATEWAY_OWNERSHIP_PATH, GATEWAY_OWNERSHIP_PROOF_HEADER, GATEWAY_PRIVATE_FRAME_MAX_BYTES,
        GatewayBootstrapFrame, GatewayCommand, GatewayReady, GatewayStatus, GatewaySupervisor,
        SupervisorError, SupervisorEvent, encode_private_frame, inherited_runtime_environment_from,
        probe_gateway_ownership, read_private_frame, validate_gateway_ready,
    };
    #[cfg(windows)]
    use process_wrap::std::{ChildWrapper, CommandWrap, CommandWrapper, JobObject};
    #[cfg(windows)]
    use std::sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    };

    #[cfg(windows)]
    #[derive(Debug)]
    struct InjectedWrapFailure {
        observed_pid: Arc<AtomicU32>,
    }

    #[cfg(windows)]
    impl CommandWrapper for InjectedWrapFailure {
        fn wrap_child(
            &mut self,
            child: Box<dyn ChildWrapper>,
            _core: &CommandWrap,
        ) -> std::io::Result<Box<dyn ChildWrapper>> {
            self.observed_pid.store(child.id(), Ordering::Release);
            Err(std::io::Error::other("injected child wrap failure"))
        }
    }

    #[test]
    fn private_bootstrap_frames_are_bounded_and_ready_identity_is_exact() {
        let nonce = "a".repeat(32);
        let capability = "b".repeat(64);
        let api_key = "fixture-private-callmesh-key";
        let encoded = encode_private_frame(&GatewayBootstrapFrame {
            schema_version: 2,
            frame_type: "gateway.bootstrap",
            startup_nonce: nonce.clone(),
            capability: capability.clone(),
            setup_generation: 7,
            callmesh_api_key: Some(api_key),
        })
        .expect("bootstrap should encode");
        assert_eq!(
            usize::try_from(u32::from_be_bytes(encoded[..4].try_into().unwrap())).unwrap(),
            encoded.len() - 4
        );
        let body = read_private_frame(&mut encoded.as_slice()).expect("frame should decode");
        assert_eq!(body, encoded[4..]);
        let decoded: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(decoded["schemaVersion"], 2);
        assert_eq!(decoded["setupGeneration"], 7);
        assert_eq!(decoded["callmeshApiKey"], api_key);
        let maximum_escaped_key = "\\".repeat(GATEWAY_CALLMESH_API_KEY_MAX_BYTES);
        let maximum_frame = encode_private_frame(&GatewayBootstrapFrame {
            schema_version: 2,
            frame_type: "gateway.bootstrap",
            startup_nonce: nonce.clone(),
            capability: capability.clone(),
            setup_generation: 7,
            callmesh_api_key: Some(&maximum_escaped_key),
        })
        .expect("maximum escaped API key should fit the bounded frame");
        assert!(maximum_frame.len() <= GATEWAY_PRIVATE_FRAME_MAX_BYTES + 4);

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert_eq!(
            read_private_frame(&mut trailing.as_slice()),
            Err(SupervisorError::BootstrapInvalid)
        );

        let ready = serde_json::json!({
            "schemaVersion": 1,
            "type": "gateway.ready",
            "pid": 42,
            "startupNonce": nonce,
            "host": "127.0.0.1",
            "port": 49152
        });
        let ready = validate_gateway_ready(
            &serde_json::to_vec(&ready).unwrap(),
            42,
            "a".repeat(32),
            capability.clone(),
        )
        .expect("ready frame should validate");
        assert_eq!(ready.pid, 42);
        assert_eq!(ready.address, "127.0.0.1:49152".parse().unwrap());
        assert_eq!(ready.capability, capability);
        let ready_debug = format!("{ready:?}");
        assert!(!ready_debug.contains(&ready.capability));
        assert!(!ready_debug.contains(&ready.startup_nonce));
        assert_eq!(ready_debug.matches("[REDACTED]").count(), 2);

        let mut oversized = Vec::from(
            u32::try_from(GATEWAY_PRIVATE_FRAME_MAX_BYTES + 1)
                .unwrap()
                .to_be_bytes(),
        );
        oversized.resize(8, 0);
        assert!(matches!(
            read_private_frame(&mut oversized.as_slice()),
            Err(SupervisorError::BootstrapInvalid)
        ));
    }

    #[test]
    fn setup_generation_is_bounded_before_private_bootstrap() {
        let command = GatewayCommand {
            program: String::from("fixture"),
            arguments: Vec::new(),
        };
        let mut supervisor = GatewaySupervisor::new(command, BackoffPolicy::default())
            .expect("supervisor should initialize");

        supervisor
            .set_setup_generation(9_007_199_254_740_991)
            .expect("maximum JavaScript-safe generation should be accepted");
        assert_eq!(
            supervisor.configured_setup_generation(),
            9_007_199_254_740_991,
        );
        assert_eq!(
            supervisor.set_setup_generation(0),
            Err(SupervisorError::BootstrapInvalid)
        );
        assert_eq!(
            supervisor.set_setup_generation(9_007_199_254_740_992),
            Err(SupervisorError::BootstrapInvalid)
        );
    }

    #[test]
    fn callmesh_key_requires_private_bootstrap_and_never_enters_child_environment() {
        let mut supervisor = GatewaySupervisor::new(fixture_command(), BackoffPolicy::default())
            .expect("supervisor should initialize");
        assert_eq!(
            supervisor.set_callmesh_api_key("fixture-private-callmesh-key"),
            Err(SupervisorError::BootstrapInvalid)
        );
        supervisor
            .enable_private_bootstrap()
            .expect("private bootstrap should enable");
        for invalid in [
            String::new(),
            String::from("control\ncharacter"),
            "x".repeat(GATEWAY_CALLMESH_API_KEY_MAX_BYTES + 1),
        ] {
            assert_eq!(
                supervisor.set_callmesh_api_key(&invalid),
                Err(SupervisorError::BootstrapInvalid)
            );
        }
        supervisor.set_environment(BTreeMap::from([(
            String::from("CMCLIENT_CALLMESH_API_KEY"),
            String::from("environment-secret"),
        )]));
        supervisor
            .set_callmesh_api_key("fixture-private-callmesh-key")
            .expect("bounded key should be accepted");
        assert!(
            !supervisor
                .environment
                .contains_key("CMCLIENT_CALLMESH_API_KEY")
        );
        assert_eq!(
            supervisor.child_output_redaction_secrets(None),
            vec![String::from("fixture-private-callmesh-key")]
        );
    }

    #[test]
    fn private_ready_rejects_wrong_pid_nonce_endpoint_and_unknown_fields() {
        let base = serde_json::json!({
            "schemaVersion": 1,
            "type": "gateway.ready",
            "pid": 42,
            "startupNonce": "a".repeat(32),
            "host": "127.0.0.1",
            "port": 49152
        });
        for mutation in ["pid", "nonce", "host", "port", "unknown"] {
            let mut value = base.clone();
            match mutation {
                "pid" => value["pid"] = serde_json::json!(43),
                "nonce" => value["startupNonce"] = serde_json::json!("c".repeat(32)),
                "host" => value["host"] = serde_json::json!("0.0.0.0"),
                "port" => value["port"] = serde_json::json!(0),
                "unknown" => value["capability"] = serde_json::json!("reflected"),
                _ => unreachable!(),
            }
            assert!(matches!(
                validate_gateway_ready(
                    &serde_json::to_vec(&value).unwrap(),
                    42,
                    "a".repeat(32),
                    "b".repeat(64),
                ),
                Err(SupervisorError::BootstrapInvalid)
            ));
        }
    }

    #[test]
    fn ownership_probe_accepts_the_exact_gateway_contract() {
        let capability = "b".repeat(64);
        let startup_nonce = "a".repeat(32);
        let (address, server) =
            spawn_ownership_proof_server(capability.clone(), 42, startup_nonce.clone());
        let ready = GatewayReady {
            pid: 42,
            address,
            startup_nonce,
            capability,
        };

        probe_gateway_ownership(&ready, Duration::from_secs(2))
            .expect("ownership proof should validate");
        server.join().expect("probe server should finish");
    }

    #[test]
    fn ownership_probe_rejects_hostile_takeover_without_disclosing_capability() {
        let capability = "c".repeat(64);
        let (address, server) = spawn_hostile_takeover_server(capability.clone());
        let ready = GatewayReady {
            pid: 42,
            address,
            startup_nonce: "e".repeat(32),
            capability,
        };

        assert_eq!(
            probe_gateway_ownership(&ready, Duration::from_secs(2)),
            Err(SupervisorError::BootstrapProbeFailed)
        );
        server.join().expect("hostile listener should finish");
    }

    #[test]
    fn ownership_probe_failure_diagnostics_do_not_reflect_capability_or_response() {
        let capability = "c".repeat(64);
        let reflected = format!(r#"{{"code":"denied","detail":"{capability}"}}"#);
        let (address, server) = spawn_rejected_probe_server(capability.clone(), reflected.clone());
        let ready = GatewayReady {
            pid: 42,
            address,
            startup_nonce: "d".repeat(32),
            capability: capability.clone(),
        };

        let error = probe_gateway_ownership(&ready, Duration::from_secs(2))
            .expect_err("rejected health should fail bootstrap");
        server.join().expect("probe server should finish");
        let diagnostic = format!("{error:?}:{}", error.code());
        assert_eq!(error, SupervisorError::BootstrapProbeFailed);
        assert!(!diagnostic.contains(&capability));
        assert!(!diagnostic.contains(&reflected));
    }
    use cmclient_runtime_logging::{LogPolicy, MIN_LOG_MAX_BYTES, StructuredLogSink};
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use std::{
        collections::BTreeMap,
        env, fs,
        io::{BufRead, Read, Write},
        net::{Ipv4Addr, SocketAddr, TcpListener},
        path::PathBuf,
        process, thread,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };

    const FIXTURE_MODE: &str = "CMCLIENT_SUPERVISOR_TEST_MODE";
    const FIXTURE_DELAY_MS: &str = "CMCLIENT_SUPERVISOR_TEST_DELAY_MS";
    const FIXTURE_MARKER: &str = "CMCLIENT_SUPERVISOR_TEST_MARKER";
    #[cfg(target_os = "windows")]
    const FIXTURE_CALLMESH_API_KEY: &str = "fixture-private-callmesh-key";

    #[test]
    fn bounds_exponential_restart_delays() {
        let policy = BackoffPolicy {
            initial_delay: Duration::from_secs(1),
            maximum_delay: Duration::from_secs(8),
        };
        assert_eq!(policy.delay_for_attempt(1), Duration::from_secs(1));
        assert_eq!(policy.delay_for_attempt(3), Duration::from_secs(4));
        assert_eq!(policy.delay_for_attempt(10), Duration::from_secs(8));
    }

    #[test]
    fn inherited_child_environment_excludes_path_and_plaintext_secret_inputs() {
        let source = BTreeMap::from([
            (String::from("PATH"), String::from("/fixture/bin")),
            (String::from("SystemRoot"), String::from("C:\\Windows")),
            (
                String::from("CMCLIENT_PLAINTEXT_SECRET_FILE"),
                String::from("/private/fixture/secrets.json"),
            ),
            (
                String::from("CMCLIENT_CALLMESH_API_KEY"),
                String::from("fixture-secret"),
            ),
        ]);

        let filtered = inherited_runtime_environment_from(|name| source.get(name).cloned());

        assert_eq!(
            filtered,
            BTreeMap::from([(String::from("SystemRoot"), String::from("C:\\Windows"),)])
        );
    }

    #[test]
    fn rejects_an_empty_gateway_program() {
        assert!(
            GatewaySupervisor::new(
                GatewayCommand {
                    program: String::new(),
                    arguments: Vec::new()
                },
                BackoffPolicy::default()
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_invalid_timing_policies() {
        let command = fixture_command();
        assert!(matches!(
            GatewaySupervisor::new_with_stable_window(
                command.clone(),
                BackoffPolicy {
                    initial_delay: Duration::ZERO,
                    maximum_delay: Duration::from_secs(1),
                },
                Duration::from_secs(1),
            ),
            Err(SupervisorError::InvalidTimingPolicy)
        ));
        assert!(matches!(
            GatewaySupervisor::new_with_stable_window(
                command,
                BackoffPolicy::default(),
                Duration::ZERO,
            ),
            Err(SupervisorError::InvalidTimingPolicy)
        ));
    }

    #[test]
    fn tick_preserves_consecutive_failures_and_enforces_each_deadline() {
        let initial_delay = Duration::from_millis(40);
        let mut supervisor = fixture_supervisor(
            "crash",
            Duration::ZERO,
            BackoffPolicy {
                initial_delay,
                maximum_delay: Duration::from_millis(160),
            },
            Duration::from_secs(1),
            None,
        );
        assert!(matches!(
            supervisor.start(),
            Ok(SupervisorEvent::Started { .. })
        ));

        let event = wait_for_event(&mut supervisor, Duration::from_secs(2), |event| {
            matches!(event, SupervisorEvent::Exited { .. })
        });
        let first_exit_observed = Instant::now();
        assert_eq!(
            event,
            SupervisorEvent::Exited {
                status: Some(7),
                restart_delay: initial_delay,
            }
        );
        assert_eq!(
            supervisor.status(),
            GatewayStatus::Backoff {
                attempt: 1,
                delay: initial_delay,
            }
        );
        assert!(matches!(
            supervisor.tick(),
            Ok(SupervisorEvent::Backoff { attempt: 1, .. })
        ));

        wait_for_event(&mut supervisor, Duration::from_secs(2), |event| {
            matches!(event, SupervisorEvent::Started { .. })
        });
        assert!(first_exit_observed.elapsed() + Duration::from_millis(2) >= initial_delay);
        let second_exit = wait_for_event(&mut supervisor, Duration::from_secs(2), |event| {
            matches!(event, SupervisorEvent::Exited { .. })
        });
        let second_exit_observed = Instant::now();
        assert_eq!(
            second_exit,
            SupervisorEvent::Exited {
                status: Some(7),
                restart_delay: Duration::from_millis(80),
            }
        );
        assert_eq!(
            supervisor.status(),
            GatewayStatus::Backoff {
                attempt: 2,
                delay: Duration::from_millis(80),
            }
        );
        assert!(matches!(
            supervisor.tick(),
            Ok(SupervisorEvent::Backoff { attempt: 2, .. })
        ));
        wait_for_event(&mut supervisor, Duration::from_secs(2), |event| {
            matches!(event, SupervisorEvent::Started { .. })
        });
        assert!(
            second_exit_observed.elapsed() + Duration::from_millis(2) >= Duration::from_millis(80)
        );
        supervisor.stop().expect("child should stop");
    }

    #[test]
    fn stable_heartbeat_resets_the_crash_loop() {
        let mut supervisor = fixture_supervisor(
            "crash",
            Duration::ZERO,
            BackoffPolicy {
                initial_delay: Duration::from_millis(20),
                maximum_delay: Duration::from_millis(80),
            },
            Duration::from_millis(40),
            None,
        );
        supervisor.start().expect("first child should start");
        wait_for_event(&mut supervisor, Duration::from_secs(2), |event| {
            matches!(event, SupervisorEvent::Exited { .. })
        });
        supervisor.set_environment(BTreeMap::from([
            (String::from(FIXTURE_MODE), String::from("delayed-exit")),
            (String::from(FIXTURE_DELAY_MS), String::from("120")),
        ]));
        wait_for_event(&mut supervisor, Duration::from_secs(2), |event| {
            matches!(event, SupervisorEvent::Started { .. })
        });

        thread::sleep(Duration::from_millis(50));
        assert!(matches!(
            supervisor.tick(),
            Ok(SupervisorEvent::Heartbeat { .. })
        ));
        let exit = wait_for_event(&mut supervisor, Duration::from_secs(2), |event| {
            matches!(event, SupervisorEvent::Exited { .. })
        });
        assert_eq!(
            exit,
            SupervisorEvent::Exited {
                status: Some(7),
                restart_delay: Duration::from_millis(20),
            }
        );
        assert_eq!(
            supervisor.status(),
            GatewayStatus::Backoff {
                attempt: 1,
                delay: Duration::from_millis(20),
            }
        );
    }

    #[test]
    fn exit_first_observed_after_the_stable_window_resets_the_crash_loop() {
        let initial_delay = Duration::from_millis(20);
        let stable_window = Duration::from_millis(40);
        let mut supervisor = fixture_supervisor(
            "crash",
            Duration::ZERO,
            BackoffPolicy {
                initial_delay,
                maximum_delay: Duration::from_millis(80),
            },
            stable_window,
            None,
        );
        supervisor.start().expect("first child should start");
        wait_for_event(&mut supervisor, Duration::from_secs(2), |event| {
            matches!(event, SupervisorEvent::Exited { .. })
        });
        supervisor.set_environment(BTreeMap::from([
            (String::from(FIXTURE_MODE), String::from("delayed-exit")),
            (String::from(FIXTURE_DELAY_MS), String::from("80")),
        ]));
        wait_for_event(&mut supervisor, Duration::from_secs(2), |event| {
            matches!(event, SupervisorEvent::Started { .. })
        });

        // Wait for the OS process to exit without advancing supervisor state. The next tick
        // must therefore be the first supervisor observation after the stable window.
        let exit_deadline = Instant::now() + Duration::from_secs(2);
        while supervisor
            .child
            .as_mut()
            .expect("child should still be supervised")
            .try_wait()
            .expect("child status should be readable")
            .is_none()
        {
            assert!(Instant::now() < exit_deadline, "child exit timed out");
            thread::sleep(Duration::from_millis(5));
        }
        assert!(
            supervisor
                .started_at
                .expect("child start should be tracked")
                .elapsed()
                >= stable_window
        );
        assert_eq!(
            supervisor.tick().expect("exit should be observed"),
            SupervisorEvent::Exited {
                status: Some(7),
                restart_delay: initial_delay,
            }
        );
        assert_eq!(
            supervisor.status(),
            GatewayStatus::Backoff {
                attempt: 1,
                delay: initial_delay,
            }
        );
    }

    #[test]
    fn stop_terminates_and_reaps_a_real_child() {
        let marker = unique_marker("stop");
        let mut supervisor = fixture_supervisor(
            "delayed-marker",
            Duration::from_millis(250),
            BackoffPolicy::default(),
            Duration::from_secs(1),
            Some(&marker),
        );
        supervisor.start().expect("child should start");
        assert_eq!(
            supervisor.stop().expect("child should stop"),
            SupervisorEvent::Stopped
        );
        assert_eq!(supervisor.status(), GatewayStatus::Stopped);
        thread::sleep(Duration::from_millis(350));
        assert_eq!(
            fs::read(&marker).expect("graceful marker should exist"),
            b"graceful shutdown"
        );
        fs::remove_file(marker).expect("marker should remove");
    }

    #[test]
    fn drop_terminates_and_reaps_a_real_child() {
        let marker = unique_marker("drop");
        {
            let mut supervisor = fixture_supervisor(
                "delayed-marker",
                Duration::from_millis(250),
                BackoffPolicy::default(),
                Duration::from_secs(1),
                Some(&marker),
            );
            supervisor.start().expect("child should start");
        }
        assert_eq!(
            fs::read(&marker).expect("graceful marker should exist"),
            b"graceful shutdown"
        );
        fs::remove_file(marker).expect("marker should remove");
    }

    #[test]
    fn stop_force_terminates_a_child_that_ignores_the_graceful_deadline() {
        let mut supervisor = fixture_supervisor(
            "ignore-shutdown",
            Duration::ZERO,
            BackoffPolicy::default(),
            Duration::from_secs(1),
            None,
        );
        supervisor.start().expect("child should start");
        let started_at = Instant::now();
        supervisor.stop().expect("child should stop after fallback");
        assert!(started_at.elapsed() < Duration::from_secs(2));
        assert_eq!(supervisor.status(), GatewayStatus::Stopped);
    }

    #[test]
    fn unexpected_parent_exit_terminates_descendant_tree_before_joining_capture() {
        let log_dir = unique_marker("descendant-stdout-log");
        let marker = unique_marker("descendant-survived");
        let mut supervisor =
            fixture_logging_supervisor(&log_dir, "spawn-descendant-exit", "tree-test-secret");
        supervisor.set_environment(BTreeMap::from([
            (String::from(FIXTURE_DELAY_MS), String::from("600")),
            (
                String::from(FIXTURE_MARKER),
                marker.to_string_lossy().into_owned(),
            ),
        ]));
        supervisor.start().expect("parent fixture should start");

        let observed_at = Instant::now();
        let event = wait_for_event(&mut supervisor, Duration::from_secs(2), |event| {
            matches!(event, SupervisorEvent::Exited { .. })
        });
        assert!(matches!(
            event,
            SupervisorEvent::Exited {
                status: Some(7),
                ..
            }
        ));
        assert!(
            observed_at.elapsed() < Duration::from_millis(500),
            "descendant-held stdout delayed output capture shutdown"
        );
        thread::sleep(Duration::from_millis(700));
        assert!(
            !marker.exists(),
            "descendant escaped supervised process tree"
        );
        fs::remove_dir_all(log_dir).expect("fixture log directory should remove");
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn child_wrap_failure_kills_and_reaps_the_suspended_windows_process() {
        let powershell = PathBuf::from(
            env::var("SystemRoot").expect("Windows system root should be configured"),
        )
        .join("System32/WindowsPowerShell/v1.0/powershell.exe");
        let mut raw_command = process::Command::new(&powershell);
        raw_command
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "Start-Sleep -Seconds 30",
            ])
            .stdin(process::Stdio::null())
            .stdout(process::Stdio::null())
            .stderr(process::Stdio::null());

        let observed_pid = Arc::new(AtomicU32::new(0));
        let mut command = CommandWrap::from(raw_command);
        command.wrap(SpawnFailureCleanup);
        command.wrap(InjectedWrapFailure {
            observed_pid: Arc::clone(&observed_pid),
        });
        command.wrap(JobObject);

        assert!(
            command.spawn().is_err(),
            "injected wrap failure must surface"
        );
        let pid = observed_pid.load(Ordering::Acquire);
        assert_ne!(pid, 0, "injected wrapper must observe the spawned child");
        let leaked = windows_process_exists(&powershell, pid);
        if leaked {
            let _ = process::Command::new(&powershell)
                .args([
                    "-NoLogo",
                    "-NoProfile",
                    "-NonInteractive",
                    "-Command",
                    &format!("Stop-Process -Id {pid} -Force -ErrorAction SilentlyContinue"),
                ])
                .status();
        }
        assert!(!leaked, "child wrap failure left a suspended process alive");
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn private_bootstrap_secrets_reach_child_only_through_the_memory_pipe() {
        let marker = unique_marker("bootstrap-success");
        let leak_marker = PathBuf::from(format!("{}.secret-leak", marker.to_string_lossy()));
        let mut supervisor = powershell_bootstrap_supervisor("bootstrap-success", &marker);
        supervisor
            .enable_private_bootstrap()
            .expect("private bootstrap should enable");
        supervisor
            .set_callmesh_api_key(FIXTURE_CALLMESH_API_KEY)
            .expect("CallMesh key should use private bootstrap");

        let event = supervisor.start();
        assert!(
            !leak_marker.exists(),
            "child observed a bootstrap secret outside stdin"
        );
        assert!(matches!(event, Ok(SupervisorEvent::Started { .. })));
        let ready = supervisor
            .gateway_ready()
            .expect("verified private route should publish");
        assert_eq!(ready.address.ip(), Ipv4Addr::LOCALHOST);
        assert_ne!(ready.address.port(), 0);
        supervisor.stop().expect("verified child should stop");
        assert!(
            !leak_marker.exists(),
            "bootstrap leak marker must remain absent"
        );
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn private_bootstrap_tolerates_bounded_windows_cold_start() {
        let marker = unique_marker("bootstrap-delayed-success");
        let mut supervisor = powershell_bootstrap_supervisor("bootstrap-delayed-success", &marker);
        supervisor.set_environment(BTreeMap::from([(
            String::from(FIXTURE_DELAY_MS),
            String::from("5500"),
        )]));
        supervisor
            .enable_private_bootstrap()
            .expect("private bootstrap should enable");

        let started_at = Instant::now();
        let event = supervisor
            .start()
            .expect("bounded Windows cold start should complete bootstrap");
        assert!(started_at.elapsed() >= Duration::from_secs(5));
        assert!(matches!(event, SupervisorEvent::Started { .. }));
        supervisor
            .stop()
            .expect("delayed verified child should stop");
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn private_bootstrap_faults_terminate_and_reap_real_children() {
        let cases = [
            ("bootstrap-wrong-pid", SupervisorError::BootstrapInvalid),
            ("bootstrap-wrong-nonce", SupervisorError::BootstrapInvalid),
            (
                "bootstrap-wrong-capability",
                SupervisorError::BootstrapProbeFailed,
            ),
            ("bootstrap-timeout", SupervisorError::BootstrapTimeout),
            ("bootstrap-early-exit", SupervisorError::BootstrapIoFailed),
            ("bootstrap-oversize", SupervisorError::BootstrapInvalid),
        ];
        let mut markers = Vec::new();

        for (mode, expected) in cases {
            let marker = unique_marker(mode);
            let mut supervisor = powershell_bootstrap_supervisor(mode, &marker);
            supervisor
                .enable_private_bootstrap()
                .expect("private bootstrap should enable");
            supervisor.bootstrap_deadline = Duration::from_secs(5);

            assert_eq!(
                supervisor
                    .start()
                    .expect_err("bootstrap fault must fail closed"),
                expected,
                "unexpected error for {mode}"
            );
            assert!(
                supervisor.child.is_none(),
                "failed child must be reaped for {mode}"
            );
            assert_eq!(supervisor.gateway_ready(), None);
            assert!(matches!(
                supervisor.status(),
                GatewayStatus::Backoff { attempt: 1, .. }
            ));
            markers.push((mode, marker));
        }

        thread::sleep(Duration::from_millis(650));
        for (mode, marker) in markers {
            let survived = marker.exists();
            if survived {
                fs::remove_file(&marker).expect("orphan marker should remove");
            }
            assert!(!survived, "failed bootstrap child survived for {mode}");
            if mode == "bootstrap-timeout" {
                let descendant_started =
                    PathBuf::from(format!("{}.descendant-started", marker.to_string_lossy()));
                assert!(
                    descendant_started.exists(),
                    "stdout-holding descendant must start before bootstrap rejection"
                );
                fs::remove_file(descendant_started)
                    .expect("descendant started marker should remove");
                let descendant_ready =
                    PathBuf::from(format!("{}.descendant-ready", marker.to_string_lossy()));
                if descendant_ready.exists() {
                    fs::remove_file(descendant_ready)
                        .expect("descendant ready marker should remove");
                }
            }
            if mode == "bootstrap-wrong-capability" {
                let server_ready =
                    PathBuf::from(format!("{}.server-ready", marker.to_string_lossy()));
                if server_ready.exists() {
                    fs::remove_file(server_ready).expect("server ready marker should remove");
                }
                let server_done =
                    PathBuf::from(format!("{}.server-done", marker.to_string_lossy()));
                assert!(
                    server_done.exists(),
                    "capability rejection server should finish"
                );
                assert_eq!(
                    fs::read_to_string(&server_done).expect("server summary should read"),
                    "capability-header-absent"
                );
                fs::remove_file(server_done).expect("server done marker should remove");
                let descendant_marker =
                    PathBuf::from(format!("{}.descendant-survived", marker.to_string_lossy()));
                if descendant_marker.exists() {
                    fs::remove_file(&descendant_marker)
                        .expect("escaped descendant marker should remove");
                    panic!("rejected bootstrap descendant escaped the process tree");
                }
            }
        }
    }

    #[test]
    fn captures_redacts_retains_and_restarts_real_gateway_output() {
        let log_dir = unique_marker("logging-restart");
        let secret = "exact-gateway-secret-value";
        let mut supervisor = fixture_logging_supervisor(&log_dir, "log-exit", secret);
        supervisor.start().expect("first child should start");
        wait_for_event(&mut supervisor, Duration::from_secs(2), |event| {
            matches!(event, SupervisorEvent::Exited { .. })
        });
        wait_for_event(&mut supervisor, Duration::from_secs(2), |event| {
            matches!(event, SupervisorEvent::Started { .. })
        });
        wait_for_event(&mut supervisor, Duration::from_secs(2), |event| {
            matches!(event, SupervisorEvent::Exited { .. })
        });
        supervisor.stop().expect("supervisor should reset");

        let (files, contents) = read_gateway_logs(&log_dir);
        assert!(
            files.len() > 1,
            "fixture should include a retained generation"
        );
        assert!(files.len() <= 5, "retention should remain bounded");
        assert!(!contents.contains(secret));
        assert!(contents.contains("[REDACTED]"));
        assert!(contents.contains("GATEWAY_FIXTURE_STDERR"));
        assert!(contents.contains("RUNTIME_LOG_STDERR_INVALID"));
        assert!(contents.contains("RUNTIME_LOG_STDOUT_OVERSIZED"));
        assert_eq!(
            supervisor.take_log_health_update(),
            super::GatewayLogHealthUpdate::default()
        );
        fs::remove_dir_all(log_dir).expect("log directory should remove");
    }

    #[test]
    fn stop_and_drop_join_and_flush_gateway_log_captures() {
        for action in ["stop", "drop"] {
            let log_dir = unique_marker(action);
            let secret = format!("exact-{action}-secret-value");
            {
                let mut supervisor = fixture_logging_supervisor(&log_dir, "log-wait", &secret);
                supervisor.start().expect("child should start");
                if action == "stop" {
                    supervisor.stop().expect("child should stop");
                }
            }
            let (_, contents) = read_gateway_logs(&log_dir);
            assert!(contents.contains("GATEWAY_FIXTURE_STOPPED"));
            assert!(contents.contains("GATEWAY_FIXTURE_STDERR"));
            assert!(!contents.contains(&secret));
            fs::remove_dir_all(log_dir).expect("log directory should remove");
        }
    }

    #[test]
    fn logging_write_failure_does_not_block_shutdown_and_is_reported_once() {
        let log_dir = unique_marker("logging-failure");
        let mut supervisor =
            fixture_logging_supervisor(&log_dir, "log-wait", "exact-write-failure-secret");
        fs::create_dir(log_dir.join("gateway.jsonl.1"))
            .expect("unsafe rotation destination should create");
        supervisor.start().expect("child should start");

        let started_at = Instant::now();
        supervisor
            .stop()
            .expect("logging failure must not block child shutdown");
        assert!(started_at.elapsed() < Duration::from_secs(2));
        assert_eq!(
            supervisor.take_log_health_update().write_error_code,
            Some("RUNTIME_LOG_FILE_UNAVAILABLE")
        );
        assert_eq!(
            supervisor.take_log_health_update(),
            super::GatewayLogHealthUpdate::default()
        );
        fs::remove_dir_all(log_dir).expect("log directory should remove");
    }

    #[test]
    fn capture_error_and_matching_write_recovery_are_reported_independently() {
        struct FailingReader;

        impl Read for FailingReader {
            fn read(&mut self, _buffer: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("injected capture read failure"))
            }
        }

        let log_dir = unique_marker("overlapping-log-health");
        fs::create_dir_all(&log_dir).expect("log directory should create");
        let sink = StructuredLogSink::open(
            &log_dir,
            "gateway.jsonl",
            "gateway",
            LogPolicy {
                max_bytes: MIN_LOG_MAX_BYTES,
                retained_files: 2,
                max_line_bytes: 256,
            },
        )
        .expect("sink should open");
        let capture = sink
            .capture(FailingReader, std::io::empty(), Vec::new())
            .expect("capture threads should start");
        assert_eq!(
            capture.finish(),
            Err(cmclient_runtime_logging::RuntimeLogError::CaptureReadFailed)
        );

        let failure_path = log_dir.join("gateway.jsonl.1");
        fs::create_dir(&failure_path).expect("write failure fixture should create");
        assert!(
            sink.write_code(
                cmclient_runtime_logging::LogLevel::Info,
                "GATEWAY_HEARTBEAT"
            )
            .is_err()
        );
        fs::remove_dir(&failure_path).expect("write failure fixture should remove");
        sink.write_code(
            cmclient_runtime_logging::LogLevel::Info,
            "GATEWAY_HEARTBEAT",
        )
        .expect("repaired write should succeed");

        let mut supervisor = fixture_supervisor(
            "log-wait",
            Duration::ZERO,
            BackoffPolicy::default(),
            Duration::from_secs(1),
            None,
        );
        supervisor
            .set_log_sink(sink)
            .expect("sink should attach before spawn");
        assert_eq!(
            supervisor.take_log_health_update(),
            super::GatewayLogHealthUpdate {
                capture_error_code: Some("RUNTIME_LOG_CAPTURE_READ_FAILED"),
                write_error_code: None,
                write_recovered_code: Some("RUNTIME_LOG_FILE_UNAVAILABLE"),
            }
        );
        fs::remove_dir_all(log_dir).expect("log directory should remove");
    }

    fn fixture_supervisor(
        mode: &str,
        delay: Duration,
        policy: BackoffPolicy,
        stable_window: Duration,
        marker: Option<&PathBuf>,
    ) -> GatewaySupervisor {
        let mut supervisor =
            GatewaySupervisor::new_with_stable_window(fixture_command(), policy, stable_window)
                .expect("fixture supervisor should initialize");
        let mut environment = BTreeMap::from([
            (String::from(FIXTURE_MODE), String::from(mode)),
            (
                String::from(FIXTURE_DELAY_MS),
                delay.as_millis().to_string(),
            ),
        ]);
        if let Some(marker) = marker {
            environment.insert(
                String::from(FIXTURE_MARKER),
                marker.to_string_lossy().into_owned(),
            );
        }
        supervisor.set_environment(environment);
        supervisor
    }

    fn fixture_logging_supervisor(
        log_dir: &PathBuf,
        mode: &str,
        secret: &str,
    ) -> GatewaySupervisor {
        let mut supervisor = fixture_supervisor(
            mode,
            Duration::ZERO,
            BackoffPolicy {
                initial_delay: Duration::from_millis(10),
                maximum_delay: Duration::from_millis(10),
            },
            Duration::from_secs(1),
            None,
        );
        supervisor.set_environment(BTreeMap::from([(
            String::from("CMCLIENT_FIXTURE_SECRET"),
            String::from(secret),
        )]));
        fs::create_dir_all(log_dir).expect("fixture log directory should create");
        fs::write(
            log_dir.join("gateway.jsonl.2000-01-01"),
            vec![b'x'; usize::try_from(MIN_LOG_MAX_BYTES - 128).expect("size should fit")],
        )
        .expect("fixture retained dated log should prefill");
        let sink = StructuredLogSink::open(
            log_dir,
            "gateway.jsonl",
            "gateway",
            LogPolicy {
                max_bytes: MIN_LOG_MAX_BYTES,
                retained_files: 4,
                max_line_bytes: 256,
            },
        )
        .expect("fixture log sink should open");
        supervisor
            .set_log_sink(sink)
            .expect("fixture log sink should attach");
        supervisor
    }

    fn read_gateway_logs(log_dir: &PathBuf) -> (Vec<PathBuf>, String) {
        let mut files: Vec<PathBuf> = fs::read_dir(log_dir)
            .expect("log directory should read")
            .map(|entry| entry.expect("log entry should read").path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name == "gateway.jsonl" || name.starts_with("gateway.jsonl.")
                    })
            })
            .collect();
        files.sort();
        let contents = files
            .iter()
            .map(|path| fs::read_to_string(path).expect("log file should read"))
            .collect::<Vec<_>>()
            .join("\n");
        (files, contents)
    }

    fn spawn_rejected_probe_server(
        capability: String,
        body: String,
    ) -> (SocketAddr, thread::JoinHandle<()>) {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .expect("probe fixture should bind loopback");
        let address = listener
            .local_addr()
            .expect("probe fixture address should resolve");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("probe request should connect");
            let request = read_probe_request(&mut stream);
            assert!(!request.contains(&capability));
            assert!(request.starts_with(&format!("GET {GATEWAY_OWNERSHIP_PATH} HTTP/1.1\r\n")));
            write_probe_response(&mut stream, "403 Forbidden", &body, &[]);
        });
        (address, server)
    }

    fn spawn_ownership_proof_server(
        capability: String,
        pid: u32,
        startup_nonce: String,
    ) -> (SocketAddr, thread::JoinHandle<()>) {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .expect("ownership proof fixture should bind loopback");
        let address = listener
            .local_addr()
            .expect("ownership proof fixture address should resolve");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("ownership probe should connect");
            let request = read_probe_request(&mut stream);
            assert!(!request.contains(&capability));
            let challenge = exact_probe_header(&request, GATEWAY_OWNERSHIP_CHALLENGE_HEADER)
                .expect("probe challenge should be singular")
                .to_owned();
            let ready = GatewayReady {
                pid,
                address,
                startup_nonce,
                capability: capability.clone(),
            };
            let mut mac = Hmac::<Sha256>::new_from_slice(capability.as_bytes()).unwrap();
            mac.update(super::gateway_ownership_transcript(&ready, &challenge).as_bytes());
            let proof = format!("{:x}", mac.finalize().into_bytes());
            write_probe_response(
                &mut stream,
                "200 OK",
                "",
                &[(GATEWAY_OWNERSHIP_PROOF_HEADER, &proof)],
            );
        });
        (address, server)
    }

    fn spawn_hostile_takeover_server(capability: String) -> (SocketAddr, thread::JoinHandle<()>) {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .expect("hostile fixture should bind loopback");
        let address = listener
            .local_addr()
            .expect("hostile fixture address should resolve");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("ownership probe should connect");
            let request = read_probe_request(&mut stream);
            assert!(!request.contains(&capability));
            assert!(exact_probe_header(&request, "x-cmclient-gateway-capability").is_none());
            let challenge = exact_probe_header(&request, GATEWAY_OWNERSHIP_CHALLENGE_HEADER)
                .expect("fresh challenge should be present");
            assert_eq!(challenge.len(), 64);
            assert!(
                challenge
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            );
            let forged = "0".repeat(64);
            write_probe_response(
                &mut stream,
                "200 OK",
                "",
                &[(GATEWAY_OWNERSHIP_PROOF_HEADER, &forged)],
            );
        });
        (address, server)
    }

    fn read_probe_request(stream: &mut std::net::TcpStream) -> String {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("probe fixture timeout should configure");
        let mut request = Vec::with_capacity(512);
        let mut chunk = [0_u8; 256];
        while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
            let count = stream.read(&mut chunk).expect("probe request should read");
            assert_ne!(count, 0, "probe request ended before headers");
            request.extend_from_slice(&chunk[..count]);
            assert!(request.len() <= 4096, "probe request must remain bounded");
        }
        String::from_utf8(request).expect("probe request should be ASCII")
    }

    fn exact_probe_header<'a>(request: &'a str, expected_name: &str) -> Option<&'a str> {
        let values = request
            .split("\r\n")
            .filter_map(|line| line.split_once(':'))
            .filter(|(name, _)| name.eq_ignore_ascii_case(expected_name))
            .map(|(_, value)| value.trim())
            .collect::<Vec<_>>();
        (values.len() == 1).then(|| values[0])
    }

    fn write_probe_response(
        stream: &mut std::net::TcpStream,
        status: &str,
        body: &str,
        headers: &[(&str, &str)],
    ) {
        write!(
            stream,
            "HTTP/1.1 {status}\r\nConnection: close\r\nContent-Length: {}\r\n",
            body.len()
        )
        .expect("probe response should write");
        for (name, value) in headers {
            write!(stream, "{name}: {value}\r\n").expect("probe header should write");
        }
        write!(stream, "\r\n{body}").expect("probe body should write");
        stream.flush().expect("probe response should flush");
    }

    fn fixture_command() -> GatewayCommand {
        GatewayCommand {
            program: env::current_exe()
                .expect("test executable should resolve")
                .to_string_lossy()
                .into_owned(),
            arguments: vec![
                String::from("--ignored"),
                String::from("--nocapture"),
                String::from("--exact"),
                String::from("tests::supervisor_child_fixture"),
            ],
        }
    }

    fn wait_for_event(
        supervisor: &mut GatewaySupervisor,
        timeout: Duration,
        predicate: impl Fn(&SupervisorEvent) -> bool,
    ) -> SupervisorEvent {
        let deadline = Instant::now() + timeout;
        loop {
            let event = supervisor.tick().expect("supervisor tick should succeed");
            if predicate(&event) {
                return event;
            }
            assert!(Instant::now() < deadline, "supervisor event timed out");
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn unique_marker(label: &str) -> PathBuf {
        let sequence = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should follow the epoch")
            .as_nanos();
        env::temp_dir().join(format!(
            "cmclient-supervisor-{label}-{}-{sequence}",
            process::id()
        ))
    }

    #[cfg(target_os = "windows")]
    fn powershell_bootstrap_supervisor(mode: &str, marker: &std::path::Path) -> GatewaySupervisor {
        let powershell = PathBuf::from(
            env::var("SystemRoot").expect("Windows system root should be configured"),
        )
        .join("System32/WindowsPowerShell/v1.0/powershell.exe");
        let mut supervisor = GatewaySupervisor::new_with_stable_window(
            GatewayCommand {
                program: powershell.to_string_lossy().into_owned(),
                arguments: vec![
                    String::from("-NoLogo"),
                    String::from("-NoProfile"),
                    String::from("-NonInteractive"),
                    String::from("-ExecutionPolicy"),
                    String::from("Bypass"),
                    String::from("-Command"),
                    String::from(POWERSHELL_BOOTSTRAP_FIXTURE),
                ],
            },
            BackoffPolicy::default(),
            Duration::from_secs(1),
        )
        .expect("PowerShell fixture supervisor should initialize");
        supervisor.set_environment(BTreeMap::from([
            (String::from(FIXTURE_MODE), String::from(mode)),
            (String::from(FIXTURE_DELAY_MS), String::from("500")),
            (
                String::from(FIXTURE_MARKER),
                marker.to_string_lossy().into_owned(),
            ),
        ]));
        supervisor
    }

    #[cfg(target_os = "windows")]
    fn windows_process_exists(powershell: &std::path::Path, pid: u32) -> bool {
        let status = process::Command::new(powershell)
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                &format!(
                    "if (Get-Process -Id {pid} -ErrorAction SilentlyContinue) {{ exit 10 }}; exit 0"
                ),
            ])
            .status()
            .expect("Windows process query should execute");
        match status.code() {
            Some(0) => false,
            Some(10) => true,
            code => panic!("Windows process query returned unexpected status {code:?}"),
        }
    }

    #[cfg(target_os = "windows")]
    const POWERSHELL_BOOTSTRAP_FIXTURE: &str = r#"
$ErrorActionPreference = 'Stop'
$inputStream = [Console]::OpenStandardInput()
$outputStream = [Console]::OpenStandardOutput()

$prefix = [byte[]]::new(4)
$offset = 0
while ($offset -lt $prefix.Length) {
    $count = $inputStream.Read($prefix, $offset, $prefix.Length - $offset)
    if ($count -eq 0) { exit 65 }
    $offset += $count
}
$length = [Net.IPAddress]::NetworkToHostOrder([BitConverter]::ToInt32($prefix, 0))
if ($length -lt 1 -or $length -gt 16384) { exit 66 }
$body = [byte[]]::new($length)
$offset = 0
while ($offset -lt $body.Length) {
    $count = $inputStream.Read($body, $offset, $body.Length - $offset)
    if ($count -eq 0) { exit 67 }
    $offset += $count
}
$bootstrap = [Text.Encoding]::UTF8.GetString($body) | ConvertFrom-Json
$nonce = [string]$bootstrap.startupNonce
$capability = [string]$bootstrap.capability
$callMeshApiKey = [string]$bootstrap.callMeshApiKey
$mode = [string]$env:CMCLIENT_SUPERVISOR_TEST_MODE

$leaked = [Environment]::GetCommandLineArgs() | Where-Object {
    ([string]$_).Contains($nonce) -or
    ([string]$_).Contains($capability) -or
    ($callMeshApiKey.Length -gt 0 -and ([string]$_).Contains($callMeshApiKey))
}
if (-not $leaked) {
    $leaked = [Environment]::GetEnvironmentVariables().Values | Where-Object {
        ([string]$_).Contains($nonce) -or
        ([string]$_).Contains($capability) -or
        ($callMeshApiKey.Length -gt 0 -and ([string]$_).Contains($callMeshApiKey))
    }
}
if ($leaked) {
    [IO.File]::WriteAllText(
        "$($env:CMCLIENT_SUPERVISOR_TEST_MARKER).secret-leak",
        'bootstrap secret reached argv or environment'
    )
    exit 70
}
if ($mode -eq 'bootstrap-success') {
    $sha256 = [Security.Cryptography.SHA256]::Create()
    $callMeshApiKeyHash = -join (
        $sha256.ComputeHash([Text.Encoding]::UTF8.GetBytes($callMeshApiKey)) |
            ForEach-Object { $_.ToString('x2') }
    )
    $sha256.Dispose()
    if ($callMeshApiKeyHash -ne 'dbc01b6ad52367ef996a43d8a223a0854f9dcac3a34cdc46039452189bebee15') {
        exit 78
    }
}

function Write-Ready([uint32]$readyPid, [string]$readyNonce, [uint16]$port) {
    $ready = [ordered]@{
        schemaVersion = 1
        type = 'gateway.ready'
        pid = $readyPid
        startupNonce = $readyNonce
        host = '127.0.0.1'
        port = $port
    }
    $bytes = [Text.Encoding]::UTF8.GetBytes(($ready | ConvertTo-Json -Compress))
    $framePrefix = [BitConverter]::GetBytes(
        [Net.IPAddress]::HostToNetworkOrder([int]$bytes.Length)
    )
    $outputStream.Write($framePrefix, 0, $framePrefix.Length)
    $outputStream.Write($bytes, 0, $bytes.Length)
    $outputStream.Flush()
}

function Wait-And-Mark {
    Start-Sleep -Milliseconds ([int]$env:CMCLIENT_SUPERVISOR_TEST_DELAY_MS)
    [IO.File]::WriteAllText(
        [string]$env:CMCLIENT_SUPERVISOR_TEST_MARKER,
        'bootstrap child survived'
    )
    Start-Sleep -Seconds 30
}

function Serve-Ownership-Proof(
    [Net.Sockets.TcpListener]$listener,
    [uint16]$port
) {
    $pending = $listener.BeginAcceptTcpClient($null, $null)
    if (-not $pending.AsyncWaitHandle.WaitOne(5000)) { exit 71 }
    $client = $listener.EndAcceptTcpClient($pending)
    $stream = $client.GetStream()
    $request = [byte[]]::new(4096)
    $offset = 0
    while ($offset -lt $request.Length) {
        $count = $stream.Read($request, $offset, $request.Length - $offset)
        if ($count -eq 0) { exit 72 }
        $offset += $count
        $text = [Text.Encoding]::ASCII.GetString($request, 0, $offset)
        if ($text.Contains("`r`n`r`n")) { break }
    }
    if (-not $text.Contains("`r`n`r`n")) { exit 73 }
    if ($text.Contains($capability)) { exit 74 }
    $match = [Text.RegularExpressions.Regex]::Match(
        $text,
        '(?im)^x-cmclient-gateway-ownership-challenge: ([0-9a-f]{64})\r?$'
    )
    if (-not $match.Success) { exit 75 }
    $challenge = $match.Groups[1].Value
    $transcript =
        "cmclient.gateway.bootstrap-ownership.v1`n$nonce`n$PID`n127.0.0.1`n$port`n$challenge"
    $hmac = [Security.Cryptography.HMACSHA256]::new(
        [Text.Encoding]::UTF8.GetBytes($capability)
    )
    $hash = $hmac.ComputeHash([Text.Encoding]::UTF8.GetBytes($transcript))
    $proof = -join ($hash | ForEach-Object { $_.ToString('x2') })
    $hmac.Dispose()
    $response = [Text.Encoding]::ASCII.GetBytes(
        "HTTP/1.1 200 OK`r`nConnection: close`r`nContent-Length: 0`r`n" +
        "x-cmclient-gateway-ownership-proof: $proof`r`n`r`n"
    )
    $stream.Write($response, 0, $response.Length)
    $stream.Flush()
    $client.Dispose()
    $listener.Stop()
}

function Start-Rejection-Server {
    $env:CMCLIENT_SUPERVISOR_TEST_SERVER_READY =
        "$($env:CMCLIENT_SUPERVISOR_TEST_MARKER).server-ready"
    $env:CMCLIENT_SUPERVISOR_TEST_SERVER_DONE =
        "$($env:CMCLIENT_SUPERVISOR_TEST_MARKER).server-done"
    $env:CMCLIENT_SUPERVISOR_TEST_DESCENDANT_MARKER =
        "$($env:CMCLIENT_SUPERVISOR_TEST_MARKER).descendant-survived"
    $serverScript = @'
$ErrorActionPreference = 'Stop'
$heldStdout = [Console]::OpenStandardOutput()
$listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
$listener.Start()
[IO.File]::WriteAllText(
    [string]$env:CMCLIENT_SUPERVISOR_TEST_SERVER_READY,
    [string]$listener.LocalEndpoint.Port
)
$pending = $listener.BeginAcceptTcpClient($null, $null)
if (-not $pending.AsyncWaitHandle.WaitOne(5000)) {
    $listener.Stop()
    exit 68
}
$client = $listener.EndAcceptTcpClient($pending)
$stream = $client.GetStream()
$request = [byte[]]::new(4096)
$count = $stream.Read($request, 0, $request.Length)
$requestText = [Text.Encoding]::ASCII.GetString($request, 0, $count)
$summary = if ($requestText -match '(?im)^x-cmclient-gateway-capability:') {
    'capability-header-present'
} else {
    'capability-header-absent'
}
[IO.File]::WriteAllText(
    [string]$env:CMCLIENT_SUPERVISOR_TEST_SERVER_DONE,
    $summary
)
$response = [Text.Encoding]::ASCII.GetBytes(
    "HTTP/1.1 403 Forbidden`r`nContent-Length: 2`r`nConnection: close`r`n`r`n{}"
)
$stream.Write($response, 0, $response.Length)
$stream.Flush()
$client.Dispose()
$listener.Stop()
[Threading.Thread]::Sleep(600)
[IO.File]::WriteAllText(
    [string]$env:CMCLIENT_SUPERVISOR_TEST_DESCENDANT_MARKER,
    'descendant survived rejection cleanup'
)
[Threading.Thread]::Sleep(30000)
'@
    $encoded = [Convert]::ToBase64String(
        [Text.Encoding]::Unicode.GetBytes($serverScript)
    )
    $null = Start-Process `
        -FilePath (Join-Path $PSHOME 'powershell.exe') `
        -ArgumentList @(
            '-NoLogo', '-NoProfile', '-NonInteractive',
            '-ExecutionPolicy', 'Bypass', '-EncodedCommand', $encoded
        ) `
        -WindowStyle Hidden `
        -PassThru
    $deadline = [DateTime]::UtcNow.AddSeconds(5)
    while (-not [IO.File]::Exists($env:CMCLIENT_SUPERVISOR_TEST_SERVER_READY)) {
        if ([DateTime]::UtcNow -ge $deadline) { exit 69 }
        Start-Sleep -Milliseconds 10
    }
    $port = [uint16][IO.File]::ReadAllText(
        $env:CMCLIENT_SUPERVISOR_TEST_SERVER_READY
    )
    [IO.File]::Delete($env:CMCLIENT_SUPERVISOR_TEST_SERVER_READY)
    return $port
}

function Start-Stdout-Descendant {
    $env:CMCLIENT_SUPERVISOR_TEST_DESCENDANT_READY =
        "$($env:CMCLIENT_SUPERVISOR_TEST_MARKER).descendant-ready"
    $env:CMCLIENT_SUPERVISOR_TEST_DESCENDANT_MARKER =
        "$($env:CMCLIENT_SUPERVISOR_TEST_MARKER).stdout-descendant-survived"
    $descendantScript = @'
$ErrorActionPreference = 'Stop'
$heldStdout = [Console]::OpenStandardOutput()
[IO.File]::WriteAllText(
    [string]$env:CMCLIENT_SUPERVISOR_TEST_DESCENDANT_READY,
    'stdout inherited'
)
    [Threading.Thread]::Sleep(30000)
'@
    $encoded = [Convert]::ToBase64String(
        [Text.Encoding]::Unicode.GetBytes($descendantScript)
    )
    $null = Start-Process `
        -FilePath (Join-Path $PSHOME 'powershell.exe') `
        -ArgumentList @(
            '-NoLogo', '-NoProfile', '-NonInteractive',
            '-ExecutionPolicy', 'Bypass', '-EncodedCommand', $encoded
        ) `
        -WindowStyle Hidden `
        -PassThru
    $deadline = [DateTime]::UtcNow.AddSeconds(5)
    while (-not [IO.File]::Exists($env:CMCLIENT_SUPERVISOR_TEST_DESCENDANT_READY)) {
        if ([DateTime]::UtcNow -ge $deadline) { exit 76 }
        Start-Sleep -Milliseconds 10
    }
    [IO.File]::WriteAllText(
        "$($env:CMCLIENT_SUPERVISOR_TEST_MARKER).descendant-started",
        'stdout-holding descendant started'
    )
    [IO.File]::Delete($env:CMCLIENT_SUPERVISOR_TEST_DESCENDANT_READY)
}

switch ($mode) {
    'bootstrap-success' {
        $listener = [Net.Sockets.TcpListener]::new(
            [Net.IPAddress]::Loopback,
            0
        )
        $listener.Start()
        $port = [uint16]$listener.LocalEndpoint.Port
        Write-Ready ([uint32]$PID) $nonce $port
        Serve-Ownership-Proof $listener $port
        $shutdown = [byte[]]::new(18)
        $offset = 0
        while ($offset -lt $shutdown.Length) {
            $count = $inputStream.Read(
                $shutdown,
                $offset,
                $shutdown.Length - $offset
            )
            if ($count -eq 0) { break }
            $offset += $count
        }
        exit 0
    }
    'bootstrap-delayed-success' {
        Start-Sleep -Milliseconds ([int]$env:CMCLIENT_SUPERVISOR_TEST_DELAY_MS)
        $listener = [Net.Sockets.TcpListener]::new(
            [Net.IPAddress]::Loopback,
            0
        )
        $listener.Start()
        $port = [uint16]$listener.LocalEndpoint.Port
        Write-Ready ([uint32]$PID) $nonce $port
        Serve-Ownership-Proof $listener $port
        $shutdown = [byte[]]::new(18)
        $offset = 0
        while ($offset -lt $shutdown.Length) {
            $count = $inputStream.Read(
                $shutdown,
                $offset,
                $shutdown.Length - $offset
            )
            if ($count -eq 0) { break }
            $offset += $count
        }
        exit 0
    }
    'bootstrap-wrong-pid' {
        Write-Ready ([uint32]($PID + 1)) $nonce 49152
        exit 0
    }
    'bootstrap-wrong-nonce' {
        $wrongNonce = 'f' * 32
        if ($wrongNonce -eq $nonce) { $wrongNonce = 'e' * 32 }
        Write-Ready ([uint32]$PID) $wrongNonce 49152
        exit 0
    }
    'bootstrap-wrong-capability' {
        $port = Start-Rejection-Server
        Write-Ready ([uint32]$PID) $nonce $port
        exit 0
    }
    'bootstrap-timeout' {
        Start-Stdout-Descendant
        Start-Sleep -Seconds 30
    }
    'bootstrap-early-exit' {
        exit 23
    }
    'bootstrap-oversize' {
        [byte[]]$oversized = 0, 0, 64, 1
        $outputStream.Write($oversized, 0, $oversized.Length)
        $outputStream.Flush()
        Wait-And-Mark
    }
    default { exit 64 }
}
"#;

    #[test]
    #[ignore = "subprocess fixture"]
    fn supervisor_child_fixture() {
        let mode = env::var(FIXTURE_MODE).expect("fixture mode should be configured");
        let delay = env::var(FIXTURE_DELAY_MS)
            .expect("fixture delay should be configured")
            .parse::<u64>()
            .expect("fixture delay should be numeric");
        match mode.as_str() {
            "crash" => process::exit(7),
            "delayed-exit" => {
                thread::sleep(Duration::from_millis(delay));
                process::exit(7);
            }
            "delayed-marker" => {
                let mut command = String::new();
                std::io::stdin()
                    .lock()
                    .read_line(&mut command)
                    .expect("shutdown command should read");
                if command.trim() == "CMCLIENT_SHUTDOWN" {
                    fs::write(
                        env::var(FIXTURE_MARKER).expect("fixture marker should be configured"),
                        b"graceful shutdown",
                    )
                    .expect("fixture marker should be writable");
                } else {
                    thread::sleep(Duration::from_millis(delay));
                }
            }
            "ignore-shutdown" => {
                let mut command = String::new();
                std::io::stdin()
                    .lock()
                    .read_line(&mut command)
                    .expect("shutdown command should read");
                thread::sleep(Duration::from_secs(30));
            }
            "spawn-descendant-exit" => {
                process::Command::new(
                    env::current_exe().expect("fixture executable should resolve"),
                )
                .args([
                    "--ignored",
                    "--nocapture",
                    "--exact",
                    "tests::supervisor_child_fixture",
                ])
                .env(FIXTURE_MODE, "delayed-descendant-marker")
                .stdin(process::Stdio::null())
                .stdout(process::Stdio::inherit())
                .stderr(process::Stdio::inherit())
                .spawn()
                .expect("descendant fixture should spawn");
                process::exit(7);
            }
            "delayed-descendant-marker" => {
                thread::sleep(Duration::from_millis(delay));
                fs::write(
                    env::var(FIXTURE_MARKER).expect("fixture marker should be configured"),
                    b"descendant escaped",
                )
                .expect("descendant marker should be writable");
                thread::sleep(Duration::from_secs(30));
            }
            "log-exit" => {
                write_log_fixture_output();
                process::exit(7);
            }
            "log-wait" => {
                write_log_fixture_output();
                let mut command = String::new();
                std::io::stdin()
                    .lock()
                    .read_line(&mut command)
                    .expect("shutdown command should read");
                if command.trim() == "CMCLIENT_SHUTDOWN" {
                    println!(
                        "{{\"level\":\"info\",\"message\":\"GATEWAY_FIXTURE_STOPPED\",\"traceId\":\"fixture-stop\"}}"
                    );
                    eprintln!("GATEWAY_FIXTURE_STDERR");
                    std::io::stdout().flush().expect("stdout should flush");
                    std::io::stderr().flush().expect("stderr should flush");
                }
            }
            _ => process::exit(64),
        }
    }

    fn write_log_fixture_output() {
        let secret =
            env::var("CMCLIENT_FIXTURE_SECRET").expect("fixture secret should be configured");
        for index in 0..8 {
            println!(
                "{{\"level\":\"info\",\"message\":\"GATEWAY_FIXTURE_RECORD\",\"traceId\":\"fixture-{index}\",\"fields\":{{\"apiKey\":\"{secret}\",\"detail\":\"prefix-{secret}-suffix\"}}}}"
            );
        }
        println!("{}", "x".repeat(600));
        eprintln!("GATEWAY_FIXTURE_STDERR");
        eprintln!("raw-{secret}");
        std::io::stdout().flush().expect("stdout should flush");
        std::io::stderr().flush().expect("stderr should flush");
    }
}
