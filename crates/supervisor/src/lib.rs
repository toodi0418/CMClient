//! Process supervision primitives owned by the Rust Agent.

use cmclient_runtime_logging::{ChildOutputCapture, RuntimeLogError, StructuredLogSink};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt,
    io::{ErrorKind, Read, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream},
    process::{Child, Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

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
pub const GATEWAY_PRIVATE_FRAME_MAX_BYTES: usize = 4096;
pub const DEFAULT_GATEWAY_BOOTSTRAP_DEADLINE: Duration = Duration::from_secs(5);
const GATEWAY_HEALTH_RESPONSE_MAX_BYTES: usize = 4096;
const GATEWAY_CAPABILITY_HEADER: &str = "x-cmclient-gateway-capability";
const GATEWAY_HEALTH_PATH: &str = "/api/v1/system/health";
const INHERITED_RUNTIME_ENVIRONMENT_NAMES: [&str; 4] = ["PATH", "SystemRoot", "WINDIR", "ComSpec"];

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

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GatewayBootstrapFrame {
    schema_version: u8,
    #[serde(rename = "type")]
    frame_type: &'static str,
    startup_nonce: String,
    capability: String,
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GatewayHealthResponse {
    status: String,
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

pub struct GatewaySupervisor {
    command: GatewayCommand,
    backoff_policy: BackoffPolicy,
    child: Option<Child>,
    environment: BTreeMap<String, String>,
    log_sink: Option<StructuredLogSink>,
    output_capture: Option<ChildOutputCapture>,
    pending_log_error_code: Option<&'static str>,
    failed_attempts: u32,
    restart_not_before: Option<Instant>,
    stable_window: Duration,
    started_at: Option<Instant>,
    private_bootstrap: bool,
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
            pending_log_error_code: None,
            failed_attempts: 0,
            restart_not_before: None,
            stable_window,
            started_at: None,
            private_bootstrap: false,
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

    pub fn set_environment(&mut self, environment: BTreeMap<String, String>) {
        self.environment.extend(environment);
    }

    pub fn enable_private_bootstrap(&mut self) -> Result<(), SupervisorError> {
        if self.child.is_some() {
            return Err(SupervisorError::ProcessIoFailed);
        }
        self.private_bootstrap = true;
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

    pub fn take_log_error_code(&mut self) -> Option<&'static str> {
        let sink_error = self
            .log_sink
            .as_ref()
            .and_then(StructuredLogSink::take_error_code);
        self.pending_log_error_code.take().or(sink_error)
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
        let child = Command::new(&self.command.program)
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
            })
            .spawn();
        let mut child = match child {
            Ok(child) => child,
            Err(_) => {
                self.register_failure(Instant::now());
                return Err(SupervisorError::SpawnFailed);
            }
        };
        let ready = if private_bootstrap {
            match perform_private_bootstrap(&mut child, self.bootstrap_deadline) {
                Ok(ready) => Some(ready),
                Err(error) => {
                    let observed_at = Instant::now();
                    let _ = terminate_child(&mut child);
                    self.register_failure(observed_at);
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
                self.child = None;
                self.ready = None;
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
        self.finish_output_capture();
        self.reset_tracking();
        Ok(SupervisorEvent::Stopped)
    }

    fn capture_child_output(
        &mut self,
        child: &mut Child,
        stdout_reserved: bool,
        ready: Option<&GatewayReady>,
    ) -> Result<(), SupervisorError> {
        let Some(sink) = self.log_sink.clone() else {
            return Ok(());
        };
        let stderr = child.stderr.take().ok_or_else(|| {
            let code = RuntimeLogError::CaptureReadFailed.code();
            self.remember_log_error_code(code);
            SupervisorError::LoggingFailed(code)
        })?;
        let mut secrets = sensitive_environment_values(&self.environment);
        if let Some(ready) = ready {
            secrets.push(ready.capability.clone());
            secrets.push(ready.startup_nonce.clone());
        }
        let capture = if stdout_reserved {
            sink.capture(std::io::empty(), stderr, secrets)
        } else {
            let stdout = child.stdout.take().ok_or_else(|| {
                let code = RuntimeLogError::CaptureReadFailed.code();
                self.remember_log_error_code(code);
                SupervisorError::LoggingFailed(code)
            })?;
            sink.capture(stdout, stderr, secrets)
        };
        self.output_capture = Some(capture.map_err(|error| {
            let code = error.code();
            self.remember_log_error_code(code);
            SupervisorError::LoggingFailed(code)
        })?);
        Ok(())
    }

    fn finish_output_capture(&mut self) {
        if let Some(capture) = self.output_capture.take()
            && let Err(error) = capture.finish()
        {
            self.remember_log_error_code(error.code());
        }
        if let Some(code) = self
            .log_sink
            .as_ref()
            .and_then(StructuredLogSink::take_error_code)
        {
            self.remember_log_error_code(code);
        }
    }

    fn remember_log_error_code(&mut self, code: &'static str) {
        if self.pending_log_error_code.is_none() {
            self.pending_log_error_code = Some(code);
        }
    }

    fn reset_tracking(&mut self) {
        self.failed_attempts = 0;
        self.restart_not_before = None;
        self.started_at = None;
        self.ready = None;
    }
}

fn perform_private_bootstrap(
    child: &mut Child,
    deadline: Duration,
) -> Result<GatewayReady, SupervisorError> {
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
        schema_version: 1,
        frame_type: "gateway.bootstrap",
        startup_nonce: startup_nonce.clone(),
        capability: capability.clone(),
    };
    let encoded = encode_private_frame(&bootstrap)?;
    child
        .stdin
        .as_mut()
        .ok_or(SupervisorError::BootstrapIoFailed)?
        .write_all(&encoded)
        .and_then(|()| {
            child
                .stdin
                .as_mut()
                .ok_or_else(|| std::io::Error::other("child stdin unavailable"))?
                .flush()
        })
        .map_err(|_| SupervisorError::BootstrapIoFailed)?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or(SupervisorError::BootstrapIoFailed)?;
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::Builder::new()
        .name(String::from("cmclient-gateway-bootstrap"))
        .spawn(move || {
            let _ = sender.send(read_private_frame(&mut stdout));
        })
        .map_err(|_| SupervisorError::BootstrapIoFailed)?;
    let bytes = receiver
        .recv_timeout(remaining_until(bootstrap_deadline)?)
        .map_err(|error| match error {
            mpsc::RecvTimeoutError::Timeout => SupervisorError::BootstrapTimeout,
            mpsc::RecvTimeoutError::Disconnected => SupervisorError::BootstrapIoFailed,
        })??;
    let ready = validate_gateway_ready(&bytes, child.id(), startup_nonce, capability)?;
    probe_gateway_health(&ready, remaining_until(bootstrap_deadline)?)?;
    Ok(ready)
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

fn probe_gateway_health(ready: &GatewayReady, timeout: Duration) -> Result<(), SupervisorError> {
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
    let mut stream = TcpStream::connect_timeout(&ready.address, remaining_until(deadline)?)
        .map_err(map_probe_io_error)?;
    stream
        .set_write_timeout(Some(remaining_until(deadline)?))
        .map_err(|_| SupervisorError::BootstrapProbeFailed)?;
    let request = format!(
        "GET {GATEWAY_HEALTH_PATH} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\n{GATEWAY_CAPABILITY_HEADER}: {}\r\nAccept: application/json\r\nConnection: close\r\n\r\n",
        ready.address.port(),
        ready.capability
    );
    stream
        .write_all(request.as_bytes())
        .and_then(|()| stream.flush())
        .map_err(map_probe_io_error)?;

    let mut response = Vec::with_capacity(512);
    let mut chunk = [0_u8; 512];
    loop {
        if validate_gateway_health_response(&response)? {
            return Ok(());
        }
        if response.len() == GATEWAY_HEALTH_RESPONSE_MAX_BYTES {
            return Err(SupervisorError::BootstrapProbeFailed);
        }
        stream
            .set_read_timeout(Some(remaining_until(deadline)?))
            .map_err(|_| SupervisorError::BootstrapProbeFailed)?;
        let count = stream.read(&mut chunk).map_err(map_probe_io_error)?;
        if count == 0 {
            return Err(SupervisorError::BootstrapProbeFailed);
        }
        if response.len().saturating_add(count) > GATEWAY_HEALTH_RESPONSE_MAX_BYTES {
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

fn validate_gateway_health_response(response: &[u8]) -> Result<bool, SupervisorError> {
    let Some(header_end) = response.windows(4).position(|bytes| bytes == b"\r\n\r\n") else {
        return Ok(false);
    };
    let header = std::str::from_utf8(&response[..header_end])
        .map_err(|_| SupervisorError::BootstrapProbeFailed)?;
    let mut lines = header.split("\r\n");
    let mut status = lines
        .next()
        .ok_or(SupervisorError::BootstrapProbeFailed)?
        .split_ascii_whitespace();
    let version = status.next().ok_or(SupervisorError::BootstrapProbeFailed)?;
    let code = status.next().ok_or(SupervisorError::BootstrapProbeFailed)?;
    if !matches!(version, "HTTP/1.0" | "HTTP/1.1") || code != "200" {
        return Err(SupervisorError::BootstrapProbeFailed);
    }

    let mut content_length = None;
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .ok_or(SupervisorError::BootstrapProbeFailed)?;
        if name.eq_ignore_ascii_case("transfer-encoding") {
            return Err(SupervisorError::BootstrapProbeFailed);
        }
        if name.eq_ignore_ascii_case("content-length") {
            if content_length.is_some() {
                return Err(SupervisorError::BootstrapProbeFailed);
            }
            content_length = Some(
                value
                    .trim()
                    .parse::<usize>()
                    .map_err(|_| SupervisorError::BootstrapProbeFailed)?,
            );
        }
    }
    let content_length = content_length.ok_or(SupervisorError::BootstrapProbeFailed)?;
    let body_start = header_end + 4;
    let response_length = body_start
        .checked_add(content_length)
        .filter(|length| *length <= GATEWAY_HEALTH_RESPONSE_MAX_BYTES)
        .ok_or(SupervisorError::BootstrapProbeFailed)?;
    if response.len() < response_length {
        return Ok(false);
    }
    if response.len() != response_length {
        return Err(SupervisorError::BootstrapProbeFailed);
    }
    let health: GatewayHealthResponse = serde_json::from_slice(&response[body_start..])
        .map_err(|_| SupervisorError::BootstrapProbeFailed)?;
    if health.status != "ok" {
        return Err(SupervisorError::BootstrapProbeFailed);
    }
    Ok(true)
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
    let mut prefix = [0_u8; 4];
    reader
        .read_exact(&mut prefix)
        .map_err(|_| SupervisorError::BootstrapIoFailed)?;
    let length = usize::try_from(u32::from_be_bytes(prefix))
        .map_err(|_| SupervisorError::BootstrapInvalid)?;
    if length == 0 || length > GATEWAY_PRIVATE_FRAME_MAX_BYTES {
        return Err(SupervisorError::BootstrapInvalid);
    }
    let mut body = vec![0_u8; length];
    reader
        .read_exact(&mut body)
        .map_err(|_| SupervisorError::BootstrapIoFailed)?;
    Ok(body)
}

impl Drop for GatewaySupervisor {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            if terminate_child(&mut child).is_err() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        self.finish_output_capture();
    }
}

fn terminate_child(child: &mut Child) -> std::io::Result<()> {
    if child.try_wait()?.is_some() {
        return Ok(());
    }
    let graceful_requested = child.stdin.take().is_some_and(|mut input| {
        input
            .write_all(SHUTDOWN_COMMAND)
            .and_then(|()| input.flush())
            .is_ok()
    });
    if graceful_requested {
        let deadline = Instant::now() + GRACEFUL_SHUTDOWN_TIMEOUT;
        loop {
            if child.try_wait()?.is_some() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                break;
            }
            thread::sleep(SHUTDOWN_POLL_INTERVAL);
        }
    }
    match child.kill() {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::InvalidInput => {}
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
    use super::{
        BackoffPolicy, GATEWAY_CAPABILITY_HEADER, GATEWAY_PRIVATE_FRAME_MAX_BYTES,
        GatewayBootstrapFrame, GatewayCommand, GatewayReady, GatewayStatus, GatewaySupervisor,
        SupervisorError, SupervisorEvent, encode_private_frame, inherited_runtime_environment_from,
        probe_gateway_health, read_private_frame, validate_gateway_ready,
    };

    #[test]
    fn private_bootstrap_frames_are_bounded_and_ready_identity_is_exact() {
        let nonce = "a".repeat(32);
        let capability = "b".repeat(64);
        let encoded = encode_private_frame(&GatewayBootstrapFrame {
            schema_version: 1,
            frame_type: "gateway.bootstrap",
            startup_nonce: nonce.clone(),
            capability: capability.clone(),
        })
        .expect("bootstrap should encode");
        assert_eq!(
            usize::try_from(u32::from_be_bytes(encoded[..4].try_into().unwrap())).unwrap(),
            encoded.len() - 4
        );
        let body = read_private_frame(&mut encoded.as_slice()).expect("frame should decode");
        assert_eq!(body, encoded[4..]);

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
    fn capability_health_probe_accepts_the_exact_gateway_contract() {
        let capability = "b".repeat(64);
        let (address, server) = spawn_capability_gate_server(capability.clone());
        let ready = GatewayReady {
            pid: 42,
            address,
            startup_nonce: "a".repeat(32),
            capability,
        };

        probe_gateway_health(&ready, Duration::from_secs(2))
            .expect("capability-gated health should validate");
        server.join().expect("probe server should finish");
    }

    #[test]
    fn capability_health_probe_rejects_an_unrelated_live_loopback_port() {
        let capability = "c".repeat(64);
        let unrelated_capability = "d".repeat(64);
        let (address, server) = spawn_capability_gate_server(unrelated_capability);
        let ready = GatewayReady {
            pid: 42,
            address,
            startup_nonce: "e".repeat(32),
            capability,
        };

        assert_eq!(
            probe_gateway_health(&ready, Duration::from_secs(2)),
            Err(SupervisorError::BootstrapProbeFailed)
        );
        server.join().expect("unrelated probe server should finish");
    }

    #[test]
    fn health_probe_failure_diagnostics_do_not_reflect_capability_or_response() {
        let capability = "c".repeat(64);
        let reflected = format!(r#"{{"code":"denied","detail":"{capability}"}}"#);
        let (address, server) = spawn_probe_server(
            capability.clone(),
            String::from("403 Forbidden"),
            reflected.clone(),
        );
        let ready = GatewayReady {
            pid: 42,
            address,
            startup_nonce: "d".repeat(32),
            capability: capability.clone(),
        };

        let error = probe_gateway_health(&ready, Duration::from_secs(2))
            .expect_err("rejected health should fail bootstrap");
        server.join().expect("probe server should finish");
        let diagnostic = format!("{error:?}:{}", error.code());
        assert_eq!(error, SupervisorError::BootstrapProbeFailed);
        assert!(!diagnostic.contains(&capability));
        assert!(!diagnostic.contains(&reflected));
    }
    use cmclient_runtime_logging::{LogPolicy, MIN_LOG_MAX_BYTES, StructuredLogSink};
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
    fn inherited_child_environment_excludes_plaintext_secret_paths() {
        let source = BTreeMap::from([
            (String::from("PATH"), String::from("/fixture/bin")),
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
            BTreeMap::from([(String::from("PATH"), String::from("/fixture/bin"))])
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
    fn captures_redacts_rotates_and_restarts_real_gateway_output() {
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
        assert!(files.len() > 1, "fixture should exercise rotation");
        assert!(files.len() <= 5, "retention should remain bounded");
        assert!(!contents.contains(secret));
        assert!(contents.contains("[REDACTED]"));
        assert!(contents.contains("GATEWAY_FIXTURE_STDERR"));
        assert!(contents.contains("RUNTIME_LOG_STDERR_INVALID"));
        assert!(contents.contains("RUNTIME_LOG_STDOUT_OVERSIZED"));
        assert!(supervisor.take_log_error_code().is_none());
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
            supervisor.take_log_error_code(),
            Some("RUNTIME_LOG_FILE_UNAVAILABLE")
        );
        assert_eq!(supervisor.take_log_error_code(), None);
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
            String::from("CMCLIENT_CALLMESH_API_KEY"),
            String::from(secret),
        )]));
        fs::create_dir_all(log_dir).expect("fixture log directory should create");
        fs::write(
            log_dir.join("gateway.jsonl"),
            vec![b'x'; usize::try_from(MIN_LOG_MAX_BYTES - 128).expect("size should fit")],
        )
        .expect("fixture active log should prefill");
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

    fn spawn_probe_server(
        expected_capability: String,
        status: String,
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
            assert_eq!(
                capability_headers(&request),
                vec![expected_capability.as_str()]
            );
            assert!(request.starts_with("GET /api/v1/system/health HTTP/1.1\r\n"));
            write_probe_response(&mut stream, &status, &body);
        });
        (address, server)
    }

    fn spawn_capability_gate_server(
        server_capability: String,
    ) -> (SocketAddr, thread::JoinHandle<()>) {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .expect("capability gate fixture should bind loopback");
        let address = listener
            .local_addr()
            .expect("capability gate fixture address should resolve");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("health probe should connect");
            let request = read_probe_request(&mut stream);
            let authorized = capability_headers(&request) == vec![server_capability.as_str()];
            if authorized {
                write_probe_response(&mut stream, "200 OK", r#"{"status":"ok"}"#);
            } else {
                write_probe_response(
                    &mut stream,
                    "403 Forbidden",
                    r#"{"code":"GATEWAY_CAPABILITY_REJECTED"}"#,
                );
            }
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

    fn capability_headers(request: &str) -> Vec<&str> {
        request
            .split("\r\n")
            .filter_map(|line| line.split_once(':'))
            .filter(|(name, _)| name.eq_ignore_ascii_case(GATEWAY_CAPABILITY_HEADER))
            .map(|(_, value)| value.trim())
            .collect()
    }

    fn write_probe_response(stream: &mut std::net::TcpStream, status: &str, body: &str) {
        write!(
            stream,
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .expect("probe response should write");
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
            env::var("CMCLIENT_CALLMESH_API_KEY").expect("fixture secret should be configured");
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
