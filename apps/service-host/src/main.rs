#[cfg(any(windows, test))]
use std::ffi::OsStr;
#[cfg(any(windows, test))]
use std::path::{Path, PathBuf};
use std::process::ExitCode;

#[cfg(windows)]
const SERVICE_NAME: &str = "CMClientAgent";

#[cfg(any(windows, test))]
fn agent_path_from_host(host: &Path) -> PathBuf {
    host.with_file_name(if cfg!(windows) {
        "cmclient-agent.exe"
    } else {
        "cmclient-agent"
    })
}

#[cfg(any(windows, test))]
fn is_sensitive_environment_name(name: &OsStr) -> bool {
    let normalized: String = name
        .to_string_lossy()
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

#[cfg(windows)]
fn sensitive_process_environment_values() -> Vec<String> {
    std::env::vars_os()
        .filter(|(name, value)| !value.is_empty() && is_sensitive_environment_name(name))
        .map(|(_, value)| value.to_string_lossy().into_owned())
        .collect()
}

#[cfg(any(windows, test))]
fn run_before_deadline<Operation>(
    deadline: std::time::Instant,
    maximum_operation_timeout: std::time::Duration,
    operation: Operation,
) -> bool
where
    Operation: FnOnce(std::time::Duration) -> bool + Send + 'static,
{
    let Some(operation_timeout) = deadline
        .checked_duration_since(std::time::Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .map(|remaining| remaining.min(maximum_operation_timeout))
    else {
        return false;
    };
    let (result_sender, result_receiver) = std::sync::mpsc::sync_channel(1);
    if std::thread::Builder::new()
        .name(String::from("cmclient-service-shutdown-request"))
        .spawn(move || {
            let _ = result_sender.send(operation(operation_timeout));
        })
        .is_err()
    {
        return false;
    }
    let Some(remaining) = deadline
        .checked_duration_since(std::time::Instant::now())
        .filter(|remaining| !remaining.is_zero())
    else {
        return false;
    };
    result_receiver.recv_timeout(remaining).unwrap_or(false)
}

#[cfg(windows)]
fn main() -> ExitCode {
    if std::env::args_os().nth(1).as_deref() != Some(std::ffi::OsStr::new("--service")) {
        eprintln!("WINDOWS_SERVICE_HOST_USAGE_INVALID");
        return ExitCode::from(64);
    }
    service::run().map_or_else(
        |_| {
            eprintln!("WINDOWS_SERVICE_HOST_START_FAILED");
            ExitCode::from(1)
        },
        |_| ExitCode::SUCCESS,
    )
}

#[cfg(not(windows))]
fn main() -> ExitCode {
    eprintln!("WINDOWS_SERVICE_HOST_UNSUPPORTED_PLATFORM");
    ExitCode::from(64)
}

#[cfg(windows)]
mod service {
    use super::{
        SERVICE_NAME, agent_path_from_host, run_before_deadline,
        sensitive_process_environment_values,
    };
    use cmclient_control_api::{ControlClient, default_local_endpoint};
    use cmclient_runtime_logging::{ChildOutputCapture, LogLevel, LogPolicy, StructuredLogSink};
    use std::{
        ffi::OsString,
        io,
        path::PathBuf,
        process::{Child, Command, ExitStatus, Stdio},
        sync::mpsc,
        thread,
        time::{Duration, Instant},
    };
    use windows_service::{
        define_windows_service,
        service::{
            ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus,
            ServiceType,
        },
        service_control_handler::{self, ServiceControlHandlerResult},
        service_dispatcher,
    };

    const SERVICE_TYPE: ServiceType = ServiceType::OWN_PROCESS;
    const AGENT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(50);
    const CONTROL_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
    const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(250);
    const SERVICE_TRANSITION_WAIT_HINT: Duration = Duration::from_secs(60);

    struct ManagedAgent {
        child: Child,
        capture: Option<ChildOutputCapture>,
        log: StructuredLogSink,
    }

    impl ManagedAgent {
        fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
            self.child.try_wait()
        }

        fn finish_capture(&mut self) {
            if let Some(capture) = self.capture.take()
                && let Err(error) = capture.finish()
            {
                eprintln!("{}", error.code());
                let _ = self.log.write_code(LogLevel::Error, error.code());
            }
        }

        fn write_code(&self, code: &str) {
            if let Err(error) = self.log.write_code(LogLevel::Info, code) {
                eprintln!("{}", error.code());
            }
        }
    }

    impl Drop for ManagedAgent {
        fn drop(&mut self) {
            if self.child.try_wait().ok().flatten().is_none() {
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
            self.finish_capture();
        }
    }

    pub fn run() -> windows_service::Result<()> {
        service_dispatcher::start(SERVICE_NAME, ffi_service_main)
    }

    define_windows_service!(ffi_service_main, service_main);

    fn service_main(_arguments: Vec<OsString>) {
        let _ = run_service();
    }

    fn run_service() -> Result<(), Box<dyn std::error::Error>> {
        let (shutdown_tx, shutdown_rx) = mpsc::channel();
        let event_handler = move |control_event| match control_event {
            ServiceControl::Stop | ServiceControl::Shutdown => {
                let _ = shutdown_tx.send(());
                ServiceControlHandlerResult::NoError
            }
            ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
            _ => ServiceControlHandlerResult::NotImplemented,
        };
        let status_handle = service_control_handler::register(SERVICE_NAME, event_handler)?;
        set_status(
            &status_handle,
            ServiceState::StartPending,
            ServiceControlAccept::empty(),
            1,
        )?;

        let runtime_root = program_data_directory();
        let mut agent = match start_agent(&runtime_root) {
            Ok(agent) => agent,
            Err(error) => {
                report_stopped(&status_handle, ServiceExitCode::Win32(1))?;
                return Err(Box::new(error));
            }
        };
        set_status(
            &status_handle,
            ServiceState::Running,
            ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN,
            0,
        )?;

        let mut exit_code = ServiceExitCode::NO_ERROR;
        loop {
            if let Some(status) = agent.try_wait()? {
                agent.finish_capture();
                agent.write_code("WINDOWS_SERVICE_AGENT_EXITED");
                if !status.success() {
                    exit_code = ServiceExitCode::Win32(1);
                }
                break;
            }
            match shutdown_rx.recv_timeout(Duration::from_millis(250)) {
                Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                    agent.write_code("WINDOWS_SERVICE_STOP_REQUESTED");
                    set_status(
                        &status_handle,
                        ServiceState::StopPending,
                        ServiceControlAccept::empty(),
                        1,
                    )?;
                    stop_agent(&mut agent, &runtime_root)?;
                    agent.write_code("WINDOWS_SERVICE_AGENT_STOPPED");
                    break;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
            }
        }
        report_stopped(&status_handle, exit_code)?;
        Ok(())
    }

    fn report_stopped(
        status_handle: &service_control_handler::ServiceStatusHandle,
        exit_code: ServiceExitCode,
    ) -> windows_service::Result<()> {
        status_handle.set_service_status(ServiceStatus {
            service_type: SERVICE_TYPE,
            current_state: ServiceState::Stopped,
            controls_accepted: ServiceControlAccept::empty(),
            exit_code,
            checkpoint: 0,
            wait_hint: Duration::ZERO,
            process_id: None,
        })
    }

    fn set_status(
        status_handle: &service_control_handler::ServiceStatusHandle,
        state: ServiceState,
        controls_accepted: ServiceControlAccept,
        checkpoint: u32,
    ) -> windows_service::Result<()> {
        status_handle.set_service_status(ServiceStatus {
            service_type: SERVICE_TYPE,
            current_state: state,
            controls_accepted,
            exit_code: ServiceExitCode::NO_ERROR,
            checkpoint,
            wait_hint: SERVICE_TRANSITION_WAIT_HINT,
            process_id: None,
        })
    }

    fn start_agent(root: &std::path::Path) -> io::Result<ManagedAgent> {
        let host = std::env::current_exe()?;
        let agent = agent_path_from_host(&host);
        let policy = LogPolicy::from_environment().map_err(runtime_log_io_error)?;
        let log = StructuredLogSink::open(
            root.join("logs"),
            "service-host.jsonl",
            "service-host",
            policy,
        )
        .map_err(runtime_log_io_error)?;
        log.write_code(LogLevel::Info, "WINDOWS_SERVICE_AGENT_STARTING")
            .map_err(runtime_log_io_error)?;
        let mut command = Command::new(agent);
        command
            .arg("--serve")
            .env("HOME", root.join("home"))
            .env("USERPROFILE", root.join("home"))
            .env("CMCLIENT_DATA_DIR", root.join("data"))
            .env("CMCLIENT_CONFIG_DIR", root.join("config"))
            .env("CMCLIENT_CACHE_DIR", root.join("cache"))
            .env("CMCLIENT_LOG_DIR", root.join("logs"))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                let _ = log.write_code(LogLevel::Error, "WINDOWS_SERVICE_AGENT_START_FAILED");
                return Err(error);
            }
        };
        let Some(stdout) = child.stdout.take() else {
            terminate_failed_capture_start(&mut child);
            let _ = log.write_code(LogLevel::Error, "RUNTIME_LOG_CAPTURE_STDOUT_MISSING");
            return Err(io::Error::other("RUNTIME_LOG_CAPTURE_STDOUT_MISSING"));
        };
        let Some(stderr) = child.stderr.take() else {
            terminate_failed_capture_start(&mut child);
            let _ = log.write_code(LogLevel::Error, "RUNTIME_LOG_CAPTURE_STDERR_MISSING");
            return Err(io::Error::other("RUNTIME_LOG_CAPTURE_STDERR_MISSING"));
        };
        let capture = match log.capture(stdout, stderr, sensitive_process_environment_values()) {
            Ok(capture) => capture,
            Err(error) => {
                terminate_failed_capture_start(&mut child);
                let _ = log.write_code(LogLevel::Error, error.code());
                return Err(runtime_log_io_error(error));
            }
        };
        let managed = ManagedAgent {
            child,
            capture: Some(capture),
            log,
        };
        managed.write_code("WINDOWS_SERVICE_AGENT_STARTED");
        Ok(managed)
    }

    fn stop_agent(agent: &mut ManagedAgent, runtime_root: &std::path::Path) -> io::Result<()> {
        let deadline = Instant::now() + AGENT_SHUTDOWN_TIMEOUT;
        let mut graceful_requested = false;
        while Instant::now() < deadline {
            if agent.try_wait()?.is_some() {
                agent.finish_capture();
                return Ok(());
            }
            if !graceful_requested {
                let endpoint = default_local_endpoint(&runtime_root.join("data"));
                graceful_requested =
                    run_before_deadline(deadline, CONTROL_REQUEST_TIMEOUT, move |timeout| {
                        ControlClient::new_with_timeout(endpoint, timeout)
                            .and_then(|client| client.shutdown_agent())
                            .is_ok()
                    });
            }
            let Some(remaining) = deadline
                .checked_duration_since(Instant::now())
                .filter(|remaining| !remaining.is_zero())
            else {
                break;
            };
            thread::sleep(remaining.min(SHUTDOWN_POLL_INTERVAL));
        }
        match agent.child.kill() {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::InvalidInput => {}
            Err(error) => return Err(error),
        }
        let _ = agent.child.wait()?;
        agent.finish_capture();
        Ok(())
    }

    fn terminate_failed_capture_start(child: &mut Child) {
        let _ = child.kill();
        let _ = child.wait();
    }

    fn runtime_log_io_error(error: cmclient_runtime_logging::RuntimeLogError) -> io::Error {
        io::Error::other(error.code())
    }

    fn program_data_directory() -> PathBuf {
        std::env::var_os("PROGRAMDATA")
            .map(PathBuf::from)
            .filter(|path| path.is_absolute())
            .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"))
            .join("CMClient")
    }
}

#[cfg(test)]
mod tests {
    use super::{agent_path_from_host, is_sensitive_environment_name, run_before_deadline};
    use std::{
        ffi::OsStr,
        path::Path,
        sync::mpsc,
        thread,
        time::{Duration, Instant},
    };

    #[test]
    fn locates_the_agent_beside_the_service_host() {
        let agent = agent_path_from_host(Path::new("/opt/cmclient/bin/cmclient-service-host"));
        assert_eq!(
            agent.file_name().and_then(|name| name.to_str()),
            Some(if cfg!(windows) {
                "cmclient-agent.exe"
            } else {
                "cmclient-agent"
            })
        );
    }

    #[test]
    fn identifies_only_secret_bearing_environment_names() {
        for name in [
            "CMCLIENT_CALLMESH_API_KEY",
            "CMCLIENT_APRS_PASSCODE",
            "CMCLIENT_MANAGEMENT_ADMIN_TOKEN",
            "HTTP_AUTHORIZATION",
        ] {
            assert!(is_sensitive_environment_name(OsStr::new(name)), "{name}");
        }
        for name in [
            "CMCLIENT_LOG_MAX_BYTES",
            "CMCLIENT_DATA_DIR",
            "CMCLIENT_GATEWAY_HOST",
        ] {
            assert!(!is_sensitive_environment_name(OsStr::new(name)), "{name}");
        }
    }

    #[test]
    fn bounds_an_unresponsive_control_request_to_the_remaining_deadline() {
        let (timeout_sender, timeout_receiver) = mpsc::sync_channel(1);
        let started_at = Instant::now();
        let result = run_before_deadline(
            started_at + Duration::from_millis(25),
            Duration::from_secs(2),
            move |timeout| {
                timeout_sender
                    .send(timeout)
                    .expect("timeout observation should send");
                thread::sleep(Duration::from_millis(500));
                true
            },
        );

        assert!(!result);
        assert!(
            timeout_receiver
                .recv_timeout(Duration::from_millis(100))
                .expect("operation should receive its timeout")
                <= Duration::from_millis(25)
        );
        assert!(started_at.elapsed() < Duration::from_millis(400));
    }
}
