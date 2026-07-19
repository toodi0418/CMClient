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
    use super::{SERVICE_NAME, agent_path_from_host, run_before_deadline};
    use cmclient_control_api::{ControlClient, default_local_endpoint};
    use std::{
        ffi::OsString,
        io,
        path::PathBuf,
        process::{Child, Command, Stdio},
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
                if !status.success() {
                    exit_code = ServiceExitCode::Win32(1);
                }
                break;
            }
            match shutdown_rx.recv_timeout(Duration::from_millis(250)) {
                Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                    set_status(
                        &status_handle,
                        ServiceState::StopPending,
                        ServiceControlAccept::empty(),
                        1,
                    )?;
                    stop_agent(&mut agent, &runtime_root)?;
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

    fn start_agent(root: &std::path::Path) -> io::Result<Child> {
        let host = std::env::current_exe()?;
        let agent = agent_path_from_host(&host);
        Command::new(agent)
            .arg("--serve")
            .env("HOME", root.join("home"))
            .env("USERPROFILE", root.join("home"))
            .env("CMCLIENT_DATA_DIR", root.join("data"))
            .env("CMCLIENT_CONFIG_DIR", root.join("config"))
            .env("CMCLIENT_CACHE_DIR", root.join("cache"))
            .env("CMCLIENT_LOG_DIR", root.join("logs"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
    }

    fn stop_agent(agent: &mut Child, runtime_root: &std::path::Path) -> io::Result<()> {
        let deadline = Instant::now() + AGENT_SHUTDOWN_TIMEOUT;
        let mut graceful_requested = false;
        while Instant::now() < deadline {
            if agent.try_wait()?.is_some() {
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
        match agent.kill() {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::InvalidInput => {}
            Err(error) => return Err(error),
        }
        let _ = agent.wait()?;
        Ok(())
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
    use super::{agent_path_from_host, run_before_deadline};
    use std::{
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
