//! Process supervision primitives owned by the Rust Agent.

use std::{
    collections::BTreeMap,
    process::{Child, Command, Stdio},
    time::Duration,
};

/// Stable workspace identity for the supervisor boundary.
pub const COMPONENT: &str = "supervisor";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayCommand {
    pub program: String,
    pub arguments: Vec<String>,
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
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisorError {
    EmptyProgram,
    SpawnFailed,
    ProcessIoFailed,
}

impl SupervisorError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::EmptyProgram => "GATEWAY_SUPERVISOR_PROGRAM_EMPTY",
            Self::SpawnFailed => "GATEWAY_SUPERVISOR_SPAWN_FAILED",
            Self::ProcessIoFailed => "GATEWAY_SUPERVISOR_PROCESS_IO_FAILED",
        }
    }
}

pub struct GatewaySupervisor {
    command: GatewayCommand,
    backoff_policy: BackoffPolicy,
    child: Option<Child>,
    environment: BTreeMap<String, String>,
    failed_attempts: u32,
}

impl GatewaySupervisor {
    pub fn new(
        command: GatewayCommand,
        backoff_policy: BackoffPolicy,
    ) -> Result<Self, SupervisorError> {
        if command.program.trim().is_empty() {
            return Err(SupervisorError::EmptyProgram);
        }
        Ok(Self {
            command,
            backoff_policy,
            child: None,
            environment: inherited_runtime_environment(),
            failed_attempts: 0,
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

    pub fn start(&mut self) -> Result<SupervisorEvent, SupervisorError> {
        if let Some(child) = self.child.as_ref() {
            return Ok(SupervisorEvent::Heartbeat { pid: child.id() });
        }
        let child = Command::new(&self.command.program)
            .args(&self.command.arguments)
            .env_clear()
            .envs(&self.environment)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|_| SupervisorError::SpawnFailed)?;
        let pid = child.id();
        self.child = Some(child);
        self.failed_attempts = 0;
        Ok(SupervisorEvent::Started { pid })
    }

    pub fn poll_heartbeat(&mut self) -> Result<SupervisorEvent, SupervisorError> {
        let Some(child) = self.child.as_mut() else {
            return Ok(SupervisorEvent::Stopped);
        };
        match child
            .try_wait()
            .map_err(|_| SupervisorError::ProcessIoFailed)?
        {
            None => Ok(SupervisorEvent::Heartbeat { pid: child.id() }),
            Some(status) => {
                self.child = None;
                self.failed_attempts = self.failed_attempts.saturating_add(1);
                let restart_delay = self.backoff_policy.delay_for_attempt(self.failed_attempts);
                Ok(SupervisorEvent::Exited {
                    status: status.code(),
                    restart_delay,
                })
            }
        }
    }

    pub fn restart_if_due(
        &mut self,
        elapsed_since_exit: Duration,
    ) -> Result<Option<SupervisorEvent>, SupervisorError> {
        if self.child.is_some() || self.failed_attempts == 0 {
            return Ok(None);
        }
        if elapsed_since_exit < self.backoff_policy.delay_for_attempt(self.failed_attempts) {
            return Ok(None);
        }
        self.start().map(Some)
    }

    pub fn stop(&mut self) -> Result<SupervisorEvent, SupervisorError> {
        let Some(mut child) = self.child.take() else {
            return Ok(SupervisorEvent::Stopped);
        };
        child.kill().map_err(|_| SupervisorError::ProcessIoFailed)?;
        child.wait().map_err(|_| SupervisorError::ProcessIoFailed)?;
        self.failed_attempts = 0;
        Ok(SupervisorEvent::Stopped)
    }
}

fn inherited_runtime_environment() -> BTreeMap<String, String> {
    ["PATH", "SystemRoot", "WINDIR", "ComSpec"]
        .into_iter()
        .filter_map(|name| {
            std::env::var(name)
                .ok()
                .map(|value| (String::from(name), value))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{BackoffPolicy, GatewayCommand, GatewayStatus, GatewaySupervisor, SupervisorEvent};
    use std::time::Duration;

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

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn reports_exit_and_backoff_after_a_gateway_crash() {
        let mut supervisor = GatewaySupervisor::new(
            GatewayCommand {
                program: String::from("sh"),
                arguments: vec![String::from("-c"), String::from("exit 7")],
            },
            BackoffPolicy::default(),
        )
        .expect("supervisor should initialize");
        assert!(matches!(
            supervisor.start(),
            Ok(SupervisorEvent::Started { .. })
        ));

        let event = loop {
            match supervisor.poll_heartbeat().expect("poll should succeed") {
                SupervisorEvent::Heartbeat { .. } => std::thread::sleep(Duration::from_millis(5)),
                event => break event,
            }
        };
        assert_eq!(
            event,
            SupervisorEvent::Exited {
                status: Some(7),
                restart_delay: Duration::from_secs(1)
            }
        );
        assert_eq!(
            supervisor.status(),
            GatewayStatus::Backoff {
                attempt: 1,
                delay: Duration::from_secs(1)
            }
        );
        assert_eq!(
            supervisor
                .restart_if_due(Duration::from_millis(999))
                .expect("restart evaluation should succeed"),
            None
        );
        assert!(matches!(
            supervisor
                .restart_if_due(Duration::from_secs(1))
                .expect("restart evaluation should succeed"),
            Some(SupervisorEvent::Started { .. })
        ));
    }
}
