//! Rust client support for communication with the local Agent.

use std::path::PathBuf;

/// Stable workspace identity for the CLI client boundary.
pub const COMPONENT: &str = "cli-client";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ExitCode {
    Success = 0,
    Usage = 2,
    Connection = 3,
    Authentication = 4,
    Validation = 5,
    OperationFailed = 6,
    PartialOrDegraded = 7,
    Timeout = 8,
}

impl ExitCode {
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlEndpointSpec {
    Local,
    UnixSocket(PathBuf),
    NamedPipe(String),
    Https(String),
}

pub fn parse_endpoint(value: &str) -> Result<ControlEndpointSpec, ExitCode> {
    if value == "local" {
        return Ok(ControlEndpointSpec::Local);
    }
    if let Some(path) = value.strip_prefix("unix://") {
        let path = PathBuf::from(path);
        return path
            .is_absolute()
            .then_some(ControlEndpointSpec::UnixSocket(path))
            .ok_or(ExitCode::Validation);
    }
    if value.starts_with(r"\\.\pipe\") {
        return Ok(ControlEndpointSpec::NamedPipe(value.to_owned()));
    }
    if value.starts_with("https://") {
        return Ok(ControlEndpointSpec::Https(value.to_owned()));
    }
    Err(ExitCode::Validation)
}

#[cfg(test)]
mod tests {
    use super::{ControlEndpointSpec, ExitCode, parse_endpoint};

    #[test]
    fn parses_only_supported_control_endpoints() {
        assert_eq!(parse_endpoint("local"), Ok(ControlEndpointSpec::Local));
        assert_eq!(
            parse_endpoint("unix:///tmp/cmclient.sock"),
            Ok(ControlEndpointSpec::UnixSocket("/tmp/cmclient.sock".into()))
        );
        assert_eq!(
            parse_endpoint("http://127.0.0.1"),
            Err(ExitCode::Validation)
        );
    }

    #[test]
    fn preserves_documented_exit_codes() {
        assert_eq!(ExitCode::Timeout.as_u8(), 8);
        assert_eq!(ExitCode::Usage.as_u8(), 2);
    }
}
