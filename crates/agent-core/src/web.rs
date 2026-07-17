use std::{
    io::{Read, Write},
    net::{IpAddr, SocketAddr, TcpListener},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagementWebConfig {
    pub enabled: bool,
    pub bind: IpAddr,
    pub port: u16,
}

impl Default for ManagementWebConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind: IpAddr::from([127, 0, 0, 1]),
            port: 7080,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagementWebError {
    Disabled,
    NonLoopbackBind,
    Io,
    InvalidHttp,
}

impl ManagementWebError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Disabled => "MANAGEMENT_WEB_DISABLED",
            Self::NonLoopbackBind => "MANAGEMENT_WEB_NON_LOOPBACK_DENIED",
            Self::Io => "MANAGEMENT_WEB_IO_FAILED",
            Self::InvalidHttp => "MANAGEMENT_WEB_HTTP_INVALID",
        }
    }
}

pub struct ManagementWebListener {
    listener: TcpListener,
}

impl ManagementWebListener {
    pub fn bind(config: &ManagementWebConfig) -> Result<Self, ManagementWebError> {
        if !config.enabled {
            return Err(ManagementWebError::Disabled);
        }
        if !config.bind.is_loopback() {
            return Err(ManagementWebError::NonLoopbackBind);
        }
        let listener = TcpListener::bind(SocketAddr::new(config.bind, config.port))
            .map_err(|_| ManagementWebError::Io)?;
        Ok(Self { listener })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, ManagementWebError> {
        self.listener
            .local_addr()
            .map_err(|_| ManagementWebError::Io)
    }

    pub fn serve_once(&self) -> Result<(), ManagementWebError> {
        let (mut stream, _) = self.listener.accept().map_err(|_| ManagementWebError::Io)?;
        let mut request = [0_u8; 8_192];
        let count = stream
            .read(&mut request)
            .map_err(|_| ManagementWebError::Io)?;
        let line = std::str::from_utf8(&request[..count])
            .map_err(|_| ManagementWebError::InvalidHttp)?
            .lines()
            .next()
            .ok_or(ManagementWebError::InvalidHttp)?;
        let (status, content_type, body) = match line
            .split_whitespace()
            .collect::<Vec<_>>()
            .as_slice()
        {
            ["GET", "/", _] | ["GET", "/index.html", _] => (
                "200 OK",
                "text/html; charset=utf-8",
                "<!doctype html><title>CMClient</title><main id=app>CMClient management web</main>",
            ),
            [_, path, _] if path.starts_with("/api/") => (
                "503 Service Unavailable",
                "application/json",
                r#"{"code":"GATEWAY_PROXY_UNAVAILABLE"}"#,
            ),
            [_, _, _] => (
                "404 Not Found",
                "application/json",
                r#"{"code":"WEB_ROUTE_NOT_FOUND"}"#,
            ),
            _ => return Err(ManagementWebError::InvalidHttp),
        };
        let header = format!(
            "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        stream
            .write_all(header.as_bytes())
            .map_err(|_| ManagementWebError::Io)?;
        stream
            .write_all(body.as_bytes())
            .map_err(|_| ManagementWebError::Io)
    }
}

#[cfg(test)]
mod tests {
    use super::{ManagementWebConfig, ManagementWebError, ManagementWebListener};
    use std::{
        io::{Read, Write},
        net::TcpStream,
        thread,
    };

    #[test]
    fn rejects_lan_bind_without_the_security_layer() {
        let config = ManagementWebConfig {
            bind: "0.0.0.0".parse().expect("IP should parse"),
            ..Default::default()
        };
        assert!(matches!(
            ManagementWebListener::bind(&config),
            Err(ManagementWebError::NonLoopbackBind)
        ));
    }

    #[test]
    fn serves_static_shell_and_gateway_proxy_placeholder() {
        let config = ManagementWebConfig {
            port: 0,
            ..Default::default()
        };
        let listener = ManagementWebListener::bind(&config).expect("listener should bind");
        let address = listener.local_addr().expect("address should load");
        let server = thread::spawn(move || listener.serve_once());
        let mut client = TcpStream::connect(address).expect("client should connect");
        client
            .write_all(b"GET /api/v1/status HTTP/1.1\r\nhost: localhost\r\n\r\n")
            .expect("request should write");
        let mut response = String::new();
        client
            .read_to_string(&mut response)
            .expect("response should read");
        assert!(response.starts_with("HTTP/1.1 503"));
        assert!(response.contains("GATEWAY_PROXY_UNAVAILABLE"));
        server
            .join()
            .expect("server should join")
            .expect("server should respond");
    }
}
