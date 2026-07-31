use cmclient_agent_core::secrets::{
    AgentSecretStore, CMCloudEnrollmentAttempt, CMCloudInstallationIdentity, SecretStoreError,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::{
            HeaderValue,
            header::{AUTHORIZATION, SEC_WEBSOCKET_PROTOCOL},
        },
    },
};
use zeroize::Zeroizing;

const AGENT_SUBPROTOCOL: &str = "cmcloud.agent.v1";
const AGENT_PROTOCOL_VERSION: u64 = 1;
const AGENT_HEARTBEAT_INTERVAL_MS: u64 = 30_000;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const CONTROL_FRAME_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CONTROL_FRAME_BYTES: usize = 16 * 1024;
const MAX_SAFE_JAVASCRIPT_INTEGER: u64 = 9_007_199_254_740_991;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CMCloudEnrollmentError {
    SecretStore,
    Transport,
    Protocol,
    Rejected,
    StaleEnrollment,
}

impl CMCloudEnrollmentError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::SecretStore => "CMCLOUD_ENROLLMENT_SECRET_STORE_UNAVAILABLE",
            Self::Transport => "CMCLOUD_ENROLLMENT_TRANSPORT_FAILED",
            Self::Protocol => "CMCLOUD_ENROLLMENT_PROTOCOL_INVALID",
            Self::Rejected => "CMCLOUD_ENROLLMENT_REJECTED",
            Self::StaleEnrollment => "CMCLOUD_ENROLLMENT_STALE",
        }
    }
}

impl From<SecretStoreError> for CMCloudEnrollmentError {
    fn from(_: SecretStoreError) -> Self {
        Self::SecretStore
    }
}

/// Complete or recover the Agent-owned CMCloud pairing flow.
///
/// The function persists a pairing transaction before it uses the code, stores
/// the server-issued credential before it sends `enrollment_ack`, and promotes
/// that credential only after the corresponding acknowledgement arrives.
pub async fn enroll_cmcloud(
    secrets: &AgentSecretStore,
    endpoint: &str,
    pairing_code: &str,
    client_version: &str,
) -> Result<CMCloudInstallationIdentity, CMCloudEnrollmentError> {
    let result = async {
        let attempt = match secrets.cmcloud_enrollment_attempt()? {
            Some(attempt) => {
                if attempt.endpoint() != endpoint
                    || attempt.client_version() != client_version
                    || attempt.pairing_code().expose_secret() != pairing_code
                {
                    return Err(CMCloudEnrollmentError::StaleEnrollment);
                }
                attempt
            }
            None => secrets.begin_cmcloud_enrollment(endpoint, pairing_code, client_version)?,
        };

        if let Some(identity) = recover_active_enrollment(secrets, &attempt).await? {
            return Ok(identity);
        }

        let (mut channel, hello) = open_session(
            attempt.endpoint(),
            attempt.pairing_code().expose_secret(),
            attempt.client_version(),
            attempt.installation_id(),
            attempt.requested_installation_generation(),
            0,
            attempt.boot_id(),
        )
        .await?;
        validate_pairing_hello(&hello)?;
        let issued_device_credential = hello
            .issued_device_credential
            .as_deref()
            .ok_or(CMCloudEnrollmentError::Protocol)?;
        secrets.record_cmcloud_issued_credential(
            hello.installation_generation,
            hello.credential_version,
            hello.connection_epoch,
            issued_device_credential,
        )?;

        channel
            .send_text(
                serde_json::to_string(&EnrollmentAck {
                    frame_type: "enrollment_ack",
                    connection_epoch: hello.connection_epoch,
                    installation_generation: hello.installation_generation,
                    credential_version: hello.credential_version,
                })
                .map_err(|_| CMCloudEnrollmentError::Protocol)?,
            )
            .await?;
        match parse_server_frame(&channel.receive_text().await?)? {
            ServerFrame::EnrollmentAcknowledged => secrets
                .activate_cmcloud_credential(
                    hello.installation_generation,
                    hello.credential_version,
                    hello.connection_epoch,
                )
                .map_err(Into::into),
            ServerFrame::Error => Err(CMCloudEnrollmentError::Rejected),
            ServerFrame::ServerHello(_) => Err(CMCloudEnrollmentError::Protocol),
        }
    }
    .await;
    match result {
        Ok(identity) => Ok(identity),
        Err(error) => {
            discard_terminal_unissued_enrollment(secrets, error);
            Err(error)
        }
    }
}

fn discard_terminal_unissued_enrollment(secrets: &AgentSecretStore, error: CMCloudEnrollmentError) {
    if !matches!(
        error,
        CMCloudEnrollmentError::Protocol | CMCloudEnrollmentError::Rejected
    ) {
        return;
    }
    let Ok(Some(attempt)) = secrets.cmcloud_enrollment_attempt() else {
        return;
    };
    if attempt.issued().is_none() {
        let _ = secrets.discard_cmcloud_enrollment();
    }
}

/// Run the bounded enrollment exchange on an Agent-owned Tokio runtime.
///
/// Management handlers call this only from `spawn_blocking`, keeping an
/// untrusted HTTP request from inheriting the management web server runtime or
/// exposing the pairing code to another component.
pub fn enroll_cmcloud_blocking(
    secrets: &AgentSecretStore,
    endpoint: &str,
    pairing_code: &str,
    client_version: &str,
) -> Result<CMCloudInstallationIdentity, CMCloudEnrollmentError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(|_| CMCloudEnrollmentError::Transport)?;
    runtime.block_on(enroll_cmcloud(
        secrets,
        endpoint,
        pairing_code,
        client_version,
    ))
}

async fn recover_active_enrollment(
    secrets: &AgentSecretStore,
    attempt: &CMCloudEnrollmentAttempt,
) -> Result<Option<CMCloudInstallationIdentity>, CMCloudEnrollmentError> {
    let Some(issued) = attempt.issued() else {
        return Ok(None);
    };
    let result = open_session(
        issued.identity().endpoint(),
        issued.device_credential().expose_secret(),
        attempt.client_version(),
        issued.identity().installation_id(),
        issued.identity().installation_generation(),
        issued.identity().credential_version(),
        attempt.boot_id(),
    )
    .await;
    let Ok((_channel, hello)) = result else {
        return Ok(None);
    };
    if hello.issued_device_credential.is_some()
        || hello.enrollment_ack_required
        || !server_hello_matches_identity(&hello, issued.identity())
    {
        return Ok(None);
    }
    secrets.record_cmcloud_issued_credential(
        hello.installation_generation,
        hello.credential_version,
        hello.connection_epoch,
        issued.device_credential().expose_secret(),
    )?;
    Ok(Some(secrets.activate_cmcloud_credential(
        hello.installation_generation,
        hello.credential_version,
        hello.connection_epoch,
    )?))
}

async fn open_session(
    endpoint: &str,
    bearer: &str,
    client_version: &str,
    installation_id: &str,
    installation_generation: u64,
    credential_version: u64,
    boot_id: &str,
) -> Result<(CMCloudChannel, ServerHello), CMCloudEnrollmentError> {
    let mut request = endpoint
        .into_client_request()
        .map_err(|_| CMCloudEnrollmentError::Transport)?;
    let authorization = Zeroizing::new(format!("Bearer {bearer}"));
    let authorization = HeaderValue::from_str(authorization.as_str())
        .map_err(|_| CMCloudEnrollmentError::Protocol)?;
    request.headers_mut().insert(AUTHORIZATION, authorization);
    request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static(AGENT_SUBPROTOCOL),
    );
    let (socket, response) = timeout(CONNECT_TIMEOUT, connect_async(request))
        .await
        .map_err(|_| CMCloudEnrollmentError::Transport)?
        .map_err(|_| CMCloudEnrollmentError::Transport)?;
    if response
        .headers()
        .get(SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        != Some(AGENT_SUBPROTOCOL)
    {
        return Err(CMCloudEnrollmentError::Protocol);
    }
    let mut channel = CMCloudChannel { socket };
    channel
        .send_text(
            serde_json::to_string(&ClientHello {
                frame_type: "client_hello",
                protocol_version: AGENT_PROTOCOL_VERSION,
                client_version,
                installation_id,
                installation_generation,
                credential_version,
                boot_id,
            })
            .map_err(|_| CMCloudEnrollmentError::Protocol)?,
        )
        .await?;
    let hello = match parse_server_frame(&channel.receive_text().await?)? {
        ServerFrame::ServerHello(hello) => hello,
        ServerFrame::Error => return Err(CMCloudEnrollmentError::Rejected),
        ServerFrame::EnrollmentAcknowledged => return Err(CMCloudEnrollmentError::Protocol),
    };
    validate_server_hello(&hello)?;
    Ok((channel, hello))
}

fn validate_pairing_hello(hello: &ServerHello) -> Result<(), CMCloudEnrollmentError> {
    if !hello.enrollment_ack_required
        || !hello
            .issued_device_credential
            .as_deref()
            .is_some_and(is_cmcloud_bearer)
    {
        return Err(CMCloudEnrollmentError::Protocol);
    }
    Ok(())
}

fn is_cmcloud_bearer(value: &str) -> bool {
    (16..=512).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn server_hello_matches_identity(
    hello: &ServerHello,
    identity: &CMCloudInstallationIdentity,
) -> bool {
    hello.installation_generation == identity.installation_generation()
        && hello.credential_version == identity.credential_version()
}

fn validate_server_hello(hello: &ServerHello) -> Result<(), CMCloudEnrollmentError> {
    if hello.protocol_version != AGENT_PROTOCOL_VERSION
        || hello.connection_epoch == 0
        || hello.connection_epoch > MAX_SAFE_JAVASCRIPT_INTEGER
        || hello.installation_generation > MAX_SAFE_JAVASCRIPT_INTEGER
        || hello.credential_version == 0
        || hello.credential_version > MAX_SAFE_JAVASCRIPT_INTEGER
        || hello.heartbeat_interval_ms != AGENT_HEARTBEAT_INTERVAL_MS
        || hello.minimum_client_version.is_empty()
        || hello.minimum_client_version.len() > 64
        || hello
            .minimum_client_version
            .bytes()
            .any(|byte| byte.is_ascii_control())
        || !matches!(hello.aprs_mode.as_str(), "disabled" | "shadow" | "enabled")
    {
        return Err(CMCloudEnrollmentError::Protocol);
    }
    Ok(())
}

struct CMCloudChannel {
    socket: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
}

impl CMCloudChannel {
    async fn send_text(&mut self, value: String) -> Result<(), CMCloudEnrollmentError> {
        if value.len() > MAX_CONTROL_FRAME_BYTES {
            return Err(CMCloudEnrollmentError::Protocol);
        }
        timeout(
            CONTROL_FRAME_TIMEOUT,
            self.socket.send(Message::Text(value.into())),
        )
        .await
        .map_err(|_| CMCloudEnrollmentError::Transport)?
        .map_err(|_| CMCloudEnrollmentError::Transport)
    }

    async fn receive_text(&mut self) -> Result<String, CMCloudEnrollmentError> {
        loop {
            let next = timeout(CONTROL_FRAME_TIMEOUT, self.socket.next())
                .await
                .map_err(|_| CMCloudEnrollmentError::Transport)?
                .ok_or(CMCloudEnrollmentError::Transport)?
                .map_err(|_| CMCloudEnrollmentError::Transport)?;
            match next {
                Message::Text(value) if value.len() <= MAX_CONTROL_FRAME_BYTES => {
                    return Ok(value.to_string());
                }
                Message::Text(_) | Message::Binary(_) | Message::Close(_) => {
                    return Err(CMCloudEnrollmentError::Protocol);
                }
                Message::Ping(payload) => {
                    timeout(
                        CONTROL_FRAME_TIMEOUT,
                        self.socket.send(Message::Pong(payload)),
                    )
                    .await
                    .map_err(|_| CMCloudEnrollmentError::Transport)?
                    .map_err(|_| CMCloudEnrollmentError::Transport)?;
                }
                Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClientHello<'a> {
    #[serde(rename = "type")]
    frame_type: &'static str,
    protocol_version: u64,
    client_version: &'a str,
    installation_id: &'a str,
    installation_generation: u64,
    credential_version: u64,
    boot_id: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EnrollmentAck {
    #[serde(rename = "type")]
    frame_type: &'static str,
    connection_epoch: u64,
    installation_generation: u64,
    credential_version: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ServerHello {
    #[serde(rename = "type")]
    frame_type: String,
    protocol_version: u64,
    connection_epoch: u64,
    installation_generation: u64,
    credential_version: u64,
    heartbeat_interval_ms: u64,
    minimum_client_version: String,
    aprs_mode: String,
    #[serde(default)]
    enrollment_ack_required: bool,
    #[serde(default)]
    issued_device_credential: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ErrorFrame {
    #[serde(rename = "type")]
    frame_type: String,
    code: String,
    message: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EnrollmentAcknowledgedFrame {
    #[serde(rename = "type")]
    frame_type: String,
}

enum ServerFrame {
    ServerHello(ServerHello),
    EnrollmentAcknowledged,
    Error,
}

fn parse_server_frame(value: &str) -> Result<ServerFrame, CMCloudEnrollmentError> {
    let raw: serde_json::Value =
        serde_json::from_str(value).map_err(|_| CMCloudEnrollmentError::Protocol)?;
    let frame_type = raw
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or(CMCloudEnrollmentError::Protocol)?;
    match frame_type {
        "server_hello" => {
            let frame: ServerHello =
                serde_json::from_value(raw).map_err(|_| CMCloudEnrollmentError::Protocol)?;
            if frame.frame_type != "server_hello" {
                return Err(CMCloudEnrollmentError::Protocol);
            }
            Ok(ServerFrame::ServerHello(frame))
        }
        "enrollment_acknowledged" => {
            let frame: EnrollmentAcknowledgedFrame =
                serde_json::from_value(raw).map_err(|_| CMCloudEnrollmentError::Protocol)?;
            if frame.frame_type != "enrollment_acknowledged" {
                return Err(CMCloudEnrollmentError::Protocol);
            }
            Ok(ServerFrame::EnrollmentAcknowledged)
        }
        "error" => {
            let frame: ErrorFrame =
                serde_json::from_value(raw).map_err(|_| CMCloudEnrollmentError::Protocol)?;
            if frame.frame_type != "error"
                || frame.code.is_empty()
                || frame.code.len() > 128
                || frame.message.is_empty()
                || frame.message.len() > 512
            {
                return Err(CMCloudEnrollmentError::Protocol);
            }
            Ok(ServerFrame::Error)
        }
        _ => Err(CMCloudEnrollmentError::Protocol),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CMCloudEnrollmentError, ServerFrame, discard_terminal_unissued_enrollment,
        parse_server_frame, validate_pairing_hello, validate_server_hello,
    };
    use cmclient_agent_core::secrets::AgentSecretStore;

    #[test]
    fn accepts_only_the_fenced_cmcloud_server_hello() {
        for aprs_mode in ["disabled", "shadow", "enabled"] {
            let frame = parse_server_frame(&format!(
                r#"{{"type":"server_hello","protocolVersion":1,"connectionEpoch":4,"installationGeneration":0,"credentialVersion":1,"heartbeatIntervalMs":30000,"minimumClientVersion":"2.0.0-rc.1","aprsMode":"{aprs_mode}","enrollmentAckRequired":true,"issuedDeviceCredential":"abcdefghijklmnopqrstuvwxyz012345"}}"#,
            ))
            .expect("server hello should parse");
            let ServerFrame::ServerHello(hello) = frame else {
                panic!("expected server hello");
            };
            validate_server_hello(&hello).expect("server hello fence should validate");
            validate_pairing_hello(&hello).expect("pairing hello should validate");
        }
    }

    #[test]
    fn rejects_an_invalid_or_ambiguous_server_frame() {
        for value in [
            r#"{"type":"server_hello"}"#,
            r#"{"type":"enrollment_acknowledged","extra":true}"#,
            r#"{"type":"error","code":"X","message":"Y","extra":true}"#,
        ] {
            assert!(matches!(
                parse_server_frame(value),
                Err(CMCloudEnrollmentError::Protocol)
            ));
        }
        let ServerFrame::ServerHello(hello) = parse_server_frame(
            r#"{"type":"server_hello","protocolVersion":1,"connectionEpoch":4,"installationGeneration":0,"credentialVersion":1,"heartbeatIntervalMs":30000,"minimumClientVersion":"2.0.0-rc.1","aprsMode":"future","enrollmentAckRequired":true,"issuedDeviceCredential":"abcdefghijklmnopqrstuvwxyz012345"}"#,
        )
        .expect("unknown mode should remain syntactically parseable")
        else {
            panic!("expected server hello");
        };
        assert!(matches!(
            validate_server_hello(&hello),
            Err(CMCloudEnrollmentError::Protocol)
        ));
        let ServerFrame::ServerHello(hello) = parse_server_frame(
            r#"{"type":"server_hello","protocolVersion":1,"connectionEpoch":4,"installationGeneration":0,"credentialVersion":1,"heartbeatIntervalMs":30000,"minimumClientVersion":"2.0.0-rc.1","aprsMode":"disabled","enrollmentAckRequired":true,"issuedDeviceCredential":"invalid credential with spaces"}"#,
        )
        .expect("invalid credential should remain syntactically parseable")
        else {
            panic!("expected server hello");
        };
        assert!(matches!(
            validate_pairing_hello(&hello),
            Err(CMCloudEnrollmentError::Protocol)
        ));
    }

    #[test]
    fn only_terminal_unissued_pairing_attempts_are_discarded() {
        let endpoint = "wss://cmcloud.example.invalid/agent/v1";
        let code = "pairing-code-fixture-0123456789";
        let store = AgentSecretStore::memory();
        store
            .begin_cmcloud_enrollment(endpoint, code, "2.0.0-rc.1")
            .expect("pending pairing should persist");
        discard_terminal_unissued_enrollment(&store, CMCloudEnrollmentError::Rejected);
        assert!(
            store
                .cmcloud_enrollment_attempt()
                .expect("pending pairing lookup should work")
                .is_none(),
            "a rejected unissued code must not block a replacement code",
        );

        store
            .begin_cmcloud_enrollment(endpoint, code, "2.0.0-rc.1")
            .expect("second pairing should persist");
        store
            .record_cmcloud_issued_credential(0, 1, 4, "device-credential-fixture-0123456789")
            .expect("issued credential should persist");
        discard_terminal_unissued_enrollment(&store, CMCloudEnrollmentError::Protocol);
        assert!(
            store
                .cmcloud_enrollment_attempt()
                .expect("pending pairing lookup should work")
                .is_some(),
            "an issued credential must remain recoverable",
        );
        discard_terminal_unissued_enrollment(&store, CMCloudEnrollmentError::Transport);
        assert!(
            store
                .cmcloud_enrollment_attempt()
                .expect("pending pairing lookup should work")
                .is_some(),
            "a transport failure must remain retryable",
        );
    }
}
