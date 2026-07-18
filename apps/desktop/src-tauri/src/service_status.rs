use cmclient_control_api::{ControlClient, ControlEndpoint, GatewayProjection};
use serde::Serialize;
use serde_json::Value;
use std::{thread, time::Duration};

const PROJECTION_TIMEOUT: Duration = Duration::from_secs(3);
const PROJECTION_INVALID: &str = "DESKTOP_GATEWAY_PROJECTION_INVALID";
const PROJECTION_FAILED: &str = "DESKTOP_GATEWAY_PROJECTION_FAILED";

type ProjectionResult = Result<Value, String>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ServiceState {
    Disabled,
    Stopped,
    Starting,
    Running,
    Backoff,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshtasticServiceStatus {
    pub(crate) state: ServiceState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) transport: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) frames_received: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reason_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AprsCallMeshServiceStatus {
    pub(crate) state: ServiceState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) aprs_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) callmesh_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) active_mapping_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pending_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) failed_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reason_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProxyServiceStatus {
    pub(crate) state: ServiceState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) active_clients: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max_clients: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reason_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DesktopServiceStatus {
    pub(crate) schema_version: u8,
    pub(crate) meshtastic: MeshtasticServiceStatus,
    pub(crate) aprs_callmesh: AprsCallMeshServiceStatus,
    pub(crate) proxy: ProxyServiceStatus,
}

pub(crate) fn load(endpoint: ControlEndpoint) -> DesktopServiceStatus {
    let meshtastic_endpoint = endpoint.clone();
    let aprs_endpoint = endpoint.clone();
    let callmesh_endpoint = endpoint.clone();
    let proxy_endpoint = endpoint;

    let meshtastic =
        thread::spawn(move || load_projection(meshtastic_endpoint, GatewayProjection::Meshtastic));
    let aprs = thread::spawn(move || load_projection(aprs_endpoint, GatewayProjection::Aprs));
    let callmesh =
        thread::spawn(move || load_projection(callmesh_endpoint, GatewayProjection::CallMesh));
    let proxy = thread::spawn(move || load_projection(proxy_endpoint, GatewayProjection::Proxy));

    project(
        join_projection(meshtastic),
        join_projection(aprs),
        join_projection(callmesh),
        join_projection(proxy),
    )
}

fn load_projection(endpoint: ControlEndpoint, projection: GatewayProjection) -> ProjectionResult {
    ControlClient::new_with_timeout(endpoint, PROJECTION_TIMEOUT)
        .and_then(|client| client.gateway_projection(projection))
        .map_err(|error| error.code().to_owned())
}

fn join_projection(handle: thread::JoinHandle<ProjectionResult>) -> ProjectionResult {
    handle
        .join()
        .unwrap_or_else(|_| Err(String::from(PROJECTION_FAILED)))
}

fn project(
    meshtastic: ProjectionResult,
    aprs: ProjectionResult,
    callmesh: ProjectionResult,
    proxy: ProjectionResult,
) -> DesktopServiceStatus {
    DesktopServiceStatus {
        schema_version: 1,
        meshtastic: project_meshtastic(meshtastic),
        aprs_callmesh: project_aprs_callmesh(aprs, callmesh),
        proxy: project_proxy(proxy),
    }
}

fn project_meshtastic(result: ProjectionResult) -> MeshtasticServiceStatus {
    let value = match result {
        Ok(value) => value,
        Err(code) => return unavailable_meshtastic(code),
    };
    let Some(configured) = value.get("configured").and_then(Value::as_bool) else {
        return unavailable_meshtastic(String::from(PROJECTION_INVALID));
    };
    if !configured {
        return MeshtasticServiceStatus {
            state: ServiceState::Disabled,
            transport: None,
            frames_received: None,
            reason_code: None,
        };
    }
    let Some(connection) = value.get("connection") else {
        return unavailable_meshtastic(String::from(PROJECTION_INVALID));
    };
    let Some(connection_state) = connection.get("status").and_then(Value::as_str) else {
        return unavailable_meshtastic(String::from(PROJECTION_INVALID));
    };
    let Some(state) = transport_state(connection_state) else {
        return unavailable_meshtastic(String::from(PROJECTION_INVALID));
    };
    let transport = connection
        .get("transport")
        .and_then(Value::as_str)
        .map(str::to_owned);
    if transport.is_none() {
        return unavailable_meshtastic(String::from(PROJECTION_INVALID));
    }

    MeshtasticServiceStatus {
        state,
        transport,
        frames_received: value
            .get("metrics")
            .and_then(|metrics| metrics.get("framesReceived"))
            .and_then(Value::as_u64),
        reason_code: connection
            .get("reasonCode")
            .and_then(Value::as_str)
            .map(str::to_owned),
    }
}

fn project_aprs_callmesh(
    aprs: ProjectionResult,
    callmesh: ProjectionResult,
) -> AprsCallMeshServiceStatus {
    let aprs = aprs.and_then(project_aprs);
    let callmesh = callmesh.and_then(project_callmesh);

    match (aprs, callmesh) {
        (Ok(aprs), Ok(callmesh)) => AprsCallMeshServiceStatus {
            state: combined_aprs_callmesh_state(aprs.state, callmesh.state),
            aprs_state: Some(aprs.raw_state),
            callmesh_state: Some(callmesh.raw_state),
            active_mapping_count: Some(callmesh.active_mapping_count),
            pending_count: Some(aprs.pending_count),
            failed_count: Some(aprs.failed_count),
            reason_code: aprs.reason_code.or(callmesh.reason_code),
        },
        (Err(aprs_error), Ok(callmesh)) => AprsCallMeshServiceStatus {
            state: if callmesh.state == ServiceState::Unavailable {
                ServiceState::Unavailable
            } else {
                ServiceState::Degraded
            },
            aprs_state: None,
            callmesh_state: Some(callmesh.raw_state),
            active_mapping_count: Some(callmesh.active_mapping_count),
            pending_count: None,
            failed_count: None,
            reason_code: Some(aprs_error),
        },
        (Ok(aprs), Err(callmesh_error)) => AprsCallMeshServiceStatus {
            state: ServiceState::Unavailable,
            aprs_state: Some(aprs.raw_state),
            callmesh_state: None,
            active_mapping_count: None,
            pending_count: Some(aprs.pending_count),
            failed_count: Some(aprs.failed_count),
            reason_code: Some(callmesh_error),
        },
        (Err(aprs_error), Err(_)) => AprsCallMeshServiceStatus {
            state: ServiceState::Unavailable,
            aprs_state: None,
            callmesh_state: None,
            active_mapping_count: None,
            pending_count: None,
            failed_count: None,
            reason_code: Some(aprs_error),
        },
    }
}

struct AprsProjection {
    state: ServiceState,
    raw_state: String,
    pending_count: u64,
    failed_count: u64,
    reason_code: Option<String>,
}

struct CallMeshProjection {
    state: ServiceState,
    raw_state: String,
    active_mapping_count: u64,
    reason_code: Option<String>,
}

fn project_callmesh(value: Value) -> Result<CallMeshProjection, String> {
    let status = value
        .get("status")
        .ok_or_else(|| String::from(PROJECTION_INVALID))?;
    let raw_state = status
        .get("state")
        .and_then(Value::as_str)
        .ok_or_else(|| String::from(PROJECTION_INVALID))?;
    let state = match raw_state {
        "unavailable" => ServiceState::Unavailable,
        "checking" => ServiceState::Starting,
        "ready" => ServiceState::Running,
        "degraded" => ServiceState::Degraded,
        _ => return Err(String::from(PROJECTION_INVALID)),
    };
    let active_mapping_count = status
        .get("activeMappingCount")
        .and_then(Value::as_u64)
        .ok_or_else(|| String::from(PROJECTION_INVALID))?;

    Ok(CallMeshProjection {
        state,
        raw_state: raw_state.to_owned(),
        active_mapping_count,
        reason_code: status
            .get("reasonCode")
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}

fn project_aprs(value: Value) -> Result<AprsProjection, String> {
    let configured = value
        .get("configured")
        .and_then(Value::as_bool)
        .ok_or_else(|| String::from(PROJECTION_INVALID))?;
    let running = value
        .get("running")
        .and_then(Value::as_bool)
        .ok_or_else(|| String::from(PROJECTION_INVALID))?;
    let raw_state = value
        .get("monitorStatus")
        .and_then(Value::as_str)
        .ok_or_else(|| String::from(PROJECTION_INVALID))?;
    let mut state = if !configured {
        ServiceState::Disabled
    } else {
        match raw_state {
            "stopped" => ServiceState::Stopped,
            "idle" | "connected" if running => ServiceState::Running,
            "idle" | "connected" => ServiceState::Stopped,
            "connecting" => ServiceState::Starting,
            "error" => ServiceState::Degraded,
            _ => return Err(String::from(PROJECTION_INVALID)),
        }
    };
    let pending_count = value
        .get("pendingOutbox")
        .and_then(Value::as_u64)
        .ok_or_else(|| String::from(PROJECTION_INVALID))?;
    let failed_count = value
        .get("failedOutbox")
        .and_then(Value::as_u64)
        .ok_or_else(|| String::from(PROJECTION_INVALID))?;
    let reason_code = value
        .get("lastErrorCode")
        .and_then(Value::as_str)
        .map(str::to_owned);
    if configured && state == ServiceState::Running && (failed_count > 0 || reason_code.is_some()) {
        state = ServiceState::Degraded;
    }
    Ok(AprsProjection {
        state,
        raw_state: raw_state.to_owned(),
        pending_count,
        failed_count,
        reason_code,
    })
}

fn combined_aprs_callmesh_state(aprs: ServiceState, callmesh: ServiceState) -> ServiceState {
    if aprs == ServiceState::Running && callmesh == ServiceState::Running {
        return ServiceState::Running;
    }
    if aprs == ServiceState::Unavailable || callmesh == ServiceState::Unavailable {
        return ServiceState::Unavailable;
    }
    if matches!(aprs, ServiceState::Starting) || matches!(callmesh, ServiceState::Starting) {
        return ServiceState::Starting;
    }
    ServiceState::Degraded
}

fn project_proxy(result: ProjectionResult) -> ProxyServiceStatus {
    let value = match result {
        Ok(value) => value,
        Err(code) => return unavailable_proxy(code),
    };
    let Some(raw_state) = value.get("state").and_then(Value::as_str) else {
        return unavailable_proxy(String::from(PROJECTION_INVALID));
    };
    let state = match raw_state {
        "stopped" => ServiceState::Stopped,
        "starting" => ServiceState::Starting,
        "running" => ServiceState::Running,
        "degraded" => ServiceState::Degraded,
        _ => return unavailable_proxy(String::from(PROJECTION_INVALID)),
    };
    let Some(policy) = value.get("policy") else {
        return unavailable_proxy(String::from(PROJECTION_INVALID));
    };
    let mode = policy
        .get("mode")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let active_clients = policy.get("activeClients").and_then(Value::as_u64);
    let max_clients = policy.get("maxClients").and_then(Value::as_u64);
    if mode.is_none() || active_clients.is_none() || max_clients.is_none() {
        return unavailable_proxy(String::from(PROJECTION_INVALID));
    }

    ProxyServiceStatus {
        state,
        mode,
        active_clients,
        max_clients,
        reason_code: value
            .get("lastErrorCode")
            .and_then(Value::as_str)
            .map(str::to_owned),
    }
}

fn transport_state(status: &str) -> Option<ServiceState> {
    match status {
        "disconnected" => Some(ServiceState::Stopped),
        "connecting" | "configuring" => Some(ServiceState::Starting),
        "ready" => Some(ServiceState::Running),
        "degraded" => Some(ServiceState::Degraded),
        "backoff" => Some(ServiceState::Backoff),
        _ => None,
    }
}

fn unavailable_meshtastic(code: String) -> MeshtasticServiceStatus {
    MeshtasticServiceStatus {
        state: ServiceState::Unavailable,
        transport: None,
        frames_received: None,
        reason_code: Some(code),
    }
}

fn unavailable_proxy(code: String) -> ProxyServiceStatus {
    ProxyServiceStatus {
        state: ServiceState::Unavailable,
        mode: None,
        active_clients: None,
        max_clients: None,
        reason_code: Some(code),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PROJECTION_INVALID, ServiceState, project, project_aprs_callmesh, project_meshtastic,
        project_proxy,
    };
    use serde_json::json;

    #[test]
    fn projects_meshtastic_connection_without_exposing_gateway_details() {
        let status = project_meshtastic(Ok(json!({
            "configured": true,
            "meshNetworkId": "mesh-a",
            "gatewayId": "gateway-secret-label",
            "connection": {
                "transport": "tcp",
                "status": "ready",
                "changedAt": "2026-07-18T00:00:00.000Z"
            },
            "metrics": {
                "framesReceived": 42
            }
        })));

        assert_eq!(status.state, ServiceState::Running);
        assert_eq!(status.transport.as_deref(), Some("tcp"));
        assert_eq!(status.frames_received, Some(42));
        assert_eq!(status.reason_code, None);
        let serialized = serde_json::to_value(status).expect("status should serialize");
        assert_eq!(serialized.get("gatewayId"), None);
        assert_eq!(serialized["framesReceived"], 42);
    }

    #[test]
    fn reports_disabled_and_rejects_malformed_meshtastic_projections() {
        let disabled = project_meshtastic(Ok(json!({ "configured": false })));
        let malformed = project_meshtastic(Ok(json!({
            "configured": true,
            "connection": { "status": "ready" }
        })));

        assert_eq!(disabled.state, ServiceState::Disabled);
        assert_eq!(malformed.state, ServiceState::Unavailable);
        assert_eq!(malformed.reason_code.as_deref(), Some(PROJECTION_INVALID));
    }

    #[test]
    fn combines_aprs_outbox_health_with_callmesh_mapping_state() {
        let status = project_aprs_callmesh(
            Ok(json!({
                "configured": true,
                "running": true,
                "monitorStatus": "connected",
                "mappedCallsigns": 3,
                "pendingOutbox": 2,
                "failedOutbox": 1
            })),
            Ok(json!({
                "status": {
                    "state": "ready",
                    "activeMappingCount": 3
                },
                "mappings": []
            })),
        );

        assert_eq!(status.state, ServiceState::Degraded);
        assert_eq!(status.aprs_state.as_deref(), Some("connected"));
        assert_eq!(status.callmesh_state.as_deref(), Some("ready"));
        assert_eq!(status.active_mapping_count, Some(3));
        assert_eq!(status.pending_count, Some(2));
        assert_eq!(status.failed_count, Some(1));
    }

    #[test]
    fn fails_closed_when_only_one_aprs_callmesh_projection_is_available() {
        let aprs_unavailable = project_aprs_callmesh(
            Err(String::from("CONTROL_TIMEOUT")),
            Ok(json!({
                "status": { "state": "ready", "activeMappingCount": 1 },
                "mappings": []
            })),
        );
        let callmesh_unavailable = project_aprs_callmesh(
            Ok(json!({
                "configured": true,
                "running": true,
                "monitorStatus": "idle",
                "mappedCallsigns": 0,
                "pendingOutbox": 0,
                "failedOutbox": 0
            })),
            Err(String::from("CONTROL_COMMAND_FAILED")),
        );

        assert_eq!(aprs_unavailable.state, ServiceState::Degraded);
        assert_eq!(
            aprs_unavailable.reason_code.as_deref(),
            Some("CONTROL_TIMEOUT")
        );
        assert_eq!(callmesh_unavailable.state, ServiceState::Unavailable);
        assert_eq!(callmesh_unavailable.pending_count, Some(0));
    }

    #[test]
    fn projects_proxy_capacity_and_stable_error_code() {
        let status = project_proxy(Ok(json!({
            "state": "degraded",
            "policy": {
                "mode": "message",
                "activeClients": 2,
                "maxClients": 8
            },
            "lastErrorCode": "PROXY_UPSTREAM_BACKOFF"
        })));

        assert_eq!(status.state, ServiceState::Degraded);
        assert_eq!(status.mode.as_deref(), Some("message"));
        assert_eq!(status.active_clients, Some(2));
        assert_eq!(status.max_clients, Some(8));
        assert_eq!(
            status.reason_code.as_deref(),
            Some("PROXY_UPSTREAM_BACKOFF")
        );
    }

    #[test]
    fn keeps_the_desktop_projection_schema_stable() {
        let status = project(
            Ok(json!({ "configured": false })),
            Ok(json!({
                "configured": true,
                "running": true,
                "monitorStatus": "idle",
                "mappedCallsigns": 0,
                "pendingOutbox": 0,
                "failedOutbox": 0
            })),
            Ok(json!({
                "status": { "state": "checking", "activeMappingCount": 0 },
                "mappings": []
            })),
            Ok(json!({
                "state": "stopped",
                "policy": { "mode": "monitor", "activeClients": 0, "maxClients": 16 }
            })),
        );
        let serialized = serde_json::to_value(status).expect("status should serialize");

        assert_eq!(serialized["schemaVersion"], 1);
        assert_eq!(serialized["meshtastic"]["state"], "disabled");
        assert_eq!(serialized["aprsCallmesh"]["state"], "starting");
        assert_eq!(serialized["proxy"]["state"], "stopped");
    }
}
