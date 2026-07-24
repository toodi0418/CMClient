import { createHash } from "node:crypto";

export interface SqlMigration {
  readonly version: number;
  readonly name: string;
  readonly sql: string;
  readonly sha256: string;
}

export class MigrationManifestError extends Error {
  readonly code = "DATABASE_MIGRATION_MANIFEST_INVALID";

  constructor() {
    super("DATABASE_MIGRATION_MANIFEST_INVALID");
    this.name = "MigrationManifestError";
  }
}

const EXPECTED_SHA256 = [
  "e3a05fb9f0cd32720057034fd07bbe50a0e3fbbdcece73b25c0dd2138d680584",
  "411a6d0eec11f40a6fac42e441ac4a374b7bd5a57e9c5c6875864350927023fc",
  "8b9f7f33b1de71a4c603633ce341bfb894e24a719302b033d16277b1f7a56dfc",
  "5ddac146d3c3ca53cc05167293c56e17d524e7d2af28810f2c984fe76cd2f82a",
  "31162cfba183875e33745743645c371f157c1e5cfdfcea4be8694f826cebd4ac",
  "6cd3e3a0d7e080ecc751e7d75352ee79e6948fb0b1f46f24babe72e4580bfba0",
  "142646f18187b29c00011b85f9fbebb1321d847bdea1c2b1703d45d74ede6642",
  "19313b12d1833738b353d52262b725614c77dc0f99bc6517430bbc2723eeeed2",
  "6441c66d388873d374c5820a57836b1173570eb409b29d37c4c5c29450fce0a6",
  "c11240044cfbebcf700361fc141234656ef9f875aee34c0bde5b743351470ef0",
  "bf76361fad97ffc3d62d3ac6166ddba9d8441e2970af4a36f01d7fc3de808ff3",
  "0e24d98f6f5581be548a95f969477283f407967d6cab8535e9586d207de95d8f",
  "7567c5aee928fb968bdc44e7be6942e76e8cb31e27c834dc53f8504591b6390c",
  "7c4eed5d8b5d6df26c5f755d36ccf80bbe7652395ac4a04c7bc4aa92daf10662",
  "f62171d84e7ac53380042fd21d1f9664d3d81ceca1fa9ad61e3a7da27ed719d8",
] as const;

function migration(
  version: number,
  name: string,
  statements: readonly string[],
): SqlMigration {
  const sql = `${statements.join(";\n")};`;
  const sha256 = createHash("sha256").update(sql, "utf8").digest("hex");
  const expected = EXPECTED_SHA256[version - 1];
  if (expected !== sha256) {
    throw new MigrationManifestError();
  }
  return Object.freeze({ version, name, sql, sha256 });
}

export function createSqlMigration(
  version: number,
  name: string,
  sql: string,
): SqlMigration {
  return Object.freeze({
    version,
    name,
    sql,
    sha256: createHash("sha256").update(sql, "utf8").digest("hex"),
  });
}

export const gatewayMigrations: readonly SqlMigration[] = Object.freeze([
  migration(1, "settings", [
    "CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
  ]),
  migration(2, "jobs", [
    "CREATE TABLE jobs (id TEXT PRIMARY KEY, type TEXT NOT NULL, status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'waiting', 'succeeded', 'failed', 'cancelling', 'cancelled', 'rolling_back', 'rolled_back')), input TEXT NOT NULL, result TEXT, error_code TEXT, error_params TEXT, idempotency_key TEXT, cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK (cancel_requested IN (0, 1)), created_at TEXT NOT NULL, updated_at TEXT NOT NULL, started_at TEXT, completed_at TEXT)",
    "CREATE UNIQUE INDEX jobs_type_idempotency_key_unique ON jobs (type, idempotency_key) WHERE idempotency_key IS NOT NULL",
    "CREATE INDEX jobs_status_updated_at_index ON jobs (status, updated_at)",
  ]),
  migration(3, "mesh_observations", [
    "CREATE TABLE mesh_observations (id TEXT PRIMARY KEY, schema_version INTEGER NOT NULL CHECK (schema_version = 1), transport TEXT NOT NULL CHECK (transport IN ('tcp', 'serial', 'simulator')), session_connected_at TEXT NOT NULL, ingested_at TEXT NOT NULL, server_ingested_at TEXT NOT NULL, device_rx_time_seconds INTEGER CHECK (device_rx_time_seconds IS NULL OR (device_rx_time_seconds >= 0 AND device_rx_time_seconds <= 4294967295)), backlog_classification TEXT NOT NULL CHECK (backlog_classification IN ('backlog', 'live', 'unknown')), normalized_from_radio TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
    "CREATE INDEX mesh_observations_session_ingested_at_index ON mesh_observations (session_connected_at, ingested_at)",
  ]),
  migration(4, "mesh_domain_records", [
    "CREATE TABLE nodes (mesh_network_id TEXT NOT NULL, node_num INTEGER NOT NULL CHECK (node_num >= 0 AND node_num <= 4294967295), user_id TEXT, long_name TEXT, short_name TEXT, hardware_model TEXT, role TEXT, first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL, last_observation_id TEXT NOT NULL, PRIMARY KEY (mesh_network_id, node_num))",
    "CREATE INDEX nodes_last_seen_at_index ON nodes (mesh_network_id, last_seen_at DESC)",
    "CREATE TABLE messages (id TEXT PRIMARY KEY, observation_id TEXT NOT NULL UNIQUE REFERENCES mesh_observations(id), mesh_network_id TEXT NOT NULL, sender INTEGER NOT NULL CHECK (sender >= 0 AND sender <= 4294967295), destination INTEGER CHECK (destination IS NULL OR (destination >= 0 AND destination <= 4294967295)), packet_id INTEGER CHECK (packet_id IS NULL OR (packet_id >= 0 AND packet_id <= 4294967295)), channel INTEGER CHECK (channel IS NULL OR (channel >= 0 AND channel <= 255)), text TEXT NOT NULL, observed_at TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
    "CREATE INDEX messages_network_sender_observed_at_index ON messages (mesh_network_id, sender, observed_at DESC)",
    "CREATE TABLE telemetry (id TEXT PRIMARY KEY, observation_id TEXT NOT NULL UNIQUE REFERENCES mesh_observations(id), mesh_network_id TEXT NOT NULL, node_num INTEGER NOT NULL CHECK (node_num >= 0 AND node_num <= 4294967295), packet_id INTEGER CHECK (packet_id IS NULL OR (packet_id >= 0 AND packet_id <= 4294967295)), metric_kind TEXT NOT NULL, metrics TEXT NOT NULL, observed_at TEXT NOT NULL, telemetry_time_seconds INTEGER CHECK (telemetry_time_seconds IS NULL OR (telemetry_time_seconds > 0 AND telemetry_time_seconds <= 4294967295)), created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
    "CREATE INDEX telemetry_network_node_observed_at_index ON telemetry (mesh_network_id, node_num, observed_at DESC)",
  ]),
  migration(5, "position_domain_records", [
    "CREATE TABLE position_observations (id TEXT PRIMARY KEY, mesh_network_id TEXT NOT NULL, node_num INTEGER NOT NULL CHECK (node_num >= 0 AND node_num <= 4294967295), mesh_observation_id TEXT NOT NULL REFERENCES mesh_observations(id), gateway_id TEXT NOT NULL, transport TEXT NOT NULL CHECK (transport IN ('tcp', 'serial', 'simulator')), session_connected_at TEXT NOT NULL, ingested_at TEXT NOT NULL, server_ingested_at TEXT NOT NULL, device_rx_time_seconds INTEGER CHECK (device_rx_time_seconds IS NULL OR (device_rx_time_seconds >= 0 AND device_rx_time_seconds <= 4294967295)), backlog_classification TEXT NOT NULL CHECK (backlog_classification IN ('backlog', 'live', 'unknown')), packet_id INTEGER CHECK (packet_id IS NULL OR (packet_id >= 0 AND packet_id <= 4294967295)), payload_hash TEXT NOT NULL, via_mqtt INTEGER CHECK (via_mqtt IS NULL OR via_mqtt IN (0, 1)), rx_snr REAL, rx_rssi INTEGER, hop_limit INTEGER CHECK (hop_limit IS NULL OR hop_limit >= 0), hop_start INTEGER CHECK (hop_start IS NULL OR hop_start >= 0), position TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
    "CREATE INDEX position_observations_network_node_ingested_at_index ON position_observations (mesh_network_id, node_num, ingested_at DESC)",
    "CREATE TABLE position_events (id TEXT PRIMARY KEY, canonical_key TEXT NOT NULL UNIQUE, mesh_network_id TEXT NOT NULL, node_num INTEGER NOT NULL CHECK (node_num >= 0 AND node_num <= 4294967295), source_observation_id TEXT NOT NULL REFERENCES position_observations(id), payload_hash TEXT NOT NULL, event_time TEXT, event_time_source TEXT CHECK (event_time_source IS NULL OR event_time_source IN ('position_timestamp', 'position_time', 'sequence')), sequence_epoch INTEGER CHECK (sequence_epoch IS NULL OR sequence_epoch >= 0), sequence_number INTEGER CHECK (sequence_number IS NULL OR (sequence_number >= 0 AND sequence_number <= 4294967295)), position TEXT NOT NULL, created_at TEXT NOT NULL)",
    "CREATE INDEX position_events_network_node_event_time_index ON position_events (mesh_network_id, node_num, event_time DESC)",
    "CREATE TABLE position_decisions (id TEXT PRIMARY KEY, observation_id TEXT NOT NULL REFERENCES position_observations(id), canonical_event_id TEXT REFERENCES position_events(id), code TEXT NOT NULL, decided_at TEXT NOT NULL, parameters TEXT NOT NULL)",
    "CREATE INDEX position_decisions_observation_decided_at_index ON position_decisions (observation_id, decided_at DESC)",
    "CREATE TABLE node_position_state (mesh_network_id TEXT NOT NULL, node_num INTEGER NOT NULL CHECK (node_num >= 0 AND node_num <= 4294967295), callsign TEXT NOT NULL, mapping_version TEXT NOT NULL, latest_canonical_event_id TEXT REFERENCES position_events(id), latest_event_time TEXT, latest_sequence_epoch INTEGER CHECK (latest_sequence_epoch IS NULL OR latest_sequence_epoch >= 0), latest_sequence_number INTEGER CHECK (latest_sequence_number IS NULL OR (latest_sequence_number >= 0 AND latest_sequence_number <= 4294967295)), latest_latitude_i INTEGER, latest_longitude_i INTEGER, updated_at TEXT NOT NULL, PRIMARY KEY (mesh_network_id, node_num, callsign, mapping_version))",
  ]),
  migration(6, "aprs_outbox", [
    "CREATE TABLE aprs_outbox (id TEXT PRIMARY KEY, callsign TEXT NOT NULL, canonical_event_id TEXT NOT NULL REFERENCES position_events(id), data TEXT NOT NULL, status TEXT NOT NULL CHECK (status IN ('queued', 'sending', 'sent', 'failed')), attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0), next_attempt_at TEXT NOT NULL, last_error_code TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, sent_at TEXT, UNIQUE (callsign, canonical_event_id))",
    "CREATE INDEX aprs_outbox_due_index ON aprs_outbox (status, next_attempt_at)",
  ]),
  migration(7, "aprs_remote_high_water", [
    "CREATE TABLE aprs_remote_high_water (mesh_network_id TEXT NOT NULL, node_num INTEGER NOT NULL CHECK (node_num >= 0 AND node_num <= 4294967295), callsign TEXT NOT NULL, mapping_version TEXT NOT NULL, latest_event_time TEXT NOT NULL, latest_event_marker TEXT NOT NULL, received_at TEXT NOT NULL, PRIMARY KEY (mesh_network_id, node_num, callsign, mapping_version))",
    "CREATE INDEX aprs_remote_high_water_callsign_event_time_index ON aprs_remote_high_water (callsign, latest_event_time DESC)",
  ]),
  migration(8, "callmesh_mappings", [
    "CREATE TABLE callmesh_mappings (version TEXT NOT NULL, effective_at TEXT NOT NULL, mesh_network_id TEXT NOT NULL, node_num INTEGER NOT NULL CHECK (node_num >= 0 AND node_num <= 4294967295), callsign TEXT NOT NULL, PRIMARY KEY (version, effective_at, mesh_network_id, node_num, callsign))",
    "CREATE INDEX callmesh_mappings_target_index ON callmesh_mappings (mesh_network_id, node_num, effective_at DESC)",
  ]),
  migration(9, "telemetry_range_query_index", [
    "CREATE INDEX telemetry_metric_observed_at_index ON telemetry (metric_kind, observed_at DESC)",
  ]),
  migration(10, "load_retention_indexes", [
    "CREATE INDEX mesh_observations_ingested_at_index ON mesh_observations (ingested_at, id)",
    "CREATE INDEX nodes_last_observation_id_index ON nodes (last_observation_id)",
    "CREATE INDEX position_observations_mesh_observation_id_index ON position_observations (mesh_observation_id)",
    "CREATE INDEX telemetry_observed_at_index ON telemetry (observed_at, id)",
    "CREATE INDEX jobs_terminal_retention_index ON jobs (completed_at, id) WHERE status IN ('succeeded', 'failed', 'cancelled', 'rolled_back')",
    "CREATE INDEX jobs_queued_created_at_index ON jobs (created_at, id) WHERE status = 'queued'",
  ]),
  migration(11, "read_projection_indexes", [
    "CREATE INDEX nodes_recent_projection_index ON nodes (last_seen_at DESC, node_num ASC)",
    "CREATE INDEX messages_recent_projection_index ON messages (observed_at DESC, id ASC)",
    "CREATE INDEX telemetry_recent_projection_index ON telemetry (observed_at DESC, id ASC)",
    "CREATE INDEX position_events_recent_projection_index ON position_events (COALESCE(event_time, created_at) DESC, id ASC)",
    "CREATE INDEX aprs_outbox_recent_projection_index ON aprs_outbox (updated_at DESC, id ASC)",
    "CREATE INDEX aprs_outbox_due_order_index ON aprs_outbox (next_attempt_at ASC, created_at ASC, id ASC) WHERE status IN ('queued', 'failed')",
    "CREATE INDEX aprs_outbox_sent_retention_index ON aprs_outbox (sent_at ASC, id ASC) WHERE status = 'sent'",
  ]),
  migration(12, "bounded_domain_retention_and_delivery_high_water", [
    "CREATE TABLE aprs_delivery_high_water (mesh_network_id TEXT NOT NULL, node_num INTEGER NOT NULL CHECK (node_num >= 0 AND node_num <= 4294967295), callsign TEXT NOT NULL, latest_canonical_event_id TEXT NOT NULL REFERENCES position_events(id), latest_event_time TEXT NOT NULL, latest_sequence_epoch INTEGER CHECK (latest_sequence_epoch IS NULL OR latest_sequence_epoch >= 0), latest_sequence_number INTEGER CHECK (latest_sequence_number IS NULL OR (latest_sequence_number >= 0 AND latest_sequence_number <= 4294967295)), delivered_at TEXT NOT NULL, PRIMARY KEY (mesh_network_id, node_num, callsign))",
    "INSERT INTO aprs_delivery_high_water (mesh_network_id, node_num, callsign, latest_canonical_event_id, latest_event_time, latest_sequence_epoch, latest_sequence_number, delivered_at) SELECT mesh_network_id, node_num, callsign, canonical_event_id, event_time, sequence_epoch, sequence_number, sent_at FROM (SELECT event.mesh_network_id, event.node_num, outbox.callsign, event.id AS canonical_event_id, event.event_time, event.sequence_epoch, event.sequence_number, outbox.sent_at, ROW_NUMBER() OVER (PARTITION BY event.mesh_network_id, event.node_num, outbox.callsign ORDER BY event.event_time DESC, outbox.sent_at DESC, event.id DESC) AS delivery_rank FROM aprs_outbox AS outbox JOIN position_events AS event ON event.id = outbox.canonical_event_id WHERE outbox.status = 'sent' AND outbox.sent_at IS NOT NULL AND event.event_time IS NOT NULL) WHERE delivery_rank = 1",
    "CREATE INDEX aprs_delivery_high_water_latest_event_id_index ON aprs_delivery_high_water (latest_canonical_event_id)",
    "CREATE INDEX node_position_state_latest_event_id_index ON node_position_state (latest_canonical_event_id)",
    "CREATE INDEX jobs_queued_type_created_at_index ON jobs (type, created_at ASC, id ASC) WHERE status = 'queued'",
    "CREATE INDEX messages_retention_index ON messages (observed_at ASC, id ASC)",
    "CREATE INDEX position_decisions_retention_index ON position_decisions (decided_at ASC, id ASC)",
    "CREATE INDEX position_decisions_canonical_event_id_index ON position_decisions (canonical_event_id)",
    "CREATE INDEX position_events_retention_index ON position_events (created_at ASC, id ASC)",
    "CREATE INDEX position_events_source_observation_id_index ON position_events (source_observation_id)",
    "CREATE INDEX position_observations_retention_index ON position_observations (ingested_at ASC, id ASC)",
    "CREATE INDEX aprs_outbox_canonical_event_id_index ON aprs_outbox (canonical_event_id)",
  ]),
  migration(13, "aprs_outbox_order_snapshots", [
    "ALTER TABLE aprs_outbox ADD COLUMN mesh_network_id TEXT",
    "ALTER TABLE aprs_outbox ADD COLUMN node_num INTEGER",
    "ALTER TABLE aprs_outbox ADD COLUMN mapping_version TEXT",
    "ALTER TABLE aprs_outbox ADD COLUMN event_time TEXT",
    "ALTER TABLE aprs_outbox ADD COLUMN sequence_epoch INTEGER",
    "ALTER TABLE aprs_outbox ADD COLUMN sequence_number INTEGER",
    "UPDATE aprs_outbox SET mesh_network_id = (SELECT event.mesh_network_id FROM position_events AS event WHERE event.id = aprs_outbox.canonical_event_id), node_num = (SELECT event.node_num FROM position_events AS event WHERE event.id = aprs_outbox.canonical_event_id), event_time = (SELECT event.event_time FROM position_events AS event WHERE event.id = aprs_outbox.canonical_event_id), sequence_epoch = (SELECT event.sequence_epoch FROM position_events AS event WHERE event.id = aprs_outbox.canonical_event_id), sequence_number = (SELECT event.sequence_number FROM position_events AS event WHERE event.id = aprs_outbox.canonical_event_id)",
    "ALTER TABLE aprs_delivery_high_water ADD COLUMN latest_mapping_version TEXT",
    "CREATE INDEX aprs_outbox_active_order_index ON aprs_outbox (mesh_network_id, node_num, callsign, event_time DESC, sequence_epoch DESC, sequence_number DESC, id ASC) WHERE status IN ('queued', 'sending', 'failed')",
  ]),
  migration(14, "callmesh_sync_high_water", [
    "CREATE TABLE callmesh_sync_state (id INTEGER PRIMARY KEY CHECK (id = 1), active INTEGER NOT NULL CHECK (active IN (0, 1)), mapping_hash TEXT NOT NULL CHECK (length(mapping_hash) BETWEEN 1 AND 128), accepted_server_time TEXT NOT NULL, mappings_fingerprint TEXT NOT NULL CHECK (length(mappings_fingerprint) = 64), last_heartbeat_at TEXT NOT NULL, mapping_synced_at TEXT NOT NULL, provision_json TEXT, provision_expires_at TEXT, provision_fingerprint TEXT CHECK (provision_fingerprint IS NULL OR length(provision_fingerprint) = 64), updated_at TEXT NOT NULL, CHECK ((provision_json IS NULL AND provision_expires_at IS NULL AND provision_fingerprint IS NULL) OR (provision_json IS NOT NULL AND provision_expires_at IS NOT NULL AND provision_fingerprint IS NOT NULL)))",
    "CREATE TABLE callmesh_sync_history (mapping_hash TEXT PRIMARY KEY CHECK (length(mapping_hash) BETWEEN 1 AND 128), first_server_time TEXT NOT NULL, last_server_time TEXT NOT NULL, mappings_fingerprint TEXT NOT NULL CHECK (length(mappings_fingerprint) = 64))",
  ]),
  migration(15, "aprs_outbox_provision_authorization", [
    "ALTER TABLE aprs_outbox ADD COLUMN provision_fingerprint TEXT CHECK (provision_fingerprint IS NULL OR (length(provision_fingerprint) = 64 AND provision_fingerprint NOT GLOB '*[^a-f0-9]*'))",
  ]),
]);

export function validateMigrationManifest(
  migrations: readonly SqlMigration[],
): void {
  if (migrations.length === 0) {
    throw new MigrationManifestError();
  }
  const names = new Set<string>();
  for (const [index, entry] of migrations.entries()) {
    const expectedVersion = index + 1;
    const digest = createHash("sha256").update(entry.sql, "utf8").digest("hex");
    if (
      entry.version !== expectedVersion ||
      !/^[a-z][a-z0-9_]{0,95}$/.test(entry.name) ||
      names.has(entry.name) ||
      entry.sql.length === 0 ||
      !entry.sql.endsWith(";") ||
      !/^[a-f0-9]{64}$/.test(entry.sha256) ||
      digest !== entry.sha256
    ) {
      throw new MigrationManifestError();
    }
    names.add(entry.name);
  }
}

validateMigrationManifest(gatewayMigrations);
