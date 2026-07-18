//! One-shot, conservative readers for CMClient Legacy migration inputs.

use serde::Serialize;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter},
    fs,
    io::Write,
    path::Path,
};

/// Legacy settings payloads are small user preference documents, not data exports.
pub const MAX_LEGACY_SETTINGS_BYTES: usize = 64 * 1024;

/// Stable workspace identity for the offline migration boundary.
pub const COMPONENT: &str = "legacy-migration";

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ImportDisposition {
    Imported,
    ManualReview,
    Removed,
    SecretSkipped,
    UnknownSkipped,
    Invalid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportFinding {
    pub field: String,
    pub disposition: ImportDisposition,
    pub code: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacySettingsReport {
    pub schema_version: u8,
    pub source_format: String,
    pub imported: Vec<ImportFinding>,
    pub findings: Vec<ImportFinding>,
    pub proposed_agent_toml: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LegacySettingsError {
    SourceTooLarge,
    JsonInvalid,
    RootInvalid,
    TargetNotAbsolute,
    TargetExists,
    NothingImportable,
    WriteFailed,
}

impl LegacySettingsError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::SourceTooLarge => "LEGACY_SETTINGS_SOURCE_TOO_LARGE",
            Self::JsonInvalid => "LEGACY_SETTINGS_JSON_INVALID",
            Self::RootInvalid => "LEGACY_SETTINGS_ROOT_INVALID",
            Self::TargetNotAbsolute => "LEGACY_SETTINGS_TARGET_NOT_ABSOLUTE",
            Self::TargetExists => "LEGACY_SETTINGS_TARGET_EXISTS",
            Self::NothingImportable => "LEGACY_SETTINGS_NOTHING_IMPORTABLE",
            Self::WriteFailed => "LEGACY_SETTINGS_WRITE_FAILED",
        }
    }
}

impl Display for LegacySettingsError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for LegacySettingsError {}

/// Parses a Legacy `client-preferences.json` object without preserving values that
/// are not part of the CMClient 2.0 Agent configuration contract.
pub fn inspect_legacy_settings(input: &[u8]) -> Result<LegacySettingsReport, LegacySettingsError> {
    if input.len() > MAX_LEGACY_SETTINGS_BYTES {
        return Err(LegacySettingsError::SourceTooLarge);
    }
    let value: Value =
        serde_json::from_slice(input).map_err(|_| LegacySettingsError::JsonInvalid)?;
    let root = value.as_object().ok_or(LegacySettingsError::RootInvalid)?;
    let preferences = root
        .get("preferences")
        .and_then(Value::as_object)
        .unwrap_or(root);

    let mut ordered = BTreeMap::new();
    for (field, value) in preferences {
        ordered.insert(field.as_str(), value);
    }

    let mut imported = Vec::new();
    let mut findings = Vec::new();
    let mut management_web_enabled = None;
    for (field, value) in ordered {
        match field {
            "webDashboardEnabled" => match value.as_bool() {
                Some(enabled) => {
                    management_web_enabled = Some(enabled);
                    imported.push(finding(
                        field,
                        ImportDisposition::Imported,
                        "LEGACY_SETTINGS_MANAGEMENT_WEB_IMPORTED",
                    ));
                }
                None => findings.push(finding(
                    field,
                    ImportDisposition::Invalid,
                    "LEGACY_SETTINGS_VALUE_INVALID",
                )),
            },
            "connectionMode" | "host" | "tcpHost" | "serialPath" | "serialBaudRate" => {
                findings.push(finding(
                    field,
                    ImportDisposition::ManualReview,
                    "LEGACY_SETTINGS_TRANSPORT_REQUIRES_REVIEW",
                ));
            }
            "aprsServer" | "aprsBeaconMinutes" => findings.push(finding(
                field,
                ImportDisposition::ManualReview,
                "LEGACY_SETTINGS_APRS_REQUIRES_REVIEW",
            )),
            "shareWithTenmanMap" => findings.push(finding(
                field,
                ImportDisposition::Removed,
                "LEGACY_SETTINGS_REMOVED_TENMAN",
            )),
            "autoTracerouteEnabled" | "tracerouteRateMinutes" | "tracerouteIntervalSeconds" => {
                findings.push(finding(
                    field,
                    ImportDisposition::Removed,
                    "LEGACY_SETTINGS_REMOVED_LEGACY_RUNTIME_FEATURE",
                ));
            }
            _ if is_secret_field(field) => findings.push(finding(
                field,
                ImportDisposition::SecretSkipped,
                "LEGACY_SETTINGS_SECRET_SKIPPED",
            )),
            _ => findings.push(finding(
                field,
                ImportDisposition::UnknownSkipped,
                "LEGACY_SETTINGS_UNKNOWN_SKIPPED",
            )),
        }
    }

    Ok(LegacySettingsReport {
        schema_version: 1,
        source_format: String::from("legacy-client-preferences-json"),
        imported,
        findings,
        proposed_agent_toml: management_web_enabled
            .map(|enabled| format!("[agent]\nmanagement_web_enabled = {enabled}\n")),
    })
}

/// Creates a new Agent configuration from a reviewed report. Existing runtime
/// configuration is intentionally never merged or overwritten by migration code.
pub fn write_new_agent_config(
    target: &Path,
    report: &LegacySettingsReport,
) -> Result<(), LegacySettingsError> {
    if !target.is_absolute() {
        return Err(LegacySettingsError::TargetNotAbsolute);
    }
    let content = report
        .proposed_agent_toml
        .as_ref()
        .ok_or(LegacySettingsError::NothingImportable)?;
    let parent = target.parent().ok_or(LegacySettingsError::WriteFailed)?;
    fs::create_dir_all(parent).map_err(|_| LegacySettingsError::WriteFailed)?;
    let temporary = parent.join(format!(".agent.toml.legacy-import-{}", std::process::id()));
    let write_result = (|| {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(|_| LegacySettingsError::WriteFailed)?;
        file.write_all(content.as_bytes())
            .map_err(|_| LegacySettingsError::WriteFailed)?;
        file.sync_all()
            .map_err(|_| LegacySettingsError::WriteFailed)?;
        fs::hard_link(&temporary, target).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                LegacySettingsError::TargetExists
            } else {
                LegacySettingsError::WriteFailed
            }
        })?;
        fs::remove_file(&temporary).map_err(|_| LegacySettingsError::WriteFailed)
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    write_result
}

fn finding(field: &str, disposition: ImportDisposition, code: &str) -> ImportFinding {
    ImportFinding {
        field: String::from(field),
        disposition,
        code: String::from(code),
    }
}

fn is_secret_field(field: &str) -> bool {
    let normalized = field.to_ascii_lowercase();
    normalized.contains("api") && normalized.contains("key")
        || normalized.contains("token")
        || normalized.contains("passcode")
        || normalized.contains("password")
        || normalized.contains("secret")
}

#[cfg(test)]
mod tests {
    use super::{
        ImportDisposition, LegacySettingsError, inspect_legacy_settings, write_new_agent_config,
    };
    use std::{fs, path::PathBuf};

    fn temporary_directory(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "cmclient-legacy-settings-{name}-{}",
            std::process::id()
        ))
    }

    #[test]
    fn imports_only_the_safe_management_web_preference_without_exposing_secret_values() {
        let report = inspect_legacy_settings(include_bytes!(
            "../../../test/fixtures/legacy-settings-sanitized.json"
        ))
        .expect("fixture should parse");

        assert_eq!(
            report.proposed_agent_toml.as_deref(),
            Some("[agent]\nmanagement_web_enabled = false\n")
        );
        assert_eq!(report.imported.len(), 1);
        assert!(report.findings.iter().any(|finding| {
            finding.field == "shareWithTenmanMap"
                && finding.disposition == ImportDisposition::Removed
        }));
        let serialized = serde_json::to_string(&report).expect("report should serialize");
        assert!(!serialized.contains("fixture-secret-not-for-import"));
        assert!(serialized.contains("LEGACY_SETTINGS_SECRET_SKIPPED"));
    }

    #[test]
    fn accepts_the_electron_preferences_wrapper_without_trusting_unrecognized_values() {
        let report = inspect_legacy_settings(
            br#"{"preferences":{"webDashboardEnabled":true,"customSetting":"ignored"}}"#,
        )
        .expect("wrapped preferences should parse");

        assert_eq!(
            report.proposed_agent_toml.as_deref(),
            Some("[agent]\nmanagement_web_enabled = true\n")
        );
        assert!(report.findings.iter().any(|finding| {
            finding.field == "customSetting"
                && finding.disposition == ImportDisposition::UnknownSkipped
        }));
    }

    #[test]
    fn rejects_invalid_or_oversized_legacy_documents() {
        assert_eq!(
            inspect_legacy_settings(br#"[]"#),
            Err(LegacySettingsError::RootInvalid)
        );
        assert_eq!(
            inspect_legacy_settings(br#"{"webDashboardEnabled":"no"}"#)
                .expect("invalid preference is still reportable")
                .proposed_agent_toml,
            None
        );
        assert_eq!(
            inspect_legacy_settings(&vec![b'x'; super::MAX_LEGACY_SETTINGS_BYTES + 1]),
            Err(LegacySettingsError::SourceTooLarge)
        );
    }

    #[test]
    fn writes_only_a_new_absolute_agent_configuration() {
        let directory = temporary_directory("write");
        let _ = fs::remove_dir_all(&directory);
        let target = directory.join("config/agent.toml");
        let report = inspect_legacy_settings(br#"{"webDashboardEnabled":false}"#)
            .expect("fixture should parse");

        write_new_agent_config(&target, &report).expect("new config should be written");
        assert_eq!(
            fs::read_to_string(&target).expect("config should exist"),
            "[agent]\nmanagement_web_enabled = false\n"
        );
        assert_eq!(
            write_new_agent_config(&target, &report),
            Err(LegacySettingsError::TargetExists)
        );
        let _ = fs::remove_dir_all(directory);
    }
}
