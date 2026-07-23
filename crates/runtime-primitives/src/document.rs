use atomic_write_file::AtomicWriteFile;
use same_file::Handle;
use serde::{Serialize, de::DeserializeOwned};
use std::{
    fmt::{Display, Formatter},
    fs::{self, File},
    io::{Read, Write},
    marker::PhantomData,
    path::{Path, PathBuf},
};

const MAX_DOCUMENT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DocumentFormat {
    Json,
    Toml,
}

pub trait DurableDocument: Serialize + DeserializeOwned {
    const FORMAT: DocumentFormat;
    const MAX_BYTES: usize;

    fn validate(&self) -> bool;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DocumentError {
    PathInvalid,
    PolicyInvalid,
    ReadFailed,
    TooLarge,
    Malformed,
    SchemaInvalid,
    EncodeFailed,
    WriteFailed,
    CommitFailed,
    DiscardFailed,
}

impl DocumentError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::PathInvalid => "RUNTIME_DOCUMENT_PATH_INVALID",
            Self::PolicyInvalid => "RUNTIME_DOCUMENT_POLICY_INVALID",
            Self::ReadFailed => "RUNTIME_DOCUMENT_READ_FAILED",
            Self::TooLarge => "RUNTIME_DOCUMENT_TOO_LARGE",
            Self::Malformed => "RUNTIME_DOCUMENT_MALFORMED",
            Self::SchemaInvalid => "RUNTIME_DOCUMENT_SCHEMA_INVALID",
            Self::EncodeFailed => "RUNTIME_DOCUMENT_ENCODE_FAILED",
            Self::WriteFailed => "RUNTIME_DOCUMENT_WRITE_FAILED",
            Self::CommitFailed => "RUNTIME_DOCUMENT_COMMIT_FAILED",
            Self::DiscardFailed => "RUNTIME_DOCUMENT_DISCARD_FAILED",
        }
    }
}

impl Display for DocumentError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for DocumentError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedDocument<T> {
    path: PathBuf,
    marker: PhantomData<T>,
}

impl<T: DurableDocument> TypedDocument<T> {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, DocumentError> {
        validate_policy::<T>()?;
        let path = path.into();
        validate_path(&path)?;
        Ok(Self {
            path,
            marker: PhantomData,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load_optional(&self) -> Result<Option<T>, DocumentError> {
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(DocumentError::ReadFailed),
        };
        let metadata = file.metadata().map_err(|_| DocumentError::ReadFailed)?;
        if !metadata.is_file() {
            return Err(DocumentError::ReadFailed);
        }
        if metadata.len() > T::MAX_BYTES as u64 {
            return Err(DocumentError::TooLarge);
        }
        let read_limit = T::MAX_BYTES as u64 + 1;
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.take(read_limit)
            .read_to_end(&mut bytes)
            .map_err(|_| DocumentError::ReadFailed)?;
        if bytes.len() > T::MAX_BYTES {
            return Err(DocumentError::TooLarge);
        }
        let value = decode::<T>(&bytes)?;
        if !value.validate() {
            return Err(DocumentError::SchemaInvalid);
        }
        Ok(Some(value))
    }

    pub fn stage(&self, value: &T) -> Result<StagedDocumentWrite, DocumentError> {
        if !value.validate() {
            return Err(DocumentError::SchemaInvalid);
        }
        let bytes = encode(value)?;
        if bytes.len() > T::MAX_BYTES {
            return Err(DocumentError::TooLarge);
        }
        let parent = self.path.parent().ok_or(DocumentError::PathInvalid)?;
        fs::create_dir_all(parent).map_err(|_| DocumentError::WriteFailed)?;
        let mut file = AtomicWriteFile::open(&self.path).map_err(|_| DocumentError::WriteFailed)?;
        let owned_temporary_file = locate_temporary_file(parent, file.as_file())
            .map_err(|_| DocumentError::WriteFailed)?;
        file.write_all(&bytes)
            .map_err(|_| DocumentError::WriteFailed)?;
        Ok(StagedDocumentWrite {
            file,
            owned_temporary_file,
        })
    }

    pub fn store(&self, value: &T) -> Result<(), DocumentError> {
        self.stage(value)?.commit()
    }
}

pub struct StagedDocumentWrite {
    file: AtomicWriteFile,
    owned_temporary_file: Option<OwnedTemporaryFile>,
}

struct OwnedTemporaryFile {
    path: PathBuf,
    identity: Handle,
}

impl StagedDocumentWrite {
    pub fn commit(self) -> Result<(), DocumentError> {
        let Self {
            file,
            owned_temporary_file,
        } = self;
        if file.commit().is_ok() {
            return Ok(());
        }
        if let Some(temporary) = owned_temporary_file {
            let _ = remove_owned_temporary_file(&temporary.path, &temporary.identity);
        }
        Err(DocumentError::CommitFailed)
    }

    pub fn discard(self) -> Result<(), DocumentError> {
        let Self {
            file,
            owned_temporary_file,
        } = self;
        if file.discard().is_ok() {
            return Ok(());
        }
        if let Some(temporary) = owned_temporary_file {
            let _ = remove_owned_temporary_file(&temporary.path, &temporary.identity);
        }
        Err(DocumentError::DiscardFailed)
    }
}

fn locate_temporary_file(
    parent: &Path,
    file: &File,
) -> std::io::Result<Option<OwnedTemporaryFile>> {
    let identity = Handle::from_file(file.try_clone()?)?;
    let mut matched_path = None;
    for entry in fs::read_dir(parent)? {
        let path = entry?.path();
        let Ok(candidate) = Handle::from_path(&path) else {
            continue;
        };
        if candidate == identity {
            if matched_path.is_some() {
                return Err(std::io::Error::other(
                    "temporary file has more than one directory entry",
                ));
            }
            matched_path = Some(path);
        }
    }
    match matched_path {
        Some(path) => Ok(Some(OwnedTemporaryFile { path, identity })),
        None => missing_temporary_file_entry(identity),
    }
}

#[cfg(target_os = "linux")]
fn missing_temporary_file_entry(_identity: Handle) -> std::io::Result<Option<OwnedTemporaryFile>> {
    // atomic-write-file may use O_TMPFILE, whose inode intentionally has no directory entry.
    Ok(None)
}

#[cfg(not(target_os = "linux"))]
fn missing_temporary_file_entry(_identity: Handle) -> std::io::Result<Option<OwnedTemporaryFile>> {
    Err(std::io::Error::other(
        "temporary file directory entry was not found",
    ))
}

fn remove_owned_temporary_file(path: &Path, identity: &Handle) -> std::io::Result<()> {
    let current = match Handle::from_path(path) {
        Ok(current) => current,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if &current != identity {
        return Err(std::io::Error::other(
            "temporary file identity changed before cleanup",
        ));
    }
    fs::remove_file(path)
}

fn validate_policy<T: DurableDocument>() -> Result<(), DocumentError> {
    if T::MAX_BYTES == 0 || T::MAX_BYTES > MAX_DOCUMENT_BYTES {
        return Err(DocumentError::PolicyInvalid);
    }
    Ok(())
}

fn validate_path(path: &Path) -> Result<(), DocumentError> {
    if !path.is_absolute()
        || path.file_name().is_none()
        || path
            .parent()
            .is_none_or(|parent| parent.as_os_str().is_empty())
    {
        return Err(DocumentError::PathInvalid);
    }
    Ok(())
}

fn decode<T: DurableDocument>(bytes: &[u8]) -> Result<T, DocumentError> {
    match T::FORMAT {
        DocumentFormat::Json => serde_json::from_slice(bytes).map_err(|error| {
            use serde_json::error::Category;

            match error.classify() {
                Category::Syntax | Category::Eof => DocumentError::Malformed,
                Category::Data => DocumentError::SchemaInvalid,
                Category::Io => DocumentError::ReadFailed,
            }
        }),
        DocumentFormat::Toml => {
            let text = std::str::from_utf8(bytes).map_err(|_| DocumentError::Malformed)?;
            toml::from_str::<toml::Value>(text).map_err(|_| DocumentError::Malformed)?;
            toml::from_str(text).map_err(|_| DocumentError::SchemaInvalid)
        }
    }
}

fn encode<T: DurableDocument>(value: &T) -> Result<Vec<u8>, DocumentError> {
    match T::FORMAT {
        DocumentFormat::Json => serde_json::to_vec(value).map_err(|_| DocumentError::EncodeFailed),
        DocumentFormat::Toml => toml::to_string(value)
            .map(String::into_bytes)
            .map_err(|_| DocumentError::EncodeFailed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::{
        env,
        process::{self, Command},
        time::{SystemTime, UNIX_EPOCH},
    };

    const CRASH_CHILD_ENV: &str = "CMCLIENT_DOCUMENT_CRASH_CHILD";
    const CRASH_PATH_ENV: &str = "CMCLIENT_DOCUMENT_CRASH_PATH";
    const CRASH_MARKER_ENV: &str = "CMCLIENT_DOCUMENT_CRASH_MARKER";
    const CRASH_CHILD_TEST: &str = "document::tests::abrupt_exit_before_commit_child";
    const POST_COMMIT_CHILD_TEST: &str = "document::tests::abrupt_exit_after_commit_child";

    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    #[serde(deny_unknown_fields, rename_all = "camelCase")]
    struct JsonFixture {
        schema_version: u8,
        value: String,
    }

    impl DurableDocument for JsonFixture {
        const FORMAT: DocumentFormat = DocumentFormat::Json;
        const MAX_BYTES: usize = 256;

        fn validate(&self) -> bool {
            self.schema_version == 1 && !self.value.is_empty()
        }
    }

    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    #[serde(deny_unknown_fields, rename_all = "camelCase")]
    struct TomlFixture {
        schema_version: u8,
        enabled: bool,
    }

    impl DurableDocument for TomlFixture {
        const FORMAT: DocumentFormat = DocumentFormat::Toml;
        const MAX_BYTES: usize = 256;

        fn validate(&self) -> bool {
            self.schema_version == 1
        }
    }

    #[derive(Deserialize, Serialize)]
    struct InvalidPolicyFixture;

    impl DurableDocument for InvalidPolicyFixture {
        const FORMAT: DocumentFormat = DocumentFormat::Json;
        const MAX_BYTES: usize = 0;

        fn validate(&self) -> bool {
            true
        }
    }

    #[test]
    fn json_and_toml_documents_round_trip_strictly() {
        let directory = temporary_directory("round-trip");
        let json = TypedDocument::<JsonFixture>::new(directory.join("state.json")).unwrap();
        let json_value = JsonFixture {
            schema_version: 1,
            value: String::from("ready"),
        };
        json.store(&json_value).unwrap();
        assert_eq!(json.load_optional().unwrap(), Some(json_value));

        let toml = TypedDocument::<TomlFixture>::new(directory.join("config.toml")).unwrap();
        let toml_value = TomlFixture {
            schema_version: 1,
            enabled: true,
        };
        toml.store(&toml_value).unwrap();
        assert_eq!(toml.load_optional().unwrap(), Some(toml_value));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn missing_malformed_unknown_and_invalid_schema_are_distinct() {
        let directory = temporary_directory("malformed");
        let path = directory.join("state.json");
        let document = TypedDocument::<JsonFixture>::new(&path).unwrap();
        assert_eq!(document.load_optional(), Ok(None));

        fs::write(&path, b"{").unwrap();
        assert_eq!(document.load_optional(), Err(DocumentError::Malformed));
        fs::write(
            &path,
            br#"{"schemaVersion":1,"value":"ready","unknown":true}"#,
        )
        .unwrap();
        assert_eq!(document.load_optional(), Err(DocumentError::SchemaInvalid));
        fs::write(&path, br#"{"schemaVersion":2,"value":"ready"}"#).unwrap();
        assert_eq!(document.load_optional(), Err(DocumentError::SchemaInvalid));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn toml_syntax_and_typed_schema_failures_are_distinct() {
        let directory = temporary_directory("toml-errors");
        let path = directory.join("config.toml");
        let document = TypedDocument::<TomlFixture>::new(&path).unwrap();

        fs::write(&path, b"enabled = [").unwrap();
        assert_eq!(document.load_optional(), Err(DocumentError::Malformed));
        fs::write(&path, b"schemaVersion = 1\nenabled = true\nunknown = 1\n").unwrap();
        assert_eq!(document.load_optional(), Err(DocumentError::SchemaInvalid));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn policy_paths_and_payload_sizes_are_bounded() {
        assert_eq!(
            TypedDocument::<JsonFixture>::new(PathBuf::from("relative.json")),
            Err(DocumentError::PathInvalid)
        );
        assert!(matches!(
            TypedDocument::<InvalidPolicyFixture>::new(std::env::temp_dir().join("invalid.json")),
            Err(DocumentError::PolicyInvalid)
        ));

        let directory = temporary_directory("oversized");
        let path = directory.join("state.json");
        let document = TypedDocument::<JsonFixture>::new(&path).unwrap();
        assert_eq!(
            document.store(&JsonFixture {
                schema_version: 1,
                value: "x".repeat(512),
            }),
            Err(DocumentError::TooLarge)
        );
        fs::write(&path, vec![b'x'; JsonFixture::MAX_BYTES + 1]).unwrap();
        assert_eq!(document.load_optional(), Err(DocumentError::TooLarge));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn dropping_or_discarding_a_stage_preserves_the_old_document() {
        let directory = temporary_directory("discard");
        let document = TypedDocument::<JsonFixture>::new(directory.join("state.json")).unwrap();
        let old = JsonFixture {
            schema_version: 1,
            value: String::from("old"),
        };
        let new = JsonFixture {
            schema_version: 1,
            value: String::from("new"),
        };
        document.store(&old).unwrap();
        drop(document.stage(&new).unwrap());
        assert_eq!(document.load_optional().unwrap(), Some(old.clone()));
        document.stage(&new).unwrap().discard().unwrap();
        assert_eq!(document.load_optional().unwrap(), Some(old));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn failed_replace_removes_only_the_owned_temporary_file() {
        let directory = temporary_directory("replace-failure");
        let target = directory.join("state.json");
        let unrelated = directory.join("unrelated.tmp");
        fs::create_dir(&target).unwrap();
        fs::write(&unrelated, b"keep").unwrap();
        let before = directory_entries(&directory);
        let document = TypedDocument::<JsonFixture>::new(&target).unwrap();
        let staged = document
            .stage(&JsonFixture {
                schema_version: 1,
                value: String::from("new"),
            })
            .unwrap();

        assert_eq!(staged.commit(), Err(DocumentError::CommitFailed));
        assert_eq!(directory_entries(&directory), before);
        assert_eq!(fs::read(unrelated).unwrap(), b"keep");
        fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn anonymous_linux_temporary_file_without_a_directory_entry_is_accepted() {
        let directory = temporary_directory("anonymous-temp");
        let path = directory.join("unlinked.tmp");
        let file = File::create(&path).unwrap();
        fs::remove_file(path).unwrap();

        assert!(locate_temporary_file(&directory, &file).unwrap().is_none());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn abrupt_child_exit_before_commit_preserves_the_old_document() {
        let directory = temporary_directory("crash");
        let path = directory.join("state.json");
        let marker = directory.join("staged.marker");
        let document = TypedDocument::<JsonFixture>::new(&path).unwrap();
        let old = JsonFixture {
            schema_version: 1,
            value: String::from("old"),
        };
        document.store(&old).unwrap();

        let status = Command::new(env::current_exe().unwrap())
            .args(["--ignored", "--exact", CRASH_CHILD_TEST, "--nocapture"])
            .env(CRASH_CHILD_ENV, "1")
            .env(CRASH_PATH_ENV, &path)
            .env(CRASH_MARKER_ENV, &marker)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(91));
        assert!(marker.is_file());
        assert_eq!(document.load_optional().unwrap(), Some(old));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn abrupt_child_exit_after_commit_publishes_the_new_document() {
        let directory = temporary_directory("post-commit-crash");
        let path = directory.join("state.json");
        let marker = directory.join("committed.marker");
        let document = TypedDocument::<JsonFixture>::new(&path).unwrap();
        document
            .store(&JsonFixture {
                schema_version: 1,
                value: String::from("old"),
            })
            .unwrap();

        let status = Command::new(env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                POST_COMMIT_CHILD_TEST,
                "--nocapture",
            ])
            .env(CRASH_CHILD_ENV, "1")
            .env(CRASH_PATH_ENV, &path)
            .env(CRASH_MARKER_ENV, &marker)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(92));
        assert!(marker.is_file());
        assert_eq!(
            document.load_optional().unwrap(),
            Some(JsonFixture {
                schema_version: 1,
                value: String::from("new"),
            })
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    #[ignore = "invoked as the abrupt-exit fixture by its parent test"]
    fn abrupt_exit_before_commit_child() {
        if env::var_os(CRASH_CHILD_ENV).is_none() {
            return;
        }
        let path = PathBuf::from(env::var_os(CRASH_PATH_ENV).unwrap());
        let marker = PathBuf::from(env::var_os(CRASH_MARKER_ENV).unwrap());
        let document = TypedDocument::<JsonFixture>::new(path).unwrap();
        let staged = document
            .stage(&JsonFixture {
                schema_version: 1,
                value: String::from("new"),
            })
            .unwrap();
        fs::write(marker, b"staged").unwrap();
        std::mem::forget(staged);
        process::exit(91);
    }

    #[test]
    #[ignore = "invoked as the post-commit abrupt-exit fixture by its parent test"]
    fn abrupt_exit_after_commit_child() {
        if env::var_os(CRASH_CHILD_ENV).is_none() {
            return;
        }
        let path = PathBuf::from(env::var_os(CRASH_PATH_ENV).unwrap());
        let marker = PathBuf::from(env::var_os(CRASH_MARKER_ENV).unwrap());
        let document = TypedDocument::<JsonFixture>::new(path).unwrap();
        document
            .store(&JsonFixture {
                schema_version: 1,
                value: String::from("new"),
            })
            .unwrap();
        fs::write(marker, b"committed").unwrap();
        process::exit(92);
    }

    #[test]
    fn errors_display_only_stable_codes() {
        for error in [
            DocumentError::PathInvalid,
            DocumentError::PolicyInvalid,
            DocumentError::ReadFailed,
            DocumentError::TooLarge,
            DocumentError::Malformed,
            DocumentError::SchemaInvalid,
            DocumentError::EncodeFailed,
            DocumentError::WriteFailed,
            DocumentError::CommitFailed,
            DocumentError::DiscardFailed,
        ] {
            assert_eq!(error.to_string(), error.code());
        }
    }

    fn temporary_directory(label: &str) -> PathBuf {
        let sequence = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = env::temp_dir().join(format!(
            "cmclient-runtime-document-{label}-{}-{sequence}",
            process::id()
        ));
        fs::create_dir(&directory).unwrap();
        directory
    }

    fn directory_entries(directory: &Path) -> Vec<PathBuf> {
        let mut entries = fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        entries.sort();
        entries
    }
}
