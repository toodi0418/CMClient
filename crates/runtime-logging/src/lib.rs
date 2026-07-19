//! Bounded structured runtime logging for supervised CMClient processes.

use serde_json::{Map, Value};
use std::{
    ffi::{OsStr, OsString},
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
    time::{SystemTime, UNIX_EPOCH},
};
use zeroize::{Zeroize, Zeroizing};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
#[cfg(windows)]
use std::os::windows::fs::{MetadataExt, OpenOptionsExt};

pub const ENV_LOG_MAX_BYTES: &str = "CMCLIENT_LOG_MAX_BYTES";
pub const ENV_LOG_RETAINED_FILES: &str = "CMCLIENT_LOG_RETAINED_FILES";
pub const ENV_LOG_MAX_LINE_BYTES: &str = "CMCLIENT_LOG_MAX_LINE_BYTES";

pub const DEFAULT_LOG_MAX_BYTES: u64 = 10 * 1024 * 1024;
pub const DEFAULT_LOG_RETAINED_FILES: usize = 5;
pub const DEFAULT_LOG_MAX_LINE_BYTES: usize = 64 * 1024;

pub const MIN_LOG_MAX_BYTES: u64 = 128 * 1024;
pub const MAX_LOG_MAX_BYTES: u64 = 64 * 1024 * 1024;
pub const MIN_LOG_RETAINED_FILES: usize = 1;
pub const MAX_LOG_RETAINED_FILES: usize = 16;
pub const MIN_LOG_MAX_LINE_BYTES: usize = 256;
pub const MAX_LOG_MAX_LINE_BYTES: usize = 1024 * 1024;

const CAPTURE_QUEUE_CAPACITY: usize = 64;
const MAX_COMPONENT_BYTES: usize = 64;
const MAX_EVENT_CODE_BYTES: usize = 128;
const MAX_IDENTIFIER_BYTES: usize = 128;
const MAX_REDACTION_DEPTH: usize = 32;
const REDACTED: &str = "[REDACTED]";
#[cfg(windows)]
const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
#[cfg(windows)]
const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogPolicy {
    pub max_bytes: u64,
    pub retained_files: usize,
    pub max_line_bytes: usize,
}

impl Default for LogPolicy {
    fn default() -> Self {
        Self {
            max_bytes: DEFAULT_LOG_MAX_BYTES,
            retained_files: DEFAULT_LOG_RETAINED_FILES,
            max_line_bytes: DEFAULT_LOG_MAX_LINE_BYTES,
        }
    }
}

impl LogPolicy {
    pub fn from_environment() -> Result<Self, RuntimeLogError> {
        Self::from_lookup(|name| std::env::var_os(name))
    }

    fn from_lookup(
        mut lookup: impl FnMut(&str) -> Option<OsString>,
    ) -> Result<Self, RuntimeLogError> {
        let defaults = Self::default();
        let policy = Self {
            max_bytes: parse_environment_number(lookup(ENV_LOG_MAX_BYTES), defaults.max_bytes)?,
            retained_files: parse_environment_number(
                lookup(ENV_LOG_RETAINED_FILES),
                defaults.retained_files,
            )?,
            max_line_bytes: parse_environment_number(
                lookup(ENV_LOG_MAX_LINE_BYTES),
                defaults.max_line_bytes,
            )?,
        };
        policy.validate()?;
        Ok(policy)
    }

    fn validate(self) -> Result<(), RuntimeLogError> {
        if !(MIN_LOG_MAX_BYTES..=MAX_LOG_MAX_BYTES).contains(&self.max_bytes)
            || !(MIN_LOG_RETAINED_FILES..=MAX_LOG_RETAINED_FILES).contains(&self.retained_files)
            || !(MIN_LOG_MAX_LINE_BYTES..=MAX_LOG_MAX_LINE_BYTES).contains(&self.max_line_bytes)
            || u64::try_from(self.max_line_bytes)
                .ok()
                .is_none_or(|line_bytes| line_bytes > self.max_bytes / 2)
        {
            return Err(RuntimeLogError::PolicyInvalid);
        }
        Ok(())
    }
}

fn parse_environment_number<T>(value: Option<OsString>, default: T) -> Result<T, RuntimeLogError>
where
    T: std::str::FromStr,
{
    let Some(value) = value else {
        return Ok(default);
    };
    let value = value
        .into_string()
        .map_err(|_| RuntimeLogError::PolicyInvalid)?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(RuntimeLogError::PolicyInvalid);
    }
    value
        .parse::<T>()
        .map_err(|_| RuntimeLogError::PolicyInvalid)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeLogError {
    PolicyInvalid,
    PathInvalid,
    DirectoryUnavailable,
    FileUnavailable,
    WriteFailed,
    StateUnavailable,
    CaptureReadFailed,
    CaptureThreadFailed,
    QueueFull,
    EventInvalid,
}

impl RuntimeLogError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::PolicyInvalid => "RUNTIME_LOG_POLICY_INVALID",
            Self::PathInvalid => "RUNTIME_LOG_PATH_INVALID",
            Self::DirectoryUnavailable => "RUNTIME_LOG_DIRECTORY_UNAVAILABLE",
            Self::FileUnavailable => "RUNTIME_LOG_FILE_UNAVAILABLE",
            Self::WriteFailed => "RUNTIME_LOG_WRITE_FAILED",
            Self::StateUnavailable => "RUNTIME_LOG_STATE_UNAVAILABLE",
            Self::CaptureReadFailed => "RUNTIME_LOG_CAPTURE_READ_FAILED",
            Self::CaptureThreadFailed => "RUNTIME_LOG_CAPTURE_THREAD_FAILED",
            Self::QueueFull => "RUNTIME_LOG_QUEUE_FULL",
            Self::EventInvalid => "RUNTIME_LOG_EVENT_INVALID",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

#[derive(Clone)]
pub struct StructuredLogSink {
    shared: Arc<SharedSink>,
}

struct SharedSink {
    state: Mutex<SinkState>,
    error: Mutex<Option<RuntimeLogError>>,
    component: String,
    policy: LogPolicy,
}

struct SinkState {
    directory: PathBuf,
    active_path: PathBuf,
    file_name: String,
    file: Option<File>,
    current_bytes: u64,
    policy: LogPolicy,
}

impl StructuredLogSink {
    pub fn open(
        log_dir: impl AsRef<Path>,
        file_name: &str,
        component: &str,
        policy: LogPolicy,
    ) -> Result<Self, RuntimeLogError> {
        policy.validate()?;
        validate_file_name(file_name)?;
        validate_component(component)?;

        let directory = log_dir.as_ref().to_path_buf();
        prepare_directory(&directory)?;
        prune_rotated_files(&directory, file_name, policy.retained_files)?;
        let active_path = directory.join(file_name);
        let (file, current_bytes) = open_active_file(&active_path)?;
        Ok(Self {
            shared: Arc::new(SharedSink {
                state: Mutex::new(SinkState {
                    directory,
                    active_path,
                    file_name: String::from(file_name),
                    file: Some(file),
                    current_bytes,
                    policy,
                }),
                error: Mutex::new(None),
                component: String::from(component),
                policy,
            }),
        })
    }

    pub fn write_code(&self, level: LogLevel, event_code: &str) -> Result<(), RuntimeLogError> {
        self.write_event(level, event_code, None, &[])
    }

    pub fn write_event(
        &self,
        level: LogLevel,
        event_code: &str,
        fields: Option<Map<String, Value>>,
        secrets: &[String],
    ) -> Result<(), RuntimeLogError> {
        if !is_stable_code(event_code) {
            return Err(RuntimeLogError::EventInvalid);
        }
        let secrets = secret_needles(secrets.iter().cloned());
        let mut record = base_record(&self.shared.component, "runtime", level, event_code);
        if let Some(fields) = fields {
            record.insert(
                String::from("fields"),
                sanitize_value(Value::Object(fields), &secrets, 0),
            );
        }
        self.write_value_or_generic(Value::Object(record), "RUNTIME_LOG_EVENT_OVERSIZED")
    }

    pub fn capture<Stdout, Stderr, Secrets>(
        &self,
        stdout: Stdout,
        stderr: Stderr,
        secrets: Secrets,
    ) -> Result<ChildOutputCapture, RuntimeLogError>
    where
        Stdout: Read + Send + 'static,
        Stderr: Read + Send + 'static,
        Secrets: IntoIterator<Item = String>,
    {
        let secrets = Arc::new(secret_needles(secrets));
        let (record_sender, record_receiver) = mpsc::sync_channel(CAPTURE_QUEUE_CAPACITY);
        let mut reader_handles = Vec::with_capacity(2);
        let mut starters = Vec::with_capacity(3);

        let (writer_start, writer_ready) = mpsc::channel();
        let writer_sink = self.clone();
        let writer_handle = match thread::Builder::new()
            .name(String::from("cmclient-log-writer"))
            .spawn(move || {
                if writer_ready.recv().is_ok() {
                    capture_writer(record_receiver, &writer_sink);
                }
            }) {
            Ok(handle) => {
                starters.push(writer_start);
                handle
            }
            Err(_) => {
                self.latch_error(RuntimeLogError::CaptureThreadFailed);
                return Err(RuntimeLogError::CaptureThreadFailed);
            }
        };

        if let Err(error) = spawn_reader(
            "cmclient-log-stdout",
            stdout,
            CaptureStream::Stdout,
            record_sender.clone(),
            self.clone(),
            Arc::clone(&secrets),
            &mut reader_handles,
            &mut starters,
        ) {
            drop(record_sender);
            abort_and_join(starters, reader_handles, writer_handle, self);
            return Err(error);
        }
        if let Err(error) = spawn_reader(
            "cmclient-log-stderr",
            stderr,
            CaptureStream::Stderr,
            record_sender,
            self.clone(),
            secrets,
            &mut reader_handles,
            &mut starters,
        ) {
            abort_and_join(starters, reader_handles, writer_handle, self);
            return Err(error);
        }

        for starter in starters {
            if starter.send(()).is_err() {
                self.latch_error(RuntimeLogError::CaptureThreadFailed);
            }
        }
        Ok(ChildOutputCapture {
            sink: self.clone(),
            reader_handles,
            writer_handle: Some(writer_handle),
            finished: false,
        })
    }

    pub fn take_error_code(&self) -> Option<&'static str> {
        match self.shared.error.lock() {
            Ok(mut error) => error.take().map(RuntimeLogError::code),
            Err(_) => Some(RuntimeLogError::StateUnavailable.code()),
        }
    }

    fn write_value_or_generic(
        &self,
        value: Value,
        oversized_code: &'static str,
    ) -> Result<(), RuntimeLogError> {
        let serialized = serde_json::to_vec(&value).map_err(|_| {
            self.latch_error(RuntimeLogError::WriteFailed);
            RuntimeLogError::WriteFailed
        })?;
        if serialized.len() <= self.shared.policy.max_line_bytes {
            return self.write_serialized(&serialized);
        }
        self.write_generic("runtime", LogLevel::Warn, oversized_code)
    }

    fn write_generic(
        &self,
        stream: &'static str,
        level: LogLevel,
        code: &'static str,
    ) -> Result<(), RuntimeLogError> {
        let record = Value::Object(base_record(&self.shared.component, stream, level, code));
        let serialized = serde_json::to_vec(&record).map_err(|_| {
            self.latch_error(RuntimeLogError::WriteFailed);
            RuntimeLogError::WriteFailed
        })?;
        self.write_serialized(&serialized)
    }

    fn write_serialized(&self, serialized: &[u8]) -> Result<(), RuntimeLogError> {
        let result = self
            .shared
            .state
            .lock()
            .map_err(|_| RuntimeLogError::StateUnavailable)
            .and_then(|mut state| state.write_line(serialized));
        if let Err(error) = result {
            self.latch_error(error);
        }
        result
    }

    fn flush(&self) -> Result<(), RuntimeLogError> {
        let result = self
            .shared
            .state
            .lock()
            .map_err(|_| RuntimeLogError::StateUnavailable)
            .and_then(|mut state| state.flush());
        if let Err(error) = result {
            self.latch_error(error);
        }
        result
    }

    fn latch_error(&self, error: RuntimeLogError) {
        if let Ok(mut current) = self.shared.error.lock()
            && current.is_none()
        {
            *current = Some(error);
        }
    }

    fn latched_error(&self) -> Option<RuntimeLogError> {
        self.shared.error.lock().ok().and_then(|error| *error)
    }
}

pub struct ChildOutputCapture {
    sink: StructuredLogSink,
    reader_handles: Vec<JoinHandle<()>>,
    writer_handle: Option<JoinHandle<()>>,
    finished: bool,
}

impl ChildOutputCapture {
    pub fn finish(mut self) -> Result<(), RuntimeLogError> {
        self.finish_inner()
    }

    fn finish_inner(&mut self) -> Result<(), RuntimeLogError> {
        if self.finished {
            return self.sink.latched_error().map_or(Ok(()), Err);
        }
        self.finished = true;
        for handle in self.reader_handles.drain(..) {
            if handle.join().is_err() {
                self.sink.latch_error(RuntimeLogError::CaptureThreadFailed);
            }
        }
        if self
            .writer_handle
            .take()
            .is_some_and(|handle| handle.join().is_err())
        {
            self.sink.latch_error(RuntimeLogError::CaptureThreadFailed);
        }
        let flush_result = self.sink.flush();
        if let Some(error) = self.sink.latched_error() {
            return Err(error);
        }
        flush_result
    }
}

impl Drop for ChildOutputCapture {
    fn drop(&mut self) {
        let _ = self.finish_inner();
    }
}

impl SinkState {
    fn write_line(&mut self, serialized: &[u8]) -> Result<(), RuntimeLogError> {
        if serialized.len() > self.policy.max_line_bytes {
            return Err(RuntimeLogError::WriteFailed);
        }
        let record_bytes = u64::try_from(serialized.len())
            .ok()
            .and_then(|bytes| bytes.checked_add(1))
            .ok_or(RuntimeLogError::WriteFailed)?;
        if record_bytes > self.policy.max_bytes {
            return Err(RuntimeLogError::WriteFailed);
        }
        if self.current_bytes.saturating_add(record_bytes) > self.policy.max_bytes {
            self.rotate()?;
        }
        let file = self.file.as_mut().ok_or(RuntimeLogError::FileUnavailable)?;
        file.write_all(serialized)
            .and_then(|()| file.write_all(b"\n"))
            .and_then(|()| file.flush())
            .map_err(|_| RuntimeLogError::WriteFailed)?;
        self.current_bytes = self.current_bytes.saturating_add(record_bytes);
        Ok(())
    }

    fn flush(&mut self) -> Result<(), RuntimeLogError> {
        self.file
            .as_mut()
            .ok_or(RuntimeLogError::FileUnavailable)?
            .flush()
            .map_err(|_| RuntimeLogError::WriteFailed)
    }

    fn rotate(&mut self) -> Result<(), RuntimeLogError> {
        if let Some(mut file) = self.file.take() {
            file.flush().map_err(|_| RuntimeLogError::WriteFailed)?;
        }
        prune_rotated_files(&self.directory, &self.file_name, self.policy.retained_files)?;
        for index in (1..=self.policy.retained_files).rev() {
            let source = if index == 1 {
                self.active_path.clone()
            } else {
                rotated_path(&self.directory, &self.file_name, index - 1)
            };
            let destination = rotated_path(&self.directory, &self.file_name, index);
            if path_exists(&destination)? {
                validate_regular_file(&destination)?;
                fs::remove_file(&destination).map_err(|_| RuntimeLogError::FileUnavailable)?;
            }
            if path_exists(&source)? {
                validate_regular_file(&source)?;
                fs::rename(&source, &destination).map_err(|_| RuntimeLogError::FileUnavailable)?;
            }
        }
        let (file, current_bytes) = open_active_file(&self.active_path)?;
        self.file = Some(file);
        self.current_bytes = current_bytes;
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum CaptureStream {
    Stdout,
    Stderr,
}

struct CaptureRecord(Vec<u8>);

#[allow(clippy::too_many_arguments)]
fn spawn_reader<Reader: Read + Send + 'static>(
    thread_name: &str,
    reader: Reader,
    stream: CaptureStream,
    sender: SyncSender<CaptureRecord>,
    sink: StructuredLogSink,
    secrets: Arc<Vec<Zeroizing<String>>>,
    handles: &mut Vec<JoinHandle<()>>,
    starters: &mut Vec<mpsc::Sender<()>>,
) -> Result<(), RuntimeLogError> {
    let (starter, ready) = mpsc::channel();
    let thread_sink = sink.clone();
    let handle = thread::Builder::new()
        .name(String::from(thread_name))
        .spawn(move || {
            if ready.recv().is_ok() {
                drain_capture(reader, stream, sender, &thread_sink, &secrets);
            }
        })
        .map_err(|_| {
            sink.latch_error(RuntimeLogError::CaptureThreadFailed);
            RuntimeLogError::CaptureThreadFailed
        })?;
    handles.push(handle);
    starters.push(starter);
    Ok(())
}

fn abort_and_join(
    starters: Vec<mpsc::Sender<()>>,
    reader_handles: Vec<JoinHandle<()>>,
    writer_handle: JoinHandle<()>,
    sink: &StructuredLogSink,
) {
    drop(starters);
    for handle in reader_handles {
        if handle.join().is_err() {
            sink.latch_error(RuntimeLogError::CaptureThreadFailed);
        }
    }
    if writer_handle.join().is_err() {
        sink.latch_error(RuntimeLogError::CaptureThreadFailed);
    }
}

fn drain_capture<Reader: Read>(
    mut reader: Reader,
    stream: CaptureStream,
    sender: SyncSender<CaptureRecord>,
    sink: &StructuredLogSink,
    secrets: &[Zeroizing<String>],
) {
    let mut input = [0_u8; 4096];
    let mut line = Vec::with_capacity(sink.shared.policy.max_line_bytes.min(input.len()));
    let mut oversized = false;
    let mut sender_connected = true;
    loop {
        match reader.read(&mut input) {
            Ok(0) => break,
            Ok(read_bytes) => {
                for byte in &input[..read_bytes] {
                    if *byte == b'\n' {
                        emit_capture_line(
                            stream,
                            &mut line,
                            oversized,
                            &sender,
                            sink,
                            secrets,
                            &mut sender_connected,
                        );
                        oversized = false;
                    } else if !oversized {
                        if line.len() < sink.shared.policy.max_line_bytes {
                            line.push(*byte);
                        } else {
                            line.zeroize();
                            line.clear();
                            oversized = true;
                        }
                    }
                }
                input[..read_bytes].zeroize();
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => {
                sink.latch_error(RuntimeLogError::CaptureReadFailed);
                break;
            }
        }
    }
    if oversized || !line.is_empty() {
        emit_capture_line(
            stream,
            &mut line,
            oversized,
            &sender,
            sink,
            secrets,
            &mut sender_connected,
        );
    }
    line.zeroize();
    input.zeroize();
}

#[allow(clippy::too_many_arguments)]
fn emit_capture_line(
    stream: CaptureStream,
    line: &mut Vec<u8>,
    oversized: bool,
    sender: &SyncSender<CaptureRecord>,
    sink: &StructuredLogSink,
    secrets: &[Zeroizing<String>],
    sender_connected: &mut bool,
) {
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    let prepared = prepare_capture_record(stream, line, oversized, sink, secrets);
    line.zeroize();
    line.clear();
    let record = match prepared {
        Ok(record) => record,
        Err(error) => {
            sink.latch_error(error);
            return;
        }
    };
    if !*sender_connected {
        return;
    }
    match sender.try_send(CaptureRecord(record)) {
        Ok(()) => {}
        Err(TrySendError::Full(mut record)) => {
            record.0.zeroize();
            sink.latch_error(RuntimeLogError::QueueFull);
        }
        Err(TrySendError::Disconnected(mut record)) => {
            record.0.zeroize();
            *sender_connected = false;
            sink.latch_error(RuntimeLogError::CaptureThreadFailed);
        }
    }
}

fn capture_writer(receiver: Receiver<CaptureRecord>, sink: &StructuredLogSink) {
    while let Ok(mut record) = receiver.recv() {
        let _ = sink.write_serialized(&record.0);
        record.0.zeroize();
    }
    let _ = sink.flush();
}

fn prepare_capture_record(
    stream: CaptureStream,
    line: &[u8],
    oversized: bool,
    sink: &StructuredLogSink,
    secrets: &[Zeroizing<String>],
) -> Result<Vec<u8>, RuntimeLogError> {
    let record = match stream {
        CaptureStream::Stdout if oversized => base_record(
            &sink.shared.component,
            "stdout",
            LogLevel::Warn,
            "RUNTIME_LOG_STDOUT_OVERSIZED",
        ),
        CaptureStream::Stdout => sanitize_stdout_record(line, &sink.shared.component, secrets)
            .unwrap_or_else(|| {
                base_record(
                    &sink.shared.component,
                    "stdout",
                    LogLevel::Warn,
                    "RUNTIME_LOG_STDOUT_INVALID",
                )
            }),
        CaptureStream::Stderr if oversized => base_record(
            &sink.shared.component,
            "stderr",
            LogLevel::Error,
            "RUNTIME_LOG_STDERR_OVERSIZED",
        ),
        CaptureStream::Stderr => {
            let code = std::str::from_utf8(line)
                .ok()
                .filter(|code| is_stable_code(code))
                .unwrap_or("RUNTIME_LOG_STDERR_INVALID");
            base_record(&sink.shared.component, "stderr", LogLevel::Error, code)
        }
    };
    let serialized =
        serde_json::to_vec(&Value::Object(record)).map_err(|_| RuntimeLogError::WriteFailed)?;
    if serialized.len() <= sink.shared.policy.max_line_bytes {
        Ok(serialized)
    } else {
        serde_json::to_vec(&Value::Object(base_record(
            &sink.shared.component,
            match stream {
                CaptureStream::Stdout => "stdout",
                CaptureStream::Stderr => "stderr",
            },
            LogLevel::Warn,
            "RUNTIME_LOG_RECORD_OVERSIZED",
        )))
        .map_err(|_| RuntimeLogError::WriteFailed)
    }
}

fn sanitize_stdout_record(
    line: &[u8],
    component: &str,
    secrets: &[Zeroizing<String>],
) -> Option<Map<String, Value>> {
    let Value::Object(mut input) = serde_json::from_slice::<Value>(line).ok()? else {
        return None;
    };
    const ALLOWED_KEYS: [&str; 5] = ["level", "message", "traceId", "correlationId", "fields"];
    if input
        .keys()
        .any(|key| !ALLOWED_KEYS.contains(&key.as_str()))
    {
        return None;
    }
    let level = input.remove("level")?.as_str()?.to_owned();
    if !matches!(level.as_str(), "debug" | "info" | "warn" | "error") {
        return None;
    }
    let message = input.remove("message")?.as_str()?.to_owned();
    if message.is_empty() {
        return None;
    }
    let trace_id = input.remove("traceId")?.as_str()?.to_owned();
    if !is_identifier(&trace_id) {
        return None;
    }
    let correlation_id = match input.remove("correlationId") {
        Some(Value::String(value)) if is_identifier(&value) => Some(value),
        Some(_) => return None,
        None => None,
    };
    let fields = match input.remove("fields") {
        Some(Value::Object(fields)) => Some(fields),
        Some(_) => return None,
        None => None,
    };

    let mut record = base_record_with_level(component, "stdout", &level, &message, secrets);
    record.insert(
        String::from("traceId"),
        Value::String(redact_needles(&trace_id, secrets)),
    );
    if let Some(correlation_id) = correlation_id {
        record.insert(
            String::from("correlationId"),
            Value::String(redact_needles(&correlation_id, secrets)),
        );
    }
    if let Some(fields) = fields {
        record.insert(
            String::from("fields"),
            sanitize_value(Value::Object(fields), secrets, 0),
        );
    }
    Some(record)
}

fn base_record(
    component: &str,
    stream: &'static str,
    level: LogLevel,
    message: &str,
) -> Map<String, Value> {
    base_record_with_level(component, stream, level.as_str(), message, &[])
}

fn base_record_with_level(
    component: &str,
    stream: &'static str,
    level: &str,
    message: &str,
    secrets: &[Zeroizing<String>],
) -> Map<String, Value> {
    Map::from_iter([
        (String::from("schemaVersion"), Value::from(1)),
        (
            String::from("timestampMs"),
            Value::from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |duration| duration.as_millis() as u64),
            ),
        ),
        (
            String::from("component"),
            Value::String(String::from(component)),
        ),
        (String::from("stream"), Value::String(String::from(stream))),
        (String::from("level"), Value::String(String::from(level))),
        (
            String::from("message"),
            Value::String(redact_needles(message, secrets)),
        ),
    ])
}

fn sanitize_value(value: Value, secrets: &[Zeroizing<String>], depth: usize) -> Value {
    if depth >= MAX_REDACTION_DEPTH {
        return Value::String(String::from(REDACTED));
    }
    match value {
        Value::Object(values) => {
            Value::Object(Map::from_iter(values.into_iter().map(|(key, value)| {
                let sensitive = is_sensitive_key(&key);
                let key = redact_needles(&key, secrets);
                let value = if sensitive {
                    Value::String(String::from(REDACTED))
                } else {
                    sanitize_value(value, secrets, depth + 1)
                };
                (key, value)
            })))
        }
        Value::Array(values) => Value::Array(
            values
                .into_iter()
                .map(|value| sanitize_value(value, secrets, depth + 1))
                .collect(),
        ),
        Value::String(value) => Value::String(redact_needles(&value, secrets)),
        value => value,
    }
}

fn secret_needles(secrets: impl IntoIterator<Item = String>) -> Vec<Zeroizing<String>> {
    secrets
        .into_iter()
        .filter(|secret| !secret.is_empty())
        .map(Zeroizing::new)
        .collect()
}

fn redact_needles(value: &str, secrets: &[Zeroizing<String>]) -> String {
    secrets
        .iter()
        .fold(String::from(value), |redacted, secret| {
            redacted.replace(secret.as_str(), REDACTED)
        })
}

fn is_sensitive_key(key: &str) -> bool {
    let normalized: String = key
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

fn is_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTIFIER_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
}

fn is_stable_code(value: &str) -> bool {
    if value.is_empty()
        || value.len() > MAX_EVENT_CODE_BYTES
        || !value.contains('_')
        || !value.as_bytes()[0].is_ascii_uppercase()
    {
        return false;
    }
    let mut previous_underscore = false;
    for (index, byte) in value.bytes().enumerate() {
        match byte {
            b'A'..=b'Z' | b'0'..=b'9' => previous_underscore = false,
            b'_' if index > 0 && index + 1 < value.len() && !previous_underscore => {
                previous_underscore = true;
            }
            _ => return false,
        }
    }
    true
}

fn validate_component(component: &str) -> Result<(), RuntimeLogError> {
    if component.is_empty()
        || component.len() > MAX_COMPONENT_BYTES
        || !component
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
    {
        return Err(RuntimeLogError::PathInvalid);
    }
    Ok(())
}

fn validate_file_name(file_name: &str) -> Result<(), RuntimeLogError> {
    if file_name.is_empty()
        || file_name == "."
        || file_name == ".."
        || file_name.contains(['/', '\\'])
        || Path::new(file_name).file_name() != Some(OsStr::new(file_name))
    {
        return Err(RuntimeLogError::PathInvalid);
    }
    Ok(())
}

fn prepare_directory(directory: &Path) -> Result<(), RuntimeLogError> {
    match fs::symlink_metadata(directory) {
        Ok(metadata) if metadata_is_link_like(&metadata) || !metadata.is_dir() => {
            return Err(RuntimeLogError::DirectoryUnavailable);
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(directory).map_err(|_| RuntimeLogError::DirectoryUnavailable)?;
        }
        Err(_) => return Err(RuntimeLogError::DirectoryUnavailable),
    }
    let metadata =
        fs::symlink_metadata(directory).map_err(|_| RuntimeLogError::DirectoryUnavailable)?;
    if metadata_is_link_like(&metadata) || !metadata.is_dir() {
        return Err(RuntimeLogError::DirectoryUnavailable);
    }
    #[cfg(unix)]
    fs::set_permissions(directory, fs::Permissions::from_mode(0o700))
        .map_err(|_| RuntimeLogError::DirectoryUnavailable)?;
    Ok(())
}

fn open_active_file(path: &Path) -> Result<(File, u64), RuntimeLogError> {
    if path_exists(path)? {
        validate_regular_file(path)?;
    }
    let mut options = OpenOptions::new();
    options.create(true).append(true).read(true);
    #[cfg(unix)]
    {
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    let file = options
        .open(path)
        .map_err(|_| RuntimeLogError::FileUnavailable)?;
    let metadata = file
        .metadata()
        .map_err(|_| RuntimeLogError::FileUnavailable)?;
    if metadata_is_link_like(&metadata) || !metadata.is_file() {
        return Err(RuntimeLogError::FileUnavailable);
    }
    #[cfg(unix)]
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|_| RuntimeLogError::FileUnavailable)?;
    Ok((file, metadata.len()))
}

fn prune_rotated_files(
    directory: &Path,
    file_name: &str,
    retained_files: usize,
) -> Result<(), RuntimeLogError> {
    let prefix = format!("{file_name}.");
    let entries = fs::read_dir(directory).map_err(|_| RuntimeLogError::DirectoryUnavailable)?;
    let mut stale = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|_| RuntimeLogError::DirectoryUnavailable)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(suffix) = name.strip_prefix(&prefix) else {
            continue;
        };
        if suffix.is_empty() || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
            continue;
        }
        let Ok(index) = suffix.parse::<usize>() else {
            continue;
        };
        if index == 0 {
            continue;
        }
        secure_regular_file(&entry.path())?;
        if index > retained_files {
            stale.push((index, entry.path()));
        }
    }
    stale.sort_by_key(|entry| std::cmp::Reverse(entry.0));
    for (_, path) in stale {
        validate_regular_file(&path)?;
        fs::remove_file(path).map_err(|_| RuntimeLogError::FileUnavailable)?;
    }
    Ok(())
}

fn rotated_path(directory: &Path, file_name: &str, index: usize) -> PathBuf {
    directory.join(format!("{file_name}.{index}"))
}

fn validate_regular_file(path: &Path) -> Result<(), RuntimeLogError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| RuntimeLogError::FileUnavailable)?;
    if metadata_is_link_like(&metadata) || !metadata.is_file() {
        return Err(RuntimeLogError::FileUnavailable);
    }
    Ok(())
}

fn secure_regular_file(path: &Path) -> Result<(), RuntimeLogError> {
    validate_regular_file(path)?;
    #[cfg(unix)]
    {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .map_err(|_| RuntimeLogError::FileUnavailable)?;
        if !file
            .metadata()
            .map_err(|_| RuntimeLogError::FileUnavailable)?
            .is_file()
        {
            return Err(RuntimeLogError::FileUnavailable);
        }
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|_| RuntimeLogError::FileUnavailable)?;
    }
    #[cfg(windows)]
    {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
            .open(path)
            .map_err(|_| RuntimeLogError::FileUnavailable)?;
        let metadata = file
            .metadata()
            .map_err(|_| RuntimeLogError::FileUnavailable)?;
        if metadata_is_link_like(&metadata) || !metadata.is_file() {
            return Err(RuntimeLogError::FileUnavailable);
        }
    }
    Ok(())
}

fn metadata_is_link_like(metadata: &fs::Metadata) -> bool {
    if metadata.file_type().is_symlink() {
        return true;
    }
    #[cfg(windows)]
    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return true;
    }
    false
}

fn path_exists(path: &Path) -> Result<bool, RuntimeLogError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(RuntimeLogError::FileUnavailable),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::BTreeMap,
        io::Cursor,
        process, thread,
        time::{Duration, Instant},
    };

    #[test]
    fn policy_defaults_and_environment_bounds_are_strict() {
        assert_eq!(
            LogPolicy::from_lookup(|_| None).expect("defaults should parse"),
            LogPolicy::default()
        );
        let policy = LogPolicy::from_lookup(|name| {
            BTreeMap::from([
                (ENV_LOG_MAX_BYTES, OsString::from("262144")),
                (ENV_LOG_RETAINED_FILES, OsString::from("3")),
                (ENV_LOG_MAX_LINE_BYTES, OsString::from("1024")),
            ])
            .remove(name)
        })
        .expect("bounded environment should parse");
        assert_eq!(
            policy,
            LogPolicy {
                max_bytes: 262144,
                retained_files: 3,
                max_line_bytes: 1024,
            }
        );

        for invalid in ["", " 262144", "+262144", "262144 ", "1.5", "-1", "no"] {
            assert_eq!(
                LogPolicy::from_lookup(|name| {
                    (name == ENV_LOG_MAX_BYTES).then(|| OsString::from(invalid))
                }),
                Err(RuntimeLogError::PolicyInvalid)
            );
        }
        assert_eq!(
            LogPolicy::from_lookup(|name| {
                (name == ENV_LOG_RETAINED_FILES).then(|| OsString::from("0"))
            }),
            Err(RuntimeLogError::PolicyInvalid)
        );
        assert_eq!(
            LogPolicy::from_lookup(|name| {
                match name {
                    ENV_LOG_MAX_BYTES => Some(OsString::from("131072")),
                    ENV_LOG_MAX_LINE_BYTES => Some(OsString::from("100000")),
                    _ => None,
                }
            }),
            Err(RuntimeLogError::PolicyInvalid)
        );
        assert_eq!(
            LogPolicy::from_lookup(|name| {
                (name == ENV_LOG_MAX_BYTES).then(|| OsString::from("67108865"))
            }),
            Err(RuntimeLogError::PolicyInvalid)
        );
        assert_eq!(
            LogPolicy::from_lookup(|name| {
                (name == ENV_LOG_RETAINED_FILES).then(|| OsString::from("17"))
            }),
            Err(RuntimeLogError::PolicyInvalid)
        );
        assert_eq!(
            LogPolicy::from_lookup(|name| {
                (name == ENV_LOG_MAX_LINE_BYTES).then(|| OsString::from("1048577"))
            }),
            Err(RuntimeLogError::PolicyInvalid)
        );
    }

    #[test]
    fn direct_events_are_structured_and_recursively_redacted() {
        let directory = temporary_directory("direct");
        let sink = test_sink(&directory, "agent.jsonl", MIN_LOG_MAX_BYTES, 512);
        let fields = Map::from_iter([
            (String::from("pid"), Value::from(42)),
            (
                String::from("nested"),
                serde_json::json!({
                    "api-key": "must-not-appear",
                    "safe": ["prefix-exact-needle-suffix"]
                }),
            ),
        ]);
        sink.write_event(
            LogLevel::Info,
            "AGENT_STARTED",
            Some(fields),
            &[String::from("exact-needle")],
        )
        .expect("event should write");
        assert_eq!(
            sink.write_code(LogLevel::Info, "not-stable"),
            Err(RuntimeLogError::EventInvalid)
        );

        let contents =
            fs::read_to_string(directory.join("agent.jsonl")).expect("runtime log should read");
        assert!(contents.contains("AGENT_STARTED"));
        assert!(contents.contains("[REDACTED]"));
        assert!(!contents.contains("must-not-appear"));
        assert!(!contents.contains("exact-needle"));
        let record: Value = serde_json::from_str(contents.trim()).expect("record should be JSON");
        assert_eq!(record["schemaVersion"], 1);
        assert_eq!(record["component"], "agent");
        assert_eq!(record["stream"], "runtime");
        remove_directory(directory);
    }

    #[test]
    fn capture_accepts_only_gateway_stdout_whitelist_and_stable_stderr_codes() {
        let directory = temporary_directory("capture");
        let sink = test_sink(&directory, "gateway.jsonl", MIN_LOG_MAX_BYTES, 1024);
        let stdout = concat!(
            "{\"level\":\"info\",\"message\":\"ready secret-value\",\"traceId\":\"trace-1\",\"fields\":{\"password\":\"raw-password\",\"nested\":{\"value\":\"secret-value\"}}}\n",
            "{\"level\":\"info\",\"message\":\"bad extra\",\"traceId\":\"trace-2\",\"extra\":\"must-not-leak\"}\n",
            "plain secret-value output\n"
        );
        let stderr = "GATEWAY_START_FAILED\nsecret-value stack trace\n";
        let capture = sink
            .capture(
                Cursor::new(stdout.as_bytes().to_vec()),
                Cursor::new(stderr.as_bytes().to_vec()),
                vec![String::from("secret-value")],
            )
            .expect("capture should start");
        capture.finish().expect("capture should finish");

        let contents =
            fs::read_to_string(directory.join("gateway.jsonl")).expect("gateway log should read");
        assert!(contents.contains("ready [REDACTED]"));
        assert!(contents.contains("GATEWAY_START_FAILED"));
        assert!(contents.contains("RUNTIME_LOG_STDOUT_INVALID"));
        assert!(contents.contains("RUNTIME_LOG_STDERR_INVALID"));
        assert!(!contents.contains("raw-password"));
        assert!(!contents.contains("secret-value"));
        assert!(!contents.contains("must-not-leak"));
        assert!(!contents.contains("stack trace"));
        for line in contents.lines() {
            let _: Value = serde_json::from_str(line).expect("each line should be JSON");
        }
        remove_directory(directory);
    }

    #[test]
    fn oversized_input_is_drained_without_preserving_raw_content() {
        let directory = temporary_directory("oversized");
        let sink = test_sink(&directory, "gateway.jsonl", MIN_LOG_MAX_BYTES, 256);
        let secret = "sensitive-oversized-payload";
        let mut stdout = secret.repeat(1024).into_bytes();
        stdout.push(b'\n');
        stdout.extend_from_slice(
            b"{\"level\":\"info\",\"message\":\"after\",\"traceId\":\"trace-after\"}\n",
        );
        let capture = sink
            .capture(
                Cursor::new(stdout),
                Cursor::new(Vec::<u8>::new()),
                vec![String::from(secret)],
            )
            .expect("capture should start");
        capture.finish().expect("capture should drain to EOF");
        let contents =
            fs::read_to_string(directory.join("gateway.jsonl")).expect("gateway log should read");
        assert!(contents.contains("RUNTIME_LOG_STDOUT_OVERSIZED"));
        assert!(contents.contains("after"));
        assert!(!contents.contains(secret));
        remove_directory(directory);
    }

    #[test]
    fn queue_full_drops_records_but_drains_and_joins_large_streams() {
        let directory = temporary_directory("queue-full");
        let sink = test_sink(&directory, "gateway.jsonl", 1024 * 1024, 256);
        let line = b"{\"level\":\"info\",\"message\":\"burst\",\"traceId\":\"trace-burst\"}\n";
        let mut stdout = Vec::with_capacity(line.len() * 100_000);
        for _ in 0..100_000 {
            stdout.extend_from_slice(line);
        }
        assert!(stdout.len() > 64 * 1024);
        let started = Instant::now();
        let capture = sink
            .capture(
                Cursor::new(stdout),
                Cursor::new(Vec::<u8>::new()),
                Vec::new(),
            )
            .expect("capture should start");
        assert_eq!(
            capture.finish(),
            Err(RuntimeLogError::QueueFull),
            "a burst should exceed the bounded queue"
        );
        assert!(started.elapsed() < Duration::from_secs(20));
        assert_eq!(sink.take_error_code(), Some("RUNTIME_LOG_QUEUE_FULL"));
        assert_eq!(sink.take_error_code(), None);
        remove_directory(directory);
    }

    #[test]
    fn finish_joins_slow_interleaved_readers_before_the_writer() {
        let directory = temporary_directory("slow-finish");
        let sink = test_sink(&directory, "gateway.jsonl", MIN_LOG_MAX_BYTES, 512);
        let stdout_line = b"{\"level\":\"info\",\"message\":\"slow\",\"traceId\":\"trace-slow\"}\n";
        let stderr_line = b"GATEWAY_SLOW_WARNING\n";
        let stdout = SlowReader::new(stdout_line.repeat(20), 13, Duration::from_millis(1));
        let stderr = SlowReader::new(stderr_line.repeat(20), 5, Duration::from_millis(1));
        let capture = sink
            .capture(stdout, stderr, Vec::new())
            .expect("capture should start");
        let started = Instant::now();
        capture.finish().expect("all capture threads should join");
        assert!(started.elapsed() < Duration::from_secs(5));
        let contents =
            fs::read_to_string(directory.join("gateway.jsonl")).expect("gateway log should read");
        assert_eq!(contents.matches("\"message\":\"slow\"").count(), 20);
        assert_eq!(contents.matches("GATEWAY_SLOW_WARNING").count(), 20);
        remove_directory(directory);
    }

    #[test]
    fn dropping_capture_joins_and_flushes_without_detaching_readers() {
        let directory = temporary_directory("drop-finish");
        let sink = test_sink(&directory, "gateway.jsonl", MIN_LOG_MAX_BYTES, 512);
        let stdout = SlowReader::new(
            b"{\"level\":\"info\",\"message\":\"drop-finished\",\"traceId\":\"trace-drop\"}\n"
                .to_vec(),
            7,
            Duration::from_millis(1),
        );
        let capture = sink
            .capture(stdout, Cursor::new(Vec::<u8>::new()), Vec::new())
            .expect("capture should start");
        drop(capture);
        let contents =
            fs::read_to_string(directory.join("gateway.jsonl")).expect("gateway log should read");
        assert!(contents.contains("drop-finished"));
        remove_directory(directory);
    }

    #[test]
    fn rotation_is_bounded_deterministic_and_prunes_old_generations() {
        let directory = temporary_directory("rotation");
        fs::write(directory.join("agent.jsonl.7"), b"stale").expect("stale rotation should write");
        let sink = StructuredLogSink::open(
            &directory,
            "agent.jsonl",
            "agent",
            LogPolicy {
                max_bytes: MIN_LOG_MAX_BYTES,
                retained_files: 2,
                max_line_bytes: 256,
            },
        )
        .expect("sink should open");
        assert!(!directory.join("agent.jsonl.7").exists());
        for _ in 0..3_000 {
            sink.write_code(LogLevel::Info, "AGENT_HEARTBEAT")
                .expect("event should rotate");
        }
        assert!(directory.join("agent.jsonl").is_file());
        assert!(directory.join("agent.jsonl.1").is_file());
        assert!(directory.join("agent.jsonl.2").is_file());
        assert!(!directory.join("agent.jsonl.3").exists());
        for name in ["agent.jsonl", "agent.jsonl.1", "agent.jsonl.2"] {
            assert!(fs::metadata(directory.join(name)).unwrap().len() <= MIN_LOG_MAX_BYTES);
        }
        remove_directory(directory);
    }

    #[cfg(unix)]
    #[test]
    fn files_are_private_and_symlinks_or_nonregular_paths_fail_closed() {
        use std::os::unix::fs::symlink;

        let directory = temporary_directory("paths");
        let sink = test_sink(&directory, "agent.jsonl", MIN_LOG_MAX_BYTES, 256);
        let mode = fs::metadata(directory.join("agent.jsonl"))
            .expect("active log should exist")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
        drop(sink);

        let target = directory.join("target");
        fs::write(&target, b"target").expect("target should write");
        symlink(&target, directory.join("linked.jsonl")).expect("symlink should create");
        assert!(matches!(
            StructuredLogSink::open(&directory, "linked.jsonl", "agent", LogPolicy::default()),
            Err(RuntimeLogError::FileUnavailable)
        ));
        fs::create_dir(directory.join("directory.jsonl")).expect("directory should create");
        assert!(matches!(
            StructuredLogSink::open(&directory, "directory.jsonl", "agent", LogPolicy::default()),
            Err(RuntimeLogError::FileUnavailable)
        ));

        symlink(&target, directory.join("rotated.jsonl.1")).expect("rotated symlink should create");
        assert!(matches!(
            StructuredLogSink::open(&directory, "rotated.jsonl", "agent", LogPolicy::default()),
            Err(RuntimeLogError::FileUnavailable)
        ));

        let retained_path = directory.join("private.jsonl.1");
        fs::write(&retained_path, b"retained").expect("retained file should write");
        fs::set_permissions(&retained_path, fs::Permissions::from_mode(0o644))
            .expect("fixture permissions should change");
        StructuredLogSink::open(&directory, "private.jsonl", "agent", LogPolicy::default())
            .expect("regular retained file should be accepted");
        assert_eq!(
            fs::metadata(&retained_path)
                .expect("retained file should exist")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );

        let linked_directory = temporary_directory("linked-target");
        let link_path = directory.join("linked-directory");
        symlink(&linked_directory, &link_path).expect("directory symlink should create");
        assert!(matches!(
            StructuredLogSink::open(&link_path, "agent.jsonl", "agent", LogPolicy::default()),
            Err(RuntimeLogError::DirectoryUnavailable)
        ));
        remove_directory(linked_directory);
        remove_directory(directory);
    }

    #[cfg(unix)]
    #[test]
    fn rotation_path_tampering_latches_a_stable_error() {
        use std::os::unix::fs::symlink;

        let directory = temporary_directory("rotation-error");
        let sink = StructuredLogSink::open(
            &directory,
            "agent.jsonl",
            "agent",
            LogPolicy {
                max_bytes: MIN_LOG_MAX_BYTES,
                retained_files: 1,
                max_line_bytes: 256,
            },
        )
        .expect("sink should open");
        let target = directory.join("target");
        fs::write(&target, b"target").expect("target should write");
        symlink(&target, directory.join("agent.jsonl.1")).expect("symlink should create");
        let mut observed = None;
        for _ in 0..2_000 {
            if let Err(error) = sink.write_code(LogLevel::Info, "AGENT_HEARTBEAT") {
                observed = Some(error);
                break;
            }
        }
        assert_eq!(observed, Some(RuntimeLogError::FileUnavailable));
        assert_eq!(sink.take_error_code(), Some("RUNTIME_LOG_FILE_UNAVAILABLE"));
        remove_directory(directory);
    }

    struct FailingReader;

    impl Read for FailingReader {
        fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("fixture failure"))
        }
    }

    struct SlowReader {
        input: Cursor<Vec<u8>>,
        chunk_bytes: usize,
        delay: Duration,
    }

    impl SlowReader {
        fn new(input: Vec<u8>, chunk_bytes: usize, delay: Duration) -> Self {
            Self {
                input: Cursor::new(input),
                chunk_bytes,
                delay,
            }
        }
    }

    impl Read for SlowReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            thread::sleep(self.delay);
            let limit = buffer.len().min(self.chunk_bytes);
            self.input.read(&mut buffer[..limit])
        }
    }

    #[test]
    fn capture_read_errors_are_latched_and_returned_after_join() {
        let directory = temporary_directory("read-error");
        let sink = test_sink(&directory, "gateway.jsonl", MIN_LOG_MAX_BYTES, 256);
        let capture = sink
            .capture(FailingReader, Cursor::new(Vec::<u8>::new()), Vec::new())
            .expect("capture should start");
        assert_eq!(capture.finish(), Err(RuntimeLogError::CaptureReadFailed));
        assert_eq!(
            sink.take_error_code(),
            Some("RUNTIME_LOG_CAPTURE_READ_FAILED")
        );
        remove_directory(directory);
    }

    fn test_sink(
        directory: &Path,
        file_name: &str,
        max_bytes: u64,
        max_line_bytes: usize,
    ) -> StructuredLogSink {
        StructuredLogSink::open(
            directory,
            file_name,
            file_name.trim_end_matches(".jsonl"),
            LogPolicy {
                max_bytes,
                retained_files: 2,
                max_line_bytes,
            },
        )
        .expect("test sink should open")
    }

    fn temporary_directory(label: &str) -> PathBuf {
        let sequence = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should follow epoch")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "cmclient-runtime-logging-{label}-{}-{sequence}",
            process::id()
        ));
        fs::create_dir(&directory).expect("temporary directory should create");
        directory
    }

    fn remove_directory(directory: PathBuf) {
        fs::remove_dir_all(directory).expect("temporary directory should remove");
    }
}
