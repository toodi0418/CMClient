use fs4::{FileExt, TryLockError};
use same_file::Handle;
use std::{
    collections::HashSet,
    fmt::{Display, Formatter},
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};

static PROCESS_LOCKS: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LockError {
    PathInvalid,
    DirectoryUnavailable,
    OpenFailed,
    Contended,
    LockFailed,
    IdentityMismatch,
    UnlockFailed,
}

impl LockError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::PathInvalid => "RUNTIME_LOCK_PATH_INVALID",
            Self::DirectoryUnavailable => "RUNTIME_LOCK_DIRECTORY_UNAVAILABLE",
            Self::OpenFailed => "RUNTIME_LOCK_OPEN_FAILED",
            Self::Contended => "RUNTIME_LOCK_CONTENDED",
            Self::LockFailed => "RUNTIME_LOCK_FAILED",
            Self::IdentityMismatch => "RUNTIME_LOCK_IDENTITY_MISMATCH",
            Self::UnlockFailed => "RUNTIME_LOCK_UNLOCK_FAILED",
        }
    }
}

impl Display for LockError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for LockError {}

pub struct ExclusiveFileLock {
    file: Option<File>,
    process_guard: Option<ProcessLockGuard>,
}

impl ExclusiveFileLock {
    pub fn try_acquire(path: &Path) -> Result<Self, LockError> {
        validate_path(path)?;
        let parent = path.parent().ok_or(LockError::PathInvalid)?;
        fs::create_dir_all(parent).map_err(|_| LockError::DirectoryUnavailable)?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)
            .map_err(|_| LockError::OpenFailed)?;
        Self::try_acquire_opened(path, file)
    }

    pub fn try_acquire_opened(path: &Path, file: File) -> Result<Self, LockError> {
        validate_path(path)?;
        if !file
            .metadata()
            .map_err(|_| LockError::OpenFailed)?
            .is_file()
        {
            return Err(LockError::OpenFailed);
        }
        let identity = fs::canonicalize(path).map_err(|_| LockError::PathInvalid)?;
        let process_guard = ProcessLockGuard::acquire(identity)?;
        match FileExt::try_lock(&file) {
            Ok(()) if opened_file_matches_path(&file, path)? => Ok(Self {
                file: Some(file),
                process_guard: Some(process_guard),
            }),
            Ok(()) => {
                FileExt::unlock(&file).map_err(|_| LockError::UnlockFailed)?;
                Err(LockError::IdentityMismatch)
            }
            Err(TryLockError::WouldBlock) => Err(LockError::Contended),
            Err(TryLockError::Error(_)) => Err(LockError::LockFailed),
        }
    }

    pub fn release(mut self) -> Result<(), LockError> {
        let result = self
            .file
            .as_ref()
            .ok_or(LockError::UnlockFailed)
            .and_then(|file| FileExt::unlock(file).map_err(|_| LockError::UnlockFailed));
        self.file.take();
        self.process_guard.take();
        result
    }
}

fn opened_file_matches_path(file: &File, path: &Path) -> Result<bool, LockError> {
    let opened = Handle::from_file(file.try_clone().map_err(|_| LockError::OpenFailed)?)
        .map_err(|_| LockError::IdentityMismatch)?;
    let current = Handle::from_path(path).map_err(|_| LockError::IdentityMismatch)?;
    Ok(opened == current)
}

impl Drop for ExclusiveFileLock {
    fn drop(&mut self) {
        if let Some(file) = self.file.as_ref() {
            let _ = FileExt::unlock(file);
        }
        self.file.take();
        self.process_guard.take();
    }
}

struct ProcessLockGuard {
    identity: PathBuf,
}

impl ProcessLockGuard {
    fn acquire(identity: PathBuf) -> Result<Self, LockError> {
        let active = PROCESS_LOCKS.get_or_init(|| Mutex::new(HashSet::new()));
        let mut active = active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !active.insert(identity.clone()) {
            return Err(LockError::Contended);
        }
        Ok(Self { identity })
    }
}

impl Drop for ProcessLockGuard {
    fn drop(&mut self) {
        let active = PROCESS_LOCKS.get_or_init(|| Mutex::new(HashSet::new()));
        active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.identity);
    }
}

fn validate_path(path: &Path) -> Result<(), LockError> {
    if !path.is_absolute()
        || path.file_name().is_none()
        || path
            .parent()
            .is_none_or(|parent| parent.as_os_str().is_empty())
    {
        return Err(LockError::PathInvalid);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        env,
        process::{self, Command},
        time::{SystemTime, UNIX_EPOCH},
    };

    const LOCK_CHILD_ENV: &str = "CMCLIENT_LOCK_CHILD";
    const LOCK_PATH_ENV: &str = "CMCLIENT_LOCK_PATH";
    const LOCK_EXPECT_ENV: &str = "CMCLIENT_LOCK_EXPECT";
    const LOCK_CHILD_TEST: &str = "lock::tests::cross_process_lock_child";

    #[test]
    fn same_process_contention_and_explicit_release_are_stable() {
        let directory = temporary_directory("same-process");
        let path = directory.join("agent.lock");
        let first = ExclusiveFileLock::try_acquire(&path).unwrap();
        assert!(matches!(
            ExclusiveFileLock::try_acquire(&path),
            Err(LockError::Contended)
        ));
        first.release().unwrap();
        ExclusiveFileLock::try_acquire(&path)
            .unwrap()
            .release()
            .unwrap();
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn an_already_opened_private_file_can_be_locked() {
        let directory = temporary_directory("opened");
        let path = directory.join("migration.lock");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .unwrap();
        ExclusiveFileLock::try_acquire_opened(&path, file)
            .unwrap()
            .release()
            .unwrap();
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn an_opened_file_must_still_be_the_file_named_by_the_lock_path() {
        let directory = temporary_directory("identity");
        let expected_path = directory.join("expected.lock");
        let other_path = directory.join("other.lock");
        fs::write(&expected_path, b"").unwrap();
        let other = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&other_path)
            .unwrap();

        assert!(matches!(
            ExclusiveFileLock::try_acquire_opened(&expected_path, other),
            Err(LockError::IdentityMismatch)
        ));
        ExclusiveFileLock::try_acquire(&other_path)
            .unwrap()
            .release()
            .unwrap();
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn cross_process_contention_uses_the_operating_system_lock() {
        let directory = temporary_directory("cross-process");
        let path = directory.join("agent.lock");
        let held = ExclusiveFileLock::try_acquire(&path).unwrap();
        run_child(&path, "contended");
        held.release().unwrap();
        run_child(&path, "acquired");
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    #[ignore = "invoked as the cross-process fixture by its parent test"]
    fn cross_process_lock_child() {
        if env::var_os(LOCK_CHILD_ENV).is_none() {
            return;
        }
        let path = PathBuf::from(env::var_os(LOCK_PATH_ENV).unwrap());
        match env::var(LOCK_EXPECT_ENV).unwrap().as_str() {
            "contended" => assert!(matches!(
                ExclusiveFileLock::try_acquire(&path),
                Err(LockError::Contended)
            )),
            "acquired" => ExclusiveFileLock::try_acquire(&path)
                .unwrap()
                .release()
                .unwrap(),
            _ => panic!("unexpected child expectation"),
        }
    }

    #[test]
    fn invalid_paths_and_error_text_are_stable() {
        assert!(matches!(
            ExclusiveFileLock::try_acquire(Path::new("relative.lock")),
            Err(LockError::PathInvalid)
        ));
        for error in [
            LockError::PathInvalid,
            LockError::DirectoryUnavailable,
            LockError::OpenFailed,
            LockError::Contended,
            LockError::LockFailed,
            LockError::IdentityMismatch,
            LockError::UnlockFailed,
        ] {
            assert_eq!(error.to_string(), error.code());
        }
    }

    fn run_child(path: &Path, expected: &str) {
        let output = Command::new(env::current_exe().unwrap())
            .args(["--ignored", "--exact", LOCK_CHILD_TEST, "--nocapture"])
            .env(LOCK_CHILD_ENV, "1")
            .env(LOCK_PATH_ENV, path)
            .env(LOCK_EXPECT_ENV, expected)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child lock fixture failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn temporary_directory(label: &str) -> PathBuf {
        let sequence = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = env::temp_dir().join(format!(
            "cmclient-runtime-lock-{label}-{}-{sequence}",
            process::id()
        ));
        fs::create_dir(&directory).unwrap();
        directory
    }
}
