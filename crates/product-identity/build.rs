use sha2::{Digest, Sha256};
use std::{env, fs, path::Path, process::Command};

fn main() {
    for name in [
        "CMCLIENT_BUILD_COMMIT",
        "CMCLIENT_BUILD_TREE",
        "CMCLIENT_BUILD_CHANNEL",
        "CMCLIENT_RUNTIME_PROFILE",
        "CMCLIENT_PACKAGE_PROFILE",
        "CMCLIENT_TARGET_ARCHITECTURE",
    ] {
        println!("cargo:rerun-if-env-changed={name}");
    }

    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is required");
    let repository = Path::new(&manifest_dir)
        .ancestors()
        .nth(2)
        .expect("product identity crate must be inside the repository");
    let commit = input_or_git("CMCLIENT_BUILD_COMMIT", repository, "HEAD");
    let tree =
        env::var("CMCLIENT_BUILD_TREE").unwrap_or_else(|_| workspace_tree_identity(repository));
    let channel = env::var("CMCLIENT_BUILD_CHANNEL").unwrap_or_else(|_| String::from("dev"));
    let profile = env::var("CMCLIENT_RUNTIME_PROFILE").unwrap_or_else(|_| String::from("native"));
    let package_profile =
        env::var("CMCLIENT_PACKAGE_PROFILE").unwrap_or_else(|_| String::from("workspace"));
    let architecture = env::var("CMCLIENT_TARGET_ARCHITECTURE")
        .or_else(|_| env::var("CARGO_CFG_TARGET_ARCH"))
        .expect("target architecture is required");
    let target_os = env::var("CARGO_CFG_TARGET_OS").expect("target OS is required");

    emit("SOURCE_COMMIT", &commit);
    emit("SOURCE_TREE", &tree);
    emit("CHANNEL", &channel);
    emit("RUNTIME_PROFILE", &profile);
    emit("PACKAGE_PROFILE", &package_profile);
    emit("TARGET_ARCHITECTURE", &architecture);
    emit("TARGET_OS", &target_os);
}

fn workspace_tree_identity(repository: &Path) -> String {
    let paths_output = git(
        repository,
        &[
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
        ],
    );
    let mut paths = paths_output
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(<[u8]>::to_vec)
        .collect::<Vec<_>>();
    paths.sort_unstable();
    for path in &paths {
        let relative = std::str::from_utf8(path).expect("repository paths must be UTF-8");
        println!(
            "cargo:rerun-if-changed={}",
            repository.join(relative).display()
        );
    }

    let status = git(
        repository,
        &["status", "--porcelain=v1", "--untracked-files=normal"],
    );
    if status.is_empty() {
        return input_or_git("CMCLIENT_BUILD_TREE", repository, "HEAD^{tree}");
    }

    let mut digest = Sha256::new();
    digest.update(b"cmclient-source-tree-v1\0");
    for path in paths {
        let relative = std::str::from_utf8(&path).expect("repository paths must be UTF-8");
        let absolute = repository.join(relative);
        let (kind, bytes): (&[u8], Vec<u8>) = match fs::symlink_metadata(&absolute) {
            Ok(metadata) if metadata.file_type().is_symlink() => (
                b"link",
                fs::read_link(&absolute)
                    .expect("source symlink must be readable")
                    .to_string_lossy()
                    .as_bytes()
                    .to_vec(),
            ),
            Ok(_) => (
                b"file",
                fs::read(&absolute).expect("source file must be readable"),
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (b"deleted", Vec::new()),
            Err(_) => panic!("source path metadata must be readable"),
        };
        digest.update((path.len() as u64).to_be_bytes());
        digest.update(&path);
        digest.update(kind);
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    }
    format!("sha256:{:x}", digest.finalize())
}

fn input_or_git(name: &str, repository: &Path, revision: &str) -> String {
    if let Ok(value) = env::var(name) {
        return value;
    }
    String::from_utf8(git(repository, &["rev-parse", revision]))
        .expect("git object ID must be UTF-8")
        .trim()
        .to_owned()
}

fn git(repository: &Path, arguments: &[&str]) -> Vec<u8> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(arguments)
        .output()
        .expect("git is required to derive workspace product identity");
    assert!(output.status.success(), "git command failed: {arguments:?}");
    output.stdout
}

fn emit(name: &str, value: &str) {
    println!("cargo:rustc-env=CMCLIENT_IDENTITY_{name}={value}");
}
