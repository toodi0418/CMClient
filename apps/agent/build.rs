fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("windows") {
        return;
    }

    let manifest = std::path::PathBuf::from(
        std::env::var_os("CARGO_MANIFEST_DIR").expect("Cargo must set CARGO_MANIFEST_DIR"),
    )
    .join("cmclient-agent.manifest");

    println!("cargo:rerun-if-changed={}", manifest.display());
    println!("cargo:rustc-link-arg-bin=cmclient-agent=/MANIFEST:EMBED");
    println!(
        "cargo:rustc-link-arg-bin=cmclient-agent=/MANIFESTINPUT:{}",
        manifest.display()
    );
}
