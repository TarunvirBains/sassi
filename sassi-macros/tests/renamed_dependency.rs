//! Verifies that public macros work when adopters rename the `sassi`
//! dependency in `Cargo.toml`.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

#[test]
fn macros_resolve_renamed_sassi_dependency() {
    let crate_dir = fresh_temp_crate("sassi-renamed-dependency");
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("sassi-macros should live below the workspace root")
        .to_path_buf();
    let sassi_path = repo_root.join("sassi");
    let target_dir = crate_dir.join("target");

    fs::create_dir_all(crate_dir.join("src")).expect("create temp crate src");
    fs::write(
        crate_dir.join("Cargo.toml"),
        format!(
            r#"[package]
name = "sassi-renamed-dependency-fixture"
version = "0.0.0"
edition = "2024"
rust-version = "1.95"

[workspace]

[dependencies]
cache = {{ package = "sassi", path = "{}" }}
"#,
            sassi_path.display()
        ),
    )
    .expect("write fixture Cargo.toml");

    fs::write(
        crate_dir.join("src/main.rs"),
        r#"
use cache::{Cacheable, Sassi};
use std::any::Any;
use std::sync::Arc;

#[derive(Clone, Debug, Cacheable)]
#[cacheable(type_name = "fixture.User")]
struct User {
    id: i64,
    name: String,
}

trait Nameable: Send + Sync + Any {
    fn name(&self) -> &str;
}

#[cache::trait_impl]
impl Nameable for User {
    fn name(&self) -> &str {
        &self.name
    }
}

fn main() {
    let _fields = User::fields();
    let _registered: Vec<Arc<dyn Nameable>> = Sassi::new().all_impl::<dyn Nameable>();
}
"#,
    )
    .expect("write fixture main.rs");

    let output = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .arg("check")
        .arg("--manifest-path")
        .arg(crate_dir.join("Cargo.toml"))
        .arg("--target-dir")
        .arg(target_dir)
        .output()
        .expect("run cargo check for renamed dependency fixture");

    assert!(
        output.status.success(),
        "renamed dependency fixture failed\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn fresh_temp_crate(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("{name}-{}", std::process::id()));
    if path.exists() {
        fs::remove_dir_all(&path).expect("remove stale temp crate");
    }
    path
}
