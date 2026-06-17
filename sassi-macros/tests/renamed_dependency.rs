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
    let src_dir = crate_dir.join("src");
    if !src_dir.starts_with(&crate_dir) {
        panic!(
            "refusing to create directory outside test root: {}",
            src_dir.display()
        );
    }
    let target_dir = crate_dir.join("target");
    if !target_dir.starts_with(&crate_dir) {
        panic!(
            "refusing to use target directory outside test root: {}",
            target_dir.display()
        );
    }
    let manifest_path = crate_dir.join("Cargo.toml");
    if !manifest_path.starts_with(&crate_dir) {
        panic!(
            "refusing to use manifest path outside test root: {}",
            manifest_path.display()
        );
    }

    fs::create_dir_all(&src_dir).expect("create temp crate src");
    let safe_write = |rel: &str, contents: &str| {
        let candidate = crate_dir.join(rel);
        if !candidate.starts_with(&crate_dir) {
            panic!(
                "refusing to write outside test root: {}",
                candidate.display()
            );
        }
        fs::write(candidate, contents).unwrap();
    };

    safe_write(
        "Cargo.toml",
        &format!(
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
    );

    safe_write(
        "src/main.rs",
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
    );

    let output = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .arg("check")
        .arg("--manifest-path")
        .arg(&manifest_path)
        .arg("--target-dir")
        .arg(&target_dir)
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

fn is_within(parent: &Path, child: &Path) -> bool {
    match (parent.canonicalize(), child.canonicalize()) {
        (Ok(parent_abs), Ok(child_abs)) => child_abs.starts_with(parent_abs),
        _ => false,
    }
}

fn fresh_temp_crate(name: &str) -> PathBuf {
    let temp_root = std::env::temp_dir()
        .canonicalize()
        .expect("canonicalize temp root");
    let path = temp_root.join(format!("{name}-{}", std::process::id()));
    if path.exists() {
        if is_within(&temp_root, &path) {
            fs::remove_dir_all(&path).expect("remove stale temp crate");
        } else {
            panic!(
                "refusing to remove path outside temp root: {}",
                path.display()
            );
        }
    }
    fs::create_dir_all(&path).expect("create temp crate root");
    path.canonicalize().expect("canonicalize temp crate root")
}
