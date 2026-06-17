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
    let src_dir = vetted_child_path(&crate_dir, "src");
    let target_dir = vetted_child_path(&crate_dir, "target");
    let manifest_path = vetted_child_path(&crate_dir, "Cargo.toml");

    fs::create_dir_all(&src_dir).expect("create temp crate src");
    let safe_write = |rel: &str, contents: &str| {
        let candidate = vetted_child_path(&crate_dir, rel);
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

fn vetted_child_path(root: &Path, rel: &str) -> PathBuf {
    let canonical_root = root
        .canonicalize()
        .unwrap_or_else(|err| panic!("canonicalize root {}: {err}", root.display()));
    let candidate = canonical_root.join(rel);
    match candidate.canonicalize() {
        Ok(canonical_candidate) => {
            if canonical_candidate.starts_with(&canonical_root) {
                canonical_candidate
            } else {
                panic!(
                    "refusing to use path outside test root: {}",
                    canonical_candidate.display()
                );
            }
        }
        Err(_) => {
            let parent = candidate.parent().unwrap_or_else(|| {
                panic!(
                    "refusing to use path without parent: {}",
                    candidate.display()
                )
            });
            let canonical_parent = parent
                .canonicalize()
                .unwrap_or_else(|err| panic!("canonicalize parent {}: {err}", parent.display()));
            let vetted = canonical_parent.join(candidate.file_name().unwrap_or_else(|| {
                panic!(
                    "refusing to use path without file name: {}",
                    candidate.display()
                )
            }));
            if vetted.starts_with(&canonical_root) {
                vetted
            } else {
                panic!(
                    "refusing to use path outside test root: {}",
                    vetted.display()
                );
            }
        }
    }
}

fn fresh_temp_crate(name: &str) -> PathBuf {
    let temp_root = std::env::temp_dir()
        .canonicalize()
        .expect("canonicalize temp root");
    let path = vetted_child_path(&temp_root, &format!("{name}-{}", std::process::id()));
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
