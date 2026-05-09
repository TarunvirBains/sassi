mod check_test_surface;

use std::path::PathBuf;

fn main() {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("check-test-surface") => {
            let root = workspace_root();
            std::process::exit(check_test_surface::run(&root));
        }
        cmd => {
            eprintln!(
                "error: unknown subcommand {:?}\n\nUsage: cargo xtask check-test-surface",
                cmd.unwrap_or("(none)")
            );
            std::process::exit(1);
        }
    }
}

/// Walk up from the current directory to find the workspace root (the
/// directory containing a `Cargo.toml` with a `[workspace]` section).
fn workspace_root() -> PathBuf {
    let mut dir = std::env::current_dir().expect("cannot determine current directory");
    loop {
        let manifest = dir.join("Cargo.toml");
        if manifest.exists()
            && let Ok(text) = std::fs::read_to_string(&manifest)
            && text.contains("[workspace]")
        {
            return dir;
        }
        if !dir.pop() {
            panic!(
                "cannot locate workspace root - started from {:?}",
                std::env::current_dir().unwrap_or_default()
            );
        }
    }
}
