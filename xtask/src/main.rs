mod check_test_surface;
mod sensitive_info;

use std::path::PathBuf;

fn main() {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("check-test-surface") => {
            let root = workspace_root();
            std::process::exit(check_test_surface::run(&root));
        }
        Some("sensitive-info") => {
            // Pass the remaining args through to the subcommand. We do not
            // need the workspace root here; the subcommand uses `git diff`
            // (which already understands the repo from cwd) for staged
            // scans, an explicit path for github-event scans, and an
            // explicit path for file/directory scans.
            let rest: Vec<String> = args.collect();
            std::process::exit(sensitive_info::run(&rest));
        }
        cmd => {
            eprintln!(
                "error: unknown subcommand {:?}\n\n\
                 Usage:\n  \
                 cargo xtask check-test-surface\n  \
                 cargo xtask sensitive-info --help",
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
