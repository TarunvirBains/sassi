//! `cargo xtask sensitive-info`
//!
//! Repo-local guard that scans text for credential-like content before it
//! lands in commits or in public GitHub text (issues, issue comments, PR
//! bodies, PR review comments). The goal is to catch:
//!
//! * Database URLs with embedded `user:password@host` credentials.
//! * Environment-style assignments such as `DATABASE_URL=...`, `PGPASSWORD=...`,
//!   `POSTGRES_PASSWORD=...`, `DB_PASSWORD=...`, `MYSQL_PWD=...`.
//! * `*_TOKEN`, `*_SECRET`, `API_KEY`, `CLIENT_SECRET`, webhook-secret style
//!   assignments.
//! * PEM `-----BEGIN ... PRIVATE KEY-----` blocks.
//! * Auth-bearing service URLs of the form `https://user:password@host/...`.
//!
//! ## Redaction contract
//!
//! The scanner reports only *metadata* about findings: source location, line
//! number (when known), rule id, and category. It never echoes the matched
//! secret, the surrounding text, or any raw event-payload content. This means
//! the same output is safe to:
//!
//! * Print in a developer terminal during a pre-commit scan.
//! * Capture in a GitHub Actions log.
//! * Post back as a comment on a public issue/PR.
//!
//! ## Allowlist (deliberately narrow)
//!
//! A candidate match is allowed when the credential-value portion of the
//! match contains one of:
//!
//! * an angle-bracket placeholder such as `<password>` or `<your-token>`,
//! * a recognized dummy/example token: `dummy`, `placeholder`, `redacted`,
//!   `example`, `xxx`, `***`, `your-password`, `your_password`, `your-token`,
//!   `your_token`,
//! * the empty string.
//!
//! Local-mode scans additionally honor two opt-in markers:
//!
//! * **Per-line:** `sassi:allow-secret` anywhere on the line (e.g.
//!   `// sassi:allow-secret` or `# sassi:allow-secret`). Useful for a
//!   single test fixture line.
//! * **File-level:** `sassi:allow-secret-file` anywhere in the first 60
//!   lines of a file. Used only for files that are themselves test
//!   fixtures of the scanner (this module is the canonical example).
//!
//! The GitHub-event mode does **not** honor either marker: a public
//! submitter must not be able to bypass the guard by adding a marker
//! themselves.
//
// sassi:allow-secret-file
// The unit-test fixtures below intentionally include credential-shaped
// strings ("PGPASSWORD=hunter2", "postgres://app:hunter2@host/db", etc.)
// as inputs to validate detection. The file-level marker above tells the
// local-mode scanner to skip this file when invoked via `--path` or
// `--staged`. Public-text scans NEVER honor this marker.
//!
//! ## Modes
//!
//! * `--staged` - scan the staged-diff (added lines only) via `git diff
//!   --cached -U0`. Used as a pre-commit check.
//! * `--github-event <path>` - scan a GitHub webhook event payload.
//!   Extracts only the documented text fields (`issue.title`, `issue.body`,
//!   `comment.body`, `pull_request.title`, `pull_request.body`,
//!   `review.body`) and scans those.
//! * `--path <file-or-dir>` - scan files or recursively a directory. Useful
//!   for ad-hoc local checks and for tests.

use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Stable identifier for a detection rule. Reported back to the user so they
/// can look up what was matched without us echoing the matched value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleId {
    /// `<scheme>://user:password@host[...]` for a database-like scheme.
    DbUrlWithCred,
    /// `<scheme>://user:password@host[...]` for an HTTP/HTTPS/WS scheme.
    AuthBearingUrl,
    /// `DATABASE_URL=...`, `PGPASSWORD=...`, `POSTGRES_PASSWORD=...`,
    /// `DB_PASSWORD=...`, `MYSQL_PWD=...` with a non-placeholder value.
    DbPasswordEnv,
    /// `<ANY>_TOKEN=value` (whole-token suffix match).
    GenericToken,
    /// `<ANY>_SECRET=value` (whole-token suffix match).
    GenericSecret,
    /// `API_KEY=value`.
    ApiKey,
    /// `CLIENT_SECRET=value`.
    ClientSecret,
    /// `WEBHOOK_SECRET=value` (and `WEBHOOK-SECRET`).
    WebhookSecret,
    /// `-----BEGIN ... PRIVATE KEY-----` block.
    PrivateKeyBlock,
}

impl RuleId {
    /// Stable string id, used in logs and posted comments. Kept ASCII and
    /// underscore-only so it is greppable and copy-paste safe.
    pub fn as_str(&self) -> &'static str {
        match self {
            RuleId::DbUrlWithCred => "DB_URL_WITH_CRED",
            RuleId::AuthBearingUrl => "AUTH_BEARING_URL",
            RuleId::DbPasswordEnv => "DB_PASSWORD_ENV",
            RuleId::GenericToken => "GENERIC_TOKEN",
            RuleId::GenericSecret => "GENERIC_SECRET",
            RuleId::ApiKey => "API_KEY",
            RuleId::ClientSecret => "CLIENT_SECRET",
            RuleId::WebhookSecret => "WEBHOOK_SECRET",
            RuleId::PrivateKeyBlock => "PRIVATE_KEY_BLOCK",
        }
    }

    /// Coarse category label (database vs token vs key vs url).
    pub fn category(&self) -> &'static str {
        match self {
            RuleId::DbUrlWithCred => "database-url",
            RuleId::AuthBearingUrl => "service-url",
            RuleId::DbPasswordEnv => "database-env",
            RuleId::GenericToken
            | RuleId::GenericSecret
            | RuleId::ApiKey
            | RuleId::ClientSecret
            | RuleId::WebhookSecret => "token",
            RuleId::PrivateKeyBlock => "private-key",
        }
    }
}

/// A redacted finding. `source` is a logical location (file path, GitHub
/// event field path, etc.). `line` is 1-based when the source has a line
/// concept. The scanner never stores or transmits the matched value.
#[derive(Debug, Clone)]
pub struct Finding {
    pub source: String,
    pub line: Option<usize>,
    pub rule: RuleId,
}

impl Finding {
    /// Single-line redacted representation. This is the only public way to
    /// print a finding; it intentionally has no field for raw content.
    pub fn redacted_display(&self) -> String {
        let line = match self.line {
            Some(n) => format!(":{}", n),
            None => String::new(),
        };
        format!(
            "sensitive-info: {}{}: rule={} category={}",
            self.source,
            line,
            self.rule.as_str(),
            self.rule.category()
        )
    }
}

/// Configuration that varies between local and CI/workflow scans.
#[derive(Debug, Clone, Copy)]
pub struct ScanConfig {
    /// Honor an explicit `sassi:allow-secret` marker on a matched line.
    /// Local pre-commit scans honor markers (so test fixtures and other
    /// intentional examples can opt-in). The GitHub-event mode does NOT
    /// honor markers: a public submitter must not be able to bypass the
    /// guard by adding the marker themselves.
    pub honor_allow_marker: bool,
}

impl ScanConfig {
    /// Configuration for the local pre-commit (`--staged`, `--path`) scans.
    pub const fn local() -> Self {
        Self {
            honor_allow_marker: true,
        }
    }

    /// Configuration for the GitHub-event (`--github-event`) scan.
    pub const fn public_text() -> Self {
        Self {
            honor_allow_marker: false,
        }
    }
}

// ---------------------------------------------------------------------------
// CLI entry point
// ---------------------------------------------------------------------------

/// Print the subcommand's usage string. Used both by `--help` and by the
/// error path for unrecognized arguments.
pub fn print_usage() {
    println!(
        "Usage:\n  \
         cargo xtask sensitive-info --staged\n  \
         cargo xtask sensitive-info --github-event <event-json-path>\n  \
         cargo xtask sensitive-info --path <file-or-dir>\n  \
         cargo xtask sensitive-info --help\n\n\
         Scans for credential-like content. Reports redacted rule names\n\
         only; never echoes matched secrets or surrounding raw text."
    );
}

/// Run from `cargo xtask sensitive-info <args>`. Returns the process exit
/// code: 0 if no findings, 1 if findings, 2 if usage error.
pub fn run(args: &[String]) -> i32 {
    let mut mode: Option<&str> = None;
    let mut path_arg: Option<String> = None;

    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_usage();
                return 0;
            }
            "--staged" => {
                if mode.is_some() {
                    eprintln!("error: multiple modes provided");
                    print_usage();
                    return 2;
                }
                mode = Some("staged");
            }
            "--github-event" => {
                if mode.is_some() {
                    eprintln!("error: multiple modes provided");
                    print_usage();
                    return 2;
                }
                mode = Some("github-event");
                path_arg = it.next().cloned();
                if path_arg.is_none() {
                    eprintln!("error: --github-event requires a path argument");
                    return 2;
                }
            }
            "--path" => {
                if mode.is_some() {
                    eprintln!("error: multiple modes provided");
                    print_usage();
                    return 2;
                }
                mode = Some("path");
                path_arg = it.next().cloned();
                if path_arg.is_none() {
                    eprintln!("error: --path requires a path argument");
                    return 2;
                }
            }
            other => {
                eprintln!("error: unknown argument {:?}", other);
                print_usage();
                return 2;
            }
        }
    }

    let findings = match mode {
        Some("staged") => match scan_staged() {
            Ok(f) => f,
            Err(e) => {
                eprintln!("error: {}", e);
                return 2;
            }
        },
        Some("github-event") => {
            let path = path_arg.expect("validated above");
            match scan_github_event_file(Path::new(&path)) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("error: {}", e);
                    return 2;
                }
            }
        }
        Some("path") => {
            let path = path_arg.expect("validated above");
            match scan_path(Path::new(&path)) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("error: {}", e);
                    return 2;
                }
            }
        }
        Some(_) => unreachable!(),
        None => {
            print_usage();
            return 2;
        }
    };

    if findings.is_empty() {
        eprintln!("sensitive-info: OK (no findings)");
        0
    } else {
        for f in &findings {
            println!("{}", f.redacted_display());
        }
        eprintln!(
            "sensitive-info: {} finding(s); see CONTRIBUTING.md for guidance",
            findings.len()
        );
        1
    }
}

// ---------------------------------------------------------------------------
// Staged-diff mode
// ---------------------------------------------------------------------------

/// Drive `git diff --cached` and scan only the *added* lines of the staged
/// diff. Removed lines and context lines are ignored. File paths and line
/// numbers reported come from the diff itself.
///
/// Findings are filtered post-hoc against the file-level allow marker: if
/// the working-tree copy of a referenced file carries `sassi:allow-secret-file`
/// within its leading lines, its findings are dropped. This lets the
/// scanner's own source file (a test-fixture file by nature) opt out
/// without per-line markers.
pub fn scan_staged() -> Result<Vec<Finding>, String> {
    let diff = run_git_staged_diff()?;
    let raw = scan_diff_text(&diff, ScanConfig::local());
    Ok(filter_file_marker(raw))
}

/// Post-hoc filter: drop findings whose `source` resolves to a working-tree
/// file that carries the file-level allow marker. Caches the disk read per
/// distinct source so a large diff with many findings from one fixture file
/// only reads that file once.
fn filter_file_marker(findings: Vec<Finding>) -> Vec<Finding> {
    use std::collections::HashMap;
    let mut cache: HashMap<String, bool> = HashMap::new();
    findings
        .into_iter()
        .filter(|f| {
            let marked = cache
                .entry(f.source.clone())
                .or_insert_with(|| file_has_file_level_marker(Path::new(&f.source)));
            !*marked
        })
        .collect()
}

fn run_git_staged_diff() -> Result<String, String> {
    use std::process::Command;
    let output = Command::new("git")
        .args([
            "diff",
            "--cached",
            "--no-color",
            "--no-ext-diff",
            "-U0",
            "--src-prefix=a/",
            "--dst-prefix=b/",
        ])
        .output()
        .map_err(|e| format!("failed to run git: {}", e))?;
    if !output.status.success() {
        return Err(format!(
            "git diff failed with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    String::from_utf8(output.stdout).map_err(|e| format!("git diff output not utf-8: {}", e))
}

/// Parse a unified diff and return findings for *added* lines.
///
/// Recognized headers:
/// * `diff --git a/X b/Y` - paired path information (not used directly; we
///   prefer `+++ b/Y`)
/// * `--- a/X` - old path (ignored)
/// * `+++ b/Y` - new path (becomes the current source)
/// * `+++ /dev/null` - file deletion; no added-line scan needed
/// * `@@ -A,B +C,D @@` - hunk header; sets the running new-line cursor
///
/// Within a hunk, lines starting with `+` (but not `+++`) are *added* and
/// scanned; lines starting with `-` advance nothing; lines starting with
/// ` ` (context) advance the cursor without being scanned.
pub fn scan_diff_text(diff: &str, config: ScanConfig) -> Vec<Finding> {
    let mut out = Vec::new();
    let mut current_file: Option<String> = None;
    let mut new_line: usize = 0;

    for raw in diff.lines() {
        if let Some(rest) = raw.strip_prefix("+++ ") {
            // "+++ b/path" -> "path", "+++ /dev/null" -> None.
            let trimmed = rest.trim();
            if trimmed == "/dev/null" {
                current_file = None;
            } else if let Some(p) = trimmed.strip_prefix("b/") {
                current_file = Some(p.to_string());
            } else {
                current_file = Some(trimmed.to_string());
            }
            continue;
        }
        if raw.starts_with("--- ") {
            continue;
        }
        if let Some(rest) = raw.strip_prefix("@@") {
            // @@ -a[,b] +c[,d] @@ optional-section-heading
            if let Some(plus_pos) = rest.find('+') {
                let after_plus = &rest[plus_pos + 1..];
                let mut num = String::new();
                for c in after_plus.chars() {
                    if c.is_ascii_digit() {
                        num.push(c);
                    } else {
                        break;
                    }
                }
                if let Ok(n) = num.parse::<usize>() {
                    // git uses 0 as the "no-context" start when count=0; in
                    // that case the cursor sits at the line *after* which
                    // the next addition happens, which is effectively (n+1).
                    // For -U0 inserts this matches the GitHub line view.
                    new_line = if n == 0 { 1 } else { n };
                }
            }
            continue;
        }
        if raw.starts_with("+++") || raw.starts_with("---") {
            // Header sub-prefixes already handled above.
            continue;
        }
        if let Some(content) = raw.strip_prefix('+') {
            // An added line. Scan it against the current file.
            if let Some(file) = &current_file {
                for rule in scan_line(content, config) {
                    out.push(Finding {
                        source: file.clone(),
                        line: Some(new_line),
                        rule,
                    });
                }
            }
            new_line += 1;
            continue;
        }
        if let Some(_removed) = raw.strip_prefix('-') {
            // Removed line; do not advance new_line.
            continue;
        }
        if raw.starts_with(' ') {
            // Context line (only present at -U>0).
            new_line += 1;
            continue;
        }
        // Anything else (binary patch headers, "\ No newline at end of file",
        // etc.) we skip.
    }

    out
}

// ---------------------------------------------------------------------------
// GitHub event mode
// ---------------------------------------------------------------------------

/// Scan a GitHub webhook event payload from a JSON file on disk.
///
/// Only the documented text fields are extracted and scanned:
/// `issue.title`, `issue.body`, `comment.body`, `pull_request.title`,
/// `pull_request.body`, `review.body`. The payload file itself is never
/// echoed and unknown fields are not scanned.
pub fn scan_github_event_file(path: &Path) -> Result<Vec<Finding>, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("read {}: {}", path.display(), e))?;
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|e| format!("parse {}: {}", path.display(), e))?;
    Ok(scan_github_event_value(&value))
}

/// Public test entry point that takes an already-parsed event value.
pub fn scan_github_event_value(event: &serde_json::Value) -> Vec<Finding> {
    let mut out = Vec::new();
    let config = ScanConfig::public_text();
    for (label, path) in EVENT_TEXT_FIELDS {
        if let Some(s) = get_str_path(event, path) {
            scan_text_into(label, s, config, &mut out);
        }
    }
    out
}

/// Logical name + JSON pointer of each text field we will scan in an event
/// payload. Names appear in the redacted output as the `source` field, so
/// they should be stable and human-readable.
const EVENT_TEXT_FIELDS: &[(&str, &[&str])] = &[
    ("issue.title", &["issue", "title"]),
    ("issue.body", &["issue", "body"]),
    ("comment.body", &["comment", "body"]),
    ("pull_request.title", &["pull_request", "title"]),
    ("pull_request.body", &["pull_request", "body"]),
    ("review.body", &["review", "body"]),
];

fn get_str_path<'a>(root: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    let mut cur = root;
    for key in path {
        cur = cur.get(*key)?;
    }
    cur.as_str()
}

// ---------------------------------------------------------------------------
// Path mode
// ---------------------------------------------------------------------------

/// Scan a single file or directory recursively. Skips hidden directories and
/// `target/`. Reads files as UTF-8; non-UTF-8 files are silently skipped
/// (we don't have anything credential-like to find in binary blobs).
pub fn scan_path(root: &Path) -> Result<Vec<Finding>, String> {
    let mut out = Vec::new();
    let config = ScanConfig::local();
    if root.is_file() {
        scan_one_file(root, config, &mut out);
    } else if root.is_dir() {
        walk_dir(root, config, &mut out);
    } else {
        return Err(format!("{} is not a file or directory", root.display()));
    }
    Ok(out)
}

fn walk_dir(dir: &Path, config: ScanConfig, out: &mut Vec<Finding>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut paths: Vec<PathBuf> = entries.flatten().map(|e| e.path()).collect();
    paths.sort();
    for path in paths {
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if should_skip_dir(name) {
            continue;
        }
        if path.is_dir() {
            walk_dir(&path, config, out);
        } else if path.is_file() {
            scan_one_file(&path, config, out);
        }
    }
}

fn should_skip_dir(name: &str) -> bool {
    matches!(name, ".git" | ".codex" | ".worktrees" | "target")
}

fn scan_one_file(path: &Path, config: ScanConfig, out: &mut Vec<Finding>) {
    let Ok(text) = std::fs::read_to_string(path) else {
        return;
    };
    // Honor the file-level allow marker only when this config honors
    // markers (i.e. local scans). Public-text scans never read files, so
    // this branch is local-only by construction; the gate is kept for
    // defense-in-depth in case a future caller passes the public config.
    if config.honor_allow_marker && text_has_file_level_marker(&text) {
        return;
    }
    scan_text_into(&path.display().to_string(), &text, config, out);
}

/// Check the leading lines of `text` for the file-level allow marker
/// `sassi:allow-secret-file`. The cap prevents a malicious commit from
/// burying a fake marker deep in a generated file and expecting it to
/// silence the scanner; the limit is set high enough to comfortably cover
/// a prose-style module-doc header but low enough that the marker stays
/// visible to a reviewer opening the file.
pub fn text_has_file_level_marker(text: &str) -> bool {
    const MAX_LINES: usize = 60;
    for (i, line) in text.lines().enumerate() {
        if i >= MAX_LINES {
            return false;
        }
        if line.contains("sassi:allow-secret-file") {
            return true;
        }
    }
    false
}

/// Disk-backed wrapper around [`text_has_file_level_marker`]. Returns
/// `false` for unreadable or non-UTF-8 files.
pub fn file_has_file_level_marker(path: &Path) -> bool {
    std::fs::read_to_string(path)
        .map(|t| text_has_file_level_marker(&t))
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Core scanner
// ---------------------------------------------------------------------------

/// Scan a multi-line text blob against all rules, appending findings to
/// `out`. `source` becomes the finding's source label; line numbers are
/// 1-based and reflect the position in `text`.
pub fn scan_text_into(source: &str, text: &str, config: ScanConfig, out: &mut Vec<Finding>) {
    // Private-key blocks can span multiple lines; we still detect them on
    // the first BEGIN line, which is sufficient and avoids storing the body.
    for (idx, line) in text.lines().enumerate() {
        let line_no = idx + 1;
        for rule in scan_line(line, config) {
            out.push(Finding {
                source: source.to_string(),
                line: Some(line_no),
                rule,
            });
        }
    }
}

/// Apply every rule to one line. Returns the list of rule ids that matched
/// AND were not allowlisted. The line itself is consumed only locally; the
/// returned `Vec<RuleId>` carries no raw content.
pub fn scan_line(line: &str, config: ScanConfig) -> Vec<RuleId> {
    if config.honor_allow_marker && line_has_allow_marker(line) {
        return Vec::new();
    }

    let mut hits = Vec::new();

    // PRIVATE_KEY_BLOCK: pattern check is independent of the env/url checks.
    if line_has_private_key_marker(line) && !value_is_allowed_placeholder(line) {
        hits.push(RuleId::PrivateKeyBlock);
    }

    // URL-with-credentials: scheme://user:password@host
    for url_hit in find_credentialed_urls(line) {
        if !value_is_allowed_placeholder(url_hit.password) {
            hits.push(url_hit.rule);
        }
    }

    // Env-style assignments: NAME=value or NAME: value
    for env_hit in find_env_assignments(line) {
        if value_is_allowed_placeholder(env_hit.value) {
            continue;
        }
        // Recursive URL check on a URL-shaped value is implicit through
        // the URL detector above scanning the same line. We don't double-
        // emit because the URL detector matched the URL substring directly.
        hits.push(env_hit.rule);
    }

    // De-duplicate while preserving order (the same rule firing twice on
    // one line is redundant; the user only needs to fix it once).
    let mut deduped = Vec::new();
    for hit in hits {
        if !deduped.contains(&hit) {
            deduped.push(hit);
        }
    }
    deduped
}

/// Return true if the line carries an explicit `sassi:allow-secret` marker.
/// We accept the marker in any of the common comment styles a contributor
/// might naturally reach for:
///
/// * Rust: `// sassi:allow-secret`
/// * Sh/yaml/toml/python: `# sassi:allow-secret`
/// * HTML/Markdown: `<!-- sassi:allow-secret -->`
fn line_has_allow_marker(line: &str) -> bool {
    line.contains("sassi:allow-secret")
}

/// PEM private-key block opener (one of: RSA, EC, DSA, OPENSSH, generic).
/// We match the start marker; that is enough. Multi-line bodies don't add
/// value to the finding and we don't want to retain that text.
fn line_has_private_key_marker(line: &str) -> bool {
    // We accept every standard variant of the PEM start marker:
    //
    // * `-----BEGIN PRIVATE KEY-----`        (no kind, generic)
    // * `-----BEGIN RSA PRIVATE KEY-----`
    // * `-----BEGIN EC PRIVATE KEY-----`
    // * `-----BEGIN OPENSSH PRIVATE KEY-----`
    //
    // Strategy: locate `-----BEGIN ` then locate `PRIVATE KEY-----` in the
    // remainder. The text between them ("") or ("RSA ") is the kind and is
    // tolerated; an embedded angle bracket means we are looking at a
    // documentation template, not a real block.
    let begin = "-----BEGIN ";
    let key = "PRIVATE KEY-----";
    if let Some(start) = line.find(begin) {
        let after = &line[start + begin.len()..];
        if let Some(end) = after.find(key) {
            let middle = &after[..end];
            // Template guard: angle-bracket kind ("<KIND>") signals a doc
            // sample, not real key material.
            return !middle.contains('<');
        }
    }
    false
}

#[derive(Debug)]
struct UrlHit<'a> {
    rule: RuleId,
    password: &'a str,
}

/// Find substrings of the shape `scheme://user:password@host`. Returns one
/// `UrlHit` per credentialed URL on the line.
fn find_credentialed_urls(line: &str) -> Vec<UrlHit<'_>> {
    let mut hits = Vec::new();
    let bytes = line.as_bytes();
    let n = bytes.len();

    let mut i = 0;
    while i + 3 <= n {
        // Look for "://"
        if bytes[i] == b':' && i + 2 < n && bytes[i + 1] == b'/' && bytes[i + 2] == b'/' {
            // Find scheme by scanning backward to the start of the scheme
            // (letters, digits, +, -, .).
            let mut s = i;
            while s > 0 {
                let b = bytes[s - 1];
                if is_scheme_byte(b) {
                    s -= 1;
                } else {
                    break;
                }
            }
            if s == i {
                // No scheme letters; skip past.
                i += 3;
                continue;
            }
            let scheme_bytes = &line[s..i];
            let scheme_lower = scheme_bytes.to_ascii_lowercase();

            // Find the host start (just after "://").
            let after_slashes = i + 3;
            // Find end of the "user:password@host..." section. Stop at the
            // first whitespace or quote character. URLs in prose are
            // typically terminated by one of those.
            let mut j = after_slashes;
            while j < n && !is_url_terminator(bytes[j]) {
                j += 1;
            }
            let segment = &line[after_slashes..j];

            // We need both a `:` and an `@` in the segment, with `:`
            // appearing before `@`, and no `/` between them (otherwise the
            // `:` belongs to a host port, not a password).
            if let Some(at_rel) = segment.find('@') {
                let user_pass = &segment[..at_rel];
                if let Some(colon_rel) = user_pass.find(':') {
                    let user = &user_pass[..colon_rel];
                    let pass = &user_pass[colon_rel + 1..];
                    let no_slash_in_userpass = !user.contains('/') && !pass.contains('/');
                    let no_at_in_userpass = !user.contains('@') && !pass.contains('@');
                    if no_slash_in_userpass && no_at_in_userpass && !pass.is_empty() {
                        let rule = if is_db_scheme(&scheme_lower) {
                            RuleId::DbUrlWithCred
                        } else if is_http_scheme(&scheme_lower) {
                            RuleId::AuthBearingUrl
                        } else {
                            // Some other auth-bearing scheme (amqp, mqtt, etc.) -
                            // treat as a service URL.
                            RuleId::AuthBearingUrl
                        };
                        hits.push(UrlHit {
                            rule,
                            password: pass,
                        });
                    }
                }
            }
            i = j;
            continue;
        }
        i += 1;
    }

    hits
}

fn is_scheme_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'+' || b == b'-' || b == b'.'
}

fn is_url_terminator(b: u8) -> bool {
    // URL ends at whitespace, common quote/bracket characters, or end-of-line
    // markers that already broke the line.
    matches!(
        b,
        b' ' | b'\t' | b'\n' | b'\r' | b'"' | b'\'' | b'<' | b'>' | b'`' | b','
    )
}

fn is_db_scheme(scheme_lower: &str) -> bool {
    matches!(
        scheme_lower,
        "postgres"
            | "postgresql"
            | "mysql"
            | "mariadb"
            | "mongodb"
            | "mongodb+srv"
            | "redis"
            | "rediss"
            | "amqp"
            | "amqps"
            | "kafka"
            | "clickhouse"
            | "cockroachdb"
    )
}

fn is_http_scheme(scheme_lower: &str) -> bool {
    matches!(scheme_lower, "http" | "https" | "ws" | "wss")
}

#[derive(Debug)]
struct EnvHit<'a> {
    rule: RuleId,
    value: &'a str,
}

/// Find env-style assignments on a line. Recognized syntaxes:
///
/// * Shell-style: `NAME=value` (no space around `=`)
/// * YAML-style: `NAME: value` (space after `:`, key on same line as value)
/// * Quoted variants: `NAME="value"`, `NAME='value'`
///
/// We do not try to interpret YAML block scalars (`NAME: |\n  value`) or
/// shell here-docs; if a credential is buried across multiple lines, the
/// scanner catches each line independently.
fn find_env_assignments(line: &str) -> Vec<EnvHit<'_>> {
    let mut hits = Vec::new();
    let bytes = line.as_bytes();
    let n = bytes.len();

    let mut i = 0;
    while i < n {
        // A name starts with an ASCII uppercase letter or '_'.
        if is_name_start(bytes[i]) {
            // Reject if this is mid-identifier (preceded by an ident byte).
            if i > 0 && is_name_byte(bytes[i - 1]) {
                i += 1;
                continue;
            }
            let name_start = i;
            while i < n && is_name_byte(bytes[i]) {
                i += 1;
            }
            let name = &line[name_start..i];
            // Skip whitespace, then look for '=' or ':'.
            let mut j = i;
            while j < n && (bytes[j] == b' ' || bytes[j] == b'\t') {
                j += 1;
            }
            if j < n && (bytes[j] == b'=' || bytes[j] == b':') {
                let assign_byte = bytes[j];
                j += 1;
                // Tolerate whitespace after the assign char.
                while j < n && (bytes[j] == b' ' || bytes[j] == b'\t') {
                    j += 1;
                }
                // Reject the YAML-style `:` when there was *no* whitespace
                // after `:` AND the next byte starts a new identifier. That
                // pattern is usually a typed-call like `Type::method`, not
                // an assignment.
                if assign_byte == b':' && (j == i + 1) && j < n && is_name_byte(bytes[j]) {
                    continue;
                }
                // Read the value up to a value-terminator. We support
                // bare values, single-quoted, and double-quoted values.
                let (value, end) = read_value(line, j);
                if let Some(rule) = match_env_name(name) {
                    hits.push(EnvHit { rule, value });
                }
                i = end;
                continue;
            }
        }
        i += 1;
    }

    hits
}

fn is_name_start(b: u8) -> bool {
    b.is_ascii_uppercase() || b == b'_'
}

fn is_name_byte(b: u8) -> bool {
    b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_'
}

/// Read a value after an `=` or `:` separator. Returns the trimmed value
/// substring (excluding surrounding quotes) and the cursor position after
/// the value.
fn read_value(line: &str, start: usize) -> (&str, usize) {
    let bytes = line.as_bytes();
    let n = bytes.len();
    if start >= n {
        return ("", start);
    }
    if line[start..].starts_with("${{")
        && let Some(end_rel) = line[start + 3..].find("}}")
    {
        let end = start + 3 + end_rel + 2;
        return (&line[start..end], end);
    }
    let first = bytes[start];
    if first == b'"' || first == b'\'' {
        // Quoted value. Read until matching closing quote.
        let quote = first;
        let mut k = start + 1;
        while k < n && bytes[k] != quote {
            // Allow backslash escapes inside double-quoted shell values.
            if quote == b'"' && bytes[k] == b'\\' && k + 1 < n {
                k += 2;
            } else {
                k += 1;
            }
        }
        let value = &line[start + 1..k.min(n)];
        let end = if k < n { k + 1 } else { k };
        (value, end)
    } else {
        // Bare value. Stop at whitespace or shell separators that commonly
        // terminate a token; everything before the terminator is the value.
        let mut k = start;
        while k < n && !matches!(bytes[k], b' ' | b'\t' | b'\n' | b'\r' | b'#' | b';' | b'`') {
            k += 1;
        }
        (&line[start..k], k)
    }
}

/// Map an environment-variable-style name to a rule, if it matches one of
/// the documented credential conventions. Returns `None` for unrelated
/// upper-case identifiers (DEBUG, CARGO_TERM_COLOR, etc.).
fn match_env_name(name: &str) -> Option<RuleId> {
    // Exact-match credentials.
    match name {
        "DATABASE_URL" | "PGPASSWORD" | "POSTGRES_PASSWORD" | "DB_PASSWORD" | "MYSQL_PWD" => {
            return Some(RuleId::DbPasswordEnv);
        }
        "API_KEY" => return Some(RuleId::ApiKey),
        "CLIENT_SECRET" => return Some(RuleId::ClientSecret),
        "WEBHOOK_SECRET" => return Some(RuleId::WebhookSecret),
        _ => {}
    }
    // Suffix patterns (whole-token).
    if name.ends_with("_TOKEN") && name.len() > "_TOKEN".len() {
        return Some(RuleId::GenericToken);
    }
    if name.ends_with("_SECRET") && name.len() > "_SECRET".len() {
        return Some(RuleId::GenericSecret);
    }
    None
}

/// Return true if `value` is an obvious placeholder, dummy, or template
/// stand-in. This is intentionally conservative: it does *not* accept
/// values just because they look short or contain `local`/`localhost`,
/// because real local credentials must still be flagged.
fn value_is_allowed_placeholder(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return true;
    }
    // Angle-bracket placeholder anywhere in the value: <user>, <password>,
    // <your-token>, etc. The angle-bracket convention is documented in
    // CONTRIBUTING.md.
    if has_angle_bracket_placeholder(trimmed) {
        return true;
    }
    // Whole-token presence of a dummy marker. Case-insensitive.
    let lower = trimmed.to_ascii_lowercase();
    const DUMMY_TOKENS: &[&str] = &[
        "dummy",
        "placeholder",
        "redacted",
        "example",
        "your-password",
        "your_password",
        "your-token",
        "your_token",
        "your-secret",
        "your_secret",
        "your-api-key",
        "your_api_key",
        "fake",
    ];
    for tok in DUMMY_TOKENS {
        if contains_whole_token(&lower, tok) {
            return true;
        }
    }
    // Sentinel-only values: `xxx`, `***`, `...`.
    if matches!(trimmed, "xxx" | "***" | "...") {
        return true;
    }
    // GitHub Actions secret references are indirections, not raw secret
    // values. This keeps workflow snippets such as
    // `GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}` from being treated as a leak.
    if trimmed.starts_with("${{") && trimmed.ends_with("}}") && trimmed.contains("secrets.") {
        return true;
    }
    false
}

fn has_angle_bracket_placeholder(value: &str) -> bool {
    // Detect `<word>` substrings: `<`, then >=1 non-`<>` byte, then `>`.
    let bytes = value.as_bytes();
    let n = bytes.len();
    let mut i = 0;
    while i < n {
        if bytes[i] == b'<' {
            let mut j = i + 1;
            while j < n && bytes[j] != b'>' && bytes[j] != b'<' {
                j += 1;
            }
            if j < n && bytes[j] == b'>' && j > i + 1 {
                return true;
            }
        }
        i += 1;
    }
    false
}

fn contains_whole_token(haystack: &str, needle: &str) -> bool {
    let bytes = haystack.as_bytes();
    let n = bytes.len();
    let m = needle.len();
    if m == 0 || m > n {
        return false;
    }
    let needle_bytes = needle.as_bytes();
    let mut i = 0;
    while i + m <= n {
        if &bytes[i..i + m] == needle_bytes {
            let before_ok = i == 0 || !is_token_byte(bytes[i - 1]);
            let after_ok = i + m == n || !is_token_byte(bytes[i + m]);
            if before_ok && after_ok {
                return true;
            }
        }
        i += 1;
    }
    false
}

/// "Token byte" for the *allowlist* token-boundary check. Underscore and
/// hyphen are intentionally NOT token bytes here: we want `dummy_password`
/// to count as containing the token `dummy` (the underscore acts as a
/// separator), while `dummysomething` is still rejected because the `s`
/// extends the token.
fn is_token_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric()
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    // sassi:allow-secret  -- the strings in this module are deliberate
    // fixtures. The allow-marker is honored for local scans; CI/public-text
    // scans don't honor it, which is exactly what the public-text tests
    // below exercise.

    use super::*;

    fn local() -> ScanConfig {
        ScanConfig::local()
    }
    fn public() -> ScanConfig {
        ScanConfig::public_text()
    }

    // -----------------------------------------------------------------------
    // DB_URL_WITH_CRED detection
    // -----------------------------------------------------------------------

    #[test]
    fn flags_postgres_url_with_password() {
        // sassi:allow-secret
        let line = "postgres://app:hunter2@localhost:5432/db";
        let hits = scan_line(line, public());
        assert!(
            hits.contains(&RuleId::DbUrlWithCred),
            "expected DB_URL_WITH_CRED, got {:?}",
            hits
        );
    }

    #[test]
    fn flags_mysql_url_with_password() {
        // sassi:allow-secret
        let line = "mysql://root:supers3cret@db.example.com/main";
        let hits = scan_line(line, public());
        assert!(hits.contains(&RuleId::DbUrlWithCred));
    }

    #[test]
    fn flags_mongodb_srv_url() {
        // sassi:allow-secret
        let line = "mongodb+srv://user:realpw@cluster.mongodb.net/app";
        let hits = scan_line(line, public());
        assert!(hits.contains(&RuleId::DbUrlWithCred));
    }

    #[test]
    fn flags_redis_url_with_password() {
        // sassi:allow-secret
        let line = "redis://:hunter2@redis.internal:6379/0";
        let hits = scan_line(line, public());
        assert!(
            hits.contains(&RuleId::DbUrlWithCred),
            "redis AUTH URLs commonly omit the username but still carry a password; got {:?}",
            hits
        );
    }

    #[test]
    fn allows_postgres_url_with_angle_bracket_placeholder() {
        let line = "postgres://<user>:<password>@<host>:<port>/<database>";
        let hits = scan_line(line, public());
        assert!(
            hits.is_empty(),
            "placeholder URL must be allowed, got {:?}",
            hits
        );
    }

    #[test]
    fn allows_postgres_url_with_dummy_password() {
        let line = "postgres://app:dummy@localhost/db";
        let hits = scan_line(line, public());
        assert!(hits.is_empty(), "dummy password must be allowed");
    }

    #[test]
    fn does_not_flag_postgres_url_without_credentials() {
        let line = "postgres://localhost:5432/db";
        let hits = scan_line(line, public());
        assert!(hits.is_empty(), "uncredentialed URL must not be flagged");
    }

    #[test]
    fn does_not_flag_redis_url_without_credentials() {
        let line = "redis://localhost:6379";
        let hits = scan_line(line, public());
        assert!(hits.is_empty(), "redis://localhost without creds is fine");
    }

    // -----------------------------------------------------------------------
    // AUTH_BEARING_URL detection (http(s))
    // -----------------------------------------------------------------------

    #[test]
    fn flags_https_url_with_basic_auth() {
        // sassi:allow-secret
        let line = "fetch https://svc:supers3cret@api.example.com/v1";
        let hits = scan_line(line, public());
        assert!(
            hits.contains(&RuleId::AuthBearingUrl),
            "https user:pass@ must be flagged, got {:?}",
            hits
        );
    }

    #[test]
    fn allows_https_url_with_dummy_basic_auth() {
        let line = "https://user:placeholder@api.example.com";
        let hits = scan_line(line, public());
        assert!(hits.is_empty());
    }

    #[test]
    fn does_not_flag_plain_https_url() {
        let line = "https://github.com/TarunvirBains/sassi/issues";
        let hits = scan_line(line, public());
        assert!(hits.is_empty());
    }

    // -----------------------------------------------------------------------
    // DB_PASSWORD_ENV detection
    // -----------------------------------------------------------------------

    #[test]
    fn flags_pgpassword_assignment() {
        // sassi:allow-secret
        let line = "PGPASSWORD=hunter2";
        let hits = scan_line(line, public());
        assert!(hits.contains(&RuleId::DbPasswordEnv));
    }

    #[test]
    fn flags_postgres_password_assignment() {
        // sassi:allow-secret
        let line = "POSTGRES_PASSWORD=verysecretpassword";
        let hits = scan_line(line, public());
        assert!(hits.contains(&RuleId::DbPasswordEnv));
    }

    #[test]
    fn flags_db_password_assignment() {
        // sassi:allow-secret
        let line = "DB_PASSWORD: realpassword";
        let hits = scan_line(line, public());
        assert!(hits.contains(&RuleId::DbPasswordEnv));
    }

    #[test]
    fn flags_database_url_assignment_with_url() {
        // sassi:allow-secret
        let line = "DATABASE_URL=postgres://app:hunter2@host/db";
        let hits = scan_line(line, public());
        // Both the env-style assignment AND the URL match; we expect at
        // least the env hit (the URL also matches).
        assert!(hits.contains(&RuleId::DbPasswordEnv));
    }

    #[test]
    fn allows_database_url_with_placeholder() {
        let line = "DATABASE_URL=<your-db-url>";
        let hits = scan_line(line, public());
        assert!(hits.is_empty(), "placeholder env must be allowed");
    }

    #[test]
    fn allows_pgpassword_with_placeholder() {
        let line = "PGPASSWORD=<your-password>";
        let hits = scan_line(line, public());
        assert!(hits.is_empty());
    }

    #[test]
    fn allows_empty_pgpassword() {
        let line = "PGPASSWORD=";
        let hits = scan_line(line, public());
        assert!(hits.is_empty());
    }

    #[test]
    fn allows_quoted_empty_pgpassword() {
        let line = "PGPASSWORD=\"\"";
        let hits = scan_line(line, public());
        assert!(hits.is_empty());
    }

    // -----------------------------------------------------------------------
    // GENERIC_TOKEN / GENERIC_SECRET / API_KEY / CLIENT_SECRET / WEBHOOK_SECRET
    // -----------------------------------------------------------------------

    #[test]
    fn flags_generic_token() {
        // sassi:allow-secret
        let line = "STRIPE_TOKEN=sk_live_abcd1234";
        let hits = scan_line(line, public());
        assert!(hits.contains(&RuleId::GenericToken));
    }

    #[test]
    fn flags_generic_secret() {
        // sassi:allow-secret
        let line = "DEPLOY_SECRET=verysecretvalue";
        let hits = scan_line(line, public());
        assert!(hits.contains(&RuleId::GenericSecret));
    }

    #[test]
    fn flags_api_key() {
        // sassi:allow-secret
        let line = "API_KEY=abcdef1234";
        let hits = scan_line(line, public());
        assert!(hits.contains(&RuleId::ApiKey));
    }

    #[test]
    fn flags_client_secret() {
        // sassi:allow-secret
        let line = "CLIENT_SECRET=xyz12345";
        let hits = scan_line(line, public());
        assert!(hits.contains(&RuleId::ClientSecret));
    }

    #[test]
    fn flags_webhook_secret() {
        // sassi:allow-secret
        let line = "WEBHOOK_SECRET=whsec_abcdef";
        let hits = scan_line(line, public());
        assert!(hits.contains(&RuleId::WebhookSecret));
    }

    #[test]
    fn allows_api_key_with_placeholder() {
        let line = "API_KEY=<your-api-key>";
        let hits = scan_line(line, public());
        assert!(hits.is_empty());
    }

    #[test]
    fn allows_client_secret_with_dummy() {
        let line = "CLIENT_SECRET=dummy_value";
        let hits = scan_line(line, public());
        assert!(hits.is_empty());
    }

    #[test]
    fn does_not_flag_bare_env_name_in_prose() {
        // The string `PGPASSWORD` appears in CONTRIBUTING.md / scanner source
        // as a bare identifier, not followed by `=value`. Confirm we do not
        // false-positive on bare mentions in prose.
        let line = "PGPASSWORD and POSTGRES_PASSWORD are example env names.";
        let hits = scan_line(line, public());
        assert!(
            hits.is_empty(),
            "bare env names in prose must not be flagged, got {:?}",
            hits
        );
    }

    #[test]
    fn does_not_flag_unrelated_uppercase_assignments() {
        // Ordinary CARGO_TERM_COLOR-style envs should pass through.
        let line = "CARGO_TERM_COLOR=always";
        let hits = scan_line(line, public());
        assert!(hits.is_empty());
    }

    #[test]
    fn does_not_flag_rust_path_call() {
        // `Type::method` has `::` and uppercase prefix; confirm we don't
        // false-positive as an env assignment.
        let line = "let v = MyType::CONSTANT;";
        let hits = scan_line(line, public());
        assert!(hits.is_empty());
    }

    // -----------------------------------------------------------------------
    // PRIVATE_KEY_BLOCK
    // -----------------------------------------------------------------------

    #[test]
    fn flags_pem_private_key_block() {
        let line = "-----BEGIN RSA PRIVATE KEY-----";
        let hits = scan_line(line, public());
        assert!(hits.contains(&RuleId::PrivateKeyBlock));
    }

    #[test]
    fn flags_openssh_private_key_block() {
        let line = "-----BEGIN OPENSSH PRIVATE KEY-----";
        let hits = scan_line(line, public());
        assert!(hits.contains(&RuleId::PrivateKeyBlock));
    }

    #[test]
    fn flags_generic_private_key_block() {
        let line = "-----BEGIN PRIVATE KEY-----";
        let hits = scan_line(line, public());
        assert!(hits.contains(&RuleId::PrivateKeyBlock));
    }

    #[test]
    fn allows_private_key_template_with_angle_brackets() {
        // A documentation template marker, not a real block.
        let line = "-----BEGIN <KIND> PRIVATE KEY-----";
        let hits = scan_line(line, public());
        assert!(hits.is_empty());
    }

    // -----------------------------------------------------------------------
    // sassi:allow-secret marker (local mode only)
    // -----------------------------------------------------------------------

    #[test]
    fn local_mode_honors_allow_marker() {
        // sassi:allow-secret
        let line = "PGPASSWORD=hunter2 # sassi:allow-secret";
        let hits = scan_line(line, local());
        assert!(hits.is_empty(), "local mode must honor allow marker");
    }

    #[test]
    fn public_mode_ignores_allow_marker() {
        // The submitter cannot opt out of public-text scanning.
        let line = "PGPASSWORD=hunter2 sassi:allow-secret"; // sassi:allow-secret
        let hits = scan_line(line, public());
        assert!(
            !hits.is_empty(),
            "public-text scan must not honor allow marker; got {:?}",
            hits
        );
    }

    // -----------------------------------------------------------------------
    // Redacted display
    // -----------------------------------------------------------------------

    #[test]
    fn redacted_display_does_not_contain_secrets() {
        let f = Finding {
            source: "issue.body".to_string(),
            line: Some(7),
            rule: RuleId::DbPasswordEnv,
        };
        let s = f.redacted_display();
        // The display is a fixed shape; the only dynamic content is the
        // source, line, rule id, and category; none of which can carry
        // the matched secret.
        assert!(s.contains("issue.body"));
        assert!(s.contains(":7:"));
        assert!(s.contains("DB_PASSWORD_ENV"));
        assert!(s.contains("database-env"));
        assert!(!s.to_ascii_lowercase().contains("hunter2"));
    }

    // -----------------------------------------------------------------------
    // Diff-mode parser
    // -----------------------------------------------------------------------

    #[test]
    fn diff_added_line_with_pgpassword_is_flagged() {
        // A minimal unified diff: one added line in foo/bar.txt at line 5.
        // sassi:allow-secret
        let diff = "\
diff --git a/foo/bar.txt b/foo/bar.txt
--- a/foo/bar.txt
+++ b/foo/bar.txt
@@ -4,0 +5,1 @@
+PGPASSWORD=hunter2
";
        let findings = scan_diff_text(diff, public());
        assert_eq!(
            findings.len(),
            1,
            "expected one finding, got {:?}",
            findings
        );
        assert_eq!(findings[0].source, "foo/bar.txt");
        assert_eq!(findings[0].line, Some(5));
        assert_eq!(findings[0].rule, RuleId::DbPasswordEnv);
    }

    #[test]
    fn diff_removed_line_with_password_is_not_flagged() {
        // sassi:allow-secret
        let diff = "\
diff --git a/foo/bar.txt b/foo/bar.txt
--- a/foo/bar.txt
+++ b/foo/bar.txt
@@ -4,1 +4,0 @@
-PGPASSWORD=hunter2
";
        let findings = scan_diff_text(diff, public());
        assert!(findings.is_empty(), "removed lines must not be flagged");
    }

    #[test]
    fn diff_multiple_files_and_hunks() {
        // sassi:allow-secret
        let diff = "\
diff --git a/a.env b/a.env
--- a/a.env
+++ b/a.env
@@ -0,0 +1,1 @@
+PGPASSWORD=hunter2
diff --git a/b.env b/b.env
--- a/b.env
+++ b/b.env
@@ -10,0 +11,1 @@
+API_KEY=realkey
";
        let findings = scan_diff_text(diff, public());
        assert_eq!(findings.len(), 2);
        assert_eq!(findings[0].source, "a.env");
        assert_eq!(findings[1].source, "b.env");
        assert_eq!(findings[1].line, Some(11));
    }

    #[test]
    fn diff_file_deletion_does_not_panic() {
        // `+++ /dev/null` indicates a file deletion; no added lines.
        let diff = "\
diff --git a/gone.env b/gone.env
deleted file mode 100644
--- a/gone.env
+++ /dev/null
@@ -1,1 +0,0 @@
-PGPASSWORD=hunter2
";
        let findings = scan_diff_text(diff, public());
        assert!(findings.is_empty());
    }

    // -----------------------------------------------------------------------
    // GitHub event extraction
    // -----------------------------------------------------------------------

    #[test]
    fn github_event_extracts_issue_body() {
        // sassi:allow-secret
        let payload = serde_json::json!({
            "action": "opened",
            "issue": {
                "title": "found it",
                "body": "please help: PGPASSWORD=hunter2 isn't working"
            }
        });
        let findings = scan_github_event_value(&payload);
        assert!(
            findings
                .iter()
                .any(|f| f.source == "issue.body" && f.rule == RuleId::DbPasswordEnv),
            "expected issue.body PGPASSWORD finding, got {:?}",
            findings
        );
    }

    #[test]
    fn github_event_extracts_comment_body() {
        // sassi:allow-secret
        let payload = serde_json::json!({
            "action": "created",
            "comment": {
                "body": "my creds: postgres://app:realpass@host/db"
            }
        });
        let findings = scan_github_event_value(&payload);
        assert!(
            findings
                .iter()
                .any(|f| f.source == "comment.body" && f.rule == RuleId::DbUrlWithCred),
            "expected comment.body finding, got {:?}",
            findings
        );
    }

    #[test]
    fn github_event_extracts_pr_title_and_body() {
        // sassi:allow-secret
        let payload = serde_json::json!({
            "action": "opened",
            "pull_request": {
                "title": "API_KEY=realkey leak",
                "body": "see DATABASE_URL=postgres://a:b@h/d in code"
            }
        });
        let findings = scan_github_event_value(&payload);
        assert!(
            findings
                .iter()
                .any(|f| f.source == "pull_request.title" && f.rule == RuleId::ApiKey),
            "expected pr title API_KEY finding, got {:?}",
            findings
        );
        assert!(
            findings
                .iter()
                .any(|f| f.source == "pull_request.body" && f.rule == RuleId::DbPasswordEnv),
            "expected pr body DATABASE_URL finding, got {:?}",
            findings
        );
    }

    #[test]
    fn github_event_extracts_review_body() {
        // sassi:allow-secret
        let payload = serde_json::json!({
            "action": "created",
            "review": {
                "body": "your CLIENT_SECRET=abcd1234 is wrong"
            }
        });
        let findings = scan_github_event_value(&payload);
        assert!(
            findings
                .iter()
                .any(|f| f.source == "review.body" && f.rule == RuleId::ClientSecret),
            "expected review.body CLIENT_SECRET finding, got {:?}",
            findings
        );
    }

    #[test]
    fn github_event_ignores_allow_marker_in_body() {
        // sassi:allow-secret
        let payload = serde_json::json!({
            "action": "opened",
            "issue": {
                "body": "PGPASSWORD=hunter2 # sassi:allow-secret"
            }
        });
        let findings = scan_github_event_value(&payload);
        assert!(
            findings.iter().any(|f| f.rule == RuleId::DbPasswordEnv),
            "public scan must not honor allow-marker"
        );
    }

    #[test]
    fn github_event_clean_body_has_no_findings() {
        let payload = serde_json::json!({
            "action": "opened",
            "issue": {
                "title": "Question about caching strategy",
                "body": "How should I configure Punnu for refresh on miss?"
            }
        });
        let findings = scan_github_event_value(&payload);
        assert!(findings.is_empty(), "clean body must not produce findings");
    }

    #[test]
    fn github_event_missing_fields_does_not_panic() {
        let payload = serde_json::json!({
            "action": "edited"
        });
        let findings = scan_github_event_value(&payload);
        assert!(findings.is_empty());
    }

    #[test]
    fn github_event_non_string_field_is_skipped() {
        // `body` set to a non-string (object). Real events always make
        // these strings, but we must not panic on malformed payloads.
        let payload = serde_json::json!({
            "issue": {
                "body": {"unexpected": "shape"}
            }
        });
        let findings = scan_github_event_value(&payload);
        assert!(findings.is_empty());
    }

    // -----------------------------------------------------------------------
    // Allowlist edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn placeholder_with_angle_brackets_in_middle_of_value() {
        let line = "DATABASE_URL=postgres://app:<your-password>@host/db";
        let hits = scan_line(line, public());
        // The URL has `<your-password>` inside the credential position;
        // this matches the angle-bracket placeholder rule and should be
        // allowed.
        assert!(
            hits.is_empty(),
            "angle-bracket inside URL must be allowed; got {:?}",
            hits
        );
    }

    #[test]
    fn whole_token_dummy_match_only() {
        // `dummysomething` is NOT a whole-token match for `dummy`, but
        // `dummy_password` IS. Conservative behavior keeps `dummysomething`
        // flagged so we don't accidentally allow real-looking values
        // that happen to begin with `dummy`.
        // sassi:allow-secret
        let line = "API_KEY=dummysomething";
        let hits = scan_line(line, public());
        // `dummy` appears at the start but is not a separate token; not
        // allowed.
        assert!(
            hits.contains(&RuleId::ApiKey),
            "non-whole-token dummy prefix must still be flagged; got {:?}",
            hits
        );
    }

    #[test]
    fn dummy_underscored_value_is_allowed() {
        let line = "API_KEY=dummy_key";
        let hits = scan_line(line, public());
        assert!(hits.is_empty(), "dummy_key must be allowed");
    }

    #[test]
    fn xxx_sentinel_value_is_allowed() {
        let line = "API_KEY=xxx";
        let hits = scan_line(line, public());
        assert!(hits.is_empty());
    }

    #[test]
    fn redacted_sentinel_value_is_allowed() {
        let line = "API_KEY=REDACTED";
        let hits = scan_line(line, public());
        assert!(hits.is_empty());
    }

    #[test]
    fn github_actions_secret_expression_is_allowed() {
        let line = "GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}";
        let hits = scan_line(line, public());
        assert!(
            hits.is_empty(),
            "GitHub secret references are not raw values"
        );
    }

    // -----------------------------------------------------------------------
    // Diff parsing - line cursor edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn hunk_with_only_added_lines() {
        // `@@ -0,0 +1,3 @@` - pure-insert at the top of a new file.
        // sassi:allow-secret
        let diff = "\
diff --git a/new.env b/new.env
--- /dev/null
+++ b/new.env
@@ -0,0 +1,3 @@
+# comment
+PGPASSWORD=hunter2
+
";
        let findings = scan_diff_text(diff, public());
        assert_eq!(findings.len(), 1);
        // Line 2 because line 1 is the comment and the cursor starts at 1.
        assert_eq!(findings[0].line, Some(2));
        assert_eq!(findings[0].source, "new.env");
    }

    // -----------------------------------------------------------------------
    // File-level marker
    // -----------------------------------------------------------------------

    #[test]
    fn file_level_marker_detected_in_first_30_lines() {
        let text = "//! header\n// sassi:allow-secret-file\nfn x() {}\n";
        assert!(text_has_file_level_marker(text));
    }

    #[test]
    fn file_level_marker_buried_past_cap_is_ignored() {
        // The marker must appear within the first 60 lines to count. A
        // marker buried deeper is treated as inert: the scanner's intent
        // is to keep the marker visible to a reviewer opening the file.
        let mut text = String::new();
        for _ in 0..80 {
            text.push_str("// filler\n");
        }
        text.push_str("// sassi:allow-secret-file\n");
        assert!(
            !text_has_file_level_marker(&text),
            "marker buried past the 60-line cap must not count"
        );
    }

    #[test]
    fn file_without_marker_is_not_skipped() {
        let text = "//! header\nfn x() {}\n";
        assert!(!text_has_file_level_marker(text));
    }

    #[test]
    fn path_scan_skip_list_does_not_skip_github_workflows() {
        assert!(!should_skip_dir(".github"));
        assert!(should_skip_dir(".git"));
        assert!(should_skip_dir("target"));
    }

    #[test]
    fn public_text_scan_does_not_honor_file_marker() {
        // A submitter who pastes "sassi:allow-secret-file" at the top of
        // their issue body must NOT silence the scanner: scan_text_into
        // does not consult the file-level marker at all, and the
        // public-text scanner uses scan_text_into directly.
        // sassi:allow-secret
        let body = "// sassi:allow-secret-file\nPGPASSWORD=hunter2\n";
        let payload = serde_json::json!({
            "issue": { "body": body }
        });
        let findings = scan_github_event_value(&payload);
        assert!(
            findings.iter().any(|f| f.rule == RuleId::DbPasswordEnv),
            "public scan must still flag credentials despite a file-marker-shaped\
             string in the body; got {:?}",
            findings
        );
    }
}
