//! `cargo xtask check-test-surface`
//!
//! Scans repo-owned test surfaces for patterns that hide test failures:
//!
//! * Rust `#[ignore]` attributes (including whitespace variants) in `src/`,
//!   `tests/`, `benches/`, and `examples/`.
//! * `--ignored` / `--include-ignored` / quarantine-list patterns in
//!   `.github/workflows/*.yml` (YAML comments are skipped).
//!
//! Comments, string literals, and raw-string literals are stripped from Rust
//! source before scanning so that rustdoc `ignore` fences and prose do not
//! trigger false positives.

use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Run the check from `workspace_root`.  Returns 0 on success, 1 if any
/// violations are found.
pub fn run(workspace_root: &Path) -> i32 {
    let mut violations: Vec<(PathBuf, usize, String)> = Vec::new();

    // --- Rust files --------------------------------------------------------
    let rust_files = collect_rust_files(workspace_root);
    for path in &rust_files {
        scan_rust_file(path, &mut violations);
    }

    // --- GitHub Actions workflow files -------------------------------------
    let workflow_dir = workspace_root.join(".github/workflows");
    let workflow_count = count_workflow_files(&workflow_dir);
    scan_workflow_dir(&workflow_dir, &mut violations);

    if violations.is_empty() {
        println!(
            "check-test-surface: OK ({} Rust file(s), {} workflow file(s) scanned)",
            rust_files.len(),
            workflow_count,
        );
        0
    } else {
        for (path, line, msg) in &violations {
            eprintln!("{}:{}: {}", path.display(), line, msg);
        }
        eprintln!(
            "\ncheck-test-surface: {} violation(s) found",
            violations.len()
        );
        1
    }
}

// ---------------------------------------------------------------------------
// File collection
// ---------------------------------------------------------------------------

fn collect_rust_files(workspace_root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    walk_for_rust(workspace_root, workspace_root, &mut files);
    files.sort();
    files
}

/// Recursively collect `.rs` files that live under a `src/`, `tests/`,
/// `benches/`, or `examples/` path component.  Skips `target/` and hidden
/// directories.
fn walk_for_rust(workspace_root: &Path, dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name.starts_with('.') || name == "target" {
            continue;
        }
        if path.is_dir() {
            walk_for_rust(workspace_root, &path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs")
            && let Ok(rel) = path.strip_prefix(workspace_root)
        {
            let in_scan_root = rel.components().any(|c| {
                matches!(
                    c.as_os_str().to_str(),
                    Some("src" | "tests" | "benches" | "examples")
                )
            });
            if in_scan_root {
                out.push(path);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Rust file scanning
// ---------------------------------------------------------------------------

fn scan_rust_file(path: &Path, violations: &mut Vec<(PathBuf, usize, String)>) {
    let Ok(source) = std::fs::read_to_string(path) else {
        return;
    };
    let stripped = strip_rust_noise(&source);
    find_rust_violations(path, &stripped, violations);
}

/// Strip line comments, block comments (with nesting), regular string
/// literals, byte-string literals, and raw-string literals from Rust source.
///
/// Newlines are preserved so that line numbers in the returned string match
/// those in the original source.  All stripped bytes are replaced with ASCII
/// space (`0x20`).
pub fn strip_rust_noise(source: &str) -> String {
    let bytes = source.as_bytes();
    let n = bytes.len();
    let mut out = Vec::with_capacity(n);
    let mut i = 0usize;

    // Replace a byte with space, but keep newlines so line numbers stay valid.
    #[inline]
    fn blank(b: u8) -> u8 {
        if b == b'\n' { b'\n' } else { b' ' }
    }

    while i < n {
        // --------------------------------------------------------------------
        // Raw string literals: r"...", r#"..."#, br"...", br#"..."#, cr"...", etc.
        // --------------------------------------------------------------------
        let raw_pfx: usize = if i + 1 < n && matches!((bytes[i], bytes[i + 1]), (b'b' | b'c', b'r'))
        {
            2
        } else if bytes[i] == b'r' {
            1
        } else {
            0
        };

        if raw_pfx > 0 {
            let after = i + raw_pfx;
            if after < n && (bytes[after] == b'#' || bytes[after] == b'"') {
                // Count leading '#'s
                let mut h = 0usize;
                while after + h < n && bytes[after + h] == b'#' {
                    h += 1;
                }
                if after + h < n && bytes[after + h] == b'"' {
                    // Confirmed raw string - blank the opening delimiter.
                    let open_end = after + h + 1;
                    for byte in bytes.iter().take(open_end).skip(i) {
                        out.push(blank(*byte));
                    }
                    i = open_end;
                    // Scan for the closing '"' followed by exactly `h` '#'s.
                    'raw: loop {
                        if i >= n {
                            break 'raw;
                        }
                        if bytes[i] == b'"' {
                            let mut ok = true;
                            for hh in 0..h {
                                if i + 1 + hh >= n || bytes[i + 1 + hh] != b'#' {
                                    ok = false;
                                    break;
                                }
                            }
                            if ok {
                                let close_end = i + 1 + h;
                                for byte in bytes.iter().take(close_end).skip(i) {
                                    out.push(blank(*byte));
                                }
                                i = close_end;
                                break 'raw;
                            }
                        }
                        out.push(blank(bytes[i]));
                        i += 1;
                    }
                    continue;
                }
            }
        }

        // --------------------------------------------------------------------
        // Byte string literals: b"..."
        // Handles b"..." only; b'x' (byte char) passes through (no false-
        // positive risk - #[ignore] cannot fit in a single char literal).
        // --------------------------------------------------------------------
        if bytes[i] == b'b' && i + 1 < n && bytes[i + 1] == b'"' {
            out.push(b' '); // blank 'b'
            out.push(b' '); // blank '"'
            i += 2;
            loop {
                if i >= n {
                    break;
                }
                if bytes[i] == b'\\' {
                    out.push(b' ');
                    i += 1;
                    if i < n {
                        out.push(blank(bytes[i]));
                        i += 1;
                    }
                } else if bytes[i] == b'"' {
                    out.push(b' ');
                    i += 1;
                    break;
                } else {
                    out.push(blank(bytes[i]));
                    i += 1;
                }
            }
            continue;
        }

        // --------------------------------------------------------------------
        // C-string literals (Rust 1.77+): c"..."
        // --------------------------------------------------------------------
        if bytes[i] == b'c' && i + 1 < n && bytes[i + 1] == b'"' {
            out.push(b' '); // blank 'c'
            out.push(b' '); // blank '"'
            i += 2;
            loop {
                if i >= n {
                    break;
                }
                if bytes[i] == b'\\' {
                    out.push(b' ');
                    i += 1;
                    if i < n {
                        out.push(blank(bytes[i]));
                        i += 1;
                    }
                } else if bytes[i] == b'"' {
                    out.push(b' ');
                    i += 1;
                    break;
                } else {
                    out.push(blank(bytes[i]));
                    i += 1;
                }
            }
            continue;
        }

        // --------------------------------------------------------------------
        // Char literals: 'x'  and  '\x'
        //
        // Must be handled BEFORE the regular-string check so that `'"'`
        // (a char literal whose value is `"`) does not get mistaken for the
        // start of a string literal.  Also covers byte-char variants: the
        // preceding `b` byte passes through normally, then `'"'` is caught
        // here before the `"` can open a spurious string.
        //
        // We only handle the two patterns that appear in practice:
        //   'x'   - single non-backslash char  (3 bytes)
        //   '\x'  - single escaped char        (4 bytes, covers \n \t \\ \' \0 ...)
        // Longer escapes (\xNN, \u{...}) are uncommon and fall through; they
        // pass through as-is without risk of a false-positive `#[ignore]`
        // match.  Lifetime annotations ('a, 'static) also fall through.
        // --------------------------------------------------------------------
        if bytes[i] == b'\'' {
            // 'x' - non-escaped single char literal
            if i + 2 < n && bytes[i + 1] != b'\\' && bytes[i + 2] == b'\'' {
                out.push(b' ');
                out.push(blank(bytes[i + 1]));
                out.push(b' ');
                i += 3;
                continue;
            }
            // '\x' - escaped char literal (e.g. '\n', '\\', '\'', '\0')
            if i + 3 < n && bytes[i + 1] == b'\\' && bytes[i + 3] == b'\'' {
                out.push(b' ');
                out.push(b' ');
                out.push(blank(bytes[i + 2]));
                out.push(b' ');
                i += 4;
                continue;
            }
            // Lifetime annotation or longer escape - pass through.
            out.push(bytes[i]);
            i += 1;
            continue;
        }

        // --------------------------------------------------------------------
        // Regular string literals: "..."
        // --------------------------------------------------------------------
        if bytes[i] == b'"' {
            out.push(b' ');
            i += 1;
            loop {
                if i >= n {
                    break;
                }
                if bytes[i] == b'\\' {
                    out.push(b' ');
                    i += 1;
                    if i < n {
                        out.push(blank(bytes[i]));
                        i += 1;
                    }
                } else if bytes[i] == b'"' {
                    out.push(b' ');
                    i += 1;
                    break;
                } else {
                    out.push(blank(bytes[i]));
                    i += 1;
                }
            }
            continue;
        }

        // --------------------------------------------------------------------
        // Line comments: // ...  (includes //!, ///, //!!, ///!)
        // --------------------------------------------------------------------
        if bytes[i] == b'/' && i + 1 < n && bytes[i + 1] == b'/' {
            out.push(b' ');
            out.push(b' ');
            i += 2;
            while i < n && bytes[i] != b'\n' {
                out.push(b' ');
                i += 1;
            }
            // Leave the '\n' for the main loop to output naturally.
            continue;
        }

        // --------------------------------------------------------------------
        // Block comments: /* ... */  (Rust supports nested block comments)
        // --------------------------------------------------------------------
        if bytes[i] == b'/' && i + 1 < n && bytes[i + 1] == b'*' {
            let mut depth = 1usize;
            out.push(b' ');
            out.push(b' ');
            i += 2;
            while i < n && depth > 0 {
                if bytes[i] == b'/' && i + 1 < n && bytes[i + 1] == b'*' {
                    depth += 1;
                    out.push(b' ');
                    out.push(b' ');
                    i += 2;
                } else if bytes[i] == b'*' && i + 1 < n && bytes[i + 1] == b'/' {
                    depth -= 1;
                    out.push(b' ');
                    out.push(b' ');
                    i += 2;
                } else {
                    out.push(blank(bytes[i]));
                    i += 1;
                }
            }
            continue;
        }

        // --------------------------------------------------------------------
        // Pass everything else through unchanged.
        // --------------------------------------------------------------------
        out.push(bytes[i]);
        i += 1;
    }

    // SAFETY: we only emit bytes that appeared verbatim in the original
    // UTF-8 source or ASCII space/newline, so the result is always valid UTF-8.
    String::from_utf8(out).unwrap_or_default()
}

/// Scan stripped Rust source for banned `#[ignore]` attributes.
///
/// The scan is position-aware: it reports the 1-based line number of each
/// violation in `path`.
pub fn find_rust_violations(
    path: &Path,
    stripped: &str,
    violations: &mut Vec<(PathBuf, usize, String)>,
) {
    let bytes = stripped.as_bytes();
    let n = bytes.len();
    let mut i = 0usize;
    let mut line = 1usize;

    while i < n {
        if bytes[i] == b'\n' {
            line += 1;
            i += 1;
            continue;
        }

        if bytes[i] == b'#' {
            let attr_line = line;
            let mut j = i + 1;

            // Skip optional whitespace between '#' and '['
            while j < n && is_ws(bytes[j]) {
                j += 1;
            }

            if j < n && bytes[j] == b'[' {
                let Some(attr_end) = find_attribute_end(bytes, j) else {
                    i += 1;
                    continue;
                };
                let attr = &stripped[j + 1..attr_end];
                if attr_starts_with_ident(attr, "ignore")
                    || (attr_starts_with_ident(attr, "cfg_attr")
                        && cfg_attr_payload_contains_ident(attr, "ignore"))
                {
                    violations.push((
                        path.to_path_buf(),
                        attr_line,
                        "found #[ignore] attribute - \
                         no test may be quarantined or skipped via #[ignore]"
                            .to_string(),
                    ));
                }
            }
        }

        i += 1;
    }
}

fn find_attribute_end(bytes: &[u8], start: usize) -> Option<usize> {
    debug_assert_eq!(bytes.get(start), Some(&b'['));

    let mut depth = 0usize;
    let mut cursor = start;

    while cursor < bytes.len() {
        match bytes[cursor] {
            b'[' => depth += 1,
            b']' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(cursor);
                }
            }
            _ => {}
        }
        cursor += 1;
    }

    None
}

fn attr_starts_with_ident(attr: &str, ident: &str) -> bool {
    let bytes = attr.as_bytes();
    let start = skip_ascii_whitespace(bytes, 0);

    starts_with(bytes, start, ident.as_bytes())
        && !bytes
            .get(start + ident.len())
            .is_some_and(|byte| is_ident_byte(*byte))
}

fn cfg_attr_payload_contains_ident(attr: &str, ident: &str) -> bool {
    let Some(open_paren) = attr.find('(') else {
        return false;
    };
    let mut depth = 0usize;

    for (relative_index, byte) in attr.as_bytes()[open_paren + 1..].iter().enumerate() {
        match *byte {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth = depth.saturating_sub(1),
            b',' if depth == 0 => {
                let payload_start = open_paren + 1 + relative_index + 1;
                return contains_identifier(&attr[payload_start..], ident);
            }
            _ => {}
        }
    }

    false
}

fn contains_identifier(source: &str, ident: &str) -> bool {
    let mut offset = 0;

    while let Some(relative_index) = source[offset..].find(ident) {
        let index = offset + relative_index;
        let before = index.checked_sub(1).and_then(|i| source.as_bytes().get(i));
        let after_index = index + ident.len();
        let after = source.as_bytes().get(after_index);

        if !before.is_some_and(|byte| is_ident_byte(*byte))
            && !after.is_some_and(|byte| is_ident_byte(*byte))
        {
            return true;
        }

        offset = after_index;
    }

    false
}

fn skip_ascii_whitespace(bytes: &[u8], mut cursor: usize) -> usize {
    while bytes.get(cursor).is_some_and(|byte| is_ws(*byte)) {
        cursor += 1;
    }
    cursor
}

fn is_ws(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r')
}

fn is_ident_byte(byte: u8) -> bool {
    byte == b'_' || byte.is_ascii_alphanumeric()
}

fn starts_with(bytes: &[u8], index: usize, needle: &[u8]) -> bool {
    bytes.get(index..index + needle.len()) == Some(needle)
}

// ---------------------------------------------------------------------------
// Workflow file scanning
// ---------------------------------------------------------------------------

fn count_workflow_files(workflow_dir: &Path) -> usize {
    std::fs::read_dir(workflow_dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| {
            let name = e.file_name();
            let s = name.to_string_lossy();
            s.ends_with(".yml") || s.ends_with(".yaml")
        })
        .count()
}

fn scan_workflow_dir(workflow_dir: &Path, violations: &mut Vec<(PathBuf, usize, String)>) {
    let Ok(entries) = std::fs::read_dir(workflow_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if (name.ends_with(".yml") || name.ends_with(".yaml"))
            && let Ok(content) = std::fs::read_to_string(&path)
        {
            find_workflow_violations(&path, &content, violations);
        }
    }
}

/// Banned patterns in workflow command lines.
///
/// * `--ignored` / `--include-ignored` - `cargo test` flags that re-enable
///   tests marked `#[ignore]`.
/// * `run-ignored` - custom skip-lane naming that can hide ignored tests.
/// * `quarantine` - quarantine list/naming patterns that exclude tests.
const WORKFLOW_BANNED: &[&str] = &[
    "--ignored",
    "--include-ignored",
    "run-ignored",
    "quarantine",
];

/// Scan workflow file `content` for banned patterns on active (non-comment)
/// lines.
///
/// YAML comment lines (trimmed first non-whitespace character is `#`) and
/// inline YAML comments (` #` suffix) are stripped before matching.
pub fn find_workflow_violations(
    path: &Path,
    content: &str,
    violations: &mut Vec<(PathBuf, usize, String)>,
) {
    for (idx, line) in content.lines().enumerate() {
        let line_num = idx + 1;

        // Skip pure YAML comment lines.
        if line.trim_start().starts_with('#') {
            continue;
        }

        // Strip inline YAML comments: ' #' marks the start of an inline
        // comment per the YAML spec (section6.8.1 - # must be preceded by whitespace).
        let active = strip_yaml_inline_comment(line);

        for &pattern in WORKFLOW_BANNED {
            if workflow_line_has_pattern(active, pattern) {
                violations.push((
                    path.to_path_buf(),
                    line_num,
                    format!(
                        "workflow active line contains banned test-quarantine pattern {:?}",
                        pattern
                    ),
                ));
            }
        }
    }
}

/// Strip the inline YAML comment from a line, returning the active portion.
///
/// A ` #` sequence (space + `#`) indicates the start of an inline comment
/// per the YAML specification.
fn strip_yaml_inline_comment(line: &str) -> &str {
    if let Some(pos) = line.find(" #") {
        &line[..pos]
    } else {
        line
    }
}

/// Return `true` if `line` contains `pattern` in a way that constitutes a
/// violation.
///
/// * For flag patterns (`--ignored`, `--include-ignored`): simple substring
///   match - the `--` prefix already anchors against most false positives.
/// * For `quarantine`: case-insensitive whole-word match.
fn workflow_line_has_pattern(line: &str, pattern: &str) -> bool {
    if pattern == "quarantine" {
        // Case-insensitive whole-word search.
        let lower = line.to_ascii_lowercase();
        let pat_len = pattern.len();
        let lb = lower.as_bytes();
        let mut start = 0usize;
        while start + pat_len <= lb.len() {
            if let Some(rel) = lower[start..].find(pattern) {
                let abs = start + rel;
                let before_ok = abs == 0
                    || !matches!(lb[abs - 1], b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_');
                let after_ok = abs + pat_len >= lb.len()
                    || !matches!(
                        lb[abs + pat_len],
                        b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_'
                    );
                if before_ok && after_ok {
                    return true;
                }
                start = abs + 1;
            } else {
                break;
            }
        }
        false
    } else {
        line.contains(pattern)
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn rust_violations(src: &str) -> Vec<(PathBuf, usize, String)> {
        let stripped = strip_rust_noise(src);
        let mut v = Vec::new();
        find_rust_violations(Path::new("test.rs"), &stripped, &mut v);
        v
    }

    fn workflow_violations(content: &str) -> Vec<(PathBuf, usize, String)> {
        let mut v = Vec::new();
        find_workflow_violations(Path::new("ci.yml"), content, &mut v);
        v
    }

    // -----------------------------------------------------------------------
    // Rust: #[ignore] detection
    // -----------------------------------------------------------------------

    #[test]
    fn rejects_plain_ignore_attr() {
        let src = "#[ignore]\nfn test_foo() {}";
        assert!(
            !rust_violations(src).is_empty(),
            "plain #[ignore] must be flagged"
        );
    }

    #[test]
    fn rejects_ignore_attr_with_inner_spaces() {
        let src = "#[ ignore ]\nfn test_foo() {}";
        assert!(
            !rust_violations(src).is_empty(),
            "#[ ignore ] (whitespace variant) must be flagged"
        );
    }

    #[test]
    fn rejects_ignore_attr_with_outer_space() {
        // Unusual but syntactically valid in Rust.
        let src = "# [ignore]\nfn test_foo() {}";
        assert!(
            !rust_violations(src).is_empty(),
            "# [ignore] (space before '[') must be flagged"
        );
    }

    #[test]
    fn rejects_ignore_with_reason() {
        // #[ignore = "reason"] is also banned.
        let src = "#[ignore = \"flaky on CI\"]\nfn test_foo() {}";
        assert!(
            !rust_violations(src).is_empty(),
            "#[ignore = \"...\"] must be flagged"
        );
    }

    #[test]
    fn rejects_multiline_ignore_attr() {
        let src = "#[\n    ignore\n]\nfn test_foo() {}";
        let v = rust_violations(src);
        assert_eq!(v.len(), 1, "multiline #[ignore] must be flagged");
        assert_eq!(v[0].1, 1);
    }

    #[test]
    fn rejects_cfg_attr_ignore_attr() {
        let src = "#[cfg_attr(feature = \"slow-tests\", ignore)]\nfn test_foo() {}";
        let v = rust_violations(src);
        assert_eq!(v.len(), 1, "cfg_attr(..., ignore) must be flagged");
        assert_eq!(v[0].1, 1);
    }

    // -----------------------------------------------------------------------
    // Rust: allowed attributes must not be flagged
    // -----------------------------------------------------------------------

    #[test]
    fn allows_should_panic() {
        let src = "#[should_panic(expected = \"expected panic\")]\nfn test_foo() { panic!(\"expected panic\"); }";
        assert!(
            rust_violations(src).is_empty(),
            "#[should_panic] must not be flagged"
        );
    }

    #[test]
    fn allows_ignore_prefix_attribute() {
        // #[ignore_something] must NOT be flagged (not a whole-word match).
        let src = "#[ignore_deadlock]\nfn test_foo() {}";
        assert!(
            rust_violations(src).is_empty(),
            "#[ignore_prefix] must not be flagged"
        );
    }

    #[test]
    fn allows_test_attribute() {
        let src = "#[test]\nfn test_foo() { assert!(true); }";
        assert!(
            rust_violations(src).is_empty(),
            "#[test] must not be flagged"
        );
    }

    #[test]
    fn allows_cfg_test() {
        let src = "#[cfg(test)]\nmod tests {}";
        assert!(
            rust_violations(src).is_empty(),
            "#[cfg(test)] must not be flagged"
        );
    }

    // -----------------------------------------------------------------------
    // Rust: comments and prose must be stripped (no false positives)
    // -----------------------------------------------------------------------

    #[test]
    fn ignores_ignore_in_line_comment() {
        let src = "// #[ignore] - was disabled, now removed\nfn foo() {}";
        assert!(
            rust_violations(src).is_empty(),
            "#[ignore] inside // comment must not be flagged"
        );
    }

    #[test]
    fn ignores_ignore_in_block_comment() {
        let src = "/* #[ignore] */\nfn foo() {}";
        assert!(
            rust_violations(src).is_empty(),
            "#[ignore] inside /* */ comment must not be flagged"
        );
    }

    #[test]
    fn ignores_ignore_in_doc_comment() {
        // Doc-comment with rustdoc ignore fence - the canonical false-positive
        // case the spec calls out explicitly.
        let src = "/// ```ignore\n/// let x = 1;\n/// ```\nfn documented() {}";
        assert!(
            rust_violations(src).is_empty(),
            "rustdoc ```ignore fence must not be flagged"
        );
    }

    #[test]
    fn ignores_ignore_in_nested_block_comment() {
        let src = "/* outer /* #[ignore] */ still comment */\nfn foo() {}";
        assert!(
            rust_violations(src).is_empty(),
            "#[ignore] inside nested block comment must not be flagged"
        );
    }

    // -----------------------------------------------------------------------
    // Rust: string and raw-string literals must be stripped
    // -----------------------------------------------------------------------

    #[test]
    fn ignores_ignore_in_string_literal() {
        let src = "let s = \"#[ignore]\";";
        assert!(
            rust_violations(src).is_empty(),
            "#[ignore] inside string literal must not be flagged"
        );
    }

    #[test]
    fn ignores_ignore_in_raw_string_literal() {
        let src = "let s = r#\"#[ignore]\"#;";
        assert!(
            rust_violations(src).is_empty(),
            "#[ignore] inside r#\"...\"# must not be flagged"
        );
    }

    #[test]
    fn ignores_ignore_in_multiline_raw_string() {
        let src = "let s = r##\"\n#[ignore]\n\"##;";
        assert!(
            rust_violations(src).is_empty(),
            "#[ignore] inside multi-line r##\"...\"## must not be flagged"
        );
    }

    #[test]
    fn ignores_ignore_in_byte_string_literal() {
        let src = "let s = b\"#[ignore]\";";
        assert!(
            rust_violations(src).is_empty(),
            "#[ignore] inside b\"...\" must not be flagged"
        );
    }

    // -----------------------------------------------------------------------
    // Rust: line numbers reported correctly
    // -----------------------------------------------------------------------

    #[test]
    fn reports_correct_line_number() {
        let src = "fn before() {}\n#[ignore]\nfn test_foo() {}";
        let v = rust_violations(src);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].1, 2, "line number should be 2");
    }

    // -----------------------------------------------------------------------
    // Workflow: banned flags
    // -----------------------------------------------------------------------

    #[test]
    fn rejects_workflow_ignored_flag() {
        let content = "      run: cargo test --ignored\n";
        let v = workflow_violations(content);
        assert!(
            !v.is_empty(),
            "--ignored on an active workflow line must be flagged"
        );
    }

    #[test]
    fn rejects_workflow_include_ignored_flag() {
        let content = "      run: cargo test --include-ignored\n";
        let v = workflow_violations(content);
        assert!(
            !v.is_empty(),
            "--include-ignored on an active workflow line must be flagged"
        );
    }

    #[test]
    fn rejects_workflow_run_ignored_pattern() {
        let content = "      run: cargo xtask run-ignored\n";
        let v = workflow_violations(content);
        assert!(
            !v.is_empty(),
            "run-ignored on an active workflow line must be flagged"
        );
    }

    #[test]
    fn rejects_workflow_quarantine_pattern() {
        let content = "      run: cargo test --skip-list quarantine.txt\n";
        let v = workflow_violations(content);
        assert!(
            !v.is_empty(),
            "quarantine pattern on an active workflow line must be flagged"
        );
    }

    // -----------------------------------------------------------------------
    // Workflow: YAML comments must be ignored
    // -----------------------------------------------------------------------

    #[test]
    fn allows_yaml_comment_line_with_ignored() {
        // Pure YAML comment line - must not be flagged.
        let content =
            "      # cargo test --ignored would re-run skipped tests\n      run: cargo test\n";
        let v = workflow_violations(content);
        assert!(
            v.is_empty(),
            "--ignored inside a YAML comment line must not be flagged"
        );
    }

    #[test]
    fn allows_yaml_inline_comment_with_ignored() {
        // Inline YAML comment on an otherwise-clean run line.
        let content = "      run: cargo test  # previously used --ignored here\n";
        let v = workflow_violations(content);
        assert!(
            v.is_empty(),
            "--ignored inside an inline YAML comment must not be flagged"
        );
    }

    #[test]
    fn allows_yaml_comment_with_quarantine() {
        let content = "      # old quarantine list removed 2025-01\n      run: cargo test\n";
        let v = workflow_violations(content);
        assert!(
            v.is_empty(),
            "quarantine inside a YAML comment must not be flagged"
        );
    }

    // -----------------------------------------------------------------------
    // strip_rust_noise: sanity checks
    // -----------------------------------------------------------------------

    #[test]
    fn strip_preserves_newlines() {
        let src = "fn a() {}\n// comment\nfn b() {}\n";
        let stripped = strip_rust_noise(src);
        assert_eq!(
            src.lines().count(),
            stripped.lines().count(),
            "line count must be preserved after stripping"
        );
    }

    #[test]
    fn strip_blanks_line_comment_content() {
        let src = "// #[ignore]";
        let stripped = strip_rust_noise(src);
        assert!(
            !stripped.contains('#'),
            "# should be blanked inside a line comment"
        );
    }

    #[test]
    fn strip_blanks_block_comment_content() {
        let src = "/* #[ignore] */";
        let stripped = strip_rust_noise(src);
        assert!(
            !stripped.contains('#'),
            "# should be blanked inside a block comment"
        );
    }

    #[test]
    fn strip_blanks_string_literal_content() {
        let src = "let s = \"#[ignore]\";";
        let stripped = strip_rust_noise(src);
        // The '#' inside the string should be gone; 's', '=', ';' must remain.
        assert!(!stripped.contains('#'));
        assert!(stripped.contains("let s ="));
        assert!(stripped.contains(';'));
    }

    #[test]
    fn strip_char_literal_double_quote_does_not_open_string() {
        // b'"' (a byte-char literal whose value is `"`) must not corrupt the
        // scanner state.  Without char-literal handling, the `"` inside `'"'`
        // would be interpreted as the opening of a string literal and blank
        // everything until the next `"`, causing subsequent real string
        // literals to be missed and #[ignore] inside them to be falsely flagged.
        let src = "let q = '\"'; let s = \"#[ignore]\"; let b = b'\"'; done();";
        let stripped = strip_rust_noise(src);
        assert!(
            !stripped.contains('#'),
            "# inside the string after char literal '\"' must be blanked: {stripped:?}"
        );
        // The structural code outside literals must survive.
        assert!(stripped.contains("let q ="));
        assert!(stripped.contains("let s ="));
        assert!(stripped.contains("done()"));
    }

    #[test]
    fn strip_escaped_char_literal_does_not_confuse_scanner() {
        // '\n', '\\' etc. must be treated as char literals (4 bytes), not as
        // the start of a string.
        let src = "let c = '\\n'; let s = \"#[ignore]\";";
        let stripped = strip_rust_noise(src);
        assert!(
            !stripped.contains('#'),
            "# inside string after escaped char literal must be blanked"
        );
    }

    #[test]
    fn strip_backslash_continuation_string() {
        // A string with `\<newline>` line continuation must have ALL content
        // blanked, including any `#[ignore]` on the continuation line.
        let src =
            "push(\"found #[ignore] attribute \\\n         no test via #[ignore]\".to_string());";
        let stripped = strip_rust_noise(src);
        assert!(
            !stripped.contains('#'),
            "# inside backslash-continuation string must be blanked: {stripped:?}"
        );
    }
}
