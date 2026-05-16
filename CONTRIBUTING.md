# Contributing to Sassi

Thanks for taking time to contribute. This document captures the workflow
expectations that apply to every change: local hygiene, the public-text
guard, and how the repo's checks fit together.

If anything here is unclear, please open an issue (the next section
explains how to avoid leaking credentials when you do).

## Do not paste credentials into public text

Sassi has zero use for your real connection string, your real API tokens,
your real client secrets, or your private keys. Pasting any of those into
a public GitHub issue, PR description, issue comment, or PR review comment
exposes them to anyone who can read the repo, including search engines and
future leak datasets. **Treat any value pasted into a public GitHub text
field as compromised.**

### What to do instead

Use placeholders. The repo's convention is angle-bracket placeholders for
identity components:

```text
postgres://<user>:<password>@<host>:<port>/<database>
DATABASE_URL=<your-db-url>
PGPASSWORD=<your-password>
API_KEY=<your-api-key>
CLIENT_SECRET=<your-client-secret>
STRIPE_TOKEN=<your-stripe-token>
WEBHOOK_SECRET=<your-webhook-secret>
```

When you need to show a *real-shaped* value for context (e.g. the rough
length, character set, or scheme of a token), prefer an obvious dummy:

```text
API_KEY=dummy_key
CLIENT_SECRET=dummy_value
DATABASE_URL=postgres://dummy:dummy@db.example/db
```

The repo's sensitive-info guard (described below) treats `dummy`,
`placeholder`, `redacted`, `example`, `xxx`, `***`, and the `your-*`
prefix family as opt-in dummy markers; values containing those tokens are
accepted as placeholders. Real-looking values that don't match the
placeholder pattern are flagged.

If you genuinely need to share a corrupted/expired credential to reproduce
a bug, please email the maintainer privately rather than posting it
publicly.

### What if I already pasted a real secret?

1. **Rotate the credential immediately.** Treat it as compromised. Don't
   try to "scrub" the GitHub trail and assume it's gone; issue history,
   email notifications, and search caches likely already have a copy.
2. Edit the issue or comment to remove the value. The scanner workflow
   will run again and clear the advisory once the text is safe.
3. If the value was a customer/production secret, follow the relevant
   downstream incident process (your org's rotation runbook, the cloud
   provider's audit-trail review, etc.).

This repo does **not** rewrite git history, prune branches, or rebase
issue/PR threads to remove leaked secrets. The remediation is rotation,
not cleanup.

## Pre-commit: scan staged changes locally

Before committing, run:

```bash
cargo xtask sensitive-info --staged
```

This invokes `git diff --cached -U0` under the hood and scans only the
*added* lines of your staged diff against the same rule set as the
public-text guard. Output is redacted: you get the rule name, the file,
and the line number, never the matched value.

Exit codes:

* `0` - no findings.
* `1` - at least one finding.
* `2` - usage error (bad flag, missing path, etc.).

Hooking into git:

```bash
# .git/hooks/pre-commit  (chmod +x to enable)
#!/usr/bin/env sh
exec cargo xtask sensitive-info --staged
```

The hook is opt-in and not auto-installed; sassi treats developer tooling
as a personal choice. If you don't want the hook, run the command manually
before every commit.

### Test fixtures and intentional examples

Some tests, docs, and CI snippets need to include credential-shaped strings
on purpose. For those lines you can add an explicit allow-marker on the
same line:

```rust
let line = "PGPASSWORD=hunter2"; // sassi:allow-secret
```

```yaml
run: my-tool --token=dummy_token  # sassi:allow-secret
```

The local-mode scanner honors the marker; the public-text (GitHub event)
scanner does **not**, so the marker cannot be used to bypass the
issue/PR guard. Prefer the placeholder or `dummy_*` patterns above when
possible; keep the explicit marker for cases where a placeholder would
break a test fixture or a CI assertion.

For files that are *themselves* test fixtures for the scanner, currently
just `xtask/src/sensitive_info.rs`, a file-level marker
`sassi:allow-secret-file` placed in the first 60 lines opts the whole
file out of local-mode scanning. Like the per-line marker, the
public-text scanner does not honor it. Add the file-level marker only
when you are deliberately writing a scanner test fixture; do not use it
to silence findings in production code.

CI containers in this repo (e.g. `redis://localhost:6379` in
`.github/workflows/ci.yml`) do not embed credentials. If a future
contributor adds a service container that needs a credential value, mark
it dummy/test-only with one of the conventions above so it remains
obviously not real.

## Public text: scanned on submission

The repo runs `.github/workflows/sensitive-info.yml` on every:

* opened or edited issue,
* created or edited issue comment,
* opened, edited, or reopened PR,
* created or edited PR review comment.

If a finding is detected, the workflow:

1. Fails (you'll see a red X on the issue/PR).
2. Posts a **redacted** advisory comment that names the rule(s) and the
   field that triggered them. The comment never echoes the matched value.

To clear the advisory: edit your submission, replace the real value with
a placeholder, and the workflow will re-run automatically.

The scanner is in `xtask/src/sensitive_info.rs` and shares its rule logic
between local and CI invocations; there's exactly one place where
detection rules are defined, so the local and public guards cannot drift.

## Other contributor expectations

### The "no djogi pressure" gate

Sassi is a standalone crate. Every PR description should make sense to a
Rust adopter who has never heard of djogi (the sibling framework that
consumes sassi). If a change is motivated by a djogi requirement, reword
the description to explain why a vanilla Rust adopter would want it too.

### Workspace conventions

* **Edition:** 2024.
* **MSRV:** Rust 1.95.
* **License:** dual MIT OR Apache-2.0.
* **Branch:** `main`.

Pre-commit-equivalent checks the CI will run on your PR:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo xtask check-test-surface     # no #[ignore] / quarantine
cargo xtask sensitive-info --staged  # no credential-like content
```

Atomic commits: each commit is one logical unit and should pass tests in
isolation.

### Where to file issues

* Bug reports / API friction / docs feedback: open a GitHub issue.
  Remember to use placeholders in any URL or env-style sample.
* Security-sensitive concerns: please email the maintainer rather than
  filing publicly. Sassi has no separate security mailing list yet, so
  use the maintainer's GitHub-listed email address.

## Quick reference

| Task                                                | Command                                       |
|-----------------------------------------------------|-----------------------------------------------|
| Scan staged diff for credentials                    | `cargo xtask sensitive-info --staged`         |
| Scan a file or directory                            | `cargo xtask sensitive-info --path docs`      |
| Scan a saved GitHub webhook payload                 | `cargo xtask sensitive-info --github-event event.json` |
| Show scanner help                                   | `cargo xtask sensitive-info --help`           |
| Check no `#[ignore]` / quarantine in tests          | `cargo xtask check-test-surface`              |
| Format check                                        | `cargo fmt --all -- --check`                  |
| Lint                                                | `cargo clippy --workspace --all-targets --all-features -- -D warnings` |
| Workspace tests                                     | `cargo test --workspace`                      |
