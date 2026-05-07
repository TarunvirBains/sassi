# Red-Team Release Gate

This playbook captures the process Sassi used before the v0.1.0 alpha release.
It is meant to be reused for Sassi, Djogi, HeeRanjId, and other crates where a
small API can still hide serious correctness or security footguns.

The goal is not to make a release feel perfect. The goal is to make the release
harder to misuse, harder to corrupt, and honest about the contracts it does and
does not provide.

## When To Use It

Run this gate before publishing an alpha, beta, or stable crate release when any
of these are true:

- the crate stores or replays user data;
- the crate crosses process, runtime, file, network, database, tenant, or auth
  boundaries;
- correctness depends on cache invalidation, query isolation, identity, refresh,
  serialization, concurrency, or generated code;
- docs could plausibly teach adopters an unsafe pattern.

For Sassi-like systems, a plausible correctness or security footgun is
publish-blocking until it is fixed, downgraded with evidence, or documented as a
deliberate limitation.

## Review Shape

Use one compound reviewer instruction:

```text
Find the risks I failed to name AND check these risks.
```

Do not let the known-risk checklist anchor the whole review. The first pass
should deliberately look outside the named concerns. Then the second pass should
cover the named concerns with file and test evidence.

Good reviewer instruction:

```text
This is a release-gate red-team review: find the risks I failed to name AND
check these risks. The checklist is required but not exhaustive; do not let it
anchor your review.

For each finding, include area, evidence, repro/test idea, impact, likelihood,
fix size, and publish decision.
```

Avoid prompts that only say "check these risks." They make the review inherit
the author's blind spots.

## Issue Register

Every agent writes findings in this shape:

```text
area:
finding:
evidence:
repro/test idea:
impact:
likelihood:
fix size:
publish decision:
```

Use these publish decisions:

- `BLOCK`: do not publish until fixed or disproven.
- `FIX_BEFORE_RELEASE`: fix before this release because the blast radius is real.
- `DOC_BEFORE_RELEASE`: code behavior is acceptable for this release, but docs/API
  wording can cause dangerous misuse.
- `POST_RELEASE`: worth tracking, not release-blocking.
- `NO_ACTION`: checked and safe enough with current evidence.

Confirmed bugs outrank speculative hardening. A suspicious area becomes a
blocker when there is a plausible repro path and meaningful impact.

## Agent Mix

Use independent agents with different prompts and models when available. The
standing Sassi-style red team is:

- Codex at `xhigh`: code-aware root cause analysis, focused repros, TDD fixes,
  and final integration judgment.
- Gemini at its highest available reasoning mode: adversarial architecture and
  broad release-gate review.
- Claude Opus via Claude Code CLI at max effort: adopter-docs, API misuse,
  architecture coherence, and release-risk review.
- DeepSeek v4 Pro via OpenRouter/opencode at max variant: independent subsystem
  bug hunts and implementation-risk review.
- Kimi K2.6 via OpenRouter/opencode at max variant: independent bug hunts,
  design blind spots, and alternative-reasoning review.

Treat this as a reviewer panel, not a vote. One well-evidenced blocker from any
model is enough to stop the release until the claim is fixed, disproven, or
consciously rescoped.

Known local invocation patterns:

```bash
# Codex subagent from the current session
# Use an xhigh reviewer prompt with the same issue-register format.

# Gemini CLI
GEMINI_CLI_TRUST_WORKSPACE=true gemini -p "$PROMPT"

# Claude Code CLI
claude --model claude-opus-4-7 --effort max \
  --no-session-persistence --tools "" -p "$PROMPT"

# OpenRouter through opencode
opencode run "$PROMPT" \
  --model openrouter/deepseek/deepseek-v4-pro \
  --variant max

opencode run "$PROMPT" \
  --model openrouter/moonshotai/kimi-k2.6 \
  --variant max
```

For each model family, preserve independence:

- do not paste another agent's full findings into the first prompt unless the
  task is explicitly a de-duplication or follow-up review;
- do give enough project context to be useful;
- do include known active fixes so agents do not waste the whole pass re-finding
  already-fixed issues;
- ask for surprising areas checked, not only findings.

## Prompt Template

```text
You are an independent red-team reviewer for <repo>.

This is a BROAD BLIND-SPOT plus KNOWN-RISK audit.

Find the risks I failed to name AND check these risks. The known-risk list is
required but not exhaustive; do not let it anchor your whole review.

Rules:
- Read-only unless explicitly assigned an implementation task.
- Do not run destructive commands.
- Treat plausible correctness/security footguns as publish-blocking when impact
  is high enough.
- Prefer concrete findings over speculative hardening.
- If an apparent issue is safe after inspection, include the evidence briefly.

Project context:
- <short description>
- <release target>
- <owner priorities>

Known active findings, for de-duplication:
- <finding or "none">

Known high-risk areas to check:
- identity and cache key stability
- tenant/auth/RLS and query isolation
- stale data through refresh, invalidation, TTL, LRU, tombstones, recovery
- backend keyspace, filesystem, Redis, database, and wire-format behavior
- feature gates and runtime assumptions
- generated code and public API contracts
- release packaging and docs/API mismatch

Output:
- Issue register ordered by publish risk.
- For each issue: area, finding, file:line evidence, repro/test idea, impact,
  likelihood, fix size, publish decision.
- Surprising Areas Checked.
- Top next actions.
```

## Triage Rules

For every finding:

1. Restate the technical claim.
2. Verify against the code and tests.
3. Decide whether it is a confirmed bug, a plausible hazard, a documentation
   hazard, or a false positive.
4. For code changes, use TDD: write the failing test, watch it fail, then fix.
5. For docs changes, remove overclaims and state boundaries plainly.
6. Re-run targeted tests first, then the relevant broader suite.

Do not blindly implement external review suggestions. Check whether the finding
is true for this codebase and whether the suggested fix matches the public API
and release goals.

## Final Gate

Before publishing, there should be:

- no unresolved `BLOCK` findings;
- no unresolved `FIX_BEFORE_RELEASE` findings unless the release is intentionally
  delayed or rescoped;
- `DOC_BEFORE_RELEASE` findings either documented or consciously promoted to code
  fixes;
- a short issue register recording what was fixed, what was documented, and what
  remains after this release;
- targeted tests for every fixed bug;
- full release verification rerun after the final patch set.

The release can still be alpha. It should not be vague about sharp edges.
