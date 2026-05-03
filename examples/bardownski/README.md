# Bardownski

Dependency-light terminal showcase for `sassi` predicate algebra and `Punnu` over an offline hockey shot CSV.

The sample data in `data/sample.csv` is a compact MoneyPuck-style shot subset with plausible NHL rink coordinates, shot types, rebound flags, goals, and expected-goal values. It is included for offline demos and is inspired by MoneyPuck's public shot data format: <https://moneypuck.com/data.htm>.

## Run

Run the interactive TUI from the workspace root:

```bash
cargo run -p bardownski -- --period 2 --high-danger
```

For a non-interactive smoke test:

```bash
cargo run -p bardownski -- --summary --on-rebound
```

The future Dioxus Bardownski belongs to `../djogi` Phase 10. This in-repo example is intentionally native-only: no WASM, no Redis, and no Dioxus.
