//! Trybuild driver.
//!
//! Runs every `tests/compile_fail/*.rs` fixture and asserts that
//! compilation fails with the expected `.stderr`. Also runs every
//! `tests/compile_pass/*.rs` fixture and asserts that compilation
//! succeeds — used to prove negative-property guarantees about
//! macro expansion (e.g., that `#[sassi::trait_impl]` does not
//! introduce `unsafe` attributes into adopter crates that set
//! `#![forbid(unsafe_code)]`).

#[test]
fn compile_fail() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/*.rs");
}

#[test]
fn compile_pass() {
    let t = trybuild::TestCases::new();
    t.pass("tests/compile_pass/*.rs");
}
