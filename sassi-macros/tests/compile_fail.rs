//! Trybuild driver — runs every `tests/compile_fail/*.rs` fixture and
//! asserts that compilation fails with the expected `.stderr`.

#[test]
fn compile_fail() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/*.rs");
}
