// compile_fail: orphan rule enforcement.
// #[sassi::trait_impl] inherits this constraint: if you can't write
// `impl ForeignTrait for ForeignType`, you certainly can't annotate
// it with #[sassi::trait_impl]. The marker impl emitted by the macro
// adds no new orphan-rule restriction beyond what the original impl
// already requires.

impl std::fmt::Display for String {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

fn main() {}
