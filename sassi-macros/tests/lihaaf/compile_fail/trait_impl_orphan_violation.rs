extern crate std;

#[sassi::trait_impl]
impl std::fmt::Display for std::vec::Vec<i32> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "vec")
    }
}

fn main() {}
