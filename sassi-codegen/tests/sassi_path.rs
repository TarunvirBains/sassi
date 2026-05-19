//! Direct unit tests for `resolve_sassi_path`.
//!
//! These tests construct `FoundCrate` values directly — bypassing the
//! `crate_name()` filesystem probe — so they are deterministic regardless
//! of the ambient `CARGO_MANIFEST_DIR`. Each arm of the path-resolution
//! match is exercised and its emitted token string is asserted, ensuring
//! a regression (e.g. changing `::sassi` back to `crate`) fails
//! immediately.

use proc_macro_crate::FoundCrate;
use sassi_codegen::resolve_sassi_path;

/// Verify that `FoundCrate::Itself` emits exactly `:: sassi`.
///
/// This is the arm exercised when `#[derive(Cacheable)]` or
/// `#[sassi::trait_impl]` expands inside the `sassi` crate itself
/// (e.g., in `sassi`'s own integration tests). The emitted path must
/// be the absolute crate path `::sassi`, not the relative `crate` keyword
/// — proc-macro expansions must never use `crate` to reference their
/// source crate, since `crate` in the expanded token stream refers to
/// the *caller's* crate, not `sassi`.
#[test]
fn itself_emits_absolute_sassi_path() {
    let tokens = resolve_sassi_path(FoundCrate::Itself);
    // `quote!(::sassi)` formats as `":: sassi"` (proc_macro2 inserts a
    // space after `::` in its Display impl).
    assert_eq!(
        tokens.to_string(),
        ":: sassi",
        "FoundCrate::Itself must emit `::sassi`; got `{tokens}` — \
         check that the Itself arm of resolve_sassi_path uses \
         `quote!(::sassi)` and has not regressed to `quote!(crate)`"
    );
}

/// Verify that `FoundCrate::Name` emits the adopter's alias as an
/// absolute path.
///
/// This arm fires when an adopter renames the `sassi` dependency in
/// their `Cargo.toml` (e.g. `cache = { package = "sassi", ... }`).
/// Covered here primarily as a guard against accidentally breaking the
/// `Name` arm while touching the `Itself` arm, and to document the
/// expected token shape.
#[test]
fn name_emits_absolute_aliased_path() {
    let tokens = resolve_sassi_path(FoundCrate::Name("cache".to_owned()));
    assert_eq!(
        tokens.to_string(),
        ":: cache",
        "FoundCrate::Name(\"cache\") must emit `::cache`; got `{tokens}`"
    );
}
