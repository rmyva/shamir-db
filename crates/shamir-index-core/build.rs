// F-1103 (#1103) — isolate the loom model-checker cfg to THIS crate only.
//
// The loom module in `reader_drain_gate.rs` is compiled under `cfg(loom)`.
// We emit that cfg HERE (gated on the `loom` cargo feature) rather than via a
// global `RUSTFLAGS="--cfg loom"`, so dependencies such as `concurrent-queue`
// (which carry their OWN `#[cfg(loom)]` blocks tied to a different loom API
// version) are NOT dragged into their loom code paths and break the build.
//
// Run the loom model with:
//   cargo test -p shamir-index-core --features loom --lib \
//       reader_drain_gate::loom_model -- --nocapture
fn main() {
    if std::env::var("CARGO_FEATURE_LOOM").is_ok() {
        println!("cargo::rustc-cfg=loom");
    }
}
