# Vendored hnsw_rs

Upstream: <https://github.com/jean-pierreBoth/hnswlib-rs>, crate `hnsw_rs` **0.3.5**
(crates.io checksum `c9b4dcb9840006fae9964e97dfdb354cd00b7a15d69d143caae7eeca0410af9f`).
Licence: MIT / Apache-2.0 (`LICENSE-MIT`, `LICENSE-APACHE`).

Shipped dependency: **0.3.5 + Shamir patch**, identified by the repository release
commit, not an unmodified crates.io package. The checksum above records upstream
origin, not the modified tree (`Cargo.toml:136-137`,
`vendor/hnsw_rs/Cargo.toml:8-9`).

Wired in through `[patch.crates-io]` in the workspace `Cargo.toml`; the exact pin
`hnsw_rs = "=0.3.5"` in `crates/shamir-index/Cargo.toml` and the dump-format guard
`HNSW_RS_VERSION` stay unchanged because the vendored version is still 0.3.5.

## Why

Concurrent `Hnsw::insert` can deadlock (ABBA on the per-point `neighbours` locks):
`reverse_update_neighborhood_simple` iterated `new_point.neighbours.read()[l]` while taking
`q.neighbours.write()` for each neighbour, so a read guard on the new point was held across
a write acquisition on another point. With parking_lot's writer-preferring `RwLock`, a cycle of
three or more inserts (or two, with a level >= 2 point) parks forever, together with every
`search_layer` reader queued behind the pending writers. The same code is in 0.3.5; there is no
upstream fix yet.

## Differences from upstream 0.3.5

- `Cargo.toml`: trimmed (no example/test targets, no hdf5/ndarray/itertools/skiplist
  dev-dependencies), `publish = false`. The `simdeez_f` and `stdsimd` features
  are empty rather than forwarding upstream's `anndists` features: this avoids
  pulling simdeez and unmaintained `paste` into workspace all-feature resolution
  for features Shamir does not enable (`vendor/hnsw_rs/Cargo.toml:28-34`).
- `rustfmt.toml`: formatting disabled for this directory.
- `src/lib.rs`: `#![allow(warnings, clippy::all)]` (third-party code must not trip the
  workspace `-D warnings` gates) and the `SHAMIR_REVERSE_UPDATE_LOCK_FIX` marker.
- `src/hnsw.rs`: `reverse_update_neighborhood_simple` snapshots the neighbour list under the read
  lock and releases it before taking any write lock, so no `neighbours` lock is ever acquired
  while another is held; regression test `reverse_update_never_holds_read_across_write`.

## Dropping the vendor copy

Remove `vendor/hnsw_rs`, the `[patch.crates-io]` entry and the `members` entry once an upstream
release contains an equivalent fix and has passed the dependency cooldown (3 days); re-pin `HNSW_RS_VERSION`.
