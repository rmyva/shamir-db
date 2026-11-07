//! Vector similarity backends for the ShamirDB index system.
//!
//! Owns the concrete vector backends: the `VectorAdapter` trait,
//! in-process brute force, the HNSW adapter (`hnsw_rs`), SQ8
//! quantization with SIMD kernels, snapshots, and the `VectorBackend`
//! facade. The backend-agnostic model and the `IndexBackend` trait
//! live in `shamir-index-core`; `shamir-index` re-exports this crate
//! as its `vector` module.

pub mod hnsw_adapter;
pub mod quant;
pub mod simd;
pub mod snapshot;
pub mod vector_backend;

pub use quant::adapter;
pub use quant::adapter::{SearchOpts, VectorAdapter};
pub use quant::brute_force;
pub use quant::quant_meta;
pub use quant::quantized_dist;
pub use quant::sq8;
pub use vector_backend::VectorBackend;

#[cfg(test)]
mod tests;
