//! Quantization primitives (SQ8, quantized distance, quant metadata)
//! and the flat/brute-force adapter stack they serve.

pub mod adapter;
pub mod brute_force;
pub mod quant_meta;
pub mod quantized_dist;
pub mod sq8;
