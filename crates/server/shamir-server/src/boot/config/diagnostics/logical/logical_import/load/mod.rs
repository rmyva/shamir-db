//! Artifact analysis, load and validation of the staged data directory.

pub(crate) mod preflight;
pub(crate) mod stage_load;
#[cfg(test)]
mod tests;
pub(crate) mod validate;
