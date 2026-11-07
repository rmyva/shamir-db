//! Quantized graph loading with independent file and loader ownership.

use super::manifest::validate_basename;
use super::SnapshotError;
use crate::quantized_dist::ShamirDistU8;
use crate::sq8::Sq8Quantizer;
use hnsw_rs::hnsw::Hnsw;
use hnsw_rs::hnswio::HnswIo;
use shamir_index_core::kind::VectorMetric;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::sync::Arc;

pub(super) fn load_quantized_graph(
    temp_dir: tempfile::TempDir,
    basename: &str,
    graph_path: PathBuf,
    data_path: PathBuf,
    quantizer: Arc<Sq8Quantizer>,
    metric: VectorMetric,
) -> Result<Arc<Hnsw<'static, u8, ShamirDistU8>>, SnapshotError> {
    validate_basename(basename)?;
    let expected_graph_path = temp_dir.path().join(format!("{basename}.hnsw.graph"));
    let expected_data_path = temp_dir.path().join(format!("{basename}.hnsw.data"));
    if graph_path != expected_graph_path || data_path != expected_data_path {
        return Err(SnapshotError::Corrupt(
            "quantized graph path mismatch".into(),
        ));
    }

    let loader: &'static HnswIo = Box::leak(Box::new(HnswIo::new(temp_dir.path(), basename)));
    let distance = ShamirDistU8::new(quantizer, metric);
    // hnsw_rs unwraps while parsing the description, so a corrupt/truncated
    // dump panics instead of returning Err; contain it as a load error.
    let loaded = catch_unwind(AssertUnwindSafe(|| loader.load_hnsw_with_dist(distance)))
        .map_err(|_| SnapshotError::Corrupt("hnsw_rs load_hnsw_with_dist (u8) panicked".into()))?;
    let graph = loaded.map_err(|error| {
        SnapshotError::Corrupt(format!("hnsw_rs load_hnsw_with_dist (u8) failed: {error}"))
    })?;
    Ok(Arc::new(graph))
}
