use crate::quantized_dist::ShamirDistU8;
use crate::snapshot::quantized_load::load_quantized_graph;
use crate::sq8::Sq8Quantizer;
use hnsw_rs::api::AnnT;
use hnsw_rs::hnsw::Hnsw;
use shamir_index_core::kind::VectorMetric;
use std::sync::Arc;

fn dumped_u8_graph() -> (String, Vec<u8>, Vec<u8>, Arc<Sq8Quantizer>) {
    let points = [vec![0.0f32, 1.0], vec![1.0, 0.0], vec![0.5, 0.5]];
    let quantizer = Arc::new(Sq8Quantizer::fit(&points, 2));
    let distance = ShamirDistU8::new(Arc::clone(&quantizer), VectorMetric::L2);
    // hnsw_rs dumps only graphs built with NB_LAYER_MAX (16) layers.
    let graph = Hnsw::<u8, ShamirDistU8>::new(8, 16, 16, 32, distance);
    let codes: Vec<(Vec<u8>, usize)> = points
        .iter()
        .enumerate()
        .map(|(id, point)| (quantizer.quantize(point), id))
        .collect();
    let refs: Vec<(&Vec<u8>, usize)> = codes.iter().map(|(codes, id)| (codes, *id)).collect();
    graph.parallel_insert(&refs);
    let dir = tempfile::tempdir().unwrap();
    let basename = graph.file_dump(dir.path(), "rv_index_8db6b1ec").unwrap();
    let graph_bytes = std::fs::read(dir.path().join(format!("{basename}.hnsw.graph"))).unwrap();
    let data_bytes = std::fs::read(dir.path().join(format!("{basename}.hnsw.data"))).unwrap();
    (basename, graph_bytes, data_bytes, quantizer)
}

#[test]
fn rv_index_8db6b1ec_success_cleans_tempdir_and_graph_remains_live() {
    let (basename, graph_bytes, data_bytes, quantizer) = dumped_u8_graph();
    let temp_dir = tempfile::tempdir().unwrap();
    let graph_path = temp_dir.path().join(format!("{basename}.hnsw.graph"));
    let data_path = temp_dir.path().join(format!("{basename}.hnsw.data"));
    std::fs::write(&graph_path, &graph_bytes).unwrap();
    std::fs::write(&data_path, &data_bytes).unwrap();
    let path = temp_dir.path().to_path_buf();
    let graph = load_quantized_graph(
        temp_dir,
        &basename,
        graph_path,
        data_path,
        quantizer,
        VectorMetric::L2,
    )
    .unwrap();
    assert!(
        !path.exists(),
        "temporary dump dir must be removed after success"
    );
    assert!(
        !graph.search(&[0, 255], 1, 8).is_empty(),
        "loaded graph must remain usable"
    );
}

#[test]
fn rv_index_8db6b1ec_failure_cleans_tempdir() {
    let (_basename, graph_bytes, data_bytes, quantizer) = dumped_u8_graph();
    let temp = tempfile::tempdir().unwrap();
    let graph_path = temp.path().join("rv_index_8db6b1ec.hnsw.graph");
    let data_path = temp.path().join("rv_index_8db6b1ec.hnsw.data");
    std::fs::write(&graph_path, &graph_bytes[..4]).unwrap();
    std::fs::write(&data_path, &data_bytes).unwrap();
    let path = temp.path().to_path_buf();
    let result = load_quantized_graph(
        temp,
        "rv_index_8db6b1ec",
        graph_path,
        data_path,
        quantizer,
        VectorMetric::L2,
    );
    assert!(result.is_err());
    assert!(
        !path.exists(),
        "temporary dump dir must be removed after loader failure"
    );
}
