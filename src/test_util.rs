use crate::{
    error::{AnnattoError, Result},
    exporter::Exporter,
    importer::Importer,
    util::get_all_files,
    workflow::StatusSender,
    StepID,
};
use graphannis::AnnotationGraph;

use graphannis_core::{
    annostorage::{EdgeAnnotationStorage, NodeAnnotationStorage, ValueSearch},
    graph::{ANNIS_NS, NODE_NAME_KEY},
    types::{Edge, NodeID},
};
use itertools::Itertools;
use std::{cmp::Ordering, fs, io::BufWriter, path::Path};
use tempfile::TempDir;

pub fn import_as_graphml_string<I, P>(
    importer: I,
    path: P,
    graph_configuration: Option<&str>,
) -> Result<String>
where
    I: Importer,
    P: AsRef<Path>,
{
    import_as_graphml_string_2(importer, path, graph_configuration, true, None)
}

pub fn import_as_graphml_string_2<I, P>(
    importer: I,
    path: P,
    graph_configuration: Option<&str>,
    disk_based: bool,
    tx: Option<StatusSender>,
) -> Result<String>
where
    I: Importer,
    P: AsRef<Path>,
{
    let step_id = StepID {
        module_name: "import_under_test".to_string(),
        path: None,
    };
    let mut u = importer
        .import_corpus(path.as_ref(), step_id.clone(), tx)
        .map_err(|e| AnnattoError::Import {
            reason: e.to_string(),
            importer: step_id.module_name.to_string(),
            path: path.as_ref().to_path_buf(),
        })?;
    let mut g = AnnotationGraph::with_default_graphstorages(disk_based)?;
    g.apply_update(&mut u, |_| {})?;

    let mut buf = BufWriter::new(Vec::new());
    graphannis_core::graph::serialization::graphml::export_stable_order(
        &g,
        graph_configuration,
        &mut buf,
        |_| {},
    )?;
    let bytes = buf.into_inner()?;
    let actual = String::from_utf8(bytes)?;

    Ok(actual)
}

pub fn export_to_string<E>(graph: &AnnotationGraph, exporter: E) -> Result<String>
where
    E: Exporter,
{
    let output_path = TempDir::new()?;

    let step_id = StepID {
        module_name: "export_under_test".to_string(),
        path: Some(output_path.path().to_path_buf()),
    };
    exporter
        .export_corpus(graph, output_path.path(), step_id.clone(), None)
        .map_err(|_| AnnattoError::Export {
            reason: "Could not export graph to read its output.".to_string(),
            exporter: step_id.module_name.to_string(),
            path: output_path.path().to_path_buf(),
        })?;
    let mut buffer = String::new();
    for path in get_all_files(output_path.path(), &[exporter.file_extension()])? {
        let file_data = fs::read_to_string(path)?;
        buffer.push_str(&file_data);
    }
    Ok(buffer)
}

fn compare_edge_annos(
    annos1: &dyn EdgeAnnotationStorage,
    annos2: &dyn EdgeAnnotationStorage,
    items1: &[Edge],
    items2: &[Edge],
) {
    assert_eq!(items1.len(), items2.len());
    for i in 0..items1.len() {
        let mut annos1 = annos1.get_annotations_for_item(&items1[i]).unwrap();
        annos1.sort();
        let mut annos2 = annos2.get_annotations_for_item(&items2[i]).unwrap();
        annos2.sort();
        assert_eq!(annos1, annos2);
    }
}

fn compare_node_annos(
    annos1: &dyn NodeAnnotationStorage,
    annos2: &dyn NodeAnnotationStorage,
    items1: &[NodeID],
    items2: &[NodeID],
) {
    assert_eq!(items1.len(), items2.len());
    for i in 0..items1.len() {
        let mut annos1 = annos1.get_annotations_for_item(&items1[i]).unwrap();
        annos1.sort();
        let mut annos2 = annos2.get_annotations_for_item(&items2[i]).unwrap();
        annos2.sort();
        assert_eq!(annos1, annos2);
    }
}

pub fn compare_graphs(g1: &AnnotationGraph, g2: &AnnotationGraph) {
    // Check all nodes and node annotations exist in both corpora
    let nodes1: Vec<String> = g1
        .get_node_annos()
        .exact_anno_search(Some(ANNIS_NS), "node_name", ValueSearch::Any)
        .filter_map(|m| m.unwrap().extract_annotation(g1.get_node_annos()).unwrap())
        .map(|a| a.val.into())
        .sorted()
        .collect();
    let nodes2: Vec<String> = g2
        .get_node_annos()
        .exact_anno_search(Some(ANNIS_NS), "node_name", ValueSearch::Any)
        .filter_map(|m| m.unwrap().extract_annotation(g1.get_node_annos()).unwrap())
        .map(|a| a.val.into())
        .sorted()
        .collect();
    assert_eq!(&nodes1, &nodes2);

    let nodes1: Vec<NodeID> = nodes1
        .into_iter()
        .filter_map(|n| g1.get_node_annos().get_node_id_from_name(&n).unwrap())
        .collect();
    let nodes2: Vec<NodeID> = nodes2
        .into_iter()
        .filter_map(|n| g2.get_node_annos().get_node_id_from_name(&n).unwrap())
        .collect();
    compare_node_annos(g1.get_node_annos(), g2.get_node_annos(), &nodes1, &nodes2);

    // Check that the graphs have the same edges
    let mut components1 = g1.get_all_components(None, None);
    components1.sort();
    let mut components2 = g2.get_all_components(None, None);
    components2.sort();
    assert_eq!(components1, components2);

    for c in components1 {
        let gs1 = g1.get_graphstorage_as_ref(&c).unwrap();
        let gs2 = g2.get_graphstorage_as_ref(&c).unwrap();

        for i in 0..nodes1.len() {
            let start1 = nodes1[i];
            let start2 = nodes2[i];

            // Check all connected nodes for this edge
            let targets1: Result<Vec<String>> = gs1
                .get_outgoing_edges(start1)
                .filter_map_ok(|target| {
                    g1.get_node_annos()
                        .get_value_for_item(&target, &NODE_NAME_KEY)
                        .unwrap()
                })
                .map_ok(|n| n.into())
                .map(|n| n.map_err(AnnattoError::from))
                .collect();
            let mut targets1 = targets1.unwrap();
            targets1.sort();

            let targets2: Result<Vec<String>> = gs2
                .get_outgoing_edges(start2)
                .filter_map_ok(|target| {
                    g2.get_node_annos()
                        .get_value_for_item(&target, &NODE_NAME_KEY)
                        .unwrap()
                })
                .map(|n| n.map_err(AnnattoError::from))
                .map_ok(|n| n.to_string())
                .collect();
            let mut targets2 = targets2.unwrap();
            targets2.sort();
            assert_eq!(targets1, targets2);

            // Check the edge annotations for each edge
            let edges1: Vec<Edge> = targets1
                .iter()
                .map(|t| Edge {
                    source: start1,
                    target: g1
                        .get_node_annos()
                        .get_node_id_from_name(t)
                        .unwrap()
                        .unwrap(),
                })
                .collect();
            let edges2: Vec<Edge> = targets2
                .iter()
                .map(|t| Edge {
                    source: start2,
                    target: g2
                        .get_node_annos()
                        .get_node_id_from_name(t)
                        .unwrap()
                        .unwrap(),
                })
                .collect();
            compare_edge_annos(
                gs1.get_anno_storage(),
                gs2.get_anno_storage(),
                &edges1,
                &edges2,
            );
        }
    }
}

pub(crate) fn compare_results<T: Ord, E: Into<anyhow::Error>>(
    a: &std::result::Result<T, E>,
    b: &std::result::Result<T, E>,
) -> Ordering {
    if let (Ok(a), Ok(b)) = (a, b) {
        a.cmp(b)
    } else if a.is_err() {
        Ordering::Less
    } else if b.is_err() {
        Ordering::Greater
    } else {
        // Treat two errors as equal
        Ordering::Equal
    }
}
