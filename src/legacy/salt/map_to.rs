use smartstring::alias::String;
use std::collections::BTreeMap;

use crate::error::Result;
use graphannis::{
    graph::{AnnoKey, NodeID},
    model::AnnotationComponentType,
    AnnotationGraph,
};
use graphannis_core::{
    annostorage::ValueSearch,
    graph::{ANNIS_NS, NODE_TYPE},
    types::ComponentType,
};
use j4rs::{Instance, InvocationArg, Jvm};

pub fn map_document_graph(
    graph: &AnnotationGraph,
    document_id: &str,
    jvm: &Jvm,
) -> Result<Instance> {
    // Create a new document graph object
    let sgraph = jvm.invoke_static(
        "org.corpus_tools.salt.SaltFactory",
        "createSDocumentGraph",
        &[],
    )?;
    // add all nodes and their annotations
    let node_annos = graph.get_node_annos();
    for m in node_annos.exact_anno_search(Some(ANNIS_NS), NODE_TYPE, ValueSearch::Some("node")) {
        let n = map_node(graph, m.node, jvm)?;
        jvm.invoke(&sgraph, "addNode", &[InvocationArg::from(n)])?;
    }
    Ok(sgraph)
}

fn map_node(graph: &AnnotationGraph, node_id: NodeID, jvm: &Jvm) -> Result<Instance> {
    let node_annos = graph.get_node_annos();
    // get all annotations of the node as a map
    let labels: BTreeMap<AnnoKey, String> = node_annos
        .get_annotations_for_item(&node_id)
        .into_iter()
        .map(|a| (a.key, a.val))
        .collect();

    let tok_key = AnnoKey {
        name: ANNIS_NS.into(),
        ns: "tok".into(),

    };
    let new_node = if labels.contains_key(&tok_key) && !has_coverage_edge(graph, node_id) {
        jvm.invoke_static(
            "org.corpus_tools.salt.SaltFactory",
            "createSToken",
            &[],
        )?
    } else if has_dominance_edge(graph, node_id) {
        jvm.invoke_static(
            "org.corpus_tools.salt.SaltFactory",
            "createSStructure",
            &[],
        )?
    } else {
        jvm.invoke_static(
            "org.corpus_tools.salt.SaltFactory",
            "createSSpan",
            &[],
        )?
    };

    // TODO: set node name and ID
    // TODO: map labels

    Ok(new_node)
}

fn has_coverage_edge(graph: &AnnotationGraph, node_id: NodeID) -> bool {
    for c in graph.get_all_components(Some(AnnotationComponentType::Coverage), None) {
        if let Some(gs) = graph.get_graphstorage_as_ref(&c) {
            if gs.get_outgoing_edges(node_id).next().is_some() {
                return true;
            }
        }
    }
    false
}

fn has_dominance_edge(graph: &AnnotationGraph, node_id: NodeID) -> bool {
    for c in graph.get_all_components(Some(AnnotationComponentType::Dominance), None) {
        if let Some(gs) = graph.get_graphstorage_as_ref(&c) {
            if gs.get_outgoing_edges(node_id).next().is_some() {
                return true;
            }
        }
    }
    false
}
