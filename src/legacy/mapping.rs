use graphannis::{
    errors::Result,
    update::{GraphUpdate, UpdateEvent},
};
use j4rs::{Instance, InvocationArg, Jvm};
use std::convert::TryFrom;

fn nullable(o: Instance) -> Result<Option<Instance>> {
    let as_jobject = o.java_object();
    let check = as_jobject.is_null();
    if check {
        Ok(Option::Some(Instance::from_jobject(as_jobject)?))
    } else {
        Ok(Option::None)
    }
}

fn add_corpus_and_document(g: &Instance, u: &mut GraphUpdate, jvm: &Jvm) -> Result<String> {
    if let Some(doc_path) = nullable(jvm.invoke(
        &jvm.cast(&g, "org.corpus_tools.salt.core.SPathElement")?,
        "getPath",
        &[],
    )?)? {
        if let Some(segments) = nullable(jvm.invoke(&doc_path, "segments", &[])?)? {
            let segments = jvm.invoke_static(
                "java.util.Arrays",
                "asList",
                &[InvocationArg::from(segments)],
            )?;
            let segments_length: i64 = jvm.to_rust(jvm.invoke(&segments, "size", &[])?)?;
            let mut corpus_path = String::default();
            let mut last_corpus_path: Option<String> = None;
            for i in 0..segments_length {
                let s: String =
                    jvm.to_rust(jvm.invoke(&segments, "get", &[InvocationArg::try_from(i)?])?)?;

                if i > 0 {
                    corpus_path.push('/');
                }

                // Add a node of type corpus for this path
                u.add_event(UpdateEvent::AddNode {
                    node_type: "corpus".to_string(),
                    node_name: corpus_path.clone(),
                })?;
                if i == (segments_length - 1) {
                    // The last element is also marked as a document
                    u.add_event(UpdateEvent::AddNodeLabel {
                        node_name: corpus_path.clone(),
                        anno_ns: "annis".to_string(),
                        anno_name: "doc".to_string(),
                        anno_value: s,
                    })?;
                }

                if let Some(last_corpus_path) = last_corpus_path {
                    // Add relation between parent corpus and sub-corpus/document
                    u.add_event(UpdateEvent::AddEdge {
                        source_node: corpus_path.clone(),
                        target_node: last_corpus_path.to_string(),
                        component_type: "PartOf".to_string(),
                        component_name: "".to_string(),
                        layer: "".to_string(),
                    })?;
                }

                last_corpus_path = Some(corpus_path.clone());
            }
            return Ok(corpus_path);
        }
    }
    Ok("".to_string())
}

fn node_name(node: &Instance, document_name: &str, jvm: &Jvm) -> Result<String> {
    let fragment: String = jvm
        .chain(&jvm.cast(node, "org.corpus_tools.salt.core.SPathElement")?)?
        .invoke("getPath", &[])?
        .invoke("fragment", &[])?
        .to_rust()?;

    if document_name.is_empty() {
        Ok(fragment)
    } else {
        let mut result = String::default();
        result.push_str(document_name);
        result.push('#');
        result.push_str(&fragment);
        Ok(result)
    }
}

fn add_node(n: Instance, document_name: &str, u: &mut GraphUpdate, jvm: &Jvm) -> Result<()> {
    if let Ok(n) = jvm.cast(&n, "org.corpus_tools.salt.common.SStructuredNode") {
        // use the unique name
        let node_name = node_name(&n, document_name, jvm)?;
        u.add_event(UpdateEvent::AddNode {
            node_name,
            node_type: "node".to_string(),
        })?;
        // TODO: add all annotations
    }
    Ok(())
}

pub fn convert_document_graph(g: Instance, jvm: &Jvm) -> Result<GraphUpdate> {
    let mut u = GraphUpdate::default();

    // create the (sub-) corpus and the document nodes
    let document_name = add_corpus_and_document(&g, &mut u, jvm)?;

    // add all nodes and their annotations
    let nodes_iterator: Instance = jvm
        .chain(&jvm.cast(&g, "org.corpus_tools.salt.graph.Graph")?)?
        .invoke("getNodes", &[])?
        .invoke("iterator", &[])?
        .collect();
    while jvm.to_rust::<bool>(jvm.invoke(&nodes_iterator, "hasNext", &[])?)? {
        let node = jvm.invoke(&nodes_iterator, "next", &[])?;
        add_node(node, &document_name, &mut u, jvm)?;
    }

    // TODO: add token information
    // TODO: add coverage information
    // TODO: add dominance relations
    // TODO: add pointing relations

    Ok(u)
}
