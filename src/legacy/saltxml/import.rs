use graphannis::update::GraphUpdate;
use j4rs::{Instance, Jvm};

use crate::error::PepperError;

fn iterator_next(it: &Instance, jvm: &Jvm) -> Result<Option<Instance>, PepperError> {
    let has_next: bool = jvm.to_rust(jvm.invoke(it, "hasNext", &vec![])?)?;
    if has_next {
        // Get the next list element
        let result = jvm.invoke(it, "next", &vec![])?;
        Ok(Some(result))
    } else {
        Ok(None)
    }
}

fn add_node(
    n: &Instance,
    document_node_name: &str,
    u: &mut GraphUpdate,
    jvm: &Jvm,
) -> Result<(), PepperError> {
    if let Ok(n) = jvm.cast(n, "org.corpus_tools.salt.common.SStructuredNode") {}

    todo!()
}

pub fn map(g: &Instance, document_node_name: &str, jvm: &Jvm) -> Result<GraphUpdate, PepperError> {
    let mut u = GraphUpdate::default();

    // add all nodes and their annotations
    let nodes = jvm.invoke(
        &jvm.cast(g, "org.corpus_tools.salt.graph.Graph")?,
        "getNodes",
        &vec![],
    )?;
    let nodes_iterator = jvm.invoke(&nodes, "listIterator", &vec![])?;
    while let Some(n) = iterator_next(&nodes_iterator, jvm)? {
        add_node(&n, document_node_name, &mut u, jvm)?;
    }
    Ok(u)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::legacy::create_jvm;
    use j4rs::InvocationArg;
    use std::convert::TryFrom;

    fn create_example_document_graph(jvm: &Jvm) -> Result<Instance, PepperError> {
        let example_project = jvm.invoke_static(
            "org.corpus_tools.salt.samples.SampleGenerator",
            "createSaltProject",
            &vec![],
        )?;
        let corpus_graph_list = jvm.invoke(&example_project, "getCorpusGraphs", &vec![])?;
        let first_corpus_graph = jvm.invoke(
            &corpus_graph_list,
            "get",
            &vec![InvocationArg::try_from(0_i32)?.into_primitive()?],
        )?;
        let first_corpus_graph = jvm.cast(
            &first_corpus_graph,
            "org.corpus_tools.salt.common.SCorpusGraph",
        )?;
        let document_list = jvm.invoke(&first_corpus_graph, "getDocuments", &vec![])?;
        let first_document = jvm.invoke(
            &document_list,
            "get",
            &vec![InvocationArg::try_from(0_i32)?.into_primitive()?],
        )?;
        let first_document = jvm.cast(&first_document, "org.corpus_tools.salt.common.SDocument")?;
        let first_document_graph = jvm.invoke(&first_document, "getDocumentGraph", &vec![])?;
        let first_document_graph = jvm.cast(
            &first_document_graph,
            "org.corpus_tools.salt.common.SDocumentGraph",
        )?;
        Ok(first_document_graph)
    }

    #[test]
    fn test_mapping() {
        let jvm = create_jvm(false).unwrap();
        // Create a Salt sample project and get the first document graph
        let g = create_example_document_graph(&jvm).unwrap();
        map(&g, "rootCorpus/subCorpus1/doc1", &jvm).unwrap();
    }
}
