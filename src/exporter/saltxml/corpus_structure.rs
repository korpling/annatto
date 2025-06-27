use std::{ffi::OsStr, io::BufWriter};

use anyhow::{Context, Error};
use graphannis::{
    AnnotationGraph,
    graph::{Edge, NodeID},
    model::AnnotationComponentType,
};
use graphannis_core::{
    annostorage::ValueSearch,
    graph::{ANNIS_NS, NODE_TYPE},
};
use quick_xml::events::{BytesDecl, Event};

use crate::{progress::ProgressReporter, util::CorpusGraphHelper};

use super::SaltWriter;

pub(super) struct SaltCorpusStructureMapper {}

impl SaltCorpusStructureMapper {
    pub(super) fn new() -> SaltCorpusStructureMapper {
        SaltCorpusStructureMapper {}
    }

    pub(super) fn map_corpus_structure(
        &self,
        graph: &AnnotationGraph,
        output_path: &std::path::Path,
        progress: &ProgressReporter,
    ) -> anyhow::Result<Vec<NodeID>> {
        let corpus_name = output_path
            .file_name()
            .unwrap_or_else(|| OsStr::new("corpus"));

        let mut documents = Vec::new();

        let project_file_path = output_path.join("saltProject.salt");
        let output_file = std::fs::File::create(&project_file_path)?;
        let buffered_output_file = BufWriter::new(output_file);
        let mut writer = quick_xml::Writer::new_with_indent(buffered_output_file, b' ', 2);

        writer.write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))?;

        let project_elem = writer
            .create_element("saltCommon:SaltProject")
            .with_attribute(("xmlns:sCorpusStructure", "sCorpusStructure"))
            .with_attribute(("xmlns:saltCommon", "saltCommon"))
            .with_attribute(("xmlns:saltCore", "saltCore"))
            .with_attribute(("xmlns:xmi", "http://www.omg.org/XMI"))
            .with_attribute(("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance"))
            .with_attribute(("xsi:version", "2.0"));

        // The whole corpus is the equivalent of a corpus graph, so we only
        // output one sCorpusGraph and give it the name of the corpus.

        project_elem.write_inner_content::<_, Error>(|writer| {
            writer
                .create_element("sCorpusGraphs")
                .write_inner_content::<_, Error>(|writer| {
                    writer
                        .create_element("labels")
                        .with_attribute(("xsi:type", "saltCore:SFeature"))
                        .with_attribute(("namespace", "salt"))
                        .with_attribute(("name", "id"))
                        .with_attribute((
                            "value",
                            format!("T::{}", corpus_name.to_string_lossy()).as_str(),
                        ))
                        .write_empty()?;

                    let mut salt_writer =
                        SaltWriter::new(graph, writer, &project_file_path, progress)?;

                    let corpusgraph_helper = CorpusGraphHelper::new(graph);

                    // Map all corpus nodes in the graph
                    let corpus_nodes: graphannis_core::errors::Result<Vec<_>> = graph
                        .get_node_annos()
                        .exact_anno_search(Some(ANNIS_NS), NODE_TYPE, ValueSearch::Some("corpus"))
                        .collect();
                    let corpus_nodes = corpus_nodes?;

                    for n in corpus_nodes.iter() {
                        // Check if this is a document or a (sub)-corpus by testing if there are any incoming PartOfEdges
                        if corpusgraph_helper.is_document(n.node)? {
                            documents.push(n.node);
                            salt_writer
                                .write_graphannis_node(n.node, "sCorpusStructure:SDocument")?;
                        } else {
                            salt_writer
                                .write_graphannis_node(n.node, "sCorpusStructure:SCorpus")?;
                        }
                    }

                    // Map PartOf edges of this corpus/document
                    for partof_component in
                        graph.get_all_components(Some(AnnotationComponentType::PartOf), None)
                    {
                        let partof_gs = graph
                            .get_graphstorage_as_ref(&partof_component)
                            .with_context(|| {
                                format!("Missing graph storage for component {partof_component}")
                            })?;
                        for source in corpus_nodes.iter() {
                            let source = source.node;
                            for target in partof_gs.get_outgoing_edges(source) {
                                let target = target?;
                                let edge = Edge { source, target };
                                salt_writer.write_graphannis_edge(edge, &partof_component)?;
                            }
                        }
                    }

                    // Write out the layer XML nodes
                    salt_writer.write_all_layers()?;
                    Ok(())
                })?;

            Ok(())
        })?;

        Ok(documents)
    }
}
