use std::ffi::OsStr;

use anyhow::Context;
use graphannis::{
    graph::{Edge, NodeID},
    model::AnnotationComponentType,
    AnnotationGraph,
};
use graphannis_core::{
    annostorage::ValueSearch,
    graph::{ANNIS_NS, NODE_TYPE},
};
use xml::{writer::XmlEvent, EmitterConfig};

use crate::util::CorpusGraphHelper;

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
    ) -> anyhow::Result<Vec<NodeID>> {
        let corpus_name = output_path
            .file_name()
            .unwrap_or_else(|| OsStr::new("corpus"));

        let mut documents = Vec::new();

        let project_file_path = output_path.join("saltProject.salt");
        let output_file = std::fs::File::create(project_file_path)?;
        let mut writer = EmitterConfig::new()
            .perform_indent(true)
            .create_writer(output_file);

        writer.write(XmlEvent::StartDocument {
            version: xml::common::XmlVersion::Version11,
            encoding: Some("UTF-8"),
            standalone: None,
        })?;

        writer.write(
            XmlEvent::start_element("saltCommon:SaltProject")
                .ns("xmi", "http://www.omg.org/XMI")
                .ns("xsi", "http://www.w3.org/2001/XMLSchema-instance")
                .ns("sCorpusStructure", "sCorpusStructure")
                .ns("saltCore", "saltCore")
                .ns("saltCommon", "saltCommon")
                .attr("xsi:version", "2.0"),
        )?;

        // The whole corpus is the equivalent of a corpus graph, so we only
        // output one sCorpusGraph and give it the name of the corpus.
        writer.write(XmlEvent::start_element("sCorpusGraphs"))?;
        writer.write(
            XmlEvent::start_element("labels")
                .attr("xsi:type", "saltCore:SFeature")
                .attr("namespace", "salt")
                .attr("name", "id")
                .attr("value", &format!("T::{}", corpus_name.to_string_lossy())),
        )?;
        writer.write(XmlEvent::end_element())?;

        let mut salt_writer = SaltWriter::new(graph, &mut writer)?;

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
                salt_writer.write_graphannis_node(n.node, "sCorpusStructure:SDocument")?;
            } else {
                salt_writer.write_graphannis_node(n.node, "sCorpusStructure:SCorpus")?;
            }
        }

        // Map PartOf edges of this corpus/document
        for partof_component in
            graph.get_all_components(Some(AnnotationComponentType::PartOf), None)
        {
            let partof_gs = graph
                .get_graphstorage_as_ref(&partof_component)
                .with_context(|| {
                    format!("Missing graph storage for component {}", partof_component)
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

        // Close <sCorpusGraphs>
        writer.write(XmlEvent::end_element())?;
        // Close <SaltProject>
        writer.write(XmlEvent::end_element())?;

        Ok(documents)
    }
}
