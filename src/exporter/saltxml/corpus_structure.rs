use std::ffi::OsStr;

use graphannis::AnnotationGraph;
use graphannis_core::{
    annostorage::ValueSearch,
    graph::{ANNIS_NS, NODE_TYPE},
};
use xml::{writer::XmlEvent, EmitterConfig};

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
    ) -> anyhow::Result<()> {
        let corpus_name = output_path
            .file_name()
            .unwrap_or_else(|| OsStr::new("corpus"));

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

        // Map all corpus nodes in the graph
        let corpus_nodes = graph.get_node_annos().exact_anno_search(
            Some(ANNIS_NS),
            NODE_TYPE,
            ValueSearch::Some("corpus"),
        );
        for n in corpus_nodes {
            let n = n?;
            salt_writer.write_node(n.node, "sCorpusStructure:SCorpus")?;
            // TODO: Map PartOf edges of this corpus/document
        }

        writer.write(XmlEvent::end_element())?;
        writer.write(XmlEvent::end_element())?;

        Ok(())
    }
}
