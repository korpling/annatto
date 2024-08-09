use anyhow::Context;
use graphannis::{graph::NodeID, AnnotationGraph};
use graphannis_core::{dfs, graph::NODE_NAME_KEY};
use xml::{writer::XmlEvent, EmitterConfig};

use crate::util::CorpusGraphHelper;

pub(super) struct SaltDocumentGraphMapper {}

impl SaltDocumentGraphMapper {
    pub(super) fn new() -> SaltDocumentGraphMapper {
        SaltDocumentGraphMapper {}
    }

    pub(super) fn map_document_graph(
        &self,
        graph: &AnnotationGraph,
        document_node_id: NodeID,
        output_path: &std::path::Path,
    ) -> anyhow::Result<()> {
        let node_annos = graph.get_node_annos();
        let corpusgraph_helper = CorpusGraphHelper::new(graph);
        let partof_gs = corpusgraph_helper.as_edgecontainer();
        let mut last_distance = 0;
        let mut parent_folder_names = Vec::new();
        for step in dfs::CycleSafeDFS::new(&partof_gs, document_node_id, 1, usize::MAX) {
            let step = step?;
            if step.distance > last_distance {
                let full_corpus_name = node_annos
                    .get_value_for_item(&step.node, &NODE_NAME_KEY)?
                    .context("Missing node name for parent corpus")?;
                // Interpret the full corpus name as Salt ID but the the last
                // part of the splitted path as folder name.
                let folder_name = full_corpus_name
                    .split('/')
                    .last()
                    .context("Empty corpus name")?
                    .to_string();
                parent_folder_names.push(folder_name);
            }
            last_distance = step.distance;
        }
        parent_folder_names.reverse();
        let mut salt_file_path = output_path.to_path_buf();
        for p in parent_folder_names {
            salt_file_path.push(p);
            if !salt_file_path.exists() {
                std::fs::create_dir(&salt_file_path)?;
            }
        }

        let document_node_name = node_annos
            .get_value_for_item(&document_node_id, &NODE_NAME_KEY)?
            .context("Missing node name for document")?;
        let document_file_name = document_node_name
            .split('/')
            .last()
            .context("Empty document name")?;
        salt_file_path.push(format!("{document_file_name}.salt"));

        let output_file = std::fs::File::create(salt_file_path)?;
        let mut writer = EmitterConfig::new()
            .perform_indent(true)
            .create_writer(output_file);

        writer.write(XmlEvent::StartDocument {
            version: xml::common::XmlVersion::Version11,
            encoding: Some("UTF-8"),
            standalone: None,
        })?;

        writer.write(
            XmlEvent::start_element("sDocumentStructure:SDocumentGraph")
                .ns("xmi", "http://www.omg.org/XMI")
                .ns("xsi", "http://www.w3.org/2001/XMLSchema-instance")
                .ns("sDocumentStructure", "sDocumentStructure")
                .ns("saltCore", "saltCore")
                .attr("xsi:version", "2.0"),
        )?;

        // Close <SDocumentGraph>
        writer.write(XmlEvent::end_element())?;

        Ok(())
    }
}
