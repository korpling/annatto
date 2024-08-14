use std::{collections::BTreeMap, fs::File, sync::Arc};

use anyhow::Context;
use graphannis::{
    graph::{Edge, GraphStorage, NodeID},
    model::AnnotationComponentType,
    AnnotationGraph,
};
use graphannis_core::{
    annostorage::ValueSearch,
    dfs::CycleSafeDFS,
    graph::{ANNIS_NS, NODE_NAME_KEY, NODE_TYPE},
};
use xml::{writer::XmlEvent, EmitterConfig};

use crate::util::{
    token_helper::{TokenHelper, TOKEN_KEY},
    CorpusGraphHelper,
};

use super::{NodeType, SaltWriter, TOK_WHITESPACE_AFTER_KEY, TOK_WHITESPACE_BEFORE_KEY};

#[derive(Eq, PartialEq, PartialOrd, Ord, Hash, Clone, Debug)]
pub struct TextProperty {
    text_name: String,
    start: usize,
    end: usize,
}

fn node_is_span(
    n: NodeID,
    tok_helper: &TokenHelper,
    dominance_gs: &[Arc<dyn GraphStorage>],
) -> anyhow::Result<bool> {
    let mut has_dominance_edge = false;
    for gs in dominance_gs.iter() {
        if gs.has_outgoing_edges(n)? {
            has_dominance_edge = true;
            break;
        }
    }
    if !has_dominance_edge && tok_helper.has_outgoing_coverage_edges(n)? {
        Ok(true)
    } else {
        Ok(false)
    }
}

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
        let output_file = self.create_saltfile(graph, document_node_id, output_path)?;
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
        let node_annos = graph.get_node_annos();
        let node_name = node_annos
            .get_value_for_item(&document_node_id, &NODE_NAME_KEY)?
            .context("Missing node name for document graph")?;
        let salt_id = format!("T::salt:/{node_name}");
        writer.write(
            XmlEvent::start_element("labels")
                .attr("xsi:type", "saltCore:SElementId")
                .attr("namespace", "salt")
                .attr("name", "id")
                .attr("value", &salt_id),
        )?;
        writer.write(XmlEvent::end_element())?;

        let mut salt_writer = SaltWriter::new(graph, &mut writer)?;

        // Map all nodes in the annotation graph
        let nodes: graphannis_core::errors::Result<Vec<_>> = graph
            .get_node_annos()
            .exact_anno_search(Some(ANNIS_NS), NODE_TYPE, ValueSearch::Some("node"))
            .collect();
        let nodes = nodes?;

        let tok_helper = TokenHelper::new(graph)?;
        let all_dominance_gs: Vec<_> = graph
            .get_all_components(Some(AnnotationComponentType::Dominance), None)
            .into_iter()
            .filter_map(|c| graph.get_graphstorage(&c))
            .collect();
        let mut span_nodes = Vec::new();
        for n in nodes.iter() {
            let salt_type = if tok_helper.is_token(n.node)? {
                "sDocumentStructure:SToken"
            } else if node_is_span(n.node, &tok_helper, &all_dominance_gs)? {
                span_nodes.push(n.node);
                "sDocumentStructure:SSpan"
            } else {
                "sDocumentStructure:SStructure"
            };
            salt_writer.write_graphannis_node(n.node, salt_type)?;
        }

        // Map the edges
        for ctype in [
            AnnotationComponentType::Dominance,
            AnnotationComponentType::Pointing,
            AnnotationComponentType::Ordering,
        ] {
            for c in graph.get_all_components(Some(ctype), None) {
                let gs = graph
                    .get_graphstorage_as_ref(&c)
                    .context("Missing graph storage for component")?;
                for source in gs.source_nodes() {
                    let source = source?;
                    for target in gs.get_outgoing_edges(source) {
                        let target = target?;
                        let edge = Edge { source, target };
                        salt_writer.write_edge(edge, &c)?;
                    }
                }
            }
        }

        // Map coverage edges for spans
        for c in graph.get_all_components(Some(AnnotationComponentType::Coverage), None) {
            let gs = graph
                .get_graphstorage_as_ref(&c)
                .context("Missing graph storage for component")?;
            for source in span_nodes.iter() {
                for target in gs.get_outgoing_edges(*source) {
                    let target = target?;
                    let edge = Edge {
                        source: *source,
                        target,
                    };
                    salt_writer.write_edge(edge, &c)?;
                }
            }
        }

        // export textual data sources and STextualRelations to the token
        self.map_textual_ds(graph, document_node_id, &mut salt_writer)?;

        // TODO: export media file references and annis:time annotations
        // TODO: export timeline

        // Write out the layer XML nodes
        salt_writer.write_all_layers()?;

        // Close <SDocumentGraph>
        writer.write(XmlEvent::end_element())?;

        Ok(())
    }

    fn create_saltfile(
        &self,
        graph: &AnnotationGraph,
        document_node_id: NodeID,
        output_path: &std::path::Path,
    ) -> anyhow::Result<File> {
        let node_annos = graph.get_node_annos();
        let corpusgraph_helper = CorpusGraphHelper::new(graph);
        let partof_gs = corpusgraph_helper.as_edgecontainer();
        let mut last_distance = 0;
        let mut parent_folder_names = Vec::new();
        for step in CycleSafeDFS::new(&partof_gs, document_node_id, 1, usize::MAX) {
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

        Ok(output_file)
    }

    fn map_textual_ds(
        &self,
        graph: &AnnotationGraph,
        document_node_id: NodeID,
        salt_writer: &mut SaltWriter<File>,
    ) -> anyhow::Result<()> {
        let ordering_components =
            graph.get_all_components(Some(AnnotationComponentType::Ordering), None);

        let mut edges_by_text: BTreeMap<String, Vec<TextProperty>> = BTreeMap::new();

        for c in ordering_components {
            let text_name = c.name.as_str();
            let gs = graph
                .get_graphstorage_as_ref(&c)
                .context("Missing graph storage for component")?;

            // Collect the necessary edge information and the actual text for
            // this data source by iterating over the ordering edges.
            let mut content = String::new();

            for root in gs.source_nodes() {
                let root = root?;

                for step in CycleSafeDFS::new(gs.as_edgecontainer(), root, 0, usize::MAX) {
                    let step = step?;

                    if let Some(tok_whitespace_before) = graph
                        .get_node_annos()
                        .get_value_for_item(&step.node, &TOK_WHITESPACE_BEFORE_KEY)?
                    {
                        content.push_str(&tok_whitespace_before)
                    }
                    let start = content.len();
                    if let Some(tok_value) = graph
                        .get_node_annos()
                        .get_value_for_item(&step.node, &TOKEN_KEY)?
                    {
                        content.push_str(&tok_value)
                    }
                    let end = content.len();
                    if let Some(tok_whitespace_after) = graph
                        .get_node_annos()
                        .get_value_for_item(&step.node, &TOK_WHITESPACE_AFTER_KEY)?
                    {
                        content.push_str(&tok_whitespace_after)
                    }

                    let prop = TextProperty {
                        text_name: text_name.to_string(),
                        start,
                        end,
                    };
                    edges_by_text
                        .entry(text_name.to_string())
                        .or_default()
                        .push(prop);
                }
            }

            let document_node_name = graph
                .get_node_annos()
                .get_value_for_item(&document_node_id, &NODE_NAME_KEY)?
                .context("Missing node name for document")?;
            // TODO find matching "datasource" for this text and use its name
            let sname = if text_name.is_empty() {
                "sText1"
            } else {
                text_name
            };
            salt_writer.write_node(
                NodeType::Custom(format!("{document_node_name}#{sname}")),
                sname,
                "sDocumentStructure:STextualDS",
                &[],
                None,
            )?;
            // TODO: check if this actually a timeline
        }

        Ok(())
    }
}
