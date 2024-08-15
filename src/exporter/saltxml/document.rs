use std::{collections::BTreeMap, fs::File, io::BufWriter, sync::Arc};

use anyhow::{Context, Error};
use graphannis::{
    graph::{AnnoKey, Annotation, Edge, GraphStorage, NodeID},
    model::AnnotationComponentType,
    AnnotationGraph,
};
use graphannis_core::{
    annostorage::ValueSearch,
    dfs::CycleSafeDFS,
    graph::{ANNIS_NS, NODE_NAME_KEY, NODE_TYPE},
};
use quick_xml::events::{BytesDecl, Event};

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
    source_token: NodeID,
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
        let buffered_output_file = BufWriter::new(output_file);
        let mut writer = quick_xml::Writer::new_with_indent(buffered_output_file, b' ', 2);
        writer.write_event(Event::Decl(BytesDecl::new("1.1", Some("UTF-8"), None)))?;

        let graph_tag = writer
            .create_element("sDocumentStructure:SDocumentGraph")
            .with_attribute(("xmlns:sDocumentStructure", "sDocumentStructure"))
            .with_attribute(("xmlns:saltCore", "saltCore"))
            .with_attribute(("xmlns:xmi", "http://www.omg.org/XMI"))
            .with_attribute(("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance"))
            .with_attribute(("xsi:version", "2.0"));
        let node_annos = graph.get_node_annos();
        let node_name = node_annos
            .get_value_for_item(&document_node_id, &NODE_NAME_KEY)?
            .context("Missing node name for document graph")?;
        let salt_id = format!("T::salt:/{node_name}");
        graph_tag.write_inner_content::<_, Error>(|writer| {
            writer
                .create_element("labels")
                .with_attribute(("xsi:type", "saltCore:SElementId"))
                .with_attribute(("namespace", "salt"))
                .with_attribute(("name", "id"))
                .with_attribute(("value", salt_id.as_str()))
                .write_empty()?;
            let mut salt_writer = SaltWriter::new(graph, writer)?;

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
                            salt_writer.write_graphannis_edge(edge, &c)?;
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
                        salt_writer.write_graphannis_edge(edge, &c)?;
                    }
                }
            }

            // export textual data sources and STextualRelations to the token
            self.map_textual_ds(graph, document_node_id, &mut salt_writer)?;

            // TODO: export media file references and annis:time annotations
            // TODO: export timeline

            // Write out the layer XML nodes
            salt_writer.write_all_layers()?;

            Ok(())
        })?;

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

    fn map_textual_ds<W>(
        &self,
        graph: &AnnotationGraph,
        document_node_id: NodeID,
        salt_writer: &mut SaltWriter<W>,
    ) -> anyhow::Result<()>
    where
        W: std::io::Write,
    {
        let ordering_components =
            graph.get_all_components(Some(AnnotationComponentType::Ordering), None);

        let corpus_graph_helper = CorpusGraphHelper::new(graph);

        let mut collected_edges: Vec<TextProperty> = Vec::new();
        let mut textual_ds_node_names: BTreeMap<String, String> = BTreeMap::new();

        for c in ordering_components {
            let text_name = c.name.as_str();
            let gs = graph
                .get_graphstorage_as_ref(&c)
                .context("Missing graph storage for component")?;

            // Collect the necessary edge information and the actual text for
            // this data source by iterating over the ordering edges.
            let mut content = String::new();

            for root in gs.root_nodes() {
                let root = root?;
                if corpus_graph_helper.is_part_of(root, document_node_id)? {
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
                            source_token: step.node,
                        };
                        collected_edges.push(prop);
                    }
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
            let features = vec![Annotation {
                key: AnnoKey {
                    ns: "saltCommon".into(),
                    name: "SDATA".into(),
                },
                val: content.into(),
            }];
            let ds_node_name = format!("{document_node_name}#{sname}");
            salt_writer.write_node(
                NodeType::Custom(ds_node_name.clone()),
                sname,
                "sDocumentStructure:STextualDS",
                &[],
                &features,
                None,
            )?;
            textual_ds_node_names.insert(text_name.to_string(), ds_node_name);

            // TODO: check if this actually a timeline
        }

        // Write out all collected edges
        for text_property in collected_edges {
            let source = NodeType::Id(text_property.source_token);
            let target_ds = textual_ds_node_names
                .get(&text_property.text_name)
                .context("Missing STextualDS Salt ID")?;
            let target = NodeType::Custom(target_ds.clone());

            let features = vec![
                Annotation {
                    key: AnnoKey {
                        name: "SSTART".into(),
                        ns: "salt".into(),
                    },
                    val: text_property.start.to_string().into(),
                },
                Annotation {
                    key: AnnoKey {
                        name: "SEND".into(),
                        ns: "salt".into(),
                    },
                    val: text_property.end.to_string().into(),
                },
            ];

            salt_writer.write_edge(
                source,
                target,
                "sDocumentStructure:STextualRelation",
                &[],
                &features,
                None,
            )?;
        }

        Ok(())
    }
}
