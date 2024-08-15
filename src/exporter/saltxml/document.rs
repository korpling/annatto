use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    convert::TryInto,
    fs::File,
    io::BufWriter,
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Context, Error};
use graphannis::{
    graph::{AnnoKey, Edge, GraphStorage, NodeID},
    model::{AnnotationComponent, AnnotationComponentType},
    AnnotationGraph,
};
use graphannis_core::{
    dfs::CycleSafeDFS,
    graph::{ANNIS_NS, NODE_NAME_KEY},
};
use quick_xml::events::{BytesDecl, Event};
use regex::Regex;

use crate::{
    importer::saltxml::SaltObject,
    progress::ProgressReporter,
    util::{
        token_helper::{TokenHelper, TOKEN_KEY},
        CorpusGraphHelper,
    },
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

pub(super) struct SaltDocumentGraphMapper {
    textual_ds_node_names: BTreeMap<String, String>,
    collected_token: Vec<TextProperty>,
    timeline_items: Vec<TextProperty>,
}

impl SaltDocumentGraphMapper {
    pub(super) fn new() -> SaltDocumentGraphMapper {
        SaltDocumentGraphMapper {
            textual_ds_node_names: BTreeMap::new(),
            collected_token: Vec::new(),
            timeline_items: Vec::new(),
        }
    }

    pub(super) fn map_document_graph(
        &mut self,
        graph: &AnnotationGraph,
        document_node_id: NodeID,
        output_path: &std::path::Path,
        progress: &ProgressReporter,
    ) -> anyhow::Result<()> {
        let corpusgraph_helper = CorpusGraphHelper::new(graph);

        let (output_path, output_file) =
            self.create_saltfile(graph, document_node_id, &corpusgraph_helper, output_path)?;
        progress.info(&format!(
            "Writing SaltXML file {}",
            output_path.to_string_lossy()
        ))?;
        let buffered_output_file = BufWriter::new(output_file);
        let mut writer = quick_xml::Writer::new_with_indent(buffered_output_file, b' ', 2);
        writer.write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))?;

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
            let mut salt_writer = SaltWriter::new(graph, writer, &output_path, progress)?;

            // Map all nodes in the annotation graph

            let tok_helper = TokenHelper::new(graph)?;
            let all_dominance_gs: Vec<_> = graph
                .get_all_components(Some(AnnotationComponentType::Dominance), None)
                .into_iter()
                .filter_map(|c| graph.get_graphstorage(&c))
                .collect();
            let mut span_nodes = Vec::new();
            for n in corpusgraph_helper.all_nodes_part_of(document_node_id) {
                let n = n?;
                if corpusgraph_helper.is_annotation_node(n)? {
                    let salt_type = if tok_helper.is_token(n)? {
                        "sDocumentStructure:SToken"
                    } else if node_is_span(n, &tok_helper, &all_dominance_gs)? {
                        span_nodes.push(n);
                        "sDocumentStructure:SSpan"
                    } else {
                        "sDocumentStructure:SStructure"
                    };
                    salt_writer.write_graphannis_node(n, salt_type)?;
                }
            }

            // Get the graph storages for all components that are relevant for
            // the annotation graph (not the corpus graph).
            let mut anno_graph_storages = Vec::new();
            for ctype in [
                AnnotationComponentType::Dominance,
                AnnotationComponentType::Pointing,
                AnnotationComponentType::Ordering,
            ] {
                for c in graph.get_all_components(Some(ctype), None) {
                    let gs = graph
                        .get_graphstorage(&c)
                        .context("Missing graph storage for component")?;
                    anno_graph_storages.push((c, gs));
                }
            }

            // Use all nodes of the document as potential source nodes for this graph storage
            for source in corpusgraph_helper.all_nodes_part_of(document_node_id) {
                let source = source?;
                for (c, gs) in anno_graph_storages.iter() {
                    for target in gs.get_outgoing_edges(source) {
                        let target = target?;
                        let edge = Edge { source, target };
                        salt_writer.write_graphannis_edge(edge, c)?;
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
            self.map_textual_ds_and_timeline(
                graph,
                document_node_id,
                &tok_helper,
                progress,
                &mut salt_writer,
            )?;

            // TODO: export media file references and annis:time annotations

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
        corpusgraph_helper: &CorpusGraphHelper,
        output_path: &std::path::Path,
    ) -> anyhow::Result<(PathBuf, File)> {
        let node_annos = graph.get_node_annos();

        let mut last_distance = 0;
        let mut parent_folder_names = Vec::new();
        for step in CycleSafeDFS::new(corpusgraph_helper, document_node_id, 1, usize::MAX) {
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

        let output_file = std::fs::File::create(&salt_file_path)?;

        Ok((salt_file_path, output_file))
    }

    fn map_textual_ds_and_timeline<W>(
        &mut self,
        graph: &AnnotationGraph,
        document_node_id: NodeID,
        tok_helper: &TokenHelper,
        progress: &ProgressReporter,
        salt_writer: &mut SaltWriter<W>,
    ) -> anyhow::Result<()>
    where
        W: std::io::Write,
    {
        let mut ordering_components =
            graph.get_all_components(Some(AnnotationComponentType::Ordering), None);

        let corpus_graph_helper = CorpusGraphHelper::new(graph);

        let mut timeline_ordering = None;
        if ordering_components.len() > 1 {
            if let Some(idx) = ordering_components
                .iter()
                .position(|c| c.name.is_empty() && c.layer == ANNIS_NS)
            {
                timeline_ordering = Some(ordering_components.remove(idx));
            }
        };

        let document_node_name = graph
            .get_node_annos()
            .get_value_for_item(&document_node_id, &NODE_NAME_KEY)?
            .context("Missing node name for document")?;

        let timeline_id = format!("{document_node_name}#sTimeline1");
        if let Some(c) = &timeline_ordering {
            // Add a timeline with the correct number of timeline items
            let (content, timeline_items) =
                self.collect_token_for_component(c, graph, &corpus_graph_helper, document_node_id)?;
            self.timeline_items.extend(timeline_items);
            let empty_content_matcher = Regex::new("\\A\\s*\\z")?;
            if !empty_content_matcher.is_match(&content) {
                progress.warn(&format!("Text for timeline is not empty and will be omitted from the SaltXML file, because this is unsupported by Salt ({document_node_name})."))?;
            }

            let tli_count = self.timeline_items.len();

            let features = vec![(
                AnnoKey {
                    ns: "saltCommon".into(),
                    name: "SDATA".into(),
                },
                SaltObject::Integer(tli_count.try_into()?),
            )];
            salt_writer.write_node(
                NodeType::Custom(timeline_id.clone()),
                "sTimeline1",
                "sDocumentStructure:STimeline",
                &[],
                &features,
                None,
            )?;
        }

        // Collect the necessary edge information and the actual text for
        // all data sources by iterating over the ordering edges.
        for c in ordering_components {
            let (content, token_for_component) = self.collect_token_for_component(
                &c,
                graph,
                &corpus_graph_helper,
                document_node_id,
            )?;
            self.collected_token.extend(token_for_component);
            self.map_single_datasource(&c, &document_node_name, content, salt_writer)?;
        }

        // Map the coverage edges from the token as STimelineRelation
        if timeline_ordering.is_some() {
            self.map_timeline_relations(&timeline_id, tok_helper, salt_writer)?;
        }

        // Write out all STextualRelation edges from the token to the textual data sources
        self.map_textual_relations(salt_writer)?;

        Ok(())
    }

    fn map_single_datasource<W>(
        &mut self,
        c: &AnnotationComponent,
        document_node_name: &str,
        text_content: String,
        salt_writer: &mut SaltWriter<W>,
    ) -> anyhow::Result<()>
    where
        W: std::io::Write,
    {
        let text_name = c.name.as_str();

        // TODO find matching "datasource" for this text and use its name
        let sname = if text_name.is_empty() {
            "sText1"
        } else {
            text_name
        };
        let features = vec![(
            AnnoKey {
                ns: "saltCommon".into(),
                name: "SDATA".into(),
            },
            SaltObject::Text(text_content),
        )];
        let ds_node_name = format!("{document_node_name}#{sname}");
        salt_writer.write_node(
            NodeType::Custom(ds_node_name.clone()),
            sname,
            "sDocumentStructure:STextualDS",
            &[],
            &features,
            None,
        )?;
        self.textual_ds_node_names
            .insert(text_name.to_string(), ds_node_name);
        Ok(())
    }

    fn map_textual_relations<W>(&self, salt_writer: &mut SaltWriter<W>) -> anyhow::Result<()>
    where
        W: std::io::Write,
    {
        for text_property in &self.collected_token {
            // TODO: map  the timeline relations from this segmentation node
            let source = NodeType::Id(text_property.source_token);
            let target_ds = self
                .textual_ds_node_names
                .get(&text_property.text_name)
                .context("Missing STextualDS Salt ID")?;
            let target = NodeType::Custom(target_ds.clone());

            let features = vec![
                (
                    AnnoKey {
                        name: "SSTART".into(),
                        ns: "salt".into(),
                    },
                    SaltObject::Integer(text_property.start.try_into()?),
                ),
                (
                    AnnoKey {
                        name: "SEND".into(),
                        ns: "salt".into(),
                    },
                    SaltObject::Integer(text_property.end.try_into()?),
                ),
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

    fn map_timeline_relations<W>(
        &self,
        timeline_id: &str,
        tok_helper: &TokenHelper,
        salt_writer: &mut SaltWriter<W>,
    ) -> anyhow::Result<()>
    where
        W: std::io::Write,
    {
        // Create a reverse index to get the position of each TLI in the
        // timeline.
        let tli_to_pos: HashMap<_, _> = self
            .timeline_items
            .iter()
            .enumerate()
            .map(|(idx, prop)| (prop.source_token, idx))
            .collect();

        // Finde all coverage edges for this document that have a segmentation
        // token as source and a timeline token as target.
        for segmentation_node in self.collected_token.iter() {
            let source_token = segmentation_node.source_token;
            // Get the smallest and largest TLI index covered by this token.
            let mut covered_tli_idx = BTreeSet::new();
            for tli in tok_helper.covered_token(source_token)? {
                let position = tli_to_pos
                    .get(&tli)
                    .context("Referenced timeline item has no position.")?;
                let position: i64 = (*position).try_into()?;
                covered_tli_idx.insert(position);
            }
            if let (Some(start), Some(end)) = (covered_tli_idx.first(), covered_tli_idx.last()) {
                let features = vec![
                    (
                        AnnoKey {
                            name: "SSTART".into(),
                            ns: "salt".into(),
                        },
                        SaltObject::Integer(*start),
                    ),
                    (
                        AnnoKey {
                            name: "SEND".into(),
                            ns: "salt".into(),
                        },
                        SaltObject::Integer(*end + 1),
                    ),
                ];

                salt_writer.write_edge(
                    NodeType::Id(source_token),
                    NodeType::Custom(timeline_id.to_string()),
                    "sDocumentStructure:STimelineRelation",
                    &[],
                    &features,
                    None,
                )?;
            }
        }

        Ok(())
    }

    fn collect_token_for_component(
        &self,
        c: &AnnotationComponent,
        graph: &AnnotationGraph,
        corpus_graph_helper: &CorpusGraphHelper,
        document_node_id: NodeID,
    ) -> anyhow::Result<(String, Vec<TextProperty>)> {
        let mut content = String::new();
        let text_name = c.name.as_str();
        let gs = graph
            .get_graphstorage_as_ref(c)
            .context("Missing graph storage for component")?;

        let mut token = Vec::new();

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
                    token.push(prop);
                }
            }
        }

        Ok((content, token))
    }
}
