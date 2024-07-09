use super::Manipulator;
use crate::{
    util::{self, token_helper::TokenHelper},
    StepID,
};
use anyhow::{Context, Result};
use documented::{Documented, DocumentedFields};
use graphannis::AnnotationGraph;
use graphannis_core::{graph::NODE_NAME_KEY, types::NodeID};
use itertools::Itertools;
use layout::{
    adt::dag::NodeHandle,
    backends::svg::SVGWriter,
    core::{base::Orientation, style::StyleAttr},
    std_shapes::{
        render::get_shape_size,
        shapes::{Arrow, Element, ShapeKind},
    },
};
use layout::{core::utils::save_to_file, topo::layout::VisualGraph};
use serde::Deserialize;
use std::{borrow::Cow, collections::HashMap};
use struct_field_names_as_array::FieldNamesAsSlice;

#[derive(Default, Deserialize)]
pub enum Include {
    All,
    #[default]
    FirstDocument,
    Document(String),
}

/// Output the currrent graph as SVG for debugging it.
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(deny_unknown_fields)]
pub struct Visualize {
    /// Limit number of token visualized. If given, only the first token and the
    /// nodes connected to these token are included. The default value is `50`.
    #[serde(default = "default_token_number")]
    max_token_number: Option<usize>,
    /// Which root node should be used. Per default, this visualization only
    /// includes the first document.
    ///
    /// ``toml
    /// [[graph_op]]
    ///
    /// action = "visualize"
    ///
    /// [graph_op.config]
    /// root = "first_document"
    /// ```
    /// Alternativly it can be configured to include all documents (`root = "all"`) or you can give the ID of the document as argument.
    /// ``toml
    /// [graph_op.config]
    /// root = "mycorpus/subcorpus1/mydocument"
    /// ```
    #[serde(default)]
    root: Include,
}

fn default_token_number() -> Option<usize> {
    Some(50)
}

impl Visualize {
    fn create_graph(&self, graph: &AnnotationGraph) -> Result<VisualGraph> {
        let mut result = VisualGraph::new(Orientation::TopToBottom);

        let token_helper = TokenHelper::new(graph)?;

        let parent_id = self.get_root_node_name(graph)?;
        let all_token = token_helper.get_ordered_token(&parent_id, None)?;
        let included_token = if let Some(limit) = self.max_token_number {
            all_token.into_iter().take(limit).collect_vec()
        } else {
            all_token
        };

        let mut node_to_handle = HashMap::new();

        for t in included_token {
            let h = self.add_node(&mut result, t, graph)?;
            node_to_handle.insert(t, h);
        }

        // Output all edges
        for component in graph.get_all_components(None, None) {
            let gs = graph
                .get_graphstorage_as_ref(&component)
                .context("Missing graph storage")?;

            for source_node in gs.source_nodes() {
                let source_node = source_node?;
                for target_node in gs.get_outgoing_edges(source_node) {
                    let target_node = target_node?;
                    let source_handle = node_to_handle.entry(source_node).or_insert_with(|| {
                        let shape_kind = ShapeKind::new_box(&source_node.to_string());
                        let shape_size =
                            get_shape_size(Orientation::LeftToRight, &shape_kind, 14, false);
                        result.add_node(Element::create(
                            shape_kind,
                            StyleAttr::simple(),
                            Orientation::LeftToRight,
                            shape_size,
                        ))
                    });
                    let source_handle = *source_handle;
                    let target_handle = node_to_handle.entry(target_node).or_insert_with(|| {
                        let shape_kind = ShapeKind::new_box(&target_node.to_string());
                        let shape_size =
                            get_shape_size(Orientation::LeftToRight, &shape_kind, 14, false);
                        result.add_node(Element::create(
                            shape_kind,
                            StyleAttr::simple(),
                            Orientation::LeftToRight,
                            shape_size,
                        ))
                    });
                    let target_handle = *target_handle;
                    result.add_edge(
                        Arrow::simple(&component.to_string()),
                        source_handle,
                        target_handle,
                    );
                }
            }
        }

        Ok(result)
    }

    fn get_root_node_name(&self, graph: &AnnotationGraph) -> Result<String> {
        match &self.root {
            Include::All => {
                let roots = util::get_root_corpus_node_names(graph)?;
                let first_root = roots.into_iter().next().unwrap_or_default();
                Ok(first_root)
            }
            Include::FirstDocument => {
                let documents = util::get_document_node_names(graph)?;
                let first_document = documents.into_iter().next().unwrap_or_default();
                Ok(first_document)
            }
            Include::Document(node_name) => Ok(node_name.clone()),
        }
    }

    fn add_node(
        &self,
        output: &mut VisualGraph,
        n: NodeID,
        graph: &AnnotationGraph,
    ) -> Result<NodeHandle> {
        let node_name = graph
            .get_node_annos()
            .get_value_for_item(&n, &NODE_NAME_KEY)?
            .unwrap_or_else(|| Cow::Owned(n.to_string()));

        let annos = graph.get_node_annos().get_annotations_for_item(&n)?;
        let annos = annos
            .into_iter()
            .filter(|a| &a.key != NODE_NAME_KEY.as_ref())
            .sorted()
            .collect_vec();

        let anno_string = annos
            .into_iter()
            .map(|a| format!("{}:{}={}", a.key.ns, a.key.name, a.val))
            .join("\n");

        let label = format!("{node_name}\n \n{anno_string}");

        let shape_kind = ShapeKind::Box(label.to_string());
        let shape_size = get_shape_size(Orientation::LeftToRight, &shape_kind, 14, false);

        let node_element = Element::create(
            shape_kind,
            StyleAttr::simple(),
            Orientation::TopToBottom,
            shape_size,
        );

        let handle = output.add_node(node_element);

        Ok(handle)
    }
}

impl Manipulator for Visualize {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        workflow_directory: &std::path::Path,
        _step_id: StepID,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        //        let progress = ProgressReporter::new_unknown_total_work(tx, step_id)?;

        let mut vg = self.create_graph(graph)?;
        // Layout the graph and create an SVG file
        let mut svg = SVGWriter::new();
        vg.do_it(false, false, false, &mut svg);
        let content = svg.finalize();

        save_to_file(
            workflow_directory
                .join("graph-debug.svg")
                .to_string_lossy()
                .as_ref(),
            &content,
        )?;
        Ok(())
    }
}
