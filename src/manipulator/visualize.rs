use super::Manipulator;
use crate::{
    util::{self, token_helper::TokenHelper},
    StepID,
};
use anyhow::{Context, Result};
use documented::{Documented, DocumentedFields};
use graphannis::{
    model::{AnnotationComponent, AnnotationComponentType},
    AnnotationGraph,
};
use graphannis_core::types::NodeID as GraphAnnisNodeID;
use graphannis_core::{
    dfs,
    graph::{storage::union::UnionEdgeContainer, NODE_NAME_KEY},
};
use graphviz_rust::{
    cmd::Format,
    dot_generator::*,
    dot_structures::*,
    exec,
    printer::{DotPrinter, PrinterContext},
};
use itertools::Itertools;

use serde::Deserialize;
use std::{borrow::Cow, collections::HashSet, path::PathBuf};
use struct_field_names_as_array::FieldNamesAsSlice;

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case", untagged)]
pub(crate) enum Include {
    All,
    #[default]
    FirstDocument,
    Document(String),
}

/// Output the currrent graph as SVG or DOT file for debugging it.
///
/// **Important:** You need to have the[GraphViz](https://graphviz.org/)
/// software installed to use this graph operation.
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(deny_unknown_fields)]
pub struct Visualize {
    /// Configure whether to limit the number of tokens visualized. If `true`,
    ///  only the first tokens and the nodes connected to these token are
    /// included. The specific number can be configured with the parameter
    /// `token_limit`.
    /// **Per default, limiting the number of tokens is enabled**
    ///
    /// ```toml
    /// [[graph_op]]
    /// action = "visualize"
    ///
    /// [graph_op.config]
    /// limit_tokens = true
    /// token_limit = 10
    /// ```
    ///
    /// To include all token, use the value `false`.
    /// ```toml
    /// [[graph_op]]
    /// action = "visualize"
    ///
    /// [graph_op.config]
    /// limit_tokens = false
    /// ```
    #[serde(default = "default_limit_tokens")]
    limit_tokens: bool,
    /// If `limit_tokens` is set to `true`, the number of tokens to include.
    /// Default is `10`.
    #[serde(default = "default_token_limit")]
    token_limit: usize,

    /// Which root node should be used. Per default, this visualization only
    /// includes the first document.
    ///
    /// ```toml
    /// [[graph_op]]
    /// action = "visualize"
    ///
    /// [graph_op.config]
    /// root = "first_document"
    /// ```
    ///
    /// Alternativly it can be configured to include all documents (`root = "all"`) or you can give the ID of the document as argument.
    /// ``toml
    /// [graph_op.config]
    /// root = "mycorpus/subcorpus1/mydocument"
    /// ```
    #[serde(default)]
    root: Include,
    /// If set, a DOT file is created at this path (relative to the workflow directory).
    /// The default is to not create a DOT file.
    #[serde(default)]
    output_dot: Option<PathBuf>,
    /// If set, a SVG file is created at this path, which must is relative to the workflow directory.
    /// The default is to create a SVG file at the path `graph-visualization.svg`.
    #[serde(default = "default_output_svg")]
    output_svg: Option<PathBuf>,
}

fn default_limit_tokens() -> bool {
    true
}

fn default_token_limit() -> usize {
    10
}

fn default_output_svg() -> Option<PathBuf> {
    Some("graph-visualization.svg".into())
}

impl Visualize {
    fn create_graph(&self, graph: &AnnotationGraph) -> Result<Graph> {
        let mut output = Graph::DiGraph {
            id: Id::Plain("G".to_string()),
            strict: false,
            stmts: Vec::new(),
        };

        let token_helper = TokenHelper::new(graph)?;

        let parent_id = self.get_root_node_name(graph)?;
        let all_token = token_helper.get_ordered_token(&parent_id, None)?;
        let included_token = if self.limit_tokens {
            all_token.into_iter().take(self.token_limit).collect_vec()
        } else {
            all_token
        };

        let mut subgraph = subgraph!("token"; attr!("rank", "same"));
        for t in included_token.iter() {
            subgraph.stmts.push(self.create_node_stmt(*t, graph)?);
        }
        output.add_stmt(stmt!(subgraph));

        // Add all other nodes that are somehow connected to the included token
        let all_components = graph.get_all_components(None, None);
        let all_gs = all_components
            .iter()
            .filter_map(|c| graph.get_graphstorage(c))
            .collect_vec();
        let all_edge_container =
            UnionEdgeContainer::new(all_gs.iter().map(|gs| gs.as_edgecontainer()).collect_vec());

        let mut included_nodes: HashSet<graphannis_core::types::NodeID> =
            included_token.iter().copied().collect();
        for t in included_token {
            for step in dfs::CycleSafeDFS::new(&all_edge_container, t, 1, usize::MAX) {
                let n = step?.node;
                if !token_helper.is_token(n)? && included_nodes.insert(n) {
                    output.add_stmt(self.create_node_stmt(n, graph)?);
                }
            }
            for step in dfs::CycleSafeDFS::new_inverse(&all_edge_container, t, 1, usize::MAX) {
                let n = step?.node;
                if !token_helper.is_token(n)? && included_nodes.insert(n) {
                    output.add_stmt(self.create_node_stmt(n, graph)?);
                }
            }
        }

        // Output all edges grouped by their component
        for component in all_components.iter() {
            let gs = graph
                .get_graphstorage_as_ref(component)
                .context("Missing graph storage")?;

            for source_node in gs.source_nodes() {
                let source_node = source_node?;
                if included_nodes.contains(&source_node) {
                    for target_node in gs.get_outgoing_edges(source_node) {
                        let target_node = target_node?;

                        if included_nodes.contains(&source_node)
                            && included_nodes.contains(&target_node)
                        {
                            output.add_stmt(self.create_edge_stmt(
                                source_node,
                                target_node,
                                component,
                            )?);
                        }
                    }
                }
            }
        }

        Ok(output)
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

    fn create_node_stmt(&self, n: GraphAnnisNodeID, input: &AnnotationGraph) -> Result<Stmt> {
        let node_name = input
            .get_node_annos()
            .get_value_for_item(&n, &NODE_NAME_KEY)?
            .unwrap_or_else(|| Cow::Owned(n.to_string()));

        let annos = input.get_node_annos().get_annotations_for_item(&n)?;
        let annos = annos
            .into_iter()
            .filter(|a| &a.key != NODE_NAME_KEY.as_ref())
            .sorted()
            .collect_vec();

        let anno_string = annos
            .into_iter()
            .map(|a| format!("{}:{}={}", a.key.ns, a.key.name, a.val))
            .join("\\n");

        let label = format!("\"{node_name}\\n \\n{anno_string}\"");

        Ok(stmt!(
            node!(n.to_string(); attr!("shape", "box"), attr!("label", label))
        ))
    }

    fn create_edge_stmt(
        &self,
        source_node: GraphAnnisNodeID,
        target_node: GraphAnnisNodeID,
        component: &AnnotationComponent,
    ) -> Result<Stmt> {
        let component_short_code = match component.get_type() {
            AnnotationComponentType::Coverage => "C",
            AnnotationComponentType::Dominance => ">",
            AnnotationComponentType::Pointing => "->",
            AnnotationComponentType::Ordering => ".",
            AnnotationComponentType::LeftToken => "LT",
            AnnotationComponentType::RightToken => "RT",
            AnnotationComponentType::PartOf => "@",
        };
        let label = format!(
            "\"{}/{} ({component_short_code})\"",
            component.layer, component.name
        );
        let color = match component.get_type() {
            AnnotationComponentType::Ordering => "blue",
            AnnotationComponentType::Dominance => "red",
            AnnotationComponentType::Coverage => "darkgreen",
            AnnotationComponentType::LeftToken | AnnotationComponentType::RightToken => "dimgray",
            AnnotationComponentType::PartOf => "gold",
            _ => "black",
        };
        let style = match component.get_type() {
            AnnotationComponentType::Coverage => "dotted",
            AnnotationComponentType::LeftToken | AnnotationComponentType::RightToken => "dashed",
            _ => "solid",
        };
        Ok(stmt!(edge!(node_id!(source_node) => node_id!(target_node);
            attr!("label", label),
            attr!("color", color),
            attr!("fontcolor", color),
            attr!("style", style)
        )))
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

        let output = self.create_graph(graph)?;

        if let Some(file_path) = &self.output_dot {
            let graph_dot = output.print(&mut PrinterContext::default());
            std::fs::write(workflow_directory.join(file_path), graph_dot)?;
        }

        if let Some(file_path) = &self.output_svg {
            let graph_svg = exec(
                output,
                &mut PrinterContext::default(),
                vec![Format::Svg.into()],
            )?;
            std::fs::write(workflow_directory.join(file_path), graph_svg)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use insta::assert_snapshot;
    use tempfile::tempdir;

    use crate::workflow::execute_from_file;

    #[test]
    fn dot_single_sentence_limit() {
        let workflow_dir = tempdir().unwrap();
        let workflow_file = workflow_dir.path().join("visualize.toml");
        std::fs::copy(
            Path::new("./tests/workflows/visualize_limit.toml"),
            &workflow_file,
        )
        .unwrap();
        execute_from_file(&workflow_file, true, None).unwrap();
        let result_dot = std::fs::read_to_string(workflow_dir.path().join("test.dot")).unwrap();
        assert_snapshot!(result_dot);
    }

    #[test]
    fn dot_single_sentence_full() {
        let workflow_dir = tempdir().unwrap();
        let workflow_file = workflow_dir.path().join("visualize.toml");
        std::fs::copy(
            Path::new("./tests/workflows/visualize_full.toml"),
            &workflow_file,
        )
        .unwrap();
        execute_from_file(&workflow_file, true, None).unwrap();
        let result_dot = std::fs::read_to_string(workflow_dir.path().join("test.dot")).unwrap();
        assert_snapshot!(result_dot);
    }
}
