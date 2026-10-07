use super::Manipulator;
use crate::{
    StepID,
    progress::ProgressReporter,
    util::{
        CorpusGraphHelper,
        token_helper::{TOKEN_KEY, TokenHelper},
    },
};
use anyhow::{Context, Result};
use facet::Facet;
use graphannis::{
    AnnotationGraph,
    graph::GraphStorage,
    model::{AnnotationComponent, AnnotationComponentType},
};
use graphannis_core::{
    annostorage::ValueSearch,
    graph::{ANNIS_NS, NODE_TYPE},
    types::NodeID as GraphAnnisNodeID,
};
use graphannis_core::{
    dfs,
    graph::{NODE_NAME_KEY, storage::union::UnionEdgeContainer},
};
use graphviz_rust::{
    cmd::Format,
    dot_generator::*,
    dot_structures::*,
    exec,
    printer::{DotPrinter, PrinterContext},
};
use itertools::Itertools;

use serde::{Deserialize, Serialize};
use std::{borrow::Cow, collections::HashSet, path::PathBuf};

#[derive(Facet, Default, Deserialize, Serialize, Clone, PartialEq, Debug)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
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
///
/// ## Example configuration
///
/// ```toml
/// [[graph_op]]
/// action = "visualize"
///
/// [graph_op.config]
/// output_svg = "debug.svg"
/// limit_tokens = true
/// token_limit = 10
/// ```
#[derive(Facet, Deserialize, Serialize, Clone, PartialEq)]
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
    /// Alternativly it can be configured to include all documents (`root = "all"`) or you can configure it select a document by its ID.
    /// ```toml
    /// [graph_op.config]
    /// root = {document = "mycorpus/subcorpus1/mydocument"}
    /// ```
    #[serde(default)]
    root: Include,
    /// If set, a DOT file is created at this path (relative to the workflow directory).
    /// The default is to not create a DOT file.
    #[serde(default)]
    output_dot: Option<PathBuf>,
    /// If set, a SVG file is created at this path, which must is relative to the workflow directory.
    // /The default is to not create a SVG file.
    #[serde(default)]
    output_svg: Option<PathBuf>,
}

fn default_limit_tokens() -> bool {
    true
}

fn default_token_limit() -> usize {
    10
}

impl Default for Visualize {
    fn default() -> Self {
        Self {
            limit_tokens: default_limit_tokens(),
            token_limit: default_token_limit(),
            root: Default::default(),
            output_dot: Default::default(),
            output_svg: Default::default(),
        }
    }
}

impl Visualize {
    fn create_graph(
        &self,
        graph: &AnnotationGraph,
        mut progress: ProgressReporter,
    ) -> Result<Graph> {
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
        progress.info(format!(
            "visualizing {} token from {parent_id}",
            included_token.len()
        ))?;

        let mut subgraph = subgraph!("token"; attr!("rank", "same"));
        for t in included_token.iter() {
            subgraph.stmts.push(self.create_node_stmt(*t, graph)?);
        }
        output.add_stmt(stmt!(subgraph));

        // Add all other nodes that are somehow connected to the included token and the document
        let all_components = graph.get_all_components(None, None);

        let all_non_pointing_gs = all_components
            .iter()
            .filter(|c| c.get_type() != AnnotationComponentType::Pointing)
            .filter_map(|c| graph.get_graphstorage(c))
            .collect_vec();

        // Iterate over all non-pointing components to find connected nodes
        let edge_container = UnionEdgeContainer::new(
            all_non_pointing_gs
                .iter()
                .map(|gs| gs.as_edgecontainer())
                .collect_vec(),
        );

        let mut included_nodes = HashSet::new();

        progress = progress.with_total_work(included_token.len())?;
        for t in included_token {
            if included_nodes.insert(t) {
                for step in dfs::CycleSafeDFS::new(&edge_container, t, 1, usize::MAX) {
                    let step = step?;
                    let n = step.node;

                    if !token_helper.is_token(n)? && included_nodes.insert(n) {
                        output.add_stmt(self.create_node_stmt(n, graph)?);
                    }
                }
                for step in dfs::CycleSafeDFS::new_inverse(&edge_container, t, 1, usize::MAX) {
                    let n = step?.node;
                    if !token_helper.is_token(n)? && included_nodes.insert(n) {
                        output.add_stmt(self.create_node_stmt(n, graph)?);
                    }
                }
            }

            progress.worked(1)?;
        }
        // Add all datasource nodes if they are connected to the included documents have not been already added
        let part_of_gs = graph
            .get_all_components(Some(AnnotationComponentType::PartOf), None)
            .into_iter()
            .filter_map(|c| graph.get_graphstorage(&c))
            .collect_vec();
        for ds in graph.get_node_annos().exact_anno_search(
            Some(ANNIS_NS),
            NODE_TYPE,
            ValueSearch::Some("datasource"),
        ) {
            let ds = ds?.node;
            if !included_nodes.contains(&ds) {
                // The datsource must be part of a document node that is already included
                let mut outgoing = HashSet::new();
                for gs in part_of_gs.iter() {
                    for o in gs.get_outgoing_edges(ds) {
                        outgoing.insert(o?);
                    }
                }
                if outgoing.intersection(&included_nodes).next().is_some() {
                    output.add_stmt(self.create_node_stmt(ds, graph)?);
                    included_nodes.insert(ds);
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
                                gs,
                            )?);
                        }
                    }
                }
            }
        }

        Ok(output)
    }

    fn get_root_node_name(&self, graph: &AnnotationGraph) -> Result<String> {
        let corpusgraph_helper = CorpusGraphHelper::new(graph);
        match &self.root {
            Include::All => {
                let roots = corpusgraph_helper.get_root_corpus_node_names()?;
                let first_root = roots.into_iter().next().unwrap_or_default();
                Ok(first_root)
            }
            Include::FirstDocument => {
                let documents = corpusgraph_helper.get_document_node_names()?;
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

        let mut displayed_annos = Vec::new();
        // if annis::tok is part of the annotations, put it at the beginning of the list
        if let Some(tok_anno) = annos.iter().find(|a| &a.key == TOKEN_KEY.as_ref()) {
            displayed_annos.push(tok_anno.clone());
        }
        // Add all remaining annotations
        displayed_annos.extend(
            annos
                .into_iter()
                .filter(|a| &a.key != NODE_NAME_KEY.as_ref() && &a.key != TOKEN_KEY.as_ref())
                .sorted(),
        );

        let anno_string = displayed_annos
            .into_iter()
            .map(|a| {
                format!(
                    "{}:{}={}",
                    a.key.ns,
                    a.key.name,
                    a.val.replace("\"", "\\\"")
                )
            })
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
        gs: &dyn GraphStorage,
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

        let annos = gs
            .get_anno_storage()
            .get_annotations_for_item(&graphannis_core::types::Edge::from((
                source_node,
                target_node,
            )))?
            .into_iter()
            .sorted()
            .collect_vec();

        let label = if annos.is_empty() {
            format!(
                "\"{}/{} ({component_short_code})\"",
                component.layer, component.name
            )
        } else {
            let anno_string = annos
                .into_iter()
                .map(|a| format!("{}:{}={}", a.key.ns, a.key.name, a.val))
                .join("\\n");

            format!(
                "\"{}/{} ({component_short_code})\\n{anno_string}\"",
                component.layer, component.name
            )
        };

        let color = match component.get_type() {
            AnnotationComponentType::Ordering => "blue",
            AnnotationComponentType::Dominance => "red",
            AnnotationComponentType::Coverage => "darkgreen",
            AnnotationComponentType::LeftToken | AnnotationComponentType::RightToken => "dimgray",
            AnnotationComponentType::PartOf => "gold",
            AnnotationComponentType::Pointing => "black",
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
        step_id: StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let output = self.create_graph(
            graph,
            ProgressReporter::new_unknown_total_work(tx.clone(), step_id.clone())?,
        )?;
        let progress = ProgressReporter::new_unknown_total_work(tx, step_id)?;

        if let Some(file_path) = &self.output_dot {
            progress.info(format!(
                "writing visualizer output DOT file {}",
                file_path.to_string_lossy()
            ))?;
            let graph_dot = output.print(&mut PrinterContext::default());
            std::fs::write(workflow_directory.join(file_path), graph_dot)?;
        }

        if let Some(file_path) = &self.output_svg {
            progress.info(format!(
                "writing visualizer output SVG file {}",
                file_path.to_string_lossy()
            ))?;
            let graph_svg = exec(
                output,
                &mut PrinterContext::default(),
                vec![Format::Svg.into()],
            )?;
            std::fs::write(workflow_directory.join(file_path), graph_svg)?;
        }

        Ok(())
    }

    fn requires_statistics(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use graphannis::{AnnotationGraph, update::GraphUpdate};
    use insta::assert_snapshot;
    use tempfile::tempdir;

    use crate::{
        StepID, manipulator::Manipulator, util::example_generator, util::update_graph_silent,
        workflow::execute_from_file,
    };

    use super::*;

    #[test]
    fn serialize() {
        let module = Visualize::default();
        let serialization = toml::to_string(&module);
        assert!(
            serialization.is_ok(),
            "Serialization failed: {:?}",
            serialization.err()
        );
        assert_snapshot!(serialization.unwrap());
    }

    #[test]
    fn serialize_custom() {
        let module = Visualize {
            limit_tokens: true,
            token_limit: 1000,
            root: crate::manipulator::visualize::Include::All,
            output_dot: Some(PathBuf::from("anywhere/out/there.dot")),
            output_svg: Some(PathBuf::from("somewhere/else/maybe.svg")),
        };
        let serialization = toml::to_string(&module);
        assert!(
            serialization.is_ok(),
            "Serialization failed: {:?}",
            serialization.err()
        );
        assert_snapshot!(serialization.unwrap());
    }

    #[test]
    fn graph_statistics() {
        let g = AnnotationGraph::with_default_graphstorages(false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let mut u = GraphUpdate::default();
        example_generator::create_corpus_structure_simple(&mut u);
        assert!(update_graph_silent(&mut graph, &mut u).is_ok());
        let module = Visualize {
            limit_tokens: false,
            token_limit: 0,
            root: crate::manipulator::visualize::Include::All,
            output_dot: None,
            output_svg: None,
        };
        assert!(
            module
                .validate_graph(
                    &mut graph,
                    StepID {
                        module_name: "test".to_string(),
                        path: None
                    },
                    None
                )
                .is_ok()
        );
        assert!(graph.global_statistics.is_none());
    }

    #[test]
    fn dot_single_sentence_limit() {
        let workflow_dir = tempdir().unwrap();
        let workflow_file = workflow_dir.path().join("visualize.toml");
        std::fs::copy(
            Path::new("./tests/workflows/visualize_limit.toml"),
            &workflow_file,
        )
        .unwrap();
        execute_from_file(&workflow_file, true, false, None, None).unwrap();
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
        execute_from_file(&workflow_file, true, false, None, None).unwrap();
        let result_dot = std::fs::read_to_string(workflow_dir.path().join("test.dot")).unwrap();
        assert_snapshot!(result_dot);
    }

    #[test]
    fn root_node_restriction() {
        let mut updates = GraphUpdate::new();
        example_generator::create_corpus_structure_two_documents(&mut updates);
        let mut g = AnnotationGraph::with_default_graphstorages(true).unwrap();
        g.apply_update(&mut updates, |_msg| {}).unwrap();

        let op = Visualize {
            limit_tokens: false,
            token_limit: 0,
            root: super::Include::All,
            output_dot: None,
            output_svg: None,
        };
        assert_eq!("root", op.get_root_node_name(&g).unwrap());

        let op = Visualize {
            limit_tokens: false,
            token_limit: 0,
            root: super::Include::Document("root/doc2".to_string()),
            output_dot: None,
            output_svg: None,
        };
        assert_eq!("root/doc2", op.get_root_node_name(&g).unwrap());

        let op = Visualize {
            limit_tokens: false,
            token_limit: 0,
            root: super::Include::FirstDocument,
            output_dot: None,
            output_svg: None,
        };
        assert_eq!("root/doc1", op.get_root_node_name(&g).unwrap());
    }

    #[test]
    fn deserialize_document_root_config() {
        let visualizer_config_str = r#"
            limit_tokens = true
            token_limit = 10
            root = {document = "root/doc2"}
        "#;
        let op: Visualize = toml::from_str(visualizer_config_str).unwrap();
        assert_eq!(true, op.limit_tokens);
        assert_eq!(10, op.token_limit);
        assert_eq!(Include::Document("root/doc2".to_string()), op.root);
    }
}
