use super::Manipulator;
use crate::{progress::ProgressReporter, util::token_helper::TokenHelper, StepID};
use anyhow::{Context, Result};
use documented::{Documented, DocumentedFields};
use graphannis::AnnotationGraph;
use graphannis_core::{
    annostorage::ValueSearch,
    graph::{ANNIS_NS, NODE_NAME_KEY},
    types::NodeID,
};
use itertools::Itertools;
use serde::Deserialize;
use std::{borrow::Cow, fs::File, io::Write};
use struct_field_names_as_array::FieldNamesAsSlice;

/// Output the currrent graph as DOT for debugging it.
#[derive(Default, Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(deny_unknown_fields)]
pub struct DotDebug {}

impl DotDebug {
    fn output_node(&self, f: &mut File, n: NodeID, graph: &AnnotationGraph) -> Result<()> {
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
            .join("\\n");

        writeln!(f, "   {n}[label=\"{node_name}\\n\\n{anno_string}\"];")?;

        Ok(())
    }
}

impl Manipulator for DotDebug {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        workflow_directory: &std::path::Path,
        step_id: StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let progress = ProgressReporter::new_unknown_total_work(tx, step_id)?;

        // Open a temporary file to output the complete graph as DOT file
        let output_path = workflow_directory.join("graph-debug.dot");
        let mut f = std::fs::File::create(&output_path)?;

        writeln!(f, "digraph G {{")?;
        writeln!(f, "   node [shape=box];")?;

        let token_helper = TokenHelper::new(graph)?;

        // Write all token grouped by the textual data sources
        for ds in graph
            .get_node_annos()
            .exact_anno_search(Some(ANNIS_NS), "doc", ValueSearch::Any)
        {
            let ds = ds?;
            writeln!(f, "subgraph T {{")?;
            writeln!(f, "   rank = same;")?;

            let parent_id = graph
                .get_node_annos()
                .get_value_for_item(&ds.node, &NODE_NAME_KEY)?
                .unwrap_or_default();
            for t in token_helper.get_ordered_token(&parent_id, None)? {
                self.output_node(&mut f, t, graph)?;
            }
            writeln!(f, "}}")?;
        }

        for c in graph.get_all_components(None, None) {
            let gs = graph
                .get_graphstorage_as_ref(&c)
                .context("No graph storage for component")?;
            for source_node in gs.source_nodes() {
                let source_node = source_node?;
                for target_node in gs.get_outgoing_edges(source_node) {
                    let target_node = target_node?;
                    writeln!(f, "{source_node} -> {target_node}[label=\"{}\"];", &c)?;
                }
            }
        }

        writeln!(f, "}}")?;

        progress.info(&format!(
            "Wrote DOT debug file to {}",
            &output_path.display()
        ))?;

        todo!()
    }
}
