use super::Manipulator;
use crate::{progress::ProgressReporter, util::token_helper::TokenHelper, StepID};
use anyhow::Result;
use documented::{Documented, DocumentedFields};
use graphannis::AnnotationGraph;
use graphannis_core::{
    annostorage::ValueSearch,
    graph::{ANNIS_NS, NODE_NAME_KEY, NODE_TYPE},
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

        // Write the token first, so they can be grouped together
        writeln!(f, "subgraph T {{")?;
        writeln!(f, "   rank = same;")?;

        let token_helper = TokenHelper::new(graph)?;
        for n in graph.get_node_annos().exact_anno_search(
            Some(ANNIS_NS),
            NODE_TYPE,
            ValueSearch::Some("node"),
        ) {
            let n = n?.node;
            if token_helper.is_token(n)? {
                self.output_node(&mut f, n, graph)?;
            }
        }

        writeln!(f, "}}")?;

        writeln!(f, "}}")?;

        progress.info(&format!(
            "Wrote DOT debug file to {}",
            &output_path.display()
        ))?;

        todo!()
    }
}
