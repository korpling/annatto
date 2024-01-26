use crate::{progress::ProgressReporter, util::token_helper::TokenHelper, Module};
use graphannis_core::{
    annostorage::{Match, ValueSearch},
    errors::GraphAnnisCoreError,
    graph::{ANNIS_NS, NODE_NAME_KEY},
};
use itertools::Itertools;
use nlprule::tokenizer::chunk::Chunker as NlpRuleChunker;

use super::Manipulator;

pub struct Chunker {}

impl Module for Chunker {
    fn module_name(&self) -> &str {
        "Chunker"
    }
}

impl Manipulator for Chunker {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        workflow_directory: &std::path::Path,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let node_annos = graph.get_node_annos();
        // get all documents
        let documents: Result<Vec<Match>, GraphAnnisCoreError> = node_annos
            .exact_anno_search(Some(ANNIS_NS), "doc", ValueSearch::Any)
            .collect();
        let documents = documents?;

        // init helper structs
        let progress = ProgressReporter::new(tx, self.step_id(None), documents.len())?;
        let token_helper = TokenHelper::new(&graph)?;

        for document_match in documents {
            let document_node_name =
                node_annos.get_value_for_item(&document_match.node, &NODE_NAME_KEY)?;
            if let Some(parent) = document_node_name {
                // Apply chunker to reconstructed base text of the token
                let token = token_helper.get_ordered_token(&parent, None)?;
                let base_text = token_helper.spanned_text(&token)?;
                todo!("Apply nlprule tokenizer/chunker")
            }
            progress.worked(1)?;
        }

        todo!()
    }
}
