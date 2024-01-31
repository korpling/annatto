use super::Manipulator;
use crate::{progress::ProgressReporter, util::token_helper::TokenHelper, Module};
use graphannis_core::{
    annostorage::{Match, ValueSearch},
    errors::GraphAnnisCoreError,
    graph::{ANNIS_NS, NODE_NAME_KEY},
};
use text_splitter::TextSplitter;

pub struct Chunker {
    max_characters: usize,
}

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
                let splitter = TextSplitter::default();
                let chunks: Vec<_> = splitter.chunks(&base_text, self.max_characters).collect();

                dbg!(chunks);
                todo!("Add sentence span annotation")
            }
            progress.worked(1)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use graphannis::{update::GraphUpdate, AnnotationGraph};

    use crate::{manipulator::Manipulator, util::example_generator};

    use super::Chunker;

    #[test]
    fn simple_chunk_configuration() {
        let mut updates = GraphUpdate::new();
        example_generator::create_corpus_structure_simple(&mut updates);
        example_generator::create_tokens(&mut updates, Some("root/doc1"));
        let mut g = AnnotationGraph::new(false).unwrap();
        g.apply_update(&mut updates, |_msg| {}).unwrap();

        let chunker = Chunker {
            max_characters: 500,
        };

        chunker
            .manipulate_corpus(&mut g, Path::new("."), None)
            .unwrap();
    }
}
