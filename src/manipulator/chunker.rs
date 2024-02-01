use std::collections::{BTreeMap, HashMap};

use super::Manipulator;
use crate::{progress::ProgressReporter, util::token_helper::TokenHelper, Module};
use graphannis::{
    model::AnnotationComponentType,
    update::{
        GraphUpdate,
        UpdateEvent::{AddEdge, AddNode, AddNodeLabel},
    },
};
use graphannis_core::{
    annostorage::{Match, ValueSearch},
    errors::GraphAnnisCoreError,
    graph::{ANNIS_NS, NODE_NAME_KEY},
    types::NodeID,
};
use serde::de::IntoDeserializer;
use text_splitter::{ChunkSizer, TextSplitter};

pub struct Chunker {
    max_characters: usize,
    anno_namespace: String,
    anno_name: String,
    anno_value: String,
}

impl Default for Chunker {
    fn default() -> Self {
        Self {
            max_characters: 100,
            anno_name: "chunk".into(),
            anno_namespace: "".into(),
            anno_value: "".into(),
        }
    }
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
        _workflow_directory: &std::path::Path,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut updates = GraphUpdate::new();
        {
            let node_annos = graph.get_node_annos();

            // get all documents
            let documents: Result<Vec<Match>, GraphAnnisCoreError> = node_annos
                .exact_anno_search(Some(ANNIS_NS), "doc", ValueSearch::Any)
                .collect();
            let documents = documents?;

            let progress = ProgressReporter::new(tx, self.step_id(None), documents.len())?;

            let token_helper = TokenHelper::new(&graph)?;

            let mut addded_node_index = 1;

            for document_match in documents {
                let document_node_name =
                    node_annos.get_value_for_item(&document_match.node, &NODE_NAME_KEY)?;
                if let Some(parent) = document_node_name {
                    // Apply chunker to reconstructed base text of the token
                    let token = token_helper.get_ordered_token(&parent, None)?;

                    // Get span for each token but remember which part of the text belongs to which token ID
                    let mut base_text = String::default();
                    let mut offset_to_token = BTreeMap::new();
                    for (i, t) in token.iter().enumerate() {
                        let text = token_helper.spanned_text(&[*t])?;
                        if i > 0 {
                            base_text.push_str(" ");
                        }

                        offset_to_token.insert(base_text.len(), *t);
                        base_text.push_str(&text);
                    }

                    let splitter = TextSplitter::default().with_trim_chunks(true);
                    let chunks: Vec<_> = splitter
                        .chunk_indices(&base_text, self.max_characters)
                        .collect();

                    // Add chunk span annotations
                    for (chunk_offset, chunk_text) in chunks {
                        // Add chunk span annotation node
                        let node_name = format!("{}#chunkerSpanNode{}", parent, addded_node_index);
                        addded_node_index += 1;

                        updates.add_event(AddNode {
                            node_name: node_name.clone(),
                            node_type: "node".into(),
                        })?;
                        updates.add_event(AddNodeLabel {
                            node_name: node_name.clone(),
                            anno_ns: self.anno_namespace.clone(),
                            anno_name: self.anno_name.clone(),
                            anno_value: self.anno_value.clone(),
                        })?;
                        updates.add_event(AddEdge {
                            source_node: node_name.clone(),
                            target_node: parent.to_string(),
                            layer: ANNIS_NS.to_string(),
                            component_type: AnnotationComponentType::PartOf.to_string(),
                            component_name: "".into(),
                        })?;
                        let covered_token: Vec<NodeID> = offset_to_token
                            .range(chunk_offset..(chunk_offset + chunk_text.len()))
                            .map(|(_offset, t)| *t)
                            .collect();

                        for t in covered_token {
                            if let Some(token_node_name) = graph
                                .get_node_annos()
                                .get_value_for_item(&t, &NODE_NAME_KEY)?
                            {
                                updates.add_event(AddEdge {
                                    source_node: node_name.clone(),
                                    target_node: token_node_name.to_string(),
                                    layer: ANNIS_NS.into(),
                                    component_type: AnnotationComponentType::Coverage.to_string(),
                                    component_name: "".into(),
                                })?;
                            }
                        }
                    }
                }
                progress.worked(1)?;
            }
        }

        graph.apply_update(&mut updates, |_| {})?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, path::Path};

    use graphannis::{aql, update::GraphUpdate, AnnotationGraph};

    use crate::{
        manipulator::Manipulator,
        util::{example_generator, token_helper::TokenHelper},
    };

    use super::Chunker;
    use pretty_assertions::assert_eq;

    #[test]
    fn simple_chunk_configuration() {
        let mut updates = GraphUpdate::new();
        example_generator::create_corpus_structure_simple(&mut updates);
        example_generator::create_tokens(&mut updates, Some("root/doc1"));
        let mut g = AnnotationGraph::new(false).unwrap();
        g.apply_update(&mut updates, |_msg| {}).unwrap();

        let chunker = Chunker {
            max_characters: 20,
            anno_name: "segment".into(),
            anno_namespace: "chunk".into(),
            anno_value: "s".into(),
        };

        chunker
            .manipulate_corpus(&mut g, Path::new("."), None)
            .unwrap();

        let chunk_query = aql::parse("chunk:segment", false).unwrap();
        let chunks: Result<Vec<_>, graphannis::errors::GraphAnnisError> =
            aql::execute_query_on_graph(&g, &chunk_query, false, None)
                .unwrap()
                .collect();
        let chunks = chunks.unwrap();
        assert_eq!(3, chunks.len());

        // Get all covered texts and sort them to make them easier to compare
        let mut texts_covered_by_chunks = BTreeSet::new();
        let tok_helper = TokenHelper::new(&g).unwrap();

        for c in chunks {
            let covered_token = tok_helper.covered_token(c[0].node).unwrap();
            let text = tok_helper.spanned_text(&covered_token).unwrap();
            texts_covered_by_chunks.insert(text);
        }

        let texts_covered_by_chunks: Vec<_> = texts_covered_by_chunks.into_iter().collect();
        assert_eq!("Is this example more", texts_covered_by_chunks[0]);
        assert_eq!("appears to be ?", texts_covered_by_chunks[1]);
        assert_eq!("complicated than it", texts_covered_by_chunks[2]);
    }
}
