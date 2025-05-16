use std::collections::BTreeMap;

use super::Manipulator;
use crate::{
    core::update_graph_silent, progress::ProgressReporter, util::token_helper::TokenHelper, StepID,
};
use documented::{Documented, DocumentedFields};
use graphannis::{
    graph::AnnoKey,
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
use serde::{Deserialize, Serialize};
use struct_field_names_as_array::FieldNamesAsSlice;
use text_splitter::TextSplitter;

/// Add a span annotation for automatically generated chunks.
///
/// Uses the [text-splitter](https://crates.io/crates/text-splitter) crate which
/// uses sentence markers and the given maximum number of characters per chunk
/// to segment the text into chunks.
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Chunk {
    /// Maximum chunk length.
    #[serde(default = "default_max_characters")]
    max_characters: usize,
    /// Annotation key used to annotate chunks with a value.
    #[serde(default = "default_anno_key", with = "crate::estarde::anno_key")]
    anno_key: AnnoKey,
    /// Used annotation value.
    #[serde(default)]
    anno_value: String,
    /// Optional segmentation name.
    #[serde(default)]
    segmentation: Option<String>,
}

fn default_anno_key() -> AnnoKey {
    AnnoKey {
        name: default_chunk_name().into(),
        ns: "".into(),
    }
}

fn default_chunk_name() -> String {
    "chunk".to_string()
}

fn default_max_characters() -> usize {
    100
}

impl Default for Chunk {
    fn default() -> Self {
        Self {
            max_characters: default_max_characters(),
            anno_key: default_anno_key(),
            anno_value: Default::default(),
            segmentation: Default::default(),
        }
    }
}

impl Manipulator for Chunk {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        _workflow_directory: &std::path::Path,
        step_id: StepID,
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

            let progress = ProgressReporter::new(tx, step_id.clone(), documents.len())?;

            let token_helper = TokenHelper::new(graph)?;

            let mut addded_node_index = 1;

            for document_match in documents {
                let document_node_name =
                    node_annos.get_value_for_item(&document_match.node, &NODE_NAME_KEY)?;
                if let Some(parent) = document_node_name {
                    // Apply chunker to reconstructed base text of the token
                    let token =
                        token_helper.get_ordered_token(&parent, self.segmentation.as_deref())?;

                    // Get span for each token but remember which part of the text belongs to which token ID
                    let mut base_text = String::default();
                    let mut offset_to_token = BTreeMap::new();
                    for (i, t) in token.iter().enumerate() {
                        let text = token_helper.spanned_text(&[*t])?;
                        if i > 0 {
                            base_text.push(' ');
                        }

                        let all_covered_token = if self.segmentation.is_some() {
                            token_helper.covered_token(*t)?
                        } else {
                            vec![*t]
                        };
                        offset_to_token.insert(base_text.len(), all_covered_token);

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
                            anno_ns: self.anno_key.ns.to_string(),
                            anno_name: self.anno_key.name.to_string(),
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
                            .flat_map(|(_offset, t)| t)
                            .copied()
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

        update_graph_silent(graph, &mut updates)?;

        Ok(())
    }

    fn requires_statistics(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, path::Path};

    use graphannis::{
        aql,
        graph::AnnoKey,
        update::{GraphUpdate, UpdateEvent},
        AnnotationGraph,
    };
    use graphannis_core::graph::ANNIS_NS;

    use crate::{
        core::update_graph_silent,
        manipulator::Manipulator,
        util::{example_generator, token_helper::TokenHelper},
        StepID,
    };

    use super::Chunk;
    use pretty_assertions::assert_eq;

    #[test]
    fn graph_statistics() {
        let g = AnnotationGraph::with_default_graphstorages(false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let mut u = GraphUpdate::default();
        example_generator::create_corpus_structure_simple(&mut u);
        assert!(update_graph_silent(&mut graph, &mut u).is_ok());
        let module = Chunk::default();
        assert!(module
            .validate_graph(
                &mut graph,
                StepID {
                    module_name: "test".to_string(),
                    path: None
                },
                None
            )
            .is_ok());
        assert!(graph.global_statistics.is_none());
    }

    #[test]
    fn simple_chunk_configuration() {
        let mut updates = GraphUpdate::new();
        example_generator::create_corpus_structure_simple(&mut updates);
        example_generator::create_tokens(&mut updates, Some("root/doc1"));
        let mut g = AnnotationGraph::with_default_graphstorages(false).unwrap();
        g.apply_update(&mut updates, |_msg| {}).unwrap();

        let mut chunker = Chunk::default();
        chunker.max_characters = 20;

        let step_id = StepID {
            module_name: "chunker".to_string(),
            path: None,
        };

        chunker
            .manipulate_corpus(&mut g, Path::new("."), step_id, None)
            .unwrap();

        let chunk_query = aql::parse("chunk", false).unwrap();
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

    #[test]
    fn chunk_with_segmentation() {
        let mut updates = GraphUpdate::new();
        example_generator::create_corpus_structure_simple(&mut updates);
        example_generator::create_tokens(&mut updates, Some("root/doc1"));

        // Add an additional segmentation layer "This more complicated ?"
        example_generator::make_segmentation_span(
            &mut updates,
            "root/doc1#seg1",
            "root/doc1",
            "seg",
            "This",
            &["root/doc1#tok0", "root/doc1#tok1", "root/doc1#tok2"],
        );
        example_generator::make_segmentation_span(
            &mut updates,
            "root/doc1#seg2",
            "root/doc1",
            "seg",
            "more",
            &["root/doc1#tok3", "root/doc1#tok4"],
        );
        example_generator::make_segmentation_span(
            &mut updates,
            "root/doc1#seg3",
            "root/doc1",
            "seg",
            "complicated",
            &[
                "root/doc1#tok5",
                "root/doc1#tok6",
                "root/doc1#tok7",
                "root/doc1#tok8",
                "root/doc1#tok9",
            ],
        );
        example_generator::make_segmentation_span(
            &mut updates,
            "root/doc1#seg4",
            "root/doc1",
            "seg",
            "?",
            &["root/doc1#tok10"],
        );

        // add the order relations for the segmentation
        updates
            .add_event(UpdateEvent::AddEdge {
                source_node: "root/doc1#seg1".into(),
                target_node: "root/doc1#seg2".into(),
                layer: ANNIS_NS.to_string(),
                component_type: "Ordering".to_string(),
                component_name: "seg".to_string(),
            })
            .unwrap();
        updates
            .add_event(UpdateEvent::AddEdge {
                source_node: "root/doc1#seg2".into(),
                target_node: "root/doc1#seg3".into(),
                layer: ANNIS_NS.to_string(),
                component_type: "Ordering".to_string(),
                component_name: "seg".to_string(),
            })
            .unwrap();
        updates
            .add_event(UpdateEvent::AddEdge {
                source_node: "root/doc1#seg3".into(),
                target_node: "root/doc1#seg4".into(),
                layer: ANNIS_NS.to_string(),
                component_type: "Ordering".to_string(),
                component_name: "seg".to_string(),
            })
            .unwrap();

        let mut g = AnnotationGraph::with_default_graphstorages(false).unwrap();
        g.apply_update(&mut updates, |_msg| {}).unwrap();

        let chunker = Chunk {
            max_characters: 15,
            anno_key: AnnoKey {
                ns: "chunk".into(),
                name: "segment".into(),
            },
            anno_value: "s".into(),
            segmentation: Some("seg".into()),
        };

        let step_id = StepID {
            module_name: "chunker".to_string(),
            path: None,
        };

        chunker
            .manipulate_corpus(&mut g, Path::new("."), step_id, None)
            .unwrap();

        let all_chunks_query = aql::parse("chunk:segment", false).unwrap();
        let chunks: Result<Vec<_>, graphannis::errors::GraphAnnisError> =
            aql::execute_query_on_graph(&g, &all_chunks_query, false, None)
                .unwrap()
                .collect();
        assert_eq!(2, chunks.unwrap().len());

        let first_chunk_query = aql::parse(
            "chunk:segment & #1 _l_ seg=\"This\" & #1 _r_ seg=\"more\"",
            false,
        )
        .unwrap();
        let results: Result<Vec<_>, graphannis::errors::GraphAnnisError> =
            aql::execute_query_on_graph(&g, &first_chunk_query, false, None)
                .unwrap()
                .collect();
        assert_eq!(1, results.unwrap().len());

        let second_chunk_query = aql::parse(
            "chunk:segment & #1 _l_ seg=\"complicated\" & #1 _r_ seg=\"?\"",
            false,
        )
        .unwrap();
        let results: Result<Vec<_>, graphannis::errors::GraphAnnisError> =
            aql::execute_query_on_graph(&g, &second_chunk_query, false, None)
                .unwrap()
                .collect();
        assert_eq!(1, results.unwrap().len());
    }
}
