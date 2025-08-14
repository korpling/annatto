use facet::Facet;
use serde::Serialize;
use serde_derive::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use crate::StepID;
use documented::{Documented, DocumentedFields};

use super::Manipulator;

/// A graph operation that does nothing.
/// The purpose of this graph operation is to allow to omit a `format` field in
/// the `[[graph_op]]` configuration of the workflow file.
#[derive(
    Facet,
    Deserialize,
    Default,
    Documented,
    DocumentedFields,
    FieldNamesAsSlice,
    Serialize,
    Clone,
    PartialEq,
)]
#[serde(deny_unknown_fields)]
pub struct NoOp {}

impl Manipulator for NoOp {
    fn manipulate_corpus(
        &self,
        _graph: &mut graphannis::AnnotationGraph,
        _workflow_directory: &std::path::Path,
        _step_id: StepID,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    fn requires_statistics(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use graphannis::{AnnotationGraph, update::GraphUpdate};
    use insta::assert_snapshot;

    use crate::{
        StepID,
        manipulator::{Manipulator, no_op::NoOp},
        util::example_generator,
        util::update_graph_silent,
    };

    #[test]
    fn serialize() {
        let module = NoOp::default();
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
        let module = NoOp {};
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
}
