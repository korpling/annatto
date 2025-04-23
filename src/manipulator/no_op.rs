use serde_derive::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use crate::StepID;
use documented::{Documented, DocumentedFields};

use super::Manipulator;

/// A graph operation that does nothing.
/// The purpose of this graph operation is to allow to omit a `format` field in
/// the `[[graph_op]]` configuration of the workflow file.
#[derive(Deserialize, Default, Documented, DocumentedFields, FieldNamesAsSlice)]
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
    use graphannis::{update::GraphUpdate, AnnotationGraph};

    use crate::{
        core::update_graph_silent,
        manipulator::{no_op::NoOp, Manipulator},
        util::example_generator,
        StepID,
    };

    #[test]
    fn graph_statistics() {
        let g = AnnotationGraph::with_default_graphstorages(false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let mut u = GraphUpdate::default();
        example_generator::create_corpus_structure_simple(&mut u);
        assert!(update_graph_silent(&mut graph, &mut u).is_ok());
        let module = NoOp {};
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
}
