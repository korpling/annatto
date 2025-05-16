use documented::{Documented, DocumentedFields};
use serde::{Deserialize, Serialize};
use struct_field_names_as_array::FieldNamesAsSlice;

use super::Manipulator;

/// This operation pauses the conversion process. As a regular user, you usually do not need to use this feature.
#[derive(Deserialize, Default, Documented, DocumentedFields, FieldNamesAsSlice, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Sleep {
    /// Time to sleep in seconds.
    #[serde(default)]
    seconds: u64,
}

impl Manipulator for Sleep {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        _workflow_directory: &std::path::Path,
        _step_id: crate::StepID,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        std::thread::sleep(std::time::Duration::from_secs(self.seconds));
        graph.ensure_loaded_all()?;
        Ok(())
    }

    fn requires_statistics(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use graphannis::{update::GraphUpdate, AnnotationGraph};
    use insta::assert_snapshot;

    use crate::{
        core::update_graph_silent,
        manipulator::{sleep::Sleep, Manipulator},
        util::example_generator,
        StepID,
    };

    #[test]
    fn serialize() {
        let module = Sleep::default();
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
        let module = Sleep {
            seconds: 1000000000,
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
    fn deserialize() {
        let toml_str = "seconds = 10";
        let r: Result<super::Sleep, _> = toml::from_str(toml_str);
        assert!(r.is_ok());
        assert_eq!(r.unwrap().seconds, 10);
    }

    #[test]
    fn graph_statistics() {
        let g = AnnotationGraph::with_default_graphstorages(false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let mut u = GraphUpdate::default();
        example_generator::create_corpus_structure_simple(&mut u);
        assert!(update_graph_silent(&mut graph, &mut u).is_ok());
        let module = Sleep { seconds: 0 };
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
