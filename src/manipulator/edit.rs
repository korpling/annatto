use graphannis::update::{GraphUpdate, UpdateEvent};
use serde::{Deserialize, Serialize};

use crate::{manipulator::Manipulator, progress::ProgressReporter, util::update_graph_silent};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EditGraph {
    #[serde(with = "crate::estarde::update_event")]
    instructions: Vec<UpdateEvent>,
}

impl Manipulator for EditGraph {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        _workflow_directory: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let progress = ProgressReporter::new_unknown_total_work(tx, step_id)?;
        let mut update = GraphUpdate::default();
        for instruction in &self.instructions {
            dbg!(instruction);
            update.add_event(instruction.clone())?;
        }
        progress.info(format!(
            "Applying {} edit instruction(s) ...",
            self.instructions.len()
        ))?;
        update_graph_silent(graph, &mut update)?;
        Ok(())
    }

    fn requires_statistics(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use graphannis::AnnotationGraph;
    use insta::assert_snapshot;

    use crate::{
        exporter::graphml::GraphMLExporter,
        manipulator::{Manipulator, edit::EditGraph},
        test_util::export_to_string,
    };

    #[test]
    fn serde() {
        let toml_str = fs::read_to_string("tests/data/graph_op/edit/config.toml");

        let m: Result<EditGraph, _> = toml::from_str(&toml_str.unwrap());
        assert!(m.is_ok(), "Deserialization error: {:?}", m.err().unwrap());
        let serialized = toml::to_string(&m.unwrap());
        assert!(
            serialized.is_ok(),
            "Serialization error: {:?}",
            serialized.err().unwrap()
        );
        assert_snapshot!(serialized.unwrap());
    }

    #[test]
    fn build_graph() {
        let toml_str = fs::read_to_string("tests/data/graph_op/edit/config.toml").unwrap();
        let cut_off_index = toml_str.find("[[instructions]]\ndo = \"rm\"").unwrap();
        dbg!(&toml_str[0..cut_off_index]);
        let m: Result<EditGraph, _> = toml::from_str(&toml_str[0..cut_off_index]);
        assert!(m.is_ok(), "Deserialization error: {:?}", m.err().unwrap());
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(
            m.unwrap()
                .manipulate_corpus(
                    &mut graph,
                    Path::new("./"),
                    crate::StepID {
                        module_name: "test_edit".to_string(),
                        path: None
                    },
                    None
                )
                .is_ok()
        );
        let exporter: GraphMLExporter = toml::from_str("stable_order = true").unwrap();
        let serialized = export_to_string(&graph, exporter);
        assert_snapshot!(serialized.unwrap());
    }
}
