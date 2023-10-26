use graphannis::update::{GraphUpdate, UpdateEvent};
use normpath::PathExt;
use serde_derive::Deserialize;

use crate::Module;

use super::Importer;

#[derive(Deserialize, Default)]
pub struct CreateFileNodes {}

impl Importer for CreateFileNodes {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        _step_id: crate::StepID,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let base_dir = input_path.normalize()?;
        let base_dir_name = base_dir.file_name().unwrap();
        let start_index = base_dir.as_path().to_string_lossy().len() - base_dir_name.len();
        for path_r in glob::glob(format!("{}/**/*", base_dir.as_path().to_string_lossy()).as_str())?
        {
            let path = path_r?;
            let node_name = path.to_str().unwrap()[start_index..].to_string();
            if path.is_file() {
                update.add_event(UpdateEvent::AddNode {
                    node_name,
                    node_type: "file".to_string(),
                })?;
            }
        }
        Ok(update)
    }
}

const MODULE_NAME: &str = "embed_files";

impl Module for CreateFileNodes {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use graphannis::{
        update::{GraphUpdate, UpdateEvent},
        AnnotationGraph,
    };
    use graphannis_core::{annostorage::ValueSearch, graph::NODE_NAME_KEY};

    use crate::{importer::Importer, StepID};

    use super::{CreateFileNodes, MODULE_NAME};

    #[test]
    fn test_file_nodes_in_mem() {
        let r = test(false);
        assert!(r.is_ok(), "test ended with error: {:?}", r.err());
    }

    #[test]
    fn test_files_nodes_on_disk() {
        let r = test(true);
        assert!(r.is_ok(), "test ended with error: {:?}", r.err());
    }

    fn test(on_disk: bool) -> Result<(), Box<dyn std::error::Error>> {
        let mut expected_g = AnnotationGraph::new(on_disk)?;
        let mut u = GraphUpdate::default();
        u.add_event(UpdateEvent::AddNode {
            node_name: "xlsx/test_file.xlsx".to_string(),
            node_type: "file".to_string(),
        })?;
        let eur = expected_g.apply_update(&mut u, |_| {});
        assert!(eur.is_err()); // ordering component is missing, so this should be an error
        let mut test_g = AnnotationGraph::new(on_disk)?;
        let import = CreateFileNodes::default();
        let mut test_u = import.import_corpus(
            Path::new("tests/data/import/xlsx/clean/xlsx/"),
            StepID {
                module_name: MODULE_NAME.to_string(),
                path: None,
            },
            None,
        )?;
        let ur = test_g.apply_update(&mut test_u, |_| {});
        assert!(ur.is_err()); // ordering component is missing, so this should be an error
        let expected_id = expected_g.get_node_id_from_name("xlsx/test_file.xlsx")?;
        assert!(expected_id.is_some());
        let test_id = test_g.get_node_id_from_name("xlsx/test_file.xlsx")?;
        assert!(test_id.is_some());
        assert_eq!(expected_id.unwrap(), test_id.unwrap());
        Ok(())
    }
}
