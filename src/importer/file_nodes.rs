use graphannis::{
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::ANNIS_NS;
use normpath::PathExt;
use serde_derive::Deserialize;

use super::Importer;

#[derive(Deserialize, Default)]
pub struct CreateFileNodes {
    corpus_name: Option<String>,
}

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
        if let Some(link_target) = &self.corpus_name {
            update.add_event(UpdateEvent::AddNode {
                node_name: link_target.to_string(),
                node_type: "corpus".to_string(),
            })?;
        }
        for path_r in glob::glob(format!("{}/**/*", base_dir.as_path().to_string_lossy()).as_str())?
        {
            let path = path_r?;
            let node_name = path.to_str().unwrap()[start_index..].to_string();
            if path.is_file() {
                update.add_event(UpdateEvent::AddNode {
                    node_name: node_name.to_string(),
                    node_type: "file".to_string(),
                })?;
                update.add_event(UpdateEvent::AddNodeLabel {
                    node_name: node_name.to_string(),
                    anno_ns: ANNIS_NS.to_string(),
                    anno_name: "file".to_string(),
                    anno_value: node_name.to_string(),
                })?;
                if let Some(link_target) = &self.corpus_name {
                    update.add_event(UpdateEvent::AddEdge {
                        source_node: node_name,
                        target_node: link_target.to_string(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::PartOf.to_string(),
                        component_name: "".to_string(),
                    })?;
                }
            }
        }
        Ok(update)
    }

    fn file_extensions(&self) -> &[&str] {
        &[]
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use graphannis::{
        model::{AnnotationComponent, AnnotationComponentType},
        update::{GraphUpdate, UpdateEvent},
        AnnotationGraph,
    };
    use graphannis_core::graph::ANNIS_NS;
    use itertools::Itertools;

    use crate::{ImporterStep, Step};

    use super::CreateFileNodes;

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
            node_name: "xlsx".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "xlsx/test_file.xlsx".to_string(),
            node_type: "file".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "xlsx/test_file.xlsx".to_string(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "file".to_string(),
            anno_value: "xlsx/test_file.xlsx".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "xlsx/test_file.xlsx".to_string(),
            target_node: "xlsx".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            // dummy edge to pass model check
            source_node: "xlsx/test_file.xlsx".to_string(),
            target_node: "xlsx/test_file.xlsx".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::Ordering.to_string(),
            component_name: "".to_string(),
        })?;
        let eur = expected_g.apply_update(&mut u, |_| {});
        assert!(eur.is_ok()); // ordering component is missing, so this should be an error
        let mut test_g = AnnotationGraph::new(on_disk)?;
        let import = CreateFileNodes {
            corpus_name: Some("xlsx".to_string()),
        };
        let step = ImporterStep {
            module: crate::ReadFrom::Path(import),
            path: PathBuf::from("tests/data/import/xlsx/clean/xlsx/"),
        };
        let mut test_u =
            step.module
                .reader()
                .import_corpus(&step.path, step.get_step_id(), None)?;
        // add dummy node and dummy ordering edge to pass model checks when applying the update to the graph
        test_u.add_event(UpdateEvent::AddNode {
            node_name: "dummy_node".to_string(),
            node_type: "node".to_string(),
        })?;
        test_u.add_event(UpdateEvent::AddEdge {
            source_node: "dummy_node".to_string(),
            target_node: "dummy_node".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::Ordering.to_string(),
            component_name: "".to_string(),
        })?;
        // apply
        let ur = test_g.apply_update(&mut test_u, |_| {});
        assert!(ur.is_ok());
        let expected_id = expected_g
            .get_node_annos()
            .get_node_id_from_name("xlsx/test_file.xlsx")?;
        assert!(expected_id.is_some());
        let test_id = test_g
            .get_node_annos()
            .get_node_id_from_name("xlsx/test_file.xlsx")?;
        assert!(test_id.is_some());
        assert_eq!(expected_id.unwrap(), test_id.unwrap());
        let expected_matches = expected_g
            .get_node_annos()
            .exact_anno_search(
                Some(ANNIS_NS),
                "file",
                graphannis_core::annostorage::ValueSearch::Any,
            )
            .collect_vec();
        let test_matches = test_g
            .get_node_annos()
            .exact_anno_search(
                Some(ANNIS_NS),
                "file",
                graphannis_core::annostorage::ValueSearch::Any,
            )
            .collect_vec();
        assert_eq!(expected_matches.len(), test_matches.len());
        for (me, mt) in expected_matches.into_iter().zip(test_matches) {
            assert_eq!(me?, mt?);
        }
        let test_part_of_comp = test_g.get_graphstorage(&AnnotationComponent::new(
            AnnotationComponentType::PartOf,
            ANNIS_NS.into(),
            "".into(),
        ));
        assert!(test_part_of_comp.is_some());
        let test_root_node_id = test_g.get_node_annos().get_node_id_from_name("xlsx")?;
        assert!(test_root_node_id.is_some());
        let expected_part_of_comp = expected_g.get_graphstorage_as_ref(&AnnotationComponent::new(
            AnnotationComponentType::PartOf,
            ANNIS_NS.into(),
            "".into(),
        ));
        assert!(expected_part_of_comp.is_some());
        let expected_root_node_id = expected_g.get_node_annos().get_node_id_from_name("xlsx")?;
        assert!(expected_root_node_id.is_some());
        assert_eq!(
            expected_part_of_comp
                .unwrap()
                .get_ingoing_edges(expected_root_node_id.unwrap())
                .count(),
            test_part_of_comp
                .clone()
                .unwrap()
                .get_ingoing_edges(test_root_node_id.unwrap())
                .count()
        );
        assert_eq!(
            test_part_of_comp
                .unwrap()
                .get_ingoing_edges(test_root_node_id.unwrap())
                .count(),
            glob::glob("tests/data/import/xlsx/clean/xlsx/*.*")
                .into_iter()
                .count()
        );
        Ok(())
    }
}
