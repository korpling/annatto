use crate::error::Result;
use graphannis::{
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::ANNIS_NS;
use itertools::Itertools;
use std::{fs::File, io::Write, path::Path};

fn event_to_string(update_event: &UpdateEvent) -> Result<String> {
    Ok(format!("{:?}", update_event))
}

pub fn write_to_file(updates: &GraphUpdate, path: &std::path::Path) -> Result<()> {
    let mut file = File::create(path)?;
    let it = updates.iter()?;
    for update_event in it {
        let event_tuple = update_event?;
        let event_string = event_to_string(&event_tuple.1)?;
        file.write_all(event_string.as_bytes())?;
        file.write_all(b"\n")?;
    }
    Ok(())
}

pub fn insert_corpus_nodes_from_path(update: &mut GraphUpdate, root_path: &Path, document_path: &Path) -> Result<String> {
    let clean_path = normpath::BasePath::new(document_path)?;
    let clean_root_path = normpath::BasePath::new(root_path)?;
    let norm_path = normpath::BasePath::normalize(&clean_path)?;
    let norm_root_path = normpath::BasePath::normalize(&clean_root_path)?;
    let root_path_len = norm_root_path.components().count() - 1;
    let mut full_path = String::new();
    for c in &norm_path.components().collect_vec()[root_path_len..] {
        let parent = full_path.clone();
        if !full_path.is_empty() {
            full_path += "/";
        }
        full_path += &c.as_os_str().to_string_lossy();
        update.add_event(UpdateEvent::AddNode {
            node_name: full_path.to_string(),
            node_type: "corpus".to_string(),
        })?;
        if !parent.is_empty() {
            update.add_event(UpdateEvent::AddEdge {
                source_node: full_path.to_string(),
                target_node: parent,
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string(),
            })?;
        }
    }
    Ok(full_path)
}

#[cfg(test)]
mod tests {
    use std::{env::current_dir, path::Path};

    use graphannis::update::GraphUpdate;

    const TEST_PATH: &str = "test/import/xlsx";

    fn test_insert_corpus_nodes_from_path(absolute: bool) -> Result<(), Box<dyn std::error::Error>> {
        let sys_path = current_dir()?;
        let p = Path::new(TEST_PATH);
        let test_path = if absolute {
            sys_path.join(p)
        } else {
            p.to_path_buf()
        };
        let mut u = GraphUpdate::default();
        let r = super::insert_corpus_nodes_from_path(&mut u, test_path.parent().unwrap().parent().unwrap(), test_path.as_path());
        assert!(r.is_ok(), "Not okay: {:?}", r.err());
        let doc_path = r?;
        assert_eq!(doc_path, TEST_PATH.to_string());
        Ok(())
    }

    #[test]
    fn test_insert_corpus_nodes_from_path_relative() {
        let r = test_insert_corpus_nodes_from_path(false);
        assert!(r.is_ok(), "Not okay: {:?}", r.err());
    }

    #[test]
    fn test_insert_corpus_nodes_from_path_absolute() {
        let r = test_insert_corpus_nodes_from_path(true);
        assert!(r.is_ok(), "Not okay: {:?}", r.err());
    }
}
