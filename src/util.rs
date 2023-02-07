use crate::error::Result;
use graphannis::{update::{GraphUpdate, UpdateEvent}, model::AnnotationComponentType};
use graphannis_core::graph::ANNIS_NS;
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

pub fn insert_corpus_nodes_from_path(update: &mut GraphUpdate, path: &Path) -> Result<String> {
    let clean_path = normpath::BasePath::new(path)?;
    let norm_path = normpath::BasePath::normalize(&clean_path)?;
    let mut full_path = String::new();    
    let mut sys_path_components = if clean_path.is_absolute() {
        let sys_path = std::env::current_dir()?;
        sys_path.components().count()
    } else {
        0
    };
    for c in norm_path.components() {
        if sys_path_components > 0 {
            sys_path_components -= 1;
            continue;
        }
        let parent = full_path.clone();
        full_path += "/";
        full_path += &c.as_os_str().to_string_lossy();
        update.add_event(UpdateEvent::AddNode { node_name: full_path.to_string(), node_type: "corpus".to_string() })?;
        if !parent.is_empty() {
            update.add_event(UpdateEvent::AddEdge { 
                source_node: full_path.to_string(), 
                target_node: parent, 
                layer: ANNIS_NS.to_string(), 
                component_type: AnnotationComponentType::PartOf.to_string(), 
                component_name: "".to_string() })?;
        }
    }
    Ok(full_path)
}