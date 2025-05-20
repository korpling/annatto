use std::{
    borrow::Cow,
    ffi::OsStr,
    path::{Path, PathBuf},
};

use crate::Result;
use graphannis::{
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::{ANNIS_NS, DEFAULT_NS};
use normpath::PathExt;

fn add_subcorpora(
    u: &mut GraphUpdate,
    file_path: &Path,
    parent_corpus: &str,
    file_endings: &[&str],
) -> Result<Vec<(PathBuf, String)>> {
    let mut result = Vec::new();

    // Get the files and sort them according to their path, to get a predictable
    // order of adding the documents to the graph.

    if file_path.is_file()
        && file_endings
            .iter()
            .any(|ext| file_path.extension().unwrap_or_default().to_string_lossy() == *ext)
    {
        // Add the file itself as document
        let subcorpus_name = file_path
            .file_stem()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| "document".to_string());
        let node_name = format!("{}/{}", parent_corpus, subcorpus_name);
        u.add_event(UpdateEvent::AddNode {
            node_name: node_name.clone(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: node_name.clone(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "doc".to_string(),
            anno_value: subcorpus_name.to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: node_name.clone(),
            target_node: parent_corpus.to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        let result = (file_path.to_path_buf(), node_name);
        Ok(vec![result])
    } else {
        let mut files_in_directory = Vec::new();
        for entry in std::fs::read_dir(file_path)? {
            let entry = entry?;
            files_in_directory.push(entry);
        }
        files_in_directory.sort_by_key(|dir_entry| dir_entry.path());

        for entry in files_in_directory {
            let entry_type = entry.file_type()?;
            let entry_path = entry.path();
            let subcorpus_name = if entry_path.is_dir() {
                entry_path
                    .file_name() // do not strip extension!
                    .map(|f| f.to_os_string())
                    .unwrap_or_else(|| entry.file_name())
                    .to_string_lossy()
                    .to_string()
            } else {
                entry_path
                    .file_stem() // strip extension
                    .map(|f| f.to_os_string())
                    .unwrap_or_else(|| entry.file_name())
                    .to_string_lossy()
                    .to_string()
            };
            let node_name = format!("{}/{}", parent_corpus, subcorpus_name);
            let add_node = if entry_type.is_file() {
                if let Some(actual_ending) = entry.path().extension() {
                    file_endings
                        .iter()
                        .any(|ext| *ext == actual_ending.to_string_lossy().as_ref())
                } else {
                    false
                }
            } else {
                entry_type.is_dir()
            };
            if add_node {
                u.add_event(UpdateEvent::AddNode {
                    node_name: node_name.clone(),
                    node_type: "corpus".to_string(),
                })?;
                u.add_event(UpdateEvent::AddEdge {
                    source_node: node_name.clone(),
                    target_node: parent_corpus.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::PartOf.to_string(),
                    component_name: "".to_string(),
                })?;

                if entry_type.is_dir() {
                    result.extend(add_subcorpora(u, &entry.path(), &node_name, file_endings)?);
                } else if entry_type.is_file() {
                    // Also add the special "annis:doc" label to mark this as document
                    u.add_event(UpdateEvent::AddNodeLabel {
                        node_name: node_name.clone(),
                        anno_ns: ANNIS_NS.to_string(),
                        anno_name: "doc".to_string(),
                        anno_value: subcorpus_name.to_string(),
                    })?;
                    // Only add the corpus graph leafs to the result vector
                    result.push((entry.path(), node_name));
                }
            }
        }
        Ok(result)
    }
}

pub fn root_corpus_from_path(root_path: &Path) -> Result<String> {
    let norm_path = root_path.normalize()?;
    let root_name = if norm_path.is_file() {
        // remove extension
        norm_path
            .file_stem()
            .unwrap_or_else(|| OsStr::new("root-corpus"))
            .to_string_lossy()
    } else {
        // keep trailing sequences starting with a "." (e. g. version digits)
        norm_path
            .file_name()
            .unwrap_or_else(|| OsStr::new("root-corpus"))
            .to_string_lossy()
    };
    Ok(root_name.to_string())
}

/// Finds all files with a given ending in a directory and map the corpus
/// structure according to the file/directory structure.
///
/// The root directory is mapped as root corpus, sub-directories as sub-corpora
/// and files as documents. The found documents are returned with the
/// corresponding file path and the document node ID as string.
pub fn import_corpus_graph_from_files(
    u: &mut GraphUpdate,
    root_path: &Path,
    file_endings: &[&str],
) -> Result<Vec<(PathBuf, String)>> {
    let root_name = root_corpus_from_path(root_path)?;

    u.add_event(UpdateEvent::AddNode {
        node_name: root_name.clone(),
        node_type: "corpus".to_string(),
    })?;

    let mut path_tuples = add_subcorpora(u, root_path, &root_name, file_endings)?;
    path_tuples.sort();
    Ok(path_tuples)
}

pub fn map_audio_source(
    u: &mut GraphUpdate,
    audio_path: &Path,
    parent_corpus: &str,
    doc_path: &str,
) -> Result<String> {
    let node_name = format!(
        "{}/{}",
        parent_corpus,
        audio_path
            .file_name()
            .map_or_else(|| Cow::from("unknown_file"), |f| f.to_string_lossy())
    );
    u.add_event(UpdateEvent::AddNode {
        node_name: node_name.to_string(),
        node_type: "file".to_string(),
    })?;
    // TODO: make sure the file path is relative to the corpus directory. This
    // means we also have to implement copying the linked files to the same
    // location as the GraphML file.
    u.add_event(UpdateEvent::AddNodeLabel {
        node_name: node_name.to_string(),
        anno_ns: ANNIS_NS.to_string(),
        anno_name: "file".to_string(),
        anno_value: audio_path.to_string_lossy().to_string(),
    })?;
    u.add_event(UpdateEvent::AddEdge {
        source_node: node_name.to_string(),
        target_node: doc_path.to_string(),
        layer: ANNIS_NS.to_string(),
        component_type: AnnotationComponentType::PartOf.to_string(),
        component_name: "".to_string(),
    })?;

    Ok(node_name)
}

pub fn add_order_relations<S: AsRef<str>>(
    u: &mut GraphUpdate,
    node_ids: &[S],
    order_name: Option<&str>,
) -> Result<()> {
    let ordering_layer = if order_name.is_none() {
        ANNIS_NS.to_owned()
    } else {
        DEFAULT_NS.to_owned()
    };
    for i in 1..node_ids.len() {
        u.add_event(UpdateEvent::AddEdge {
            source_node: node_ids[i - 1].as_ref().to_string(),
            target_node: node_ids[i].as_ref().to_string(),
            layer: ordering_layer.clone(),
            component_type: AnnotationComponentType::Ordering.to_string(),
            component_name: order_name.unwrap_or_default().to_string(),
        })?;
    }
    Ok(())
}

pub struct NodeInfo<'a> {
    node_id: &'a str,
    doc_path: &'a str,
    text_node_name: &'a str,
}

impl<'a> NodeInfo<'a> {
    pub fn new(node_id: &'a str, doc_path: &'a str, text_node_name: &'a str) -> NodeInfo<'a> {
        NodeInfo {
            node_id,
            doc_path,
            text_node_name,
        }
    }
}

pub fn map_token(
    u: &mut GraphUpdate,
    node: &NodeInfo,
    text_name: Option<&str>,
    value: &str,
    start_time: Option<f64>,
    end_time: Option<f64>,
    add_annis_layer: bool,
) -> Result<String> {
    let tok_id = format!("{}#t{}", node.doc_path, node.node_id);
    u.add_event(UpdateEvent::AddNode {
        node_name: tok_id.clone(),
        node_type: "node".to_string(),
    })?;
    u.add_event(UpdateEvent::AddNodeLabel {
        node_name: tok_id.clone(),
        anno_ns: ANNIS_NS.to_string(),
        anno_name: "tok".to_string(),
        anno_value: value.to_string(),
    })?;
    if let Some(text_name) = text_name {
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: tok_id.clone(),
            anno_ns: "".to_string(),
            anno_name: text_name.to_string(),
            anno_value: value.to_string(),
        })?;
    };
    u.add_event(UpdateEvent::AddNodeLabel {
        node_name: tok_id.clone(),
        anno_ns: ANNIS_NS.to_string(),
        anno_name: "tok-whitespace-after".to_string(),
        anno_value: " ".to_string(),
    })?;
    u.add_event(UpdateEvent::AddEdge {
        source_node: tok_id.clone(),
        target_node: node.text_node_name.to_string(),
        layer: ANNIS_NS.to_string(),
        component_type: AnnotationComponentType::PartOf.to_string(),
        component_name: "".to_string(),
    })?;
    if let Some(start_time) = start_time {
        let time_code = if let Some(end_time) = end_time {
            if start_time >= end_time {
                return Err(crate::error::AnnattoError::EndTokenTimeLargerThanStart {
                    start: start_time,
                    end: end_time,
                });
            }
            format!("{start_time}-{end_time}")
        } else {
            format!("{start_time}-")
        };
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: tok_id.clone(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "time".to_string(),
            anno_value: time_code,
        })?;
    }
    if add_annis_layer {
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: tok_id.clone(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "layer".to_string(),
            anno_value: "default_layer".to_string(),
        })?;
    }
    Ok(tok_id)
}

pub fn map_annotations<S: AsRef<str>>(
    u: &mut GraphUpdate,
    node: &NodeInfo,
    ns: Option<&str>,
    name: Option<&str>,
    value: Option<&str>,
    targets: &[S],
) -> Result<String> {
    let span_id = format!("{}#sSpan{}", node.doc_path, node.node_id);
    u.add_event(UpdateEvent::AddNode {
        node_name: span_id.clone(),
        node_type: "node".to_string(),
    })?;
    u.add_event(UpdateEvent::AddEdge {
        source_node: span_id.to_string(),
        target_node: node.text_node_name.to_string(),
        layer: ANNIS_NS.to_string(),
        component_type: AnnotationComponentType::PartOf.to_string(),
        component_name: "".to_string(),
    })?;
    u.add_event(UpdateEvent::AddNodeLabel {
        node_name: span_id.clone(),
        anno_ns: ANNIS_NS.to_string(),
        anno_name: "layer".to_string(),
        anno_value: "default_ns".to_string(),
    })?;
    if let Some(name) = name {
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: span_id.clone(),
            anno_ns: ns.unwrap_or_default().to_string(),
            anno_name: name.to_string(),
            anno_value: value.unwrap_or_default().to_string(),
        })?;
    }
    for target in targets {
        u.add_event(UpdateEvent::AddEdge {
            source_node: span_id.clone(),
            target_node: target.as_ref().to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::Coverage.to_string(),
            component_name: "".to_string(),
        })?;
    }

    Ok(span_id)
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use graphannis::update::GraphUpdate;
    use insta::{assert_debug_snapshot, assert_snapshot};
    use itertools::Itertools;
    use tempfile::TempDir;

    use super::import_corpus_graph_from_files;

    #[test]
    fn single_file_import() {
        let mut u = GraphUpdate::new();
        let result = import_corpus_graph_from_files(
            &mut u,
            Path::new("tests/data/import/exmaralda/clean/import/exmaralda/test_doc.exb"),
            &["exb"],
        )
        .unwrap();

        assert_eq!(1, result.len());
        assert_eq!(
            "tests/data/import/exmaralda/clean/import/exmaralda/test_doc.exb",
            result[0].0.to_string_lossy()
        );
        assert_eq!("test_doc/test_doc", result[0].1);

        let created_updates: graphannis_core::errors::Result<Vec<_>> = u.iter().unwrap().collect();
        let created_updates = created_updates.unwrap();

        assert_debug_snapshot!(created_updates);
    }

    #[test]
    fn node_names_from_paths() {
        let paths = vec![
            "Sophisticated_Corpus_v1.9",
            "Sophisticated_Corpus_v1.9/lang1.1",
            "Sophisticated_Corpus_v1.9/lang2.1",
            "Sophisticated_Corpus_v1.9/lang1.1/doc1.fancyExtension",
            "Sophisticated_Corpus_v1.9/lang1.1/doc2.fancyExtension",
            "Sophisticated_Corpus_v1.9/lang2.1/doc1.fancyExtension",
            "Sophisticated_Corpus_v1.9/lang2.1/doc2.fancyExtension",
        ];
        let tmp_dir = TempDir::new().unwrap();
        assert!(fs::create_dir_all(tmp_dir.path().join(paths[1])).is_ok());
        assert!(fs::create_dir_all(tmp_dir.path().join(paths[2])).is_ok());
        paths[3..].iter().for_each(|p| {
            assert!(
                fs::write(tmp_dir.path().join(p), "".as_bytes()).is_ok(),
                "Error creating: {}",
                p
            )
        });
        let root_path = tmp_dir.path().join(paths[0]);
        let mut update = GraphUpdate::default();
        assert!(
            import_corpus_graph_from_files(&mut update, &root_path, &["fancyExtension"]).is_ok()
        );
        assert_snapshot!(update
            .iter()
            .unwrap()
            .flatten()
            .map(|(_, ue)| match ue {
                graphannis::update::UpdateEvent::AddNode { node_name, .. } => node_name.to_string(),
                _ => "".to_string(),
            })
            .filter(|s| !s.is_empty())
            .join("\n"));
    }
}
