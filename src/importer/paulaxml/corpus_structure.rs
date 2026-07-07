use anyhow::Result;
use graphannis::{
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::ANNIS_NS;
use std::path::{Path, PathBuf};

use crate::importer::GenericImportConfiguration;

pub(super) struct CorpusMapper {}
impl CorpusMapper {
    pub(super) fn new() -> CorpusMapper {
        CorpusMapper {}
    }

    pub(super) fn map_corpus_structure<P: AsRef<Path>>(
        &self,
        root_path: P,
        config: &GenericImportConfiguration,
        updates: &mut GraphUpdate,
    ) -> Result<Vec<(PathBuf, String)>> {
        let mut path_tuples = add_subcorpora(updates, root_path.as_ref(), config, None)?;
        path_tuples.sort();
        Ok(path_tuples)
    }
}

fn add_subcorpora(
    u: &mut GraphUpdate,
    file_path: &Path,
    config: &GenericImportConfiguration,
    parent_corpus: Option<&str>,
) -> Result<Vec<(PathBuf, String)>> {
    let mut result = Vec::new();

    // Get the sub-directories and sort them according to their path, to get a predictable
    // order of adding the documents to the graph.
    let mut subdirs = Vec::new();
    for entry in std::fs::read_dir(file_path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            subdirs.push(entry);
        }
    }
    subdirs.sort_by_key(|dir_entry| dir_entry.path());
    if subdirs.is_empty()
        && let Some(parent_corpus) = parent_corpus
    {
        // Add the directory itself as document
        let subcorpus_name = file_path
            .file_stem()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| "document".to_string());
        let node_name = format!("{parent_corpus}/{subcorpus_name}");
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
        // The directory is not a document but a subcorpus, since it contains more subdirectories
        let corpus_name = file_path
            .file_name()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| "corpus".to_string());
        let node_name = if let Some(parent_corpus) = parent_corpus {
            format!("{parent_corpus}/{corpus_name}")
        } else {
            if let Some(overwritten_root_corpus_name) = &config.root_as {
                overwritten_root_corpus_name.clone()
            } else {
                corpus_name
            }
        };

        u.add_event(UpdateEvent::AddNode {
            node_name: node_name.clone(),
            node_type: "corpus".to_string(),
        })?;
        if let Some(parent_corpus) = parent_corpus {
            u.add_event(UpdateEvent::AddEdge {
                source_node: node_name.clone(),
                target_node: parent_corpus.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string(),
            })?;
        }
        for entry in subdirs {
            result.extend(add_subcorpora(u, &entry.path(), config, Some(&node_name))?);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::io::BufWriter;

    use super::*;

    use graphannis::{AnnotationGraph, update::GraphUpdate};
    use insta::assert_snapshot;

    #[test]
    fn map_example_paulaxml_corpus_structure() {
        let mut updates = GraphUpdate::new();
        let corpus_mapper = CorpusMapper::new();
        let path_to_node_name = corpus_mapper
            .map_corpus_structure(
                "tests/data/import/paulaxml/rootCorpus",
                &GenericImportConfiguration::default(),
                &mut updates,
            )
            .unwrap();

        assert_eq!(4, path_to_node_name.len());
        assert_eq!(
            (
                PathBuf::from("tests/data/import/paulaxml/rootCorpus/subCorpus1/doc1"),
                "rootCorpus/subCorpus1/doc1".to_string()
            ),
            path_to_node_name[0]
        );
        assert_eq!(
            (
                PathBuf::from("tests/data/import/paulaxml/rootCorpus/subCorpus1/doc2"),
                "rootCorpus/subCorpus1/doc2".to_string()
            ),
            path_to_node_name[1]
        );
        assert_eq!(
            (
                PathBuf::from("tests/data/import/paulaxml/rootCorpus/subCorpus2/doc3"),
                "rootCorpus/subCorpus2/doc3".to_string()
            ),
            path_to_node_name[2]
        );
        assert_eq!(
            (
                PathBuf::from("tests/data/import/paulaxml/rootCorpus/subCorpus2/doc4"),
                "rootCorpus/subCorpus2/doc4".to_string()
            ),
            path_to_node_name[3]
        );

        let mut g = AnnotationGraph::with_default_graphstorages(true).unwrap();
        g.apply_update(&mut updates, |_| {}).unwrap();

        let mut buf = BufWriter::new(Vec::new());
        graphannis_core::graph::serialization::graphml::export_stable_order(
            &g,
            None,
            &mut buf,
            |_| {},
        )
        .unwrap();
        let bytes = buf.into_inner().unwrap();
        let actual = String::from_utf8(bytes).unwrap();

        assert_snapshot!(actual);
    }
}
