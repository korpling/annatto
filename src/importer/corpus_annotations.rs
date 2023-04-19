use std::{
    collections::BTreeMap,
    io::{self, BufRead},
    path::Path,
};

use graphannis::{
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::{graph::ANNIS_NS, util::split_qname};

use crate::{workflow::StatusMessage, Module};

use super::Importer;

pub const MODULE_NAME: &str = "annotate_corpus";

#[derive(Default)]
pub struct AnnotateCorpus {}

impl Module for AnnotateCorpus {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}

const KV_SEPARATOR: &str = "=";

fn read_annotations(
    path: &Path,
    tx: &Option<crate::workflow::StatusSender>,
) -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let anno_file = std::fs::File::open(path)?;
    let mut anno_map = BTreeMap::new();
    for line_r in io::BufReader::new(anno_file).lines() {
        let line = line_r?;
        if let Some((k, v)) = line.split_once(KV_SEPARATOR) {
            anno_map.insert(k.to_string(), v.to_string());
        } else if let Some(sender) = tx {
            sender.send(StatusMessage::Warning(format!(
                "Could not read data `{}` in file {}",
                &line,
                path.display()
            )))?;
        }
    }
    Ok(anno_map)
}

impl Importer for AnnotateCorpus {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        _properties: &std::collections::BTreeMap<String, String>,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let path_pattern = input_path.join("**/*.meta");
        dbg!(&path_pattern);
        let files = glob::glob(path_pattern.to_str().unwrap())?;
        for file_path_r in files {
            let file_path = file_path_r?;
            let mut corpus_nodes = Vec::new();
            for ancestor in file_path.ancestors() {
                if ancestor == input_path {
                    break;
                }
                corpus_nodes.push(ancestor);
            }
            corpus_nodes.reverse();
            let last_item = corpus_nodes.remove(corpus_nodes.len() - 1);
            let clean_name = last_item
                .to_path_buf()
                .parent()
                .unwrap()
                .join(last_item.file_stem().unwrap());
            corpus_nodes.push(clean_name.as_path());
            let start_index: usize = input_path.to_str().unwrap().len() + 1;
            let mut previous: Option<String> = None;
            for node_path in corpus_nodes {
                let node_name = (node_path.to_str().unwrap()[start_index..]).to_string();
                update.add_event(UpdateEvent::AddNode {
                    node_name: node_name.to_string(),
                    node_type: "corpus".to_string(),
                })?; // this is required, corpus annotations might be first updates to be processed
                if let Some(previous_name) = previous {
                    update.add_event(UpdateEvent::AddEdge {
                        source_node: node_name.to_string(),
                        target_node: previous_name.to_string(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::PartOf.to_string(),
                        component_name: "".to_string(),
                    })?;
                }
                previous = Some(node_name);
            }
            if let Some(corpus_doc_path) = previous {
                let path = file_path.as_path();
                let annotations = read_annotations(path, &tx)?;
                for (k, v) in annotations {
                    let (anno_ns, anno_name) = match split_qname(k.as_str()) {
                        (None, name) => ("", name),
                        (Some(ns), name) => (ns, name),
                    };
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: corpus_doc_path.to_string(),
                        anno_ns: anno_ns.to_string(),
                        anno_name: anno_name.to_string(),
                        anno_value: v,
                    })?;
                }
            }
        }
        Ok(update)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, env::temp_dir, io::Write};

    use graphannis::{
        corpusstorage::{QueryLanguage, ResultOrder, SearchQuery},
        model::AnnotationComponentType,
        update::{GraphUpdate, UpdateEvent},
        AnnotationGraph, CorpusStorage,
    };
    use graphannis_core::graph::ANNIS_NS;
    use tempfile::tempdir_in;

    use crate::importer::Importer;

    use super::AnnotateCorpus;

    #[test]
    fn test_metadata_in_mem() {
        let r = test_metadata(false);
        assert_eq!(r.is_ok(), true, "Probing core test result {:?}", r);
    }

    #[test]
    fn test_metadata_on_disk() {
        let r = test_metadata(true);
        assert_eq!(r.is_ok(), true, "Probing core test result {:?}", r);
    }

    fn test_metadata(on_disk: bool) -> Result<(), Box<dyn std::error::Error>> {
        let mut e_g = target_graph(on_disk)?;
        let add_metadata = AnnotateCorpus::default();
        let metadata = ["language=unknown", "date=yesterday"];
        let metadata_file_path = temp_dir().join("metadata").join("corpus").join("doc.meta");
        std::fs::create_dir_all(metadata_file_path.parent().unwrap())?;
        let mut metadata_file = std::fs::File::create(metadata_file_path)?;
        metadata_file.write(metadata.join("\n").as_bytes())?;
        let properties = BTreeMap::new();
        let r =
            add_metadata.import_corpus(temp_dir().join("metadata").as_path(), &properties, None);
        assert_eq!(
            true,
            r.is_ok(),
            "Applying corpus annotation updates ended with error: {:?}",
            r.err().unwrap()
        );
        let mut u = r?;
        external_updates(&mut u)?;
        let mut g = AnnotationGraph::new(on_disk)?;
        let apu = g.apply_update(&mut u, |_| {});
        assert_eq!(
            true,
            apu.is_ok(),
            "Applying updates ends with error: {:?}",
            &apu
        );
        let queries = ["tok @* language", "tok @* date"];
        let corpus_name = "current";
        let tmp_dir_e = tempdir_in(temp_dir())?;
        let tmp_dir_g = tempdir_in(temp_dir())?;
        e_g.save_to(&tmp_dir_e.path().join(corpus_name))?;
        g.save_to(&tmp_dir_g.path().join(corpus_name))?;
        let cs_e = CorpusStorage::with_auto_cache_size(&tmp_dir_e.path(), true)?;
        let cs_g = CorpusStorage::with_auto_cache_size(&tmp_dir_g.path(), true)?;
        for query_s in queries {
            let query = SearchQuery {
                corpus_names: &[corpus_name],
                query: query_s,
                query_language: QueryLanguage::AQL,
                timeout: None,
            };
            let matches_e = cs_e.find(query.clone(), 0, None, ResultOrder::Normal)?;
            let matches_g = cs_g.find(query, 0, None, ResultOrder::Normal)?;
            assert_eq!(
                matches_e.len(),
                matches_g.len(),
                "Failed with query: {}",
                query_s
            );
            for (m_e, m_g) in matches_e.into_iter().zip(matches_g.into_iter()) {
                assert_eq!(m_e, m_g);
            }
        }
        Ok(())
    }

    fn external_updates(u: &mut GraphUpdate) -> Result<(), Box<dyn std::error::Error>> {
        u.add_event(UpdateEvent::AddNode {
            node_name: "corpus/doc#1".to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "corpus/doc#t1".to_string(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "tok".to_string(),
            anno_value: "a".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "corpus/doc#t2".to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "corpus/doc#t2".to_string(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "tok".to_string(),
            anno_value: "b".to_string(),
        })?;

        u.add_event(UpdateEvent::AddNode {
            node_name: "corpus/doc#t3".to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "corpus/doc#t3".to_string(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "tok".to_string(),
            anno_value: "c".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "corpus/doc#t1".to_string(),
            target_node: "corpus/doc#t2".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::Ordering.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "corpus/doc#t2".to_string(),
            target_node: "corpus/doc#t3".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::Ordering.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "corpus/doc#t1".to_string(),
            target_node: "corpus/doc".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "corpus/doc#t2".to_string(),
            target_node: "corpus/doc".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "corpus/doc#t3".to_string(),
            target_node: "corpus/doc".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        Ok(())
    }

    fn target_graph(on_disk: bool) -> Result<AnnotationGraph, Box<dyn std::error::Error>> {
        let mut g = AnnotationGraph::new(on_disk)?;
        let mut u = GraphUpdate::default();
        u.add_event(UpdateEvent::AddNode {
            node_name: "corpus".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "corpus/doc".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "corpus/doc#1".to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "corpus/doc#t1".to_string(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "tok".to_string(),
            anno_value: "a".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "corpus/doc#t2".to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "corpus/doc#t2".to_string(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "tok".to_string(),
            anno_value: "b".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "corpus/doc#t3".to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "corpus/doc#t3".to_string(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "tok".to_string(),
            anno_value: "c".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "corpus/doc#t1".to_string(),
            target_node: "corpus/doc#t2".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::Ordering.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "corpus/doc#t2".to_string(),
            target_node: "corpus/doc#t3".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::Ordering.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "corpus/doc".to_string(),
            anno_ns: "".to_string(),
            anno_name: "language".to_string(),
            anno_value: "unknown".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "corpus/doc".to_string(),
            anno_ns: "".to_string(),
            anno_name: "date".to_string(),
            anno_value: "yesterday".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "corpus/doc".to_string(),
            target_node: "corpus".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "corpus/doc#t1".to_string(),
            target_node: "corpus/doc".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "corpus/doc#t2".to_string(),
            target_node: "corpus/doc".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "corpus/doc#t3".to_string(),
            target_node: "corpus/doc".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        g.apply_update(&mut u, |_| {})?;
        Ok(g)
    }
}
