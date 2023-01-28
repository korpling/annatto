use crate::Module;

use super::Importer;

pub const MODULE_NAME: &str = "add_metadata";

struct AddMetadata {}

impl Default for AddMetadata {
    fn default() -> Self {
        AddMetadata {}
    }
}

impl Module for AddMetadata {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}

impl Importer for AddMetadata {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        properties: &std::collections::BTreeMap<String, String>,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        todo!()
    }
}


#[cfg(test)]
mod tests {
    use std::{env::temp_dir, io::Write, ptr::metadata, collections::BTreeMap};

    use graphannis::{AnnotationGraph, update::{GraphUpdate, UpdateEvent}};
    use graphannis_core::graph::ANNIS_NS;
    use tempfile::{tempdir_in, tempfile_in};

    use crate::importer::Importer;

    use super::AddMetadata;

    #[test]
    fn test_metadata_in_mem() {
        let r = core_test(false);
        assert_eq!(r.is_ok(), true, "Probing core test result {:?}", r);
    }

    #[test]
    fn test_metadata_on_disk() {
        let r = core_test(true);
        assert_eq!(r.is_ok(), true, "Probing core test result {:?}", r);
    }

    fn test_metadata(on_disk: bool) -> Result<()> {
        let mut e_g = target_graph(on_disk)?;
        let add_metadata = AddMetadata::default();
        let metadata = ["language=unknown", "date=yesterday"];
        let metadata_file_path = temp_dir().join("metadata").join("corpus").join("doc.meta");
        std::fs::create_dir_all(metadata_file_path.parent()?)?;
        let mut metadata_file = std::fs::File::create(metadata_file_path)?;
        metadata_file.write(metadata.as_bytes())?;
        let mut properties = BTreeMap::new();
        let r = add_metadata.import_corpus(metadata_file_path.parent()?.parent()?.into(), 
                                                                                &properties, 
                                                                                None);
        assert_eq!(true, r.is_ok(), "Ended with error: ", &r);
        let mut u = r?;
        u.add_event(UpdateEvent::AddNode { node_name: "corpus/doc#t1".to_string(), node_type: "node".to_string() })?;
        u.add_event(UpdateEvent::AddNodeLabel { 
            node_name: "corpus/doc#t1".to_string(), 
            anno_ns: ANNIS_NS.to_string(), 
            anno_name: "tok".to_string(), 
            anno_value: "u".to_string() 
        })?;
        let g = AnnotationGraph::new(on_disk)?;
        let apu = g.apply_update(&mut u, |_| {});
        assert_eq!(true, apu.is_ok(), "Applying updates ends with error:", &apu);
        let queries = [
            "tok",
            "tok @* language",
            "tok @* date"
        ];
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

    fn target_graph(on_disk: bool) -> Result<AnnotationGraph> {
        let mut g = AnnotationGraph::new(on_disk)?;
        let mut u = GraphUpdate::default();
        u.add_event(UpdateEvent::AddNode { node_name: "corpus".to_string(), node_type: "corpus".to_string() })?;
        u.add_event(UpdateEvent::AddNode { node_name: "corpus/doc".to_string(), node_type: "corpus".to_string() })?;
        u.add_event(UpdateEvent::AddNodeLabel { 
            node_name: "corpus/doc".to_string(), 
            anno_ns: "".to_string(), 
            anno_name: "language".to_string(), 
            anno_value:  "unknown".to_string()
        })?;
        u.add_event(UpdateEvent::AddNodeLabel { 
            node_name: "corpus/doc".to_string(), 
            anno_ns: "".to_string(), 
            anno_name: "date".to_string(), 
            anno_value:  "yesterday".to_string()
        })?;
        u.add_event(UpdateEvent::AddNode { node_name: "corpus/doc#1".to_string(), node_type: "node".to_string() })?;
        u.add_event(UpdateEvent::AddNodeLabel { 
            node_name: "corpus/doc#t1".to_string(), 
            anno_ns: ANNIS_NS.to_string(), 
            anno_name: "tok".to_string(), 
            anno_value: "u".to_string() 
        })?;
        g.apply_update(&mut u, |_| {})?;
        Ok(g)
    }
}