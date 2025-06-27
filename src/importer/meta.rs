use std::{
    collections::BTreeMap,
    io::{self, BufRead},
    path::Path,
};

use anyhow::anyhow;
use documented::{Documented, DocumentedFields};
use graphannis::{
    graph::AnnoKey,
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::{
    graph::ANNIS_NS,
    util::{join_qname, split_qname},
};
use serde::Serialize;
use serde_derive::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use crate::{StepID, progress::ProgressReporter, util::get_all_files};

use super::Importer;

/// Works similar to the Pepper configuration value
/// [`pepper.before.readMeta`](https://corpus-tools.org/pepper/generalCustomizationProperties.html)
/// and imports metadata property files for documents and corpora by using the file
/// name as path to the document.
/// Alternatively, you can import csv-tables, that specify the target node in a specific column. The
/// header of said column has to be provided as `identifier`, which also needs to be a used annotation
/// key found in the corpus graph at the target node.
///
/// Example (for csv files):
/// ```toml
/// [import.config]
/// identifier = { ns = "annis", name = "doc" }  # this is the default and can be omitted
/// ```
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AnnotateCorpus {
    /// The annotation key identifying document nodes.
    #[serde(default = "default_identifier", with = "crate::estarde::anno_key")]
    identifier: AnnoKey,
    /// The delimiter used in csv files.
    #[serde(default = "default_delimiter")]
    delimiter: String,
}

impl Default for AnnotateCorpus {
    fn default() -> Self {
        Self {
            identifier: default_identifier(),
            delimiter: default_delimiter(),
        }
    }
}

const DEFAULT_DELIMITER: &str = ",";

fn default_identifier() -> AnnoKey {
    AnnoKey {
        ns: ANNIS_NS.into(),
        name: "doc".into(),
    }
}

fn default_delimiter() -> String {
    DEFAULT_DELIMITER.to_string()
}

const KV_SEPARATOR: &str = "=";

fn read_annotations(
    path: &Path,
    progress: &ProgressReporter,
) -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let anno_file = std::fs::File::open(path)?;
    let mut anno_map = BTreeMap::new();
    for line_r in io::BufReader::new(anno_file).lines() {
        let line = line_r?;
        if let Some((k, v)) = line.split_once(KV_SEPARATOR) {
            anno_map.insert(k.to_string(), v.to_string());
        } else {
            progress.warn(&format!(
                "Could not read data `{}` in file {}",
                &line,
                path.display()
            ))?;
        }
    }
    Ok(anno_map)
}

const FILE_EXTENSIONS: [&str; 2] = ["meta", "csv"];

impl Importer for AnnotateCorpus {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        step_id: StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let all_files = get_all_files(input_path, self.file_extensions())?;
        let progress = ProgressReporter::new(tx, step_id, all_files.len())?;
        let start_index = input_path.to_string_lossy().len() + 1;
        for file_path in all_files.into_iter().filter(|p| p.is_file()) {
            let parent_opt = &file_path.parent();
            let file_stem_opt = file_path.file_stem();
            let (parent, file_stem) = if let Some(p) = parent_opt
                && let Some(s) = file_stem_opt
            {
                (p, s)
            } else {
                progress
                    .warn(format!("Skipping file: {}", file_path.to_string_lossy()).as_str())?;
                continue;
            };
            if file_path.extension().unwrap_or_default().to_string_lossy() == "csv" {
                let s_ix = parent
                    .to_string_lossy()
                    .rfind(std::path::MAIN_SEPARATOR_STR)
                    .unwrap_or_default()
                    + 1;
                self.import_from_csv(&file_path, &parent.to_string_lossy()[s_ix..], &mut update)?;
            } else {
                let full_path = &parent.join(file_stem);
                let node_name = &full_path.to_string_lossy()[start_index..];
                update.add_event(UpdateEvent::AddNode {
                    node_name: node_name.to_string(),
                    node_type: "corpus".to_string(),
                })?; // this is required, corpus annotations might be first updates to be processed
                let annotations = read_annotations(&file_path, &progress)?;
                for (k, v) in annotations {
                    let (anno_ns, anno_name) = split_qname(&k);
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: node_name.to_string(),
                        anno_ns: anno_ns.unwrap_or_default().trim().to_string(),
                        anno_name: anno_name.trim().to_string(),
                        anno_value: v.trim().to_string(),
                    })?;
                }
            }
            progress.worked(1)?;
        }
        Ok(update)
    }

    fn file_extensions(&self) -> &[&str] {
        &FILE_EXTENSIONS
    }
}

impl AnnotateCorpus {
    fn import_from_csv(
        &self,
        path: &Path,
        parent: &str,
        update: &mut GraphUpdate,
    ) -> Result<(), anyhow::Error> {
        let node_column = if self.identifier.ns.is_empty() {
            self.identifier.name.to_string()
        } else {
            join_qname(&self.identifier.ns, &self.identifier.name)
        };
        let del = self
            .delimiter
            .as_bytes()
            .first()
            .ok_or(anyhow!("Delimiter undefined."))?;
        let mut reader = csv::ReaderBuilder::new().delimiter(*del).from_path(path)?;
        let header: Vec<String> = reader.headers()?.iter().map(|e| e.to_string()).collect();
        for line in reader.into_records().flatten() {
            let mut node_name_opt = None;
            let mut annotations = Vec::new();
            for (name, value) in header.iter().zip(line.iter()) {
                if *name == node_column {
                    node_name_opt = Some([parent, value].join("/"));
                } else {
                    annotations.push((name.to_string(), value.to_string()));
                }
            }
            if let Some(node_name) = node_name_opt {
                update.add_event(UpdateEvent::AddNode {
                    node_name: parent.to_string(),
                    node_type: "corpus".to_string(),
                })?;
                update.add_event(UpdateEvent::AddNode {
                    node_name: node_name.to_string(),
                    node_type: "corpus".to_string(),
                })?;
                update.add_event(UpdateEvent::AddEdge {
                    source_node: node_name.to_string(),
                    target_node: parent.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::PartOf.to_string(),
                    component_name: "".to_string(),
                })?;
                for (k, v) in annotations {
                    let (ns, name) = split_qname(&k);
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: node_name.to_string(),
                        anno_ns: ns.unwrap_or_default().to_string(),
                        anno_name: name.to_string(),
                        anno_value: v.trim().to_string(),
                    })?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Write, path::Path};

    use graphannis::{
        AnnotationGraph, CorpusStorage,
        corpusstorage::{QueryLanguage, ResultOrder, SearchQuery},
        graph::AnnoKey,
        model::AnnotationComponentType,
        update::{GraphUpdate, UpdateEvent},
    };
    use graphannis_core::graph::ANNIS_NS;
    use insta::assert_snapshot;
    use tempfile::tempdir;

    use crate::{
        ImporterStep, ReadFrom, exporter::graphml::GraphMLExporter, importer::Importer,
        test_util::export_to_string,
    };

    use super::AnnotateCorpus;

    #[test]
    fn serialize_custom() {
        let module = AnnotateCorpus {
            delimiter: "\t".to_string(),
            identifier: AnnoKey {
                ns: "meta".into(),
                name: "id".into(),
            },
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
    fn from_csv() {
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let mut u = GraphUpdate::default();
        assert!(external_updates(&mut u).is_ok());
        assert!(graph.apply_update(&mut u, |_| {}).is_ok());
        let toml_str = fs::read_to_string("./tests/data/import/meta/deserialize.toml");
        assert!(toml_str.is_ok());
        let import: Result<AnnotateCorpus, _> = toml::from_str(toml_str.unwrap().as_str());
        assert!(import.is_ok());
        let r = import.unwrap().import_corpus(
            Path::new("./tests/data/import/meta/corpus/"),
            crate::StepID {
                module_name: "test".to_string(),
                path: None,
            },
            None,
        );
        assert!(r.is_ok(), "ERROR: {:?}", r.err());
        u = r.unwrap();
        assert!(graph.apply_update(&mut u, |_| {}).is_ok());
        let actual = export_to_string(&graph, GraphMLExporter::default());
        assert!(actual.is_ok(), "ERROR: {:?}", actual.err());
        assert_snapshot!(actual.unwrap());
    }

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
        let mut e_g = target_graph(on_disk).map_err(|_| assert!(false)).unwrap();
        let add_metadata = ReadFrom::Meta(AnnotateCorpus::default());
        // document-level metadata
        let doc_metadata = ["language=unknown", "date=yesterday"];
        let tmp_dir = tempdir()?;
        let metadata_file_path = tmp_dir
            .path()
            .join("metadata")
            .join("corpus")
            .join("doc.meta");
        std::fs::create_dir_all(metadata_file_path.parent().unwrap())
            .map_err(|_| assert!(false))
            .unwrap();
        let mut metadata_file = std::fs::File::create(metadata_file_path)
            .map_err(|_| assert!(false))
            .unwrap();
        metadata_file
            .write(doc_metadata.join("\n").as_bytes())
            .map_err(|_| assert!(false))
            .unwrap();
        // corpus-level metadata
        let corpus_metadata = ["version=1.0", "doi=is a secret"];
        let cmetadata_file_path = tmp_dir.path().join("metadata").join("corpus.meta");
        let mut cmetadata_file = std::fs::File::create(cmetadata_file_path)
            .map_err(|_| assert!(false))
            .unwrap();
        cmetadata_file
            .write(corpus_metadata.join("\n").as_bytes())
            .map_err(|_| assert!(false))
            .unwrap();
        let import_step = ImporterStep {
            module: add_metadata,
            path: tmp_dir.path().join("metadata").to_path_buf(),
        };
        let r = import_step.execute(None);
        assert_eq!(
            true,
            r.is_ok(),
            "Applying corpus annotation updates ended with error: {:?}",
            r.err().unwrap()
        );
        let mut u = r?;
        external_updates(&mut u)
            .map_err(|_| assert!(false))
            .unwrap();
        let mut g = AnnotationGraph::with_default_graphstorages(on_disk)
            .map_err(|_| assert!(false))
            .unwrap();
        let apu = g.apply_update(&mut u, |_| {});
        assert!(
            apu.is_ok(),
            "Applying updates ends with error: {:?}",
            &apu.err()
        );
        let queries = [
            "language",
            "date",
            "version",
            "doi",
            "annis:node_name=/corpus/ _ident_ version=/1.0/ _ident_ doi=/is a secret/",
            "annis:node_name=\"corpus/doc\" _ident_ language=/unknown/ _ident_ date=/yesterday/",
        ];
        let corpus_name = "current";
        let tmp_dir_e = tempdir().map_err(|_| assert!(false)).unwrap();
        let tmp_dir_g = tempdir().map_err(|_| assert!(false)).unwrap();
        assert!(e_g.save_to(&tmp_dir_e.path().join(corpus_name)).is_ok());
        assert!(g.save_to(&tmp_dir_g.path().join(corpus_name)).is_ok());
        let cs_e = CorpusStorage::with_auto_cache_size(&tmp_dir_e.path(), true)
            .map_err(|_| assert!(false))
            .unwrap();
        let cs_g = CorpusStorage::with_auto_cache_size(&tmp_dir_g.path(), true)
            .map_err(|_| assert!(false))
            .unwrap();
        for query_s in queries {
            let query = SearchQuery {
                corpus_names: &[corpus_name],
                query: query_s,
                query_language: QueryLanguage::AQL,
                timeout: None,
            };
            let matches_e = cs_e
                .find(query.clone(), 0, None, ResultOrder::Normal)
                .map_err(|_| assert!(false))
                .unwrap();
            let matches_g = cs_g
                .find(query, 0, None, ResultOrder::Normal)
                .map_err(|_| assert!(false))
                .unwrap();
            assert!(matches_e.len() > 0, "No matches for query: {}", query_s);
            assert_eq!(
                matches_e.len(),
                matches_g.len(),
                "Failed with query: {} ({:?})",
                query_s,
                matches_g
            );
            for (m_e, m_g) in matches_e.into_iter().zip(matches_g.into_iter()) {
                assert_eq!(m_e, m_g);
            }
        }
        Ok(())
    }

    fn external_updates(u: &mut GraphUpdate) -> Result<(), Box<dyn std::error::Error>> {
        u.add_event(UpdateEvent::AddNode {
            node_name: "corpus/doc#t1".to_string(),
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
        let mut g = AnnotationGraph::with_default_graphstorages(on_disk)?;
        let mut u = GraphUpdate::default();
        u.add_event(UpdateEvent::AddNode {
            node_name: "corpus".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "corpus".to_string(),
            anno_ns: "".to_string(),
            anno_name: "version".to_string(),
            anno_value: "1.0".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "corpus".to_string(),
            anno_ns: "".to_string(),
            anno_name: "doi".to_string(),
            anno_value: "is a secret".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "corpus/doc".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "corpus/doc#t1".to_string(),
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
