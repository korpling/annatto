use std::{fs, io::Write, path::PathBuf};

use documented::{Documented, DocumentedFields};
use graphannis::{
    AnnotationGraph,
    graph::{AnnoKey, NodeID},
};
use graphannis_core::{annostorage::ValueSearch, graph::ANNIS_NS};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use struct_field_names_as_array::FieldNamesAsSlice;

use crate::progress::ProgressReporter;

use super::Exporter;

/// This module exports annattos and peppers meta data format.
/// Generally all nodes are up for export as a single document,
/// thus the `name_key` is used to subset the nodes and define the node names.
///
/// Example (with default settings):
/// ```toml
/// [[export]]
/// format = "meta"
/// path = "..."
///
/// [export.config]
/// name_key = "annis::doc"
/// only = []
/// write_ns = false
/// ```
///
/// This is equivalent to:
/// ```toml
/// [[export]]
/// format = "meta"
/// path = "..."
///
/// [export.config]
/// ```
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExportMeta {
    /// This key determines the value of the file name and which nodes are being exported into a single file,
    /// i. e., only nodes that hold a value for the provided will be exported. If values are not unique, an
    /// already written file will be overwritten.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// name_key = "my_unique_file_name_key"
    /// ```
    #[serde(default = "default_name_key", with = "crate::estarde::anno_key")]
    name_key: AnnoKey,
    /// This option allows to restrict the exported annotation keys. Also, adding keys with namespace "annis"
    /// here is allowed, as annotation keys having that namespace are ignored in the default setting.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// only = ["annis::doc", "annis::node_name", "annis::node_type", "date"]
    /// ```
    #[serde(default, with = "crate::estarde::anno_key::in_sequence")]
    only: Vec<AnnoKey>,
    /// By setting this to true, the namespaces will be exported as well. By default, this option is false.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// write_ns = "true"
    /// ```
    /// The namespace will be separated from the annotation name by `::`.
    #[serde(default)]
    write_ns: bool,
}

impl Default for ExportMeta {
    fn default() -> Self {
        Self {
            name_key: default_name_key(),
            only: Default::default(),
            write_ns: Default::default(),
        }
    }
}

fn default_name_key() -> AnnoKey {
    AnnoKey {
        name: "doc".into(),
        ns: ANNIS_NS.into(),
    }
}

const FILE_EXTENSION: &str = "meta";

impl Exporter for ExportMeta {
    fn export_corpus(
        &self,
        graph: &graphannis::AnnotationGraph,
        output_path: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let nodes = graph
            .get_node_annos()
            .exact_anno_search(
                Some(&self.name_key.ns),
                &self.name_key.name,
                ValueSearch::Any,
            )
            .flatten()
            .collect_vec();
        let progress = ProgressReporter::new(tx, step_id, nodes.len())?;
        for m in nodes {
            if let Some(name) = graph
                .get_node_annos()
                .get_value_for_item(&m.node, &self.name_key)?
            {
                let name_with_extension = format!("{name}.{}", self.file_extension());
                let file_path = output_path.join(name_with_extension);
                self.export_document(file_path, m.node, graph)?;
                progress.worked(1)?;
            }
        }
        Ok(())
    }

    fn file_extension(&self) -> &str {
        FILE_EXTENSION
    }
}

impl ExportMeta {
    fn export_document(
        &self,
        path: PathBuf,
        node: NodeID,
        graph: &AnnotationGraph,
    ) -> Result<(), anyhow::Error> {
        let mut writable = fs::File::create(path)?;
        let annos = graph.get_node_annos().get_annotations_for_item(&node)?;
        let mut lines = Vec::with_capacity(annos.len());
        for anno in annos {
            if (self.only.is_empty() && anno.key.ns.as_str() != ANNIS_NS)
                || self.only.contains(&anno.key)
            {
                let key = if self.write_ns {
                    [anno.key.ns.as_str(), anno.key.name.as_str()].join("::")
                } else {
                    anno.key.name.to_string()
                };
                let line = [key, anno.val.to_string()].join("=");
                lines.push(line);
            }
        }
        lines.sort();
        for line in lines {
            writable.write_all(line.as_bytes())?;
            writable.write_all("\n".as_bytes())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use graphannis::{AnnotationGraph, graph::AnnoKey};
    use insta::assert_snapshot;

    use crate::{
        importer::{Importer, exmaralda::ImportEXMARaLDA},
        test_util::export_to_string,
    };

    use super::ExportMeta;

    #[test]
    fn serialize() {
        let module = ExportMeta::default();
        let serialization = toml::to_string(&module);
        assert!(
            serialization.is_ok(),
            "Serialization failed: {:?}",
            serialization.err()
        );
        assert_snapshot!(serialization.unwrap());
    }

    #[test]
    fn serialize_custom() {
        let module = ExportMeta {
            name_key: AnnoKey {
                ns: "default_ns".into(),
                name: "document".into(),
            },
            only: vec![
                AnnoKey {
                    ns: "default_ns".into(),
                    name: "author".into(),
                },
                AnnoKey {
                    ns: "default_ns".into(),
                    name: "date".into(),
                },
            ],
            write_ns: true,
        };
        let serialization = toml::to_string(&module);
        assert!(
            serialization.is_ok(),
            "Serialization failed: {:?}",
            serialization.err()
        );
        assert_snapshot!(serialization.unwrap());
    }

    fn test(snapshot_name: &str, workflow_path: &Path) {
        let toml_str = fs::read_to_string(workflow_path);
        assert!(toml_str.is_ok());
        let r = toml::from_str(toml_str.unwrap().as_str());
        let exporter: ExportMeta = r.unwrap();
        let importer = ImportEXMARaLDA::default();
        let u = importer.import_corpus(
            Path::new("tests/data/import/exmaralda/clean/"),
            crate::StepID {
                module_name: "test_import".to_string(),
                path: None,
            },
            None,
        );
        assert!(u.is_ok());
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut update = u.unwrap();
        let mut graph = g.unwrap();
        assert!(graph.apply_update(&mut update, |_| {}).is_ok());
        let export = export_to_string(&graph, exporter);
        assert!(export.is_ok());
        assert_snapshot!(snapshot_name, export.unwrap());
    }

    #[test]
    fn with_ns() {
        test("with_ns", Path::new("tests/data/export/meta/ns.toml"));
    }

    #[test]
    fn default() {
        test("default", Path::new("tests/data/export/meta/default.toml"));
    }

    #[test]
    fn non_default_key() {
        test(
            "non_default",
            Path::new("tests/data/export/meta/non_default_key.toml"),
        );
    }

    #[test]
    fn only() {
        test("only", Path::new("tests/data/export/meta/only.toml"));
    }
}
