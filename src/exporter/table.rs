use std::{
    borrow::Cow,
    collections::{btree_map::Entry, BTreeMap},
    path::Path,
};

use anyhow::anyhow;
use documented::{Documented, DocumentedFields};
use graphannis::{
    graph::{AnnoKey, NodeID},
    model::{AnnotationComponent, AnnotationComponentType},
    AnnotationGraph,
};
use graphannis_core::{
    dfs::CycleSafeDFS,
    graph::{ANNIS_NS, NODE_NAME_KEY},
    util::join_qname,
};
use itertools::Itertools;
use serde::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use super::Exporter;

use crate::deserialize::deserialize_anno_key;

/// This module exports all ordered nodes and nodes connected by coverage edges of any name into a table.
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(deny_unknown_fields)]
pub struct ExportTable {
    /// The provided annotation key defines which nodes within the part-of component define a document. All nodes holding said annotation
    /// will be exported to a file with the name according to the annotation value. Therefore annotation values must not contain path
    /// delimiters.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// doc_anno = "my_namespace::document"
    /// ```
    ///
    /// The default is `annis::doc`.
    #[serde(
        deserialize_with = "deserialize_anno_key",
        default = "default_doc_anno"
    )]
    doc_anno: AnnoKey,
    /// The provided character defines the column delimiter. The default value is tab.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// delimiter = ";"
    /// ```
    #[serde(default = "default_delimiter")]
    delimiter: char,
    /// The provided character will be used for quoting values. If nothing is provided, all columns will contain bare values. If a character is provided,
    /// all values will be quoted.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// quote_char = "\""
    /// ```
    #[serde(default)]
    quote_char: Option<char>,
}

impl Default for ExportTable {
    fn default() -> Self {
        Self {
            doc_anno: default_doc_anno(),
            delimiter: default_delimiter(),
            quote_char: None,
        }
    }
}

fn default_doc_anno() -> AnnoKey {
    AnnoKey {
        name: "doc".into(),
        ns: ANNIS_NS.into(),
    }
}

fn default_delimiter() -> char {
    '\t'
}

const FILE_EXTENSION: &str = "csv";

impl Exporter for ExportTable {
    fn export_corpus(
        &self,
        graph: &graphannis::AnnotationGraph,
        output_path: &std::path::Path,
        _step_id: crate::StepID,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let base_ordering = AnnotationComponent::new(
            AnnotationComponentType::Ordering,
            ANNIS_NS.into(),
            "".into(),
        );
        let storage = graph
            .get_graphstorage(&base_ordering)
            .ok_or(anyhow!("Storage of base ordering unavailable"))?;
        let part_of_storage = graph
            .get_graphstorage(&AnnotationComponent::new(
                AnnotationComponentType::PartOf,
                ANNIS_NS.into(),
                "".into(),
            ))
            .ok_or(anyhow!("Part-of storage unavailbale."))?;
        let mut doc_node_to_start = BTreeMap::new();
        for node in storage
            .source_nodes()
            .flatten()
            .filter(|n| !storage.has_ingoing_edges(*n).unwrap_or_default())
        {
            let dfs = CycleSafeDFS::new(
                part_of_storage.as_edgecontainer(),
                node,
                0,
                NodeID::MAX as usize,
            );
            for nxt in dfs {
                let n = nxt?.node;
                if graph
                    .get_node_annos()
                    .has_value_for_item(&n, &self.doc_anno)
                    .unwrap_or_default()
                {
                    if let Entry::Vacant(e) = doc_node_to_start.entry(n) {
                        e.insert(node);
                        break;
                    } else {
                        let doc_node_name = graph
                            .get_node_annos()
                            .get_value_for_item(&n, &NODE_NAME_KEY)?
                            .unwrap_or_default();
                        return Err(anyhow!(
                            "Document {doc_node_name} has more than one start node for base ordering."
                        )
                        .into());
                    }
                }
            }
        }
        doc_node_to_start
            .into_iter()
            .try_for_each(|(doc, start)| self.export_document(graph, output_path, doc, start))?;
        Ok(())
    }

    fn file_extension(&self) -> &str {
        FILE_EXTENSION
    }
}

type Data<'a> = BTreeMap<usize, Cow<'a, str>>;

impl ExportTable {
    fn export_document(
        &self,
        graph: &AnnotationGraph,
        corpus_path: &Path,
        doc_node: NodeID,
        start_node: NodeID,
    ) -> Result<(), anyhow::Error> {
        let node_annos = graph.get_node_annos();
        let doc_node_name = node_annos
            .get_value_for_item(&doc_node, &self.doc_anno)?
            .ok_or(anyhow!("Could not determine document node name."))?;
        let ordering_storage = graph
            .get_graphstorage(&AnnotationComponent::new(
                AnnotationComponentType::Ordering,
                ANNIS_NS.into(),
                "".into(),
            ))
            .ok_or(anyhow!("Storage of ordering component unavailable."))?;
        let ordered_nodes = ordering_storage
            .find_connected(start_node, 0, std::ops::Bound::Excluded(usize::MAX))
            .flatten()
            .collect_vec();
        let mut table_data: Vec<Data> = Vec::with_capacity(ordered_nodes.len());
        let coverage_components =
            graph.get_all_components(Some(AnnotationComponentType::Coverage), None);
        let coverage_storages = coverage_components
            .iter()
            .filter_map(|c| graph.get_graphstorage(c))
            .collect_vec();
        let mut index_map = BTreeMap::default();
        for node in ordered_nodes {
            let reachable_nodes = coverage_storages
                .iter()
                .flat_map(|s| {
                    s.find_connected_inverse(node, 0, std::ops::Bound::Excluded(usize::MAX))
                })
                .flatten();
            let mut data = Data::default();
            for rn in reachable_nodes {
                let node_name = node_annos
                    .get_value_for_item(&rn, &NODE_NAME_KEY)?
                    .ok_or(anyhow!("Node has no name"))?;
                for anno_key in node_annos.get_all_keys_for_item(&rn, None, None)? {
                    if anno_key.ns.as_str() != ANNIS_NS {
                        let qname = join_qname(anno_key.ns.as_str(), anno_key.name.as_str());
                        let id_name = format!("id_{qname}");
                        let index = if let Some(index) = index_map.get(&qname) {
                            *index
                        } else {
                            index_map.insert(qname.to_string(), index_map.len());
                            index_map.insert(id_name.to_string(), index_map.len());
                            index_map.len() - 2
                        };
                        let value = node_annos
                            .get_value_for_item(&rn, &anno_key)?
                            .ok_or(anyhow!("Annotation has no value"))?;
                        data.insert(index, value);
                        data.insert(index + 1, node_name.clone());
                    }
                }
            }
            table_data.push(data);
        }
        let file_path =
            Path::new(corpus_path).join(format!("{doc_node_name}.{}", self.file_extension()));
        let mut writer_builder = csv::WriterBuilder::new();
        writer_builder.delimiter(self.delimiter as u8);
        if let Some(c) = &self.quote_char {
            writer_builder.quote(*c as u8);
            writer_builder.quote_style(csv::QuoteStyle::Always);
        }
        let mut writer = writer_builder.from_path(file_path)?;
        let header = index_map
            .iter()
            .sorted_by(|(_, v), (_, v_)| v.cmp(v_))
            .map(|(k, _)| k)
            .collect_vec();
        writer.write_record(header)?;
        let index_bound = index_map.len();
        for mut entry in table_data {
            let mut row = Vec::with_capacity(index_bound);
            for col_index in 0..index_bound {
                row.push(entry.remove(&col_index).unwrap_or_default().to_string());
            }
            if !row.iter().all(String::is_empty) {
                writer.write_record(&row)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use graphannis::AnnotationGraph;
    use insta::assert_snapshot;

    use crate::{
        exporter::table::ExportTable,
        importer::{exmaralda::ImportEXMARaLDA, Importer},
        test_util::export_to_string,
        StepID,
    };

    #[test]
    fn core() {
        let exmaralda = ImportEXMARaLDA {};
        let mprt = exmaralda.import_corpus(
            Path::new("tests/data/import/exmaralda/clean/import/exmaralda/"),
            StepID {
                module_name: "test_import_exb".to_string(),
                path: None,
            },
            None,
        );
        assert!(mprt.is_ok());
        let mut update_import = mprt.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(graph.apply_update(&mut update_import, |_| {}).is_ok());
        let export = export_to_string(&graph, ExportTable::default());
        assert!(export.is_ok(), "error: {:?}", export.err());
        assert_snapshot!(export.unwrap());
    }

    #[test]
    fn quoted() {
        let exmaralda = ImportEXMARaLDA {};
        let mprt = exmaralda.import_corpus(
            Path::new("tests/data/import/exmaralda/clean/import/exmaralda/"),
            StepID {
                module_name: "test_import_exb".to_string(),
                path: None,
            },
            None,
        );
        assert!(mprt.is_ok());
        let mut update_import = mprt.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(graph.apply_update(&mut update_import, |_| {}).is_ok());
        let export = export_to_string(
            &graph,
            ExportTable {
                quote_char: Some('"'),
                ..Default::default()
            },
        );
        assert!(export.is_ok(), "error: {:?}", export.err());
        assert_snapshot!(export.unwrap());
    }
}
