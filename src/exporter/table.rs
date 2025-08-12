use std::{
    collections::{BTreeMap, btree_map::Entry},
    ops::Bound,
    path::Path,
};

use anyhow::{anyhow, bail};
use documented::{Documented, DocumentedFields};
use graphannis::{
    AnnotationGraph,
    graph::{AnnoKey, Edge, NodeID},
    model::{AnnotationComponent, AnnotationComponentType},
};
use graphannis_core::{
    annostorage::EdgeAnnotationStorage,
    dfs::CycleSafeDFS,
    graph::{ANNIS_NS, NODE_NAME_KEY},
    util::join_qname,
};
use itertools::Itertools;
use linked_hash_set::LinkedHashSet;
use serde::{Deserialize, Serialize};
use struct_field_names_as_array::FieldNamesAsSlice;

use super::Exporter;

use crate::{progress::ProgressReporter, util::token_helper::TOKEN_KEY};

/// This module exports all ordered nodes and nodes connected by coverage edges of any name into a table.
#[derive(
    Deserialize, Documented, DocumentedFields, FieldNamesAsSlice, Serialize, Clone, PartialEq,
)]
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
    #[serde(default = "default_doc_anno", with = "crate::estarde::anno_key")]
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
    /// Provides the string sequence used for n/a. Default is the empty string.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// no_value = "n/a"
    /// ```
    #[serde(default)]
    no_value: String,
    /// By listing annotation components, the ingoing edges of that component and their annotations
    /// will be exported as well. Multiple ingoing edges will be separated by a ";". Each exported
    /// node will be checked for ingoing edges in the respective components.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// ingoing = [{ ctype = "Pointing", layer = "", ns = "dep"}]
    /// ```
    #[serde(default, with = "crate::estarde::annotation_component::in_sequence")]
    ingoing: Vec<AnnotationComponent>,
    /// By listing annotation components, the ingoing edges of that component and their annotations
    /// will be exported as well. Multiple outgoing edges will be separated by a ";". Each exported
    /// node will be checked for outgoing edges in the respective components.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// outgoing = [{ ctype = "Pointing", layer = "", ns = "reference"}]
    /// ```
    #[serde(default, with = "crate::estarde::annotation_component::in_sequence")]
    outgoing: Vec<AnnotationComponent>,
    /// If `true` (the default), always output a column with the ID of the node.
    #[serde(default = "default_id_column")]
    id_column: bool,
    /// Export the given columns (qualified annotation names) in the given order.
    #[serde(default)]
    column_names: Vec<String>,
    /// If true, do not output the first line with the column names.
    #[serde(default)]
    skip_header: bool,
    /// If true, do not output the `annis:tok` column
    #[serde(default)]
    skip_token: bool,
}

fn default_id_column() -> bool {
    true
}

impl Default for ExportTable {
    fn default() -> Self {
        Self {
            doc_anno: default_doc_anno(),
            delimiter: default_delimiter(),
            quote_char: Default::default(),
            no_value: Default::default(),
            ingoing: Default::default(),
            outgoing: Default::default(),
            id_column: default_id_column(),
            column_names: Default::default(),
            skip_header: Default::default(),
            skip_token: Default::default(),
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
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let progress = ProgressReporter::new_unknown_total_work(tx.clone(), step_id.clone())?;

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
        let coverage_storages = graph
            .get_all_components(Some(AnnotationComponentType::Coverage), None)
            .iter()
            .filter_map(|c| graph.get_graphstorage(c))
            .collect_vec();
        if coverage_storages.is_empty() {
            progress.warn("No coverage storages available")?;
        }
        let mut doc_node_to_start = BTreeMap::new();
        for node in storage.source_nodes().flatten().filter(|n| {
            !storage.has_ingoing_edges(*n).unwrap_or_default()
                && !coverage_storages
                    .iter()
                    .any(|s| s.has_outgoing_edges(*n).unwrap_or_default())
        }) {
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
        let progress = ProgressReporter::new(tx, step_id, doc_node_to_start.len())?;
        progress.info(&format!("Exporting {} documents", doc_node_to_start.len()))?;
        doc_node_to_start
            .into_iter()
            .try_for_each(move |(doc, start)| -> anyhow::Result<()> {
                self.export_document(graph, output_path, doc, start)?;
                progress.worked(1)?;
                Ok(())
            })?;
        Ok(())
    }

    fn file_extension(&self) -> &str {
        FILE_EXTENSION
    }
}

type Data = BTreeMap<usize, String>;
type EdgeData = BTreeMap<usize, LinkedHashSet<String>>; // insertion order is critical
type SingleEdgeData<'a> = (String, &'a AnnotationComponent, Vec<(String, String)>);

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
        for c in &self.column_names {
            index_map.insert(c.to_string(), index_map.len());
            if self.id_column {
                let id_name = format!("id_{c}");
                index_map.insert(id_name.to_string(), index_map.len());
            }
        }

        let follow_edges = !self.outgoing.is_empty() || !self.ingoing.is_empty();
        for node in ordered_nodes {
            let reachable_nodes = coverage_storages
                .iter()
                .flat_map(|s| {
                    s.find_connected_inverse(node, 0, std::ops::Bound::Excluded(usize::MAX))
                })
                .flatten();
            let mut data = Data::default();
            let mut edge_column_data = EdgeData::default();
            for rn in reachable_nodes {
                // reachable nodes contains the start node
                let node_name = node_annos
                    .get_value_for_item(&rn, &NODE_NAME_KEY)?
                    .ok_or(anyhow!("Node has no name"))?;
                for anno_key in node_annos.get_all_keys_for_item(&rn, None, None)? {
                    if anno_key.ns.as_str() != ANNIS_NS
                        || (!self.skip_token && anno_key.as_ref() == TOKEN_KEY.as_ref())
                    {
                        let qname = join_qname(anno_key.ns.as_str(), anno_key.name.as_str());
                        let id_name = format!("id_{qname}");
                        let index = if let Some(index) = index_map.get(&qname) {
                            *index
                        } else if self.id_column {
                            index_map.insert(qname.to_string(), index_map.len());
                            index_map.insert(id_name.to_string(), index_map.len());
                            index_map.len() - 2
                        } else {
                            index_map.insert(qname.to_string(), index_map.len());
                            index_map.len() - 1
                        };
                        let value = node_annos
                            .get_value_for_item(&rn, &anno_key)?
                            .ok_or(anyhow!("Annotation has no value"))?;
                        data.insert(index, value.to_string());
                        if self.id_column {
                            data.insert(index + 1, node_name.to_string());
                        }
                    }
                }
                if follow_edges {
                    let (sources, targets) = self.connected_nodes(graph, rn)?;
                    let mut prefixes = sources.iter().map(|_| "in").collect_vec();
                    prefixes.extend(targets.iter().map(|_| "out"));
                    for ((connected_node_name, component, mut edge_annotations), prefix) in
                        sources.into_iter().chain(targets.into_iter()).zip(prefixes)
                    {
                        let qualified_name = [
                            prefix,
                            component.get_type().to_string().as_str(),
                            component.layer.as_str(),
                            component.name.as_str(),
                        ]
                        .join("_");
                        edge_annotations.extend([("".to_string(), connected_node_name)]);
                        for (name, value) in edge_annotations {
                            let qname = if name.is_empty() {
                                qualified_name.to_string()
                            } else {
                                [qualified_name.as_str(), name.as_str()].join("_")
                            };
                            let index = if let Some(index) = index_map.get(&qname) {
                                *index
                            } else {
                                index_map.insert(qname, index_map.len());
                                index_map.len() - 1
                            };
                            match edge_column_data.entry(index) {
                                Entry::Vacant(e) => {
                                    let mut new_value = LinkedHashSet::default();
                                    new_value.insert(value);
                                    e.insert(new_value);
                                }
                                Entry::Occupied(mut e) => {
                                    e.get_mut().insert(value);
                                }
                            };
                        }
                    }
                }
            }
            data.extend(
                edge_column_data
                    .into_iter()
                    .map(|(ix, value_set)| (ix, value_set.iter().join(";").to_string())),
            );
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
        if !self.skip_header {
            let header = index_map
                .iter()
                .sorted_by(|(_, v), (_, v_)| v.cmp(v_))
                .map(|(k, _)| k)
                .collect_vec();
            writer.write_record(header)?;
        }
        let index_bound = index_map.len();
        for mut entry in table_data {
            let mut row = Vec::with_capacity(index_bound);
            for col_index in 0..index_bound {
                row.push(
                    entry
                        .remove(&col_index)
                        .unwrap_or(self.no_value.to_string())
                        .to_string(),
                );
            }
            if !row.iter().all(String::is_empty) {
                writer.write_record(&row)?;
            }
        }
        Ok(())
    }

    fn connected_nodes(
        &self,
        graph: &AnnotationGraph,
        node: NodeID,
    ) -> Result<(Vec<SingleEdgeData<'_>>, Vec<SingleEdgeData<'_>>), anyhow::Error> {
        let mut sources: Vec<SingleEdgeData> = Vec::new();
        let mut targets: Vec<SingleEdgeData> = Vec::new();
        for component in &self.ingoing {
            if let Some(storage) = graph.get_graphstorage(component) {
                let sources_ingoing = storage
                    .find_connected_inverse(node, 1, Bound::Excluded(2))
                    .flatten()
                    .collect_vec();
                for src in sources_ingoing {
                    if let Some(node_name) = graph
                        .get_node_annos()
                        .get_value_for_item(&src, &NODE_NAME_KEY)?
                    {
                        let edge = Edge {
                            source: src,
                            target: node,
                        };
                        let anno_storage = storage.get_anno_storage();
                        sources.push((
                            node_name.to_string(),
                            component,
                            edge_annos(anno_storage, &edge)?,
                        ));
                    }
                }
            } else {
                bail!(
                    "Component {}::{}::{} has no storage.",
                    component.get_type(),
                    component.layer,
                    component.name
                );
            }
        }
        for component in &self.outgoing {
            if let Some(storage) = graph.get_graphstorage(component) {
                let targets_outgoing = storage
                    .find_connected(node, 1, Bound::Excluded(2))
                    .flatten()
                    .collect_vec();
                for tgt in targets_outgoing {
                    if let Some(node_name) = graph
                        .get_node_annos()
                        .get_value_for_item(&tgt, &NODE_NAME_KEY)?
                    {
                        let edge = Edge {
                            source: node,
                            target: tgt,
                        };
                        let anno_storage = storage.get_anno_storage();
                        targets.push((
                            node_name.to_string(),
                            component,
                            edge_annos(anno_storage, &edge)?,
                        ));
                    }
                }
            } else {
                bail!(
                    "Component {}::{}::{} has no storage.",
                    component.get_type(),
                    component.layer,
                    component.name
                );
            }
        }
        Ok((sources, targets))
    }
}

fn edge_annos(
    anno_storage: &dyn EdgeAnnotationStorage,
    edge: &Edge,
) -> Result<Vec<(String, String)>, anyhow::Error> {
    let mut annotations = Vec::new();
    for anno_key in anno_storage.get_all_keys_for_item(edge, None, None)? {
        if anno_key.ns != ANNIS_NS {
            let qname = join_qname(&anno_key.ns, &anno_key.name);
            if let Some(value) = anno_storage.get_value_for_item(edge, &anno_key)? {
                annotations.push((qname, value.to_string()));
            }
        }
    }
    Ok(annotations)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use graphannis::{
        AnnotationGraph,
        graph::AnnoKey,
        model::{AnnotationComponent, AnnotationComponentType},
    };
    use graphannis_core::graph::ANNIS_NS;
    use insta::assert_snapshot;

    use crate::{
        StepID,
        exporter::table::ExportTable,
        importer::{Importer, conllu::ImportCoNLLU, exmaralda::ImportEXMARaLDA},
        test_util::export_to_string,
    };

    #[test]
    fn serialize() {
        let module = ExportTable::default();
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
        let module = ExportTable {
            column_names: vec!["text".to_string(), "lemma".to_string(), "pos".to_string()],
            doc_anno: AnnoKey {
                ns: "not_annis".into(),
                name: "not_doc".into(),
            },
            delimiter: ';',
            quote_char: Some('"'),
            no_value: "NA".to_string(),
            ingoing: vec![AnnotationComponent::new(
                AnnotationComponentType::Coverage,
                ANNIS_NS.into(),
                "".into(),
            )],
            outgoing: vec![AnnotationComponent::new(
                AnnotationComponentType::Pointing,
                "".into(),
                "reference".into(),
            )],
            id_column: true,
            skip_header: false,
            skip_token: false,
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
                skip_token: true,
                ..Default::default()
            },
        );
        assert!(export.is_ok(), "error: {:?}", export.err());
        assert_snapshot!(export.unwrap());
    }

    #[test]
    fn edge_features() {
        let to_conll = ImportCoNLLU::default();
        let mprt = to_conll.import_corpus(
            Path::new("tests/data/import/conll/valid/"),
            StepID {
                module_name: "test_import_conll".to_string(),
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
                ingoing: vec![AnnotationComponent::new(
                    AnnotationComponentType::Pointing,
                    "".into(),
                    "dep".into(),
                )],
                ..Default::default()
            },
        );
        assert!(export.is_ok(), "error: {:?}", export.err());
        assert_snapshot!(export.unwrap());
    }

    #[test]
    fn custom() {
        let to_conll = ImportCoNLLU::default();
        let mprt = to_conll.import_corpus(
            Path::new("tests/data/import/conll/valid/"),
            StepID {
                module_name: "test_import_conll".to_string(),
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
                delimiter: ';',
                no_value: "n/a".to_string(),
                quote_char: Some('\''),
                ingoing: vec![AnnotationComponent::new(
                    AnnotationComponentType::Pointing,
                    "".into(),
                    "dep".into(),
                )],
                outgoing: vec![AnnotationComponent::new(
                    AnnotationComponentType::Pointing,
                    "".into(),
                    "dep".into(),
                )],
                ..Default::default()
            },
        );
        assert!(export.is_ok(), "error: {:?}", export.err());
        assert_snapshot!(export.unwrap());
    }

    #[test]
    fn manual_column_ordering_no_id() {
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
        let m = ExportTable {
            column_names: vec![
                "norm::pos".to_string(),
                "annis::tok".to_string(),
                "dipl::sentence".to_string(),
            ],
            id_column: false,
            ..Default::default()
        };
        let export = export_to_string(&graph, m);
        assert!(export.is_ok(), "error: {:?}", export.err());
        assert_snapshot!(export.unwrap());
    }

    #[test]
    fn manual_column_ordering_with_id() {
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
        let m = ExportTable {
            column_names: vec![
                "norm::pos".to_string(),
                "annis::tok".to_string(),
                "dipl::sentence".to_string(),
            ],
            id_column: true,
            ..Default::default()
        };
        let export = export_to_string(&graph, m);
        assert!(export.is_ok(), "error: {:?}", export.err());
        assert_snapshot!(export.unwrap());
    }

    #[test]
    fn no_id_column() {
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
        let mut exporter = ExportTable::default();
        exporter.id_column = false;
        exporter.skip_token = true;
        let export = export_to_string(&graph, exporter);
        assert!(export.is_ok(), "error: {:?}", export.err());
        assert_snapshot!(export.unwrap());
    }
}
