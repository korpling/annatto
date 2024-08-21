use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{LineWriter, Write},
    ops::Bound,
    path::Path,
};

use anyhow::{anyhow, bail};
use documented::{Documented, DocumentedFields};
use graphannis::{
    graph::{AnnoKey, Edge, NodeID},
    model::{AnnotationComponent, AnnotationComponentType},
    AnnotationGraph,
};
use graphannis_core::{
    annostorage::ValueSearch,
    graph::{ANNIS_NS, NODE_NAME_KEY},
};
use itertools::Itertools;
use serde::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use super::Exporter;

use crate::deserialize::{
    deserialize_anno_key, deserialize_anno_key_opt, deserialize_anno_key_seq,
    deserialize_annotation_component, deserialize_annotation_component_opt,
    deserialize_annotation_component_seq,
};

/// This module exports a graph in CoNLL-U format.
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(deny_unknown_fields)]
pub struct ExportCoNLLU {
    /// This key is used to determine nodes that whose part-of subgraph constitutes a document, i. e. the entire input for a file.
    /// Default is `annis::doc`, or `{ ns = "annis", name = "doc" }`.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// doc = "annis::doc"
    /// ```
    #[serde(
        deserialize_with = "deserialize_anno_key",
        default = "default_doc_anno"
    )]
    doc: AnnoKey,
    /// This optional annotation key is used to identify annotation spans, that constitute a sentence. Default is no export of sentence blocks.
    /// Default is `annis::doc`, or `{ ns = "annis", name = "doc" }`.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// groupby = "norm::sentence"
    /// ```
    #[serde(deserialize_with = "deserialize_anno_key_opt", default)]
    groupby: Option<AnnoKey>,
    /// The nodes connected by this annotation component are used as nodes defining a line in a CoNLL-U file. Usually you want to use an ordering.
    /// Default is `{ ctype = "Ordering", layer = "annis", name = "" }`.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// ordering = { ctype = "Ordering", layer = "annis", name = "norm" }
    /// ```
    #[serde(
        deserialize_with = "deserialize_annotation_component",
        default = "default_ordering"
    )]
    ordering: AnnotationComponent,
    /// This annotation key is used to write the form column.
    /// Default is `{ ns = "annis", name = "tok" }`.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// form = { ns = "norm", name = "norm" }
    /// ```
    #[serde(
        deserialize_with = "deserialize_anno_key",
        default = "default_form_key"
    )]
    form: AnnoKey,
    /// This annotation key is used to write the lemma column.
    /// Default is `{ ns = "", name = "tok" }`.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// lemma = { ns = "norm", name = "lemma" }
    /// ```
    #[serde(
        deserialize_with = "deserialize_anno_key",
        default = "default_lemma_key"
    )]
    lemma: AnnoKey,
    /// This annotation key is used to write the upos column.
    /// Default is `{ ns = "", name = "upos" }`.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// upos = { ns = "norm", name = "pos" }
    /// ```
    #[serde(
        deserialize_with = "deserialize_anno_key",
        default = "default_upos_key"
    )]
    upos: AnnoKey,
    /// This annotation key is used to write the xpos column.
    /// Default is `{ ns = "", name = "xpos" }`.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// upos = { ns = "norm", name = "pos_spec" }
    /// ```
    #[serde(
        deserialize_with = "deserialize_anno_key",
        default = "default_xpos_key"
    )]
    xpos: AnnoKey,
    /// This list of annotation keys will be represented in the feature column.
    /// Default is the empty list.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// features = ["Animacy", "Tense", "VerbClass"]
    /// ```
    #[serde(deserialize_with = "deserialize_anno_key_seq", default)]
    features: Vec<AnnoKey>,
    /// The nodes connected by this annotation component are used to export dependencies.
    /// Default is none, so nothing will be exported.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// dependency_component = { ctype = "Pointing", layer = "", name = "dependencies" }
    /// ```
    #[serde(deserialize_with = "deserialize_annotation_component_opt", default)]
    dependency_component: Option<AnnotationComponent>, // this is an option, because by default no edges are exported, as dependency anotations are not usually given and exporting conll usually serves actually parsing the data
    /// This annotation key is used to write the dependency relation, which will be looked for on the dependency edges.
    /// Default is none, so nothing will be exported.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// dependency_anno = { ns = "", name = "deprel" }
    /// ```
    #[serde(deserialize_with = "deserialize_anno_key_opt", default)]
    dependency_anno: Option<AnnoKey>, // same reason for option as in component field
    /// The listed components will be used to export enhanced dependencies. More than
    /// one component can be listed.
    /// Default is the empty list, so nothing will be exported.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// enhanced_components = [{ ctype = "Pointing", layer = "", name = "dependencies" }]
    /// ```
    #[serde(deserialize_with = "deserialize_annotation_component_seq", default)]
    enhanced_components: Vec<AnnotationComponent>,
    /// This list of annotation keys defines the annotation keys, that correspond to the
    /// edge labels in the component listed in `enhanced_components`. The i-th element of
    /// one list belongs to the i-th element in the other list. Default is the empty list.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// enhanced_annos = ["func"]
    /// ```
    #[serde(deserialize_with = "deserialize_anno_key_seq", default)]
    enhanced_annos: Vec<AnnoKey>,
    /// This list of annotation keys will be represented in the misc column.
    /// Default is the empty list.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// misc = ["NoSpaceAfter", "Referent"]
    /// ```
    #[serde(deserialize_with = "deserialize_anno_key_seq", default)]
    misc: Vec<AnnoKey>,
}

fn default_doc_anno() -> AnnoKey {
    AnnoKey {
        name: "doc".into(),
        ns: ANNIS_NS.into(),
    }
}

fn default_ordering() -> AnnotationComponent {
    AnnotationComponent::new(
        AnnotationComponentType::Ordering,
        ANNIS_NS.into(),
        "".into(),
    )
}

fn default_form_key() -> AnnoKey {
    AnnoKey {
        name: "tok".into(),
        ns: ANNIS_NS.into(),
    }
}

fn default_lemma_key() -> AnnoKey {
    AnnoKey {
        name: "lemma".into(),
        ns: "".into(),
    }
}

fn default_xpos_key() -> AnnoKey {
    AnnoKey {
        name: "xpos".into(),
        ns: "".into(),
    }
}

fn default_upos_key() -> AnnoKey {
    AnnoKey {
        name: "upos".into(),
        ns: "".into(),
    }
}

impl Default for ExportCoNLLU {
    fn default() -> Self {
        Self {
            doc: default_doc_anno(),
            groupby: None,
            ordering: default_ordering(),
            form: default_form_key(),
            lemma: default_lemma_key(),
            upos: default_upos_key(),
            xpos: default_xpos_key(),
            features: vec![],
            dependency_component: None,
            dependency_anno: None,
            enhanced_components: vec![],
            enhanced_annos: vec![],
            misc: vec![],
        }
    }
}

const FILE_EXTENSION: &str = "conllu";

impl Exporter for ExportCoNLLU {
    fn export_corpus(
        &self,
        graph: &graphannis::AnnotationGraph,
        output_path: &std::path::Path,
        _step_id: crate::StepID,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut doc_nodes = graph
            .get_node_annos()
            .exact_anno_search(
                Some(self.doc.ns.as_str()),
                self.doc.name.as_str(),
                ValueSearch::Any,
            )
            .flatten();
        doc_nodes.try_for_each(|d| self.export_document(graph, d.node, output_path))?;
        Ok(())
    }

    fn file_extension(&self) -> &str {
        FILE_EXTENSION
    }
}

const NO_VALUE: &str = "_";

type NodeData<'a> = BTreeMap<&'a AnnoKey, String>;
type DependencyData = Vec<(NodeID, Option<String>)>;

impl ExportCoNLLU {
    fn export_document(
        &self,
        graph: &AnnotationGraph,
        doc_node: NodeID,
        corpus_path: &Path,
    ) -> Result<(), anyhow::Error> {
        let node_annos = graph.get_node_annos();
        let doc_name = node_annos
            .get_value_for_item(&doc_node, &self.doc)?
            .ok_or(anyhow!("Document name is not available."))?;
        let output_path = corpus_path.join(format!("{doc_name}.{}", self.file_extension()));
        let mut writer = LineWriter::new(fs::File::create(output_path)?);
        let part_of_storage = graph
            .get_graphstorage(&AnnotationComponent::new(
                AnnotationComponentType::PartOf,
                ANNIS_NS.into(),
                "".into(),
            ))
            .ok_or(anyhow!("Part-of component storage not available."))?;
        let ordering_storage = graph
            .get_graphstorage(&self.ordering)
            .ok_or(anyhow!("Ordering storage is unavailable."))?;
        let start_node = part_of_storage
            .find_connected_inverse(doc_node, 0, Bound::Included(usize::MAX))
            .flatten()
            .find(|n| {
                !ordering_storage.has_ingoing_edges(*n).unwrap_or_default()
                    && node_annos
                        .has_value_for_item(n, &self.form)
                        .unwrap_or_default()
            })
            .ok_or(anyhow!("Could not find ordering start node for {doc_name}"))?;
        let mut anno_keys = vec![&self.form, &self.lemma, &self.upos, &self.xpos];
        anno_keys.extend(&self.features);
        anno_keys.extend(&self.misc);
        let mut node_id = 1;
        let mut last_group = None;
        let ordered_nodes = ordering_storage
            .find_connected(start_node, 0, Bound::Included(usize::MAX))
            .flatten()
            .collect_vec(); // can be memory intense, but we need indices
        let node_to_index: BTreeMap<NodeID, usize> = ordered_nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (*n, i))
            .collect();
        for node in ordered_nodes {
            let (mut data, group_node, dependency_data) =
                self.node_data(graph, anno_keys.clone(), node)?;
            if let (Some(gn), Some(gn_)) = (last_group, group_node) {
                if gn != gn_ {
                    writer.write_all("\n".as_bytes())?;
                    last_group = group_node;
                    node_id = 1;
                }
            } else {
                last_group = group_node;
            }
            let mut line = Vec::new();
            line.push(node_id.to_string());
            if let Some(value) = data.remove(&self.form) {
                line.push(value);
            } else {
                bail!(
                    "No form value for node {}",
                    node_annos
                        .get_value_for_item(&node, &NODE_NAME_KEY)?
                        .unwrap_or_default()
                );
            }
            for k in [&self.lemma, &self.upos, &self.xpos] {
                line.push(data.remove(k).unwrap_or(NO_VALUE.to_string()));
            }
            let mut features = Vec::with_capacity(self.features.len());
            for k in &self.features {
                if let Some(value) = data.remove(k) {
                    features.push([k.name.to_string(), value].join("="));
                }
            }
            features.sort();
            if features.is_empty() {
                line.push(NO_VALUE.to_string());
            } else {
                line.push(features.join("|"));
            }

            // dependencies
            let (head_id, label) = if self.dependency_component.is_some() {
                map_dependency_data(dependency_data.first(), node_id, node, &node_to_index)?
            } else {
                (NO_VALUE.to_string(), NO_VALUE.to_string())
            };
            line.push(head_id);
            line.push(label);

            // enhanced dependencies
            let mut entries = Vec::with_capacity(dependency_data.len());
            for entry in &dependency_data {
                let (head, label) =
                    map_dependency_data(Some(entry), node_id, node, &node_to_index)?;
                entries.push([head, label].join(":"));
            }
            if entries.is_empty() {
                line.push(NO_VALUE.to_string());
            } else {
                line.push(entries.join("|"));
            }

            // misc
            features.clear();
            for k in &self.misc {
                if let Some(value) = data.remove(k) {
                    features.push([k.name.to_string(), value].join("="));
                }
            }
            features.sort();
            if features.is_empty() {
                line.push(NO_VALUE.to_string());
            } else {
                line.push(features.join("|"));
            }

            // finish
            writer.write_all(line.join("\t").as_bytes())?;
            writer.write_all("\n".as_bytes())?;
            node_id += 1;
        }
        writer.flush()?;
        Ok(())
    }

    fn node_data<'a>(
        &self,
        graph: &AnnotationGraph,
        keys: Vec<&'a AnnoKey>,
        node: NodeID,
    ) -> Result<(NodeData<'a>, Option<NodeID>, DependencyData), anyhow::Error> {
        let coverage_storages = graph
            .get_all_components(Some(AnnotationComponentType::Coverage), None)
            .into_iter()
            .filter_map(|c| graph.get_graphstorage(&c))
            .collect_vec();
        let mut connected_nodes = BTreeSet::default();
        for storage in coverage_storages {
            storage
                .find_connected(node, 0, Bound::Included(usize::MAX))
                .flatten()
                .for_each(|n| {
                    connected_nodes.insert(n);
                });
            let extra_nodes = connected_nodes
                .iter()
                .map(|n| {
                    storage
                        .find_connected_inverse(*n, 1, Bound::Included(usize::MAX))
                        .flatten()
                })
                .collect_vec();
            extra_nodes
                .into_iter()
                .for_each(|v| connected_nodes.extend(v));
        }
        let mut data = BTreeMap::default();
        let mut remaining_keys: BTreeSet<&AnnoKey> = keys.into_iter().collect();
        let node_annos = graph.get_node_annos();
        let mut group_node = None;
        let mut dependency_data = DependencyData::default();
        let mut dependency_storages = if let Some(c) = &self.dependency_component {
            if let Some(storage) = graph.get_graphstorage(c) {
                vec![storage]
            } else {
                bail!("No such component: {c}. Please check configuration.");
            }
        } else {
            vec![]
        };
        self.enhanced_components.iter().for_each(|c| {
            if let Some(storage) = graph.get_graphstorage(c) {
                dependency_storages.push(storage);
            }
        });
        let mut dependency_keys = if let Some(k) = &self.dependency_anno {
            vec![k]
        } else {
            vec![]
        };
        self.enhanced_annos
            .iter()
            .for_each(|k| dependency_keys.push(k));
        if dependency_storages.len() != dependency_keys.len() {
            bail!("Number of dependency components does not match number of label names.");
        }
        for node in connected_nodes {
            if let (None, Some(k)) = (group_node, &self.groupby) {
                if node_annos.has_value_for_item(&node, k)? {
                    group_node = Some(node);
                }
            }
            if !remaining_keys.is_empty() {
                let mut pop = BTreeSet::new();
                for k in &remaining_keys {
                    if let Some(value) = node_annos.get_value_for_item(&node, k)? {
                        pop.insert(*k);
                        data.insert(*k, value.to_string());
                    }
                }
                for k in pop {
                    remaining_keys.remove(k);
                }
            }
            for (storage, label_key) in dependency_storages.iter().zip(&dependency_keys) {
                if let Some(other_node) = storage.get_ingoing_edges(node).next() {
                    let id = other_node?;
                    let label = storage
                        .get_anno_storage()
                        .get_value_for_item(
                            &Edge {
                                source: id,
                                target: node,
                            },
                            label_key,
                        )?
                        .map(|v| v.to_string());
                    dependency_data.push((id, label));
                }
            }
        }
        Ok((data, group_node, dependency_data))
    }
}

fn map_dependency_data(
    dependency_data: Option<&(NodeID, Option<String>)>,
    conll_id: usize,
    internal_id: NodeID,
    node_index: &BTreeMap<NodeID, usize>,
) -> Result<(String, String), anyhow::Error> {
    if let Some((internal_head_id, label)) = dependency_data {
        let order_index_head = *node_index
            .get(internal_head_id)
            .ok_or(anyhow!("Unknown node id of dependency head."))?
            as i32;
        let order_index_dependent = *node_index
            .get(&internal_id)
            .ok_or(anyhow!("Unknown dependent id."))? as i32;
        let normalized_id = order_index_head - order_index_dependent + (conll_id as i32);
        if let Some(v) = label {
            Ok((normalized_id.to_string(), v.to_string()))
        } else {
            Ok((normalized_id.to_string(), NO_VALUE.to_string()))
        }
    } else {
        Ok((NO_VALUE.to_string(), NO_VALUE.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use graphannis::AnnotationGraph;
    use insta::assert_snapshot;

    use crate::{
        exporter::conllu::ExportCoNLLU,
        importer::{conllu::ImportCoNLLU, Importer},
        test_util::export_to_string,
        StepID,
    };

    #[test]
    fn conll_to_conll() {
        let conll_in = ImportCoNLLU::default();
        let u = conll_in.import_corpus(
            Path::new("tests/data/import/conll/valid"),
            StepID {
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
        let toml_str = fs::read_to_string("./tests/data/export/conll/deserialize.toml").unwrap();
        let conll_out: Result<ExportCoNLLU, _> = toml::from_str(toml_str.as_str());
        assert!(
            conll_out.is_ok(),
            "could not deserialize exporter: {:?}",
            conll_out.err()
        );
        let actual = export_to_string(&graph, conll_out.unwrap());
        assert!(actual.is_ok(), "failed: {:?}", actual.err());
        assert_snapshot!(actual.unwrap());
    }
}