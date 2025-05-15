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
use serde::{Deserialize, Serialize};
use struct_field_names_as_array::FieldNamesAsSlice;

use super::Exporter;

/// This module exports a graph in CoNLL-U format.
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice, Serialize)]
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
    #[serde(with = "crate::estarde::anno_key", default = "default_doc_anno")]
    doc: AnnoKey,
    /// This optional annotation key is used to identify annotation spans, that constitute a sentence. Default is no export of sentence blocks.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// groupby = "norm::sentence"
    /// ```
    #[serde(with = "crate::estarde::anno_key::as_option", default)]
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
        //deserialize_with = "deserialize_annotation_component",
        default = "default_ordering"
    )]
    #[serde(with = "crate::estarde::annotation_component")]
    ordering: AnnotationComponent,
    /// This annotation key is used to write the form column.
    /// Default is `{ ns = "annis", name = "tok" }`.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// form = { ns = "norm", name = "norm" }
    /// ```
    #[serde(with = "crate::estarde::anno_key", default = "default_form_key")]
    form: AnnoKey,
    /// This annotation key is used to write the lemma column.
    /// Default is `{ ns = "", name = "tok" }`.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// lemma = { ns = "norm", name = "lemma" }
    /// ```
    #[serde(with = "crate::estarde::anno_key", default = "default_lemma_key")]
    lemma: AnnoKey,
    /// This annotation key is used to write the upos column.
    /// Default is `{ ns = "", name = "upos" }`.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// upos = { ns = "norm", name = "pos" }
    /// ```
    #[serde(with = "crate::estarde::anno_key", default = "default_upos_key")]
    upos: AnnoKey,
    /// This annotation key is used to write the xpos column.
    /// Default is `{ ns = "", name = "xpos" }`.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// upos = { ns = "norm", name = "pos_spec" }
    /// ```
    #[serde(with = "crate::estarde::anno_key", default = "default_xpos_key")]
    xpos: AnnoKey,
    /// This list of annotation keys will be represented in the feature column.
    /// Default is the empty list.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// features = ["Animacy", "Tense", "VerbClass"]
    /// ```
    #[serde(with = "crate::estarde::anno_key::in_sequence", default)]
    features: Vec<AnnoKey>,
    /// The nodes connected by this annotation component are used to export dependencies.
    /// Default is none, so nothing will be exported.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// dependency_component = { ctype = "Pointing", layer = "", name = "dependencies" }
    /// ```
    #[serde(with = "crate::estarde::annotation_component::as_option", default)]
    dependency_component: Option<AnnotationComponent>, // this is an option, because by default no edges are exported, as dependency anotations are not usually given and exporting conll usually serves actually parsing the data
    /// This annotation key is used to write the dependency relation, which will be looked for on the dependency edges.
    /// Default is none, so nothing will be exported.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// dependency_anno = { ns = "", name = "deprel" }
    /// ```
    #[serde(with = "crate::estarde::anno_key::as_option", default)]
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
    #[serde(with = "crate::estarde::annotation_component::in_sequence", default)]
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
    #[serde(with = "crate::estarde::anno_key::in_sequence", default)]
    enhanced_annos: Vec<AnnoKey>,
    /// This list of annotation keys will be represented in the misc column.
    /// Default is the empty list.
    ///
    /// Example:
    /// ```toml
    /// [export.config]
    /// misc = ["NoSpaceAfter", "Referent"]
    /// ```
    #[serde(with = "crate::estarde::anno_key::in_sequence", default)]
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
        std::fs::create_dir_all(output_path)?;
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
            .find_connected_inverse(doc_node, 0, Bound::Unbounded)
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
            .find_connected(start_node, 0, Bound::Unbounded)
            .flatten()
            .collect_vec(); // can be memory intense, but we need indices
        let node_to_index: BTreeMap<NodeID, usize> = ordered_nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (*n, i))
            .collect();
        for node in ordered_nodes {
            let (data, group_node, dependency_data) =
                self.node_data(graph, anno_keys.clone(), node)?;
            if let (Some(gn), Some(gn_)) = (last_group, group_node) {
                if gn != gn_ {
                    writer.write_all("\n".as_bytes())?;
                    last_group = group_node;
                    node_id = 1;
                }
            } else {
                if last_group.xor(group_node).is_some()
                    && ordering_storage.has_ingoing_edges(node)?
                {
                    writer.write_all("\n".as_bytes())?;
                    node_id = 1;
                }
                last_group = group_node;
            }
            let mut line = Vec::new();
            line.push(node_id.to_string());
            if let Some(value) = data.get(&self.form) {
                line.push(value.to_string());
            } else {
                bail!(
                    "No form value for node {}",
                    node_annos
                        .get_value_for_item(&node, &NODE_NAME_KEY)?
                        .unwrap_or_default()
                );
            }
            for k in [&self.lemma, &self.upos, &self.xpos] {
                let value = if let Some(v) = data.get(k) {
                    v.to_string()
                } else {
                    NO_VALUE.to_string()
                };
                line.push(value);
            }
            let mut features = Vec::with_capacity(self.features.len());
            for k in &self.features {
                if let Some(value) = data.get(k) {
                    features.push([k.name.to_string(), value.to_string()].join("="));
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
                if let Some(value) = data.get(k) {
                    features.push([k.name.to_string(), value.to_string()].join("="));
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
                .find_connected(node, 0, Bound::Unbounded)
                .flatten()
                .for_each(|n| {
                    connected_nodes.insert(n);
                });
            let extra_nodes = connected_nodes
                .iter()
                .map(|n| {
                    storage
                        .find_connected_inverse(*n, 1, Bound::Unbounded)
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

    use graphannis::{
        graph::AnnoKey,
        model::{AnnotationComponent, AnnotationComponentType},
        update::{GraphUpdate, UpdateEvent},
        AnnotationGraph,
    };
    use graphannis_core::graph::ANNIS_NS;
    use insta::assert_snapshot;

    use crate::{
        exporter::conllu::ExportCoNLLU,
        importer::{conllu::ImportCoNLLU, Importer},
        test_util::export_to_string,
        StepID,
    };

    #[test]
    fn serialize() {
        let module = ExportCoNLLU::default();
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
        let module = ExportCoNLLU {
            dependency_anno: Some(AnnoKey {
                name: "func".into(),
                ns: "default_ns".into(),
            }),
            doc: AnnoKey {
                name: "document".into(),
                ns: "default_ns".into(),
            },
            groupby: Some(AnnoKey {
                ns: "default_ns".into(),
                name: "sentence".into(),
            }),
            ordering: AnnotationComponent::new(
                AnnotationComponentType::Ordering,
                ANNIS_NS.into(),
                "norm".into(),
            ),
            form: AnnoKey {
                name: "norm".into(),
                ns: "norm".into(),
            },
            lemma: AnnoKey {
                name: "lemma".into(),
                ns: "norm".into(),
            },
            upos: AnnoKey {
                name: "pos".into(),
                ns: "norm".into(),
            },
            xpos: AnnoKey {
                name: "pos_lang".into(),
                ns: "norm".into(),
            },
            features: vec![AnnoKey {
                ns: "norm".into(),
                name: "Tense".into(),
            }],
            dependency_component: Some(AnnotationComponent::new(
                AnnotationComponentType::Pointing,
                "".into(),
                "dep".into(),
            )),
            enhanced_components: vec![AnnotationComponent::new(
                AnnotationComponentType::Pointing,
                "".into(),
                "semantics".into(),
            )],
            enhanced_annos: vec![AnnoKey {
                ns: "norm".into(),
                name: "role".into(),
            }],
            misc: vec![
                AnnoKey {
                    ns: "norm".into(),
                    name: "author".into(),
                },
                AnnoKey {
                    ns: ANNIS_NS.into(),
                    name: "tok-whitespace-after".into(),
                },
            ],
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

    #[test]
    fn groupless_tokens() {
        let mut u = GraphUpdate::default();
        assert!(u
            .add_event(UpdateEvent::AddNode {
                node_name: "corpus".to_string(),
                node_type: "corpus".to_string()
            })
            .is_ok());
        assert!(u
            .add_event(UpdateEvent::AddNodeLabel {
                node_name: "corpus".to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "doc".to_string(),
                anno_value: "corpus".to_string()
            })
            .is_ok());
        let mut i = 0;
        for (sentence, span) in [
            (vec!["This", "is", "a", "test", "."], Some("s1")),
            (vec!["<noise>"], None),
            (vec!["And", "one", "more"], Some("s2")),
        ] {
            let span_node = if let Some(span_value) = span {
                let span_name = format!("corpus#{span_value}");
                assert!(u
                    .add_event(UpdateEvent::AddNode {
                        node_name: span_name.to_string(),
                        node_type: "node".to_string(),
                    })
                    .is_ok());
                assert!(u
                    .add_event(UpdateEvent::AddEdge {
                        source_node: span_name.to_string(),
                        target_node: "corpus".to_string(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::PartOf.to_string(),
                        component_name: "".to_string()
                    })
                    .is_ok());
                assert!(u
                    .add_event(UpdateEvent::AddNodeLabel {
                        node_name: span_name.to_string(),
                        anno_ns: "".to_string(),
                        anno_name: "sentence".to_string(),
                        anno_value: span_value.to_string()
                    })
                    .is_ok());
                Some(span_name)
            } else {
                None
            };
            for token in sentence {
                i += 1;
                let token_name = format!("corpus#t{}", i + 1);
                assert!(u
                    .add_event(UpdateEvent::AddNode {
                        node_name: token_name.to_string(),
                        node_type: "node".to_string(),
                    })
                    .is_ok());
                assert!(u
                    .add_event(UpdateEvent::AddNodeLabel {
                        node_name: token_name.to_string(),
                        anno_ns: ANNIS_NS.to_string(),
                        anno_name: "tok".to_string(),
                        anno_value: token.to_string()
                    })
                    .is_ok());
                assert!(u
                    .add_event(UpdateEvent::AddEdge {
                        source_node: token_name.to_string(),
                        target_node: "corpus".to_string(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::PartOf.to_string(),
                        component_name: "".to_string()
                    })
                    .is_ok());
                if i > 0 {
                    assert!(u
                        .add_event(UpdateEvent::AddEdge {
                            source_node: format!("corpus#t{i}"),
                            target_node: token_name.to_string(),
                            layer: ANNIS_NS.to_string(),
                            component_type: AnnotationComponentType::Ordering.to_string(),
                            component_name: "".to_string()
                        })
                        .is_ok());
                }
                if let Some(span_name) = &span_node {
                    assert!(u
                        .add_event(UpdateEvent::AddEdge {
                            source_node: span_name.to_string(),
                            target_node: token_name.to_string(),
                            layer: ANNIS_NS.to_string(),
                            component_type: AnnotationComponentType::Coverage.to_string(),
                            component_name: "".to_string()
                        })
                        .is_ok());
                }
            }
        }
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(graph.apply_update(&mut u, |_| {}).is_ok());
        let actual = export_to_string(
            &graph,
            ExportCoNLLU {
                form: AnnoKey {
                    name: "tok".into(),
                    ns: ANNIS_NS.into(),
                },
                groupby: Some(AnnoKey {
                    ns: "".into(),
                    name: "sentence".into(),
                }),
                ..Default::default()
            },
        );
        assert!(actual.is_ok());
        assert_snapshot!(actual.unwrap());
    }
}
