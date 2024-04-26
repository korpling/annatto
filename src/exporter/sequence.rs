use std::{collections::BTreeMap, fs, io::Write, path::Path, sync::Arc};

use documented::{Documented, DocumentedFields};
use graphannis::{
    graph::{AnnoKey, GraphStorage},
    model::{AnnotationComponent, AnnotationComponentType},
    AnnotationGraph,
};
use graphannis_core::{annostorage::ValueSearch, dfs::CycleSafeDFS, graph::ANNIS_NS};
use itertools::Itertools;
use serde::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use crate::{
    error::{AnnattoError, Result},
    progress::ProgressReporter,
    StepID,
};

use super::Exporter;

/// This exports a node sequence as horizontal or vertical text.
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(deny_unknown_fields, default)]
pub struct ExportSequence {
    /// Choose horizontal mode if you want one group (e. g. sentence) per line,
    /// choose false if you prefer one element per line.
    /// In the latter case groups will be seperated by empty lines.
    #[serde(default)]
    horizontal: bool,
    /// The annotation key that determines which nodes in the graph bunble a document in the part of component.
    #[serde(default = "default_fileby_key")]
    fileby: AnnoKey,
    /// The optional annotation key, that groups the sequence elements.
    #[serde(default)]
    groupby: Option<AnnoKey>,
    /// the group component type can be optionally provided to define which edges to follow
    /// to find the nodes holding the groupby anno key. The default value is `Coverage`.
    #[serde(default = "default_groupby_ctype")]
    group_component_type: Option<AnnotationComponentType>,
    /// The type of the edge component that contains the sequences that you wish to export.
    /// The default value is `ordering`.
    #[serde(default = "default_ctype")]
    component_type: AnnotationComponentType,
    /// The layer of the edge component that contains the sequences that you wish to export.
    /// The default value is `annis`.
    #[serde(default = "default_clayer")]
    component_layer: String,
    /// The name of the edge component that contains the sequences that you wish to export.
    /// The default value is the empty string.
    #[serde(default)]
    component_name: String,
    /// The annotation key that determines the values in the exported sequence (annis::tok by default).
    #[serde(default = "default_anno")]
    anno: AnnoKey,
}

fn default_anno() -> AnnoKey {
    AnnoKey {
        name: "tok".into(),
        ns: ANNIS_NS.into(),
    }
}

const fn default_ctype() -> AnnotationComponentType {
    AnnotationComponentType::Ordering
}

fn default_clayer() -> String {
    ANNIS_NS.to_string()
}

fn default_fileby_key() -> AnnoKey {
    AnnoKey {
        name: "doc".into(),
        ns: ANNIS_NS.into(),
    }
}

const fn default_groupby_ctype() -> Option<AnnotationComponentType> {
    Some(AnnotationComponentType::Coverage)
}

impl Default for ExportSequence {
    fn default() -> Self {
        Self {
            horizontal: Default::default(),
            fileby: default_fileby_key(),
            groupby: Default::default(),
            group_component_type: default_groupby_ctype(),
            component_type: default_ctype(),
            component_layer: default_clayer(),
            component_name: "".to_string(),
            anno: default_anno(),
        }
    }
}

const FILE_EXTENSION: &str = "txt";

impl Exporter for ExportSequence {
    fn export_corpus(
        &self,
        graph: &graphannis::AnnotationGraph,
        output_path: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let component = AnnotationComponent::new(
            self.component_type.clone(),
            self.component_layer.as_str().into(),
            self.component_name.as_str().into(),
        );
        let docs_and_starts = self.start_nodes_by_file_node(graph, &step_id)?;
        let groups = if let Some(k) = &self.groupby {
            self.group_nodes(graph, k)?
        } else {
            BTreeMap::default()
        };
        let progress = ProgressReporter::new(tx, step_id.clone(), docs_and_starts.len())?;
        if let Some(storage) = graph.get_graphstorage(&component) {
            for (doc_node, start_node) in docs_and_starts {
                self.export_document(
                    graph,
                    storage.clone(),
                    doc_node,
                    start_node,
                    &groups,
                    output_path,
                )?;
                progress.worked(1)?;
            }
        }
        Ok(())
    }

    fn file_extension(&self) -> &str {
        FILE_EXTENSION
    }
}

impl ExportSequence {
    fn export_document(
        &self,
        graph: &AnnotationGraph,
        storage: Arc<dyn GraphStorage>,
        file_node: u64,
        sequence_start: u64,
        groups: &BTreeMap<u64, u64>,
        target_dir: &Path,
    ) -> Result<()> {
        let node_annos = graph.get_node_annos();
        let mut values = Vec::new();
        let mut blocks = Vec::new();
        let dfs = CycleSafeDFS::new(storage.as_edgecontainer(), sequence_start, 0, usize::MAX);
        let mut last_group = groups.get(&sequence_start);
        for step in dfs {
            let node = step?.node;
            if let Some(g) = last_group {
                if let Some(ng) = groups.get(&node) {
                    last_group = Some(ng);
                    if g != ng {
                        if self.horizontal {
                            let joint_value = values.join(" ");
                            values.clear();
                            blocks.push(joint_value);
                        } else {
                            values.push("".to_string());
                        }
                    }
                }
            }
            if let Some(v) = node_annos.get_value_for_item(&node, &self.anno)? {
                values.push(v.to_string());
            }
        }
        if !values.is_empty() {
            if self.horizontal {
                let joint_value = blocks.join(" ");
                blocks.push(joint_value);
            } else {
                blocks.extend(values);
            }
        }
        let doc_name = node_annos
            .get_value_for_item(&file_node, &self.fileby)?
            .unwrap(); // at this point we know there is a value
        let out_path = target_dir.join(format!("{doc_name}.{}", self.file_extension()));
        if out_path.exists() {
            progress.warn(format!("File exists: {}", out_path.to_string_lossy()).as_str())?;
        }
        let mut out_file = fs::File::create(out_path)?;
        for value in blocks {
            out_file.write_all(value.as_bytes())?;
            out_file.write_all("\n".as_bytes())?;
        }
        out_file.write_all("\n".as_bytes())?;
        out_file.flush()?;
        Ok(())
    }

    /// This function traverses the desired component for its start nodes and climbs up the
    /// part of component to find annotations that identify a subgraph as relevant for a file.
    /// It returns tuples of file level nodes and sequence component start nodes.
    fn start_nodes_by_file_node(
        &self,
        graph: &AnnotationGraph,
        step_id: &StepID,
    ) -> Result<Vec<(u64, u64)>> {
        let component = AnnotationComponent::new(
            self.component_type.clone(),
            self.component_layer.as_str().into(),
            self.component_name.as_str().into(),
        );
        let component_storage = graph
            .get_graphstorage(&component)
            .ok_or(AnnattoError::Export {
                reason: format!("Source component undefined: {}", &component),
                exporter: step_id.module_name.to_string(),
                path: Path::new("./").to_path_buf(),
            })?;
        let part_of_storage = graph
            .get_graphstorage(&AnnotationComponent::new(
                AnnotationComponentType::PartOf,
                ANNIS_NS.into(),
                "".into(),
            ))
            .ok_or(AnnattoError::Export {
                reason: "Graph has no PartOf component".to_string(),
                exporter: step_id.module_name.to_string(),
                path: Path::new("./").to_path_buf(),
            })?;
        let start_nodes = component_storage
            .source_nodes()
            .filter_map_ok(|n| {
                if component_storage.get_ingoing_edges(n).count() == 0 {
                    Some(n)
                } else {
                    None
                }
            })
            .collect_vec();
        let node_annos = graph.get_node_annos();
        let mut node_pairs = Vec::with_capacity(start_nodes.len());
        for sn in start_nodes {
            let start_node = sn?;
            let mut dfs = CycleSafeDFS::new(
                part_of_storage.as_edgecontainer(),
                start_node,
                0,
                usize::MAX,
            );
            let doc_node = dfs.find(|r| {
                if let Ok(step) = r {
                    let p = node_annos.has_value_for_item(&step.node, &self.fileby);
                    p.is_ok() && p.unwrap()
                } else {
                    false
                }
            });
            if let Some(r) = doc_node {
                node_pairs.push((r?.node, start_node));
            }
        }
        Ok(node_pairs)
    }

    /// This function computes to which group node a sequence node belongs.
    /// It returns a map from a sequence node id to a group node id.
    fn group_nodes(&self, graph: &AnnotationGraph, key: &AnnoKey) -> Result<BTreeMap<u64, u64>> {
        let ns = match key.ns.as_str() {
            "" => None,
            _ => Some(key.ns.as_str()),
        };
        let name = key.name.as_str();
        let mut groups = BTreeMap::default();
        let node_annos = graph.get_node_annos();
        for m in node_annos.exact_anno_search(ns, name, ValueSearch::Any) {
            let node = m?.node;
            for component in graph.get_all_components(self.group_component_type.clone(), None) {
                if let Some(storage) = graph.get_graphstorage(&component) {
                    let dfs = CycleSafeDFS::new(storage.as_edgecontainer(), node, 1, usize::MAX);
                    for step in dfs {
                        let n = step?.node;
                        if node_annos.has_value_for_item(&n, &self.anno)? {
                            groups.insert(n, node);
                        }
                    }
                }
            }
        }
        Ok(groups)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use graphannis::{graph::AnnoKey, model::AnnotationComponentType, AnnotationGraph};
    use graphannis_core::graph::ANNIS_NS;
    use insta::assert_snapshot;

    use crate::{
        exporter::sequence::{default_anno, default_fileby_key},
        test_util::export_to_string,
    };

    use super::ExportSequence;

    #[test]
    fn vertical() {
        let file = fs::File::open("tests/data/export/sequence/test_graph.graphml");
        assert!(file.is_ok());
        let g =
            graphannis_core::graph::serialization::graphml::import(file.unwrap(), false, |_| {});
        assert!(g.is_ok());
        let graph: AnnotationGraph = g.unwrap().0;
        let exporter = ExportSequence {
            anno: default_anno(),
            horizontal: false,
            fileby: default_fileby_key(),
            groupby: Some(AnnoKey {
                ns: "".into(),
                name: "sent_id".into(),
            }),
            group_component_type: Some(AnnotationComponentType::Coverage),
            component_type: AnnotationComponentType::Ordering,
            component_layer: ANNIS_NS.to_string(),
            component_name: "".to_string(),
        };
        let estr = export_to_string(&graph, exporter);
        assert!(estr.is_ok());
        assert_snapshot!(estr.unwrap());
    }

    #[test]
    fn horizontal() {
        let file = fs::File::open("tests/data/export/sequence/test_graph.graphml");
        assert!(file.is_ok());
        let g =
            graphannis_core::graph::serialization::graphml::import(file.unwrap(), false, |_| {});
        assert!(g.is_ok());
        let graph: AnnotationGraph = g.unwrap().0;
        let exporter = ExportSequence {
            anno: default_anno(),
            horizontal: true,
            fileby: default_fileby_key(),
            groupby: Some(AnnoKey {
                ns: "".into(),
                name: "sent_id".into(),
            }),
            group_component_type: Some(AnnotationComponentType::Coverage),
            component_type: AnnotationComponentType::Ordering,
            component_layer: ANNIS_NS.to_string(),
            component_name: "".to_string(),
        };
        let estr = export_to_string(&graph, exporter);
        assert!(estr.is_ok());
        assert_snapshot!(estr.unwrap());
    }

    #[test]
    fn deserialize_self_from_complete_config() {
        let toml_path = "tests/data/export/sequence/complete_config.toml";
        let toml_str = fs::read_to_string(toml_path);
        assert!(toml_str.is_ok());
        let sq: Result<ExportSequence, _> = toml::from_str(toml_str.unwrap().as_str());
        assert!(sq.is_ok(), "Could not deserialize, error: {:?}", sq.err());
    }

    #[test]
    fn deserialize_self_from_partial_config() {
        let toml_path = "tests/data/export/sequence/partial_config.toml";
        let toml_str = fs::read_to_string(toml_path);
        assert!(toml_str.is_ok());
        let sq: Result<ExportSequence, _> = toml::from_str(toml_str.unwrap().as_str());
        assert!(sq.is_ok(), "Could not deserialize, error: {:?}", sq.err());
    }
}
