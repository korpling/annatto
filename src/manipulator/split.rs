use std::{collections::BTreeMap, ops::Not};

use facet::Facet;
use graphannis::{
    graph::AnnoKey,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::{annostorage::ValueSearch, graph::NODE_NAME_KEY};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{error::Result, progress::ProgressReporter, util::update_graph};

use super::Manipulator;

/// This operation splits conflated annotation values into individual annotations.
#[derive(Facet, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SplitValues {
    /// This is the delimiter between the parts of the conflated annotation in the input graph
    #[serde(default = "default_delimiter")]
    delimiter: String,
    /// The delimiter is a regular expression.
    #[serde(default)]
    regex: bool,
    /// The annotation that holds the conflated values.
    #[serde(with = "crate::estarde::anno_key")]
    anno: AnnoKey,
    /// This maps a target annotation name to a list of potential values to be found in the split parts.
    #[serde(default)]
    layers: Vec<Layer>,
    /// If set to `true`, the original annotations will be deleted.
    #[serde(default)]
    delete: bool,
}

#[derive(Facet, Deserialize, Serialize, Clone, PartialEq)]
#[serde(untagged)]
#[repr(u8)]
enum Layer {
    ByIndex {
        index: usize,
        #[serde(with = "crate::estarde::anno_key")]
        key: AnnoKey,
    },
    ByValues {
        #[serde(with = "crate::estarde::anno_key")]
        key: AnnoKey,
        #[serde(default)]
        values: Vec<String>,
    },
}

const DEFAULT_DELIMITER: &str = "-";

fn default_delimiter() -> String {
    DEFAULT_DELIMITER.to_string()
}

impl Manipulator for SplitValues {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        _workflow_directory: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let node_annos = graph.get_node_annos();
        let (ns, name) = {
            (
                self.anno
                    .ns
                    .is_empty()
                    .not()
                    .then_some(self.anno.ns.as_str()),
                self.anno.name.as_str(),
            )
        };
        let value_map = self
            .layers
            .iter()
            .flat_map(|l| match l {
                Layer::ByIndex { .. } => vec![],
                Layer::ByValues { key, values } => {
                    values.iter().map(|vv| (vv.as_str(), key)).collect_vec()
                }
            })
            .collect::<BTreeMap<&str, &AnnoKey>>();
        let index_to_key: BTreeMap<usize, &AnnoKey> = self
            .layers
            .iter()
            .filter_map(|l| match l {
                Layer::ByIndex { index, key } => Some((*index, key)),
                Layer::ByValues { .. } => None,
            })
            .collect();
        let match_vec = node_annos
            .exact_anno_search(ns, name, ValueSearch::Any)
            .collect_vec();
        let progress = ProgressReporter::new(tx.clone(), step_id.clone(), match_vec.len())?;
        let anno_as_key = AnnoKey {
            name: name.into(),
            ns: ns.unwrap_or("").into(),
        };
        for m in match_vec {
            let n = m?.node;
            if let Some(node_name) = node_annos.get_value_for_item(&n, &NODE_NAME_KEY)? {
                if self.delete {
                    update.add_event(UpdateEvent::DeleteNodeLabel {
                        node_name: node_name.to_string(),
                        anno_ns: ns.unwrap_or("").to_string(),
                        anno_name: name.to_string(),
                    })?;
                }
                if let Some(v) = node_annos.get_value_for_item(&n, &anno_as_key)? {
                    self.map(&mut update, &node_name, &v, &value_map, &index_to_key)?;
                };
            }
            progress.worked(1)?;
        }
        update_graph(graph, &mut update, Some(step_id), tx)?;
        Ok(())
    }

    fn requires_statistics(&self) -> bool {
        false
    }
}

impl SplitValues {
    fn map(
        &self,
        update: &mut GraphUpdate,
        node_name: &str,
        value: &str,
        value_map: &BTreeMap<&str, &AnnoKey>,
        index_map: &BTreeMap<usize, &AnnoKey>,
    ) -> Result<()> {
        let splits = if self.regex {
            let p = regex::Regex::new(&self.delimiter)?;
            p.split(value).collect_vec()
        } else {
            value.split(&self.delimiter).collect_vec()
        };
        for (i, v) in splits.into_iter().enumerate() {
            if let Some(key) = index_map.get(&(i + 1)) {
                update.add_event(UpdateEvent::AddNodeLabel {
                    node_name: node_name.to_string(),
                    anno_ns: key.ns.to_string(),
                    anno_name: key.name.to_string(),
                    anno_value: v.to_string(),
                })?;
            } else {
                // use value map
                if let Some(anno_key) = value_map.get(v) {
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: node_name.to_string(),
                        anno_ns: anno_key.ns.to_string(),
                        anno_name: anno_key.name.to_string(),
                        anno_value: v.to_string(),
                    })?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fmt::Write, fs, path::Path};

    use graphannis::{AnnotationGraph, graph::AnnoKey, update::GraphUpdate};
    use insta::assert_snapshot;

    use crate::{
        StepID,
        exporter::graphml::GraphMLExporter,
        importer::{GenericImportConfiguration, Importer, treetagger::ImportTreeTagger},
        manipulator::{
            Manipulator,
            split::{Layer, SplitValues, default_delimiter},
        },
        test_util::export_to_string,
        util::{example_generator, update_graph_silent},
    };

    #[test]
    fn serialize_custom() {
        let module = SplitValues {
            anno: AnnoKey {
                name: "rftag".into(),
                ns: "ud".into(),
            },
            delimiter: ".".to_string(),
            regex: false,
            layers: vec![
                Layer::ByIndex {
                    index: 1,
                    key: AnnoKey {
                        name: "feature1".into(),
                        ns: "rf".into(),
                    },
                },
                Layer::ByValues {
                    key: AnnoKey {
                        name: "Tense".into(),
                        ns: "rf".into(),
                    },
                    values: vec![
                        "past".to_string(),
                        "present".to_string(),
                        "future".to_string(),
                    ],
                },
            ],
            delete: true,
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
    fn graph_statistics() {
        let g = AnnotationGraph::with_default_graphstorages(false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let mut u = GraphUpdate::default();
        example_generator::create_corpus_structure_simple(&mut u);
        assert!(update_graph_silent(&mut graph, &mut u).is_ok());
        let module = SplitValues {
            delimiter: default_delimiter(),
            regex: false,
            anno: AnnoKey {
                ns: "".into(),
                name: "".into(),
            },
            layers: vec![],
            delete: false,
        };
        assert!(
            module
                .validate_graph(
                    &mut graph,
                    StepID {
                        module_name: "test".to_string(),
                        path: None
                    },
                    None
                )
                .is_ok()
        );
        assert!(graph.global_statistics.is_none());
    }

    #[test]
    fn split_features() {
        let importer = ImportTreeTagger::default();
        let input_path = Path::new("tests/data/graph_op/split_values/");
        let u = importer.import_corpus(
            input_path,
            crate::StepID {
                module_name: "import_treetagger".to_string(),
                path: None,
            },
            GenericImportConfiguration::new_with_default_extensions(&importer),
            None,
        );
        assert!(u.is_ok());
        let mut update = u.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(graph.apply_update(&mut update, |_| {}).is_ok());
        let config_path = "tests/data/graph_op/split_values/config.toml";
        let tstr = fs::read_to_string(config_path);
        assert!(tstr.is_ok());
        let toml_str = tstr.unwrap();
        let op: Result<SplitValues, _> = toml::from_str(toml_str.as_str());
        assert!(
            op.is_ok(),
            "An error occured on deserialization: {:?}",
            op.err()
        );
        let split_op = op.unwrap();
        assert!(
            split_op
                .manipulate_corpus(
                    &mut graph,
                    Path::new("./"),
                    crate::StepID {
                        module_name: "split_conflated".to_string(),
                        path: None
                    },
                    None
                )
                .is_ok()
        );
        let gml = export_to_string(&graph, GraphMLExporter::default());
        assert!(gml.is_ok());
        let graphml = gml.unwrap();
        let start = graphml.find("<node ").unwrap_or(0);
        let end = graphml.rfind("</node>").unwrap_or(usize::MAX);
        assert_snapshot!(&graphml[start..end + 7]);
    }

    #[test]
    fn regex() {
        let split = SplitValues {
            anno: AnnoKey {
                name: "doesn't matter for this test".to_string(),
                ns: "".to_string(),
            },
            delimiter: String::from(r#"-|\(|\)"#),
            regex: true,
            layers: vec![],
            delete: false,
        };
        let mut u = GraphUpdate::default();
        let mut index_map = BTreeMap::default();
        let k1 = AnnoKey {
            ns: "".to_string(),
            name: "start".to_string(),
        };
        let k2 = AnnoKey {
            ns: "".to_string(),
            name: "end".to_string(),
        };
        let k3 = AnnoKey {
            ns: "".to_string(),
            name: "id".to_string(),
        };
        index_map.insert(1, &k1);
        index_map.insert(2, &k2);
        index_map.insert(3, &k3);
        let mut value_map = BTreeMap::default();
        let k4 = AnnoKey {
            ns: "".to_string(),
            name: "Number".to_string(),
        };
        value_map.insert("Pl", &k4);
        assert!(
            split
                .map(&mut u, "random_node", "0-3(7)Pl", &value_map, &index_map,)
                .is_ok()
        );
        let mut buf = String::new();
        u.iter()
            .unwrap()
            .flatten()
            .for_each(|(_, ue)| write!(&mut buf, "{ue:?}\n").unwrap());
        assert_snapshot!(buf);
    }
}
