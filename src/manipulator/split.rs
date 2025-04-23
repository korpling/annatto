use std::{collections::BTreeMap, ops::Not};

use documented::{Documented, DocumentedFields};
use graphannis::{
    graph::AnnoKey,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::{annostorage::ValueSearch, graph::NODE_NAME_KEY};
use itertools::Itertools;
use serde::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use crate::{
    core::update_graph, deserialize::deserialize_anno_key, error::Result,
    progress::ProgressReporter,
};

use super::Manipulator;

/// This operation splits conflated annotation values into individual annotations.
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(deny_unknown_fields)]
pub struct SplitValues {
    /// This is the delimiter between the parts of the conflated annotation in the input graph
    #[serde(default = "default_delimiter")]
    delimiter: String,
    /// The annotation that holds the conflated values.
    #[serde(deserialize_with = "deserialize_anno_key")]
    anno: AnnoKey,
    /// This maps a target annotation name to a list of potential values to be found in the split parts.
    #[serde(default)]
    layers: Vec<Layer>,
    /// If set to `true`, the original annotations will be deleted.
    #[serde(default)]
    delete: bool,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Layer {
    ByIndex {
        index: usize,
        #[serde(deserialize_with = "deserialize_anno_key")]
        key: AnnoKey,
    },
    ByValues {
        #[serde(deserialize_with = "deserialize_anno_key")]
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
                self.anno.ns.is_empty().not().then(|| self.anno.ns.as_str()),
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
        for (i, v) in value.split(&self.delimiter).enumerate() {
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
    use std::{fs, path::Path};

    use graphannis::AnnotationGraph;
    use insta::assert_snapshot;

    use crate::{
        exporter::graphml::GraphMLExporter,
        importer::{treetagger::ImportTreeTagger, Importer},
        manipulator::{split::SplitValues, Manipulator},
        test_util::export_to_string,
    };

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
        assert!(split_op
            .manipulate_corpus(
                &mut graph,
                Path::new("./"),
                crate::StepID {
                    module_name: "split_conflated".to_string(),
                    path: None
                },
                None
            )
            .is_ok());
        let gml = export_to_string(&graph, GraphMLExporter::default());
        assert!(gml.is_ok());
        let graphml = gml.unwrap();
        let start = graphml.find("<node ").unwrap_or(0);
        let end = graphml.rfind("</node>").unwrap_or(usize::MAX);
        assert_snapshot!(&graphml[start..end + 7]);
    }
}
