use std::collections::BTreeMap;

use documented::{Documented, DocumentedFields};
use graphannis::{
    graph::AnnoKey,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::{annostorage::ValueSearch, graph::NODE_NAME_KEY, util::split_qname};
use itertools::Itertools;
use serde::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use crate::{error::Result, progress::ProgressReporter};

use super::Manipulator;

/// This operation splits conflated annotation values into individual annotations.
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(deny_unknown_fields)]
pub struct SplitValues {
    /// This is the delimiter between the parts of the conflated annotation in the input graph
    delimiter: String,
    /// The annotation that holds the conflated values. Can be qualified with a namespace using `::` as delimiter.
    anno: String,
    /// This maps a target annotation name to a list of potential values to be found in the split parts.
    layer_map: BTreeMap<String, Vec<String>>,
    /// This maps annotation names that occur in a fixed position in the conflation sequence. This is easier especially for large numbers of annotation values.
    index_map: BTreeMap<String, usize>,
    /// Whether or not to keep the original annotation.
    keep: bool,
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
        let (ns, name) = split_qname(&self.anno);
        let value_map = self
            .layer_map
            .iter()
            .flat_map(|(k, v)| v.iter().map(|vv| (vv.as_str(), k.as_str())).collect_vec())
            .collect::<BTreeMap<&str, &str>>();
        let index_to_key: BTreeMap<usize, AnnoKey> = self
            .index_map
            .iter()
            .map(|(k, i)| {
                let (ns, name) = split_qname(k);
                (
                    *i,
                    AnnoKey {
                        ns: ns.unwrap_or("").into(),
                        name: name.into(),
                    },
                )
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
                if !self.keep {
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
        let application_progress = ProgressReporter::new_unknown_total_work(tx, step_id)?;
        graph.apply_update(&mut update, move |_| {
            let _ = application_progress.worked(1);
        })?;
        Ok(())
    }
}

impl SplitValues {
    fn map(
        &self,
        update: &mut GraphUpdate,
        node_name: &str,
        value: &str,
        value_map: &BTreeMap<&str, &str>,
        index_map: &BTreeMap<usize, AnnoKey>,
    ) -> Result<()> {
        for (i, v) in value.to_string().split(&self.delimiter).enumerate() {
            if let Some(key) = index_map.get(&(i + 1)) {
                update.add_event(UpdateEvent::AddNodeLabel {
                    node_name: node_name.to_string(),
                    anno_ns: key.ns.to_string(),
                    anno_name: key.name.to_string(),
                    anno_value: v.to_string(),
                })?;
            } else {
                // use value map
                if let Some(anno_key_str) = value_map.get(v) {
                    let (ns, name) = split_qname(anno_key_str);
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: node_name.to_string(),
                        anno_ns: ns.unwrap_or("").to_string(),
                        anno_name: name.to_string(),
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
        let g = AnnotationGraph::new(true);
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
        assert_snapshot!(graphml);
    }
}
