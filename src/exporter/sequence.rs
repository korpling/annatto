use std::{collections::BTreeMap, fs, io::Write, path::Path, sync::Arc};

use facet::Facet;
use graphannis::{
    AnnotationGraph,
    graph::{AnnoKey, GraphStorage},
    model::{AnnotationComponent, AnnotationComponentType},
};
use graphannis_core::{annostorage::ValueSearch, dfs::CycleSafeDFS, graph::ANNIS_NS};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    StepID,
    error::{AnnattoError, Result},
    progress::ProgressReporter,
};

use super::Exporter;

/// This exports a node sequence as horizontal or vertical text.
#[derive(Facet, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExportSequence {
    /// This influences the way the output is shaped. The default value is '\n', that means each annotation value
    /// for the configured annotation key will be in a new line. Setting this to a single whitespace (' ') will lead
    /// to one line per group (see groupby configuration). Setting this to the empty string can be useful for corpora,
    /// in which each token corresponds to a character.
    #[serde(default = "default_delimiter")]
    delimiter: String,
    /// The annotation key that determines which nodes in the graph bunble a document in the part of component.
    #[serde(default = "default_fileby_key", with = "crate::estarde::anno_key")]
    fileby: AnnoKey,
    /// The optional annotation key, that groups the sequence elements.
    #[serde(default, with = "crate::estarde::anno_key::as_option")]
    groupby: Option<AnnoKey>,
    /// the group component type can be optionally provided to define which edges to follow
    /// to find the nodes holding the groupby anno key. The default value is `Coverage`.
    #[serde(default = "default_groupby_ctype")]
    group_component_type: Option<AnnotationComponentType>,
    /// This configures the edge component that contains the sequences that you wish to export.
    /// The default value ctype is `Ordering`, the default layer is `annis`, and the default
    /// name is empty.
    /// Example:
    /// ```toml
    /// [export.config]
    /// component = { ctype = "Pointing", layer = "", name = "coreference" }
    /// ```
    #[serde(
        default = "default_component",
        with = "crate::estarde::annotation_component"
    )]
    component: AnnotationComponent,
    /// The annotation key that determines the values in the exported sequence (annis::tok by default).
    #[serde(default = "default_anno", with = "crate::estarde::anno_key")]
    anno: AnnoKey,
}

fn default_anno() -> AnnoKey {
    AnnoKey {
        name: "tok".into(),
        ns: ANNIS_NS.into(),
    }
}

fn default_component() -> AnnotationComponent {
    AnnotationComponent::new(default_ctype(), default_clayer(), "".to_string())
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

fn default_delimiter() -> String {
    "\n".to_string()
}

impl Default for ExportSequence {
    fn default() -> Self {
        Self {
            delimiter: default_delimiter(),
            fileby: default_fileby_key(),
            groupby: Default::default(),
            group_component_type: default_groupby_ctype(),
            component: default_component(),
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
        let docs_and_starts = self.start_nodes_by_file_node(graph, &step_id)?;
        let groups = if let Some(k) = &self.groupby {
            self.group_nodes(graph, k)?
        } else {
            BTreeMap::default()
        };
        let progress = ProgressReporter::new(tx, step_id.clone(), docs_and_starts.len())?;
        if let Some(storage) = graph.get_graphstorage(&self.component) {
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
        let delimiter_bytes = self.delimiter.as_bytes();
        let new_group_bytes = if self.delimiter == "\n" {
            "\n\n".as_bytes()
        } else {
            "\n".as_bytes()
        };
        let node_annos = graph.get_node_annos();
        let dfs = CycleSafeDFS::new(storage.as_edgecontainer(), sequence_start, 0, usize::MAX);
        let mut last_group = groups.get(&sequence_start);
        let doc_name = if let Some(v) = node_annos.get_value_for_item(&file_node, &self.fileby)? {
            v
        } else {
            return Err(AnnattoError::Export {
                reason: "Could not determine file name.".to_string(),
                exporter: "text sequence".to_string(),
                path: target_dir.to_path_buf(),
            });
        };
        let out_path = target_dir.join(format!("{doc_name}.{}", self.file_extension()));
        let mut out_file = fs::File::create(out_path)?;
        let mut write_delimiter = false;
        for step in dfs {
            let node = step?.node;
            if let Some(g) = last_group
                && let Some(ng) = groups.get(&node)
            {
                if g != ng {
                    out_file.write_all(new_group_bytes)?;
                    write_delimiter = false;
                }
                last_group = Some(ng);
            }
            if let Some(v) = node_annos.get_value_for_item(&node, &self.anno)? {
                if write_delimiter {
                    out_file.write_all(delimiter_bytes)?;
                }
                out_file.write_all(v.as_bytes())?;
                write_delimiter = true;
            }
        }
        out_file.write_all(new_group_bytes)?;
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
        let component_storage =
            graph
                .get_graphstorage(&self.component)
                .ok_or(AnnattoError::Export {
                    reason: format!("Source component undefined: {}", &self.component),
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
                    p.unwrap_or_default()
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
        let mut downward_groups = BTreeMap::default();
        let node_annos = graph.get_node_annos();
        // downward pass
        for m in node_annos.exact_anno_search(ns, name, ValueSearch::Any) {
            let node = m?.node;
            for component in graph.get_all_components(self.group_component_type.clone(), None) {
                if let Some(storage) = graph.get_graphstorage(&component) {
                    let dfs = CycleSafeDFS::new(storage.as_edgecontainer(), node, 1, usize::MAX);
                    for step in dfs {
                        let n = step?.node;
                        if node_annos.has_value_for_item(&n, &self.anno)?
                            || !storage.has_outgoing_edges(n)?
                        // we did not pass a relevant node, but reached terminal nodes (real tokens, which requires an upward pass)
                        {
                            downward_groups.insert(n, node);
                        }
                    }
                }
            }
        }
        // upward pass, this is necessary if the segments (text nodes, whatever you want to call it) are not terminal coverage nodes
        // we could condition the upward pass on the question whether or not we needed to insert terminals (see above), but that would
        // rely on the fact that a model either has empty + virtual tokens XOR valued tokens, but is never mixed, which we cannot know
        // for the future. If perfomance is ever critical, consider adding a flag for such behaviour.
        let mut groups = BTreeMap::default();
        for component in graph.get_all_components(Some(AnnotationComponentType::Coverage), None) {
            if let Some(storage) = graph.get_graphstorage(&component) {
                for (member, group) in &downward_groups {
                    for rn in CycleSafeDFS::new_inverse(storage.as_edgecontainer(), *member, 0, 1)
                    // start from 0 to insert self as well
                    {
                        let reachable_node = rn?.node;
                        groups.insert(reachable_node, *group);
                    }
                }
            }
        }
        Ok(groups)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs,
        path::Path,
    };

    use graphannis::{
        AnnotationGraph,
        graph::AnnoKey,
        model::{AnnotationComponent, AnnotationComponentType},
    };
    use graphannis_core::graph::{ANNIS_NS, DEFAULT_NS};
    use insta::assert_snapshot;

    use crate::{
        exporter::sequence::{default_anno, default_delimiter, default_fileby_key},
        importer::{Importer, xlsx::ImportSpreadsheet},
        test_util::export_to_string,
    };

    use super::ExportSequence;

    #[test]
    fn serialize() {
        let module = ExportSequence::default();
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
        let module = ExportSequence {
            anno: AnnoKey {
                name: "norm".into(),
                ns: "norm".into(),
            },
            component: AnnotationComponent::new(
                AnnotationComponentType::Coverage,
                ANNIS_NS.into(),
                "".into(),
            ),
            delimiter: " ".to_string(),
            fileby: AnnoKey {
                name: "file_data".into(),
                ns: "org".into(),
            },
            groupby: Some(AnnoKey {
                ns: "dipl".into(),
                name: "sentence".into(),
            }),
            group_component_type: Some(AnnotationComponentType::Coverage),
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
    fn vertical() {
        let file = fs::File::open("tests/data/export/sequence/test_graph.graphml");
        assert!(file.is_ok());
        let g =
            graphannis_core::graph::serialization::graphml::import(file.unwrap(), false, |_| {});
        assert!(g.is_ok());
        let graph: AnnotationGraph = g.unwrap().0;
        let exporter = ExportSequence {
            anno: default_anno(),
            delimiter: default_delimiter(),
            fileby: default_fileby_key(),
            groupby: Some(AnnoKey {
                ns: "".into(),
                name: "sent_id".into(),
            }),
            group_component_type: Some(AnnotationComponentType::Coverage),
            ..Default::default()
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
            delimiter: " ".to_string(),
            fileby: default_fileby_key(),
            groupby: Some(AnnoKey {
                ns: "".into(),
                name: "sent_id".into(),
            }),
            group_component_type: Some(AnnotationComponentType::Coverage),
            ..Default::default()
        };
        let estr = export_to_string(&graph, exporter);
        assert!(estr.is_ok());
        assert_snapshot!(estr.unwrap());
    }

    #[test]
    fn with_virtual_tokens() {
        let mut column_map = BTreeMap::default();
        let mut columns = BTreeSet::default();
        columns.insert("seg".to_string());
        column_map.insert("norm".to_string(), columns);
        let import_spec = "column_map = {\"norm\" = [\"seg\"]}";
        let mprt: Result<ImportSpreadsheet, _> = toml::from_str(import_spec);
        assert!(mprt.is_ok(), "Import error: {:?}", mprt.err());
        let import = mprt.unwrap();
        let u = import.import_corpus(
            Path::new("tests/data/import/xlsx/clean/xlsx"),
            crate::StepID {
                module_name: "test_import_xlsx".to_string(),
                path: None,
            },
            None,
        );
        assert!(u.is_ok());
        let mut update = u.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let au = graph.apply_update(&mut update, |_| {});
        assert!(au.is_ok(), "Could not apply update: {:?}", au.err());
        assert!(graph.ensure_loaded_all().is_ok());
        let exporter = ExportSequence {
            anno: AnnoKey {
                name: "norm".into(),
                ns: "norm".into(),
            },
            delimiter: " ".to_string(),
            fileby: default_fileby_key(),
            groupby: Some(AnnoKey {
                ns: "norm".into(),
                name: "seg".into(),
            }),
            group_component_type: Some(AnnotationComponentType::Coverage),
            component: AnnotationComponent::new(
                AnnotationComponentType::Ordering,
                DEFAULT_NS.into(),
                "norm".into(),
            ),
        };
        let estr = export_to_string(&graph, exporter);
        assert!(estr.is_ok(), "Export failed: {:?}", estr.err());
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
