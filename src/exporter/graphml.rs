use std::{
    borrow::Cow,
    collections::BTreeMap,
    fs::{File, create_dir_all},
    io::BufReader,
    path::{Path, PathBuf},
};

use crate::{
    StepID, error::AnnattoError, exporter::Exporter, progress::ProgressReporter,
    workflow::StatusSender,
};
use facet::Facet;
use graphannis::{
    AnnotationGraph,
    graph::{Edge, NodeID},
    model::AnnotationComponent,
};
use graphannis::{graph::AnnoKey, model::AnnotationComponentType};
use graphannis_core::{
    annostorage::{NodeAnnotationStorage, ValueSearch},
    dfs::CycleSafeDFS,
    graph::{
        ANNIS_NS, NODE_NAME_KEY, NODE_TYPE, NODE_TYPE_KEY, storage::union::UnionEdgeContainer,
    },
    util::disk_collections::{DiskMap, EvictionStrategy},
};
use itertools::Itertools;
use roxmltree::NodeId;
use serde_derive::{Deserialize, Serialize};
use zip::ZipWriter;

/// Exports files as [GraphML](http://graphml.graphdrawing.org/) files which
/// conform to the [graphANNIS data model](https://korpling.github.io/graphANNIS/docs/v2/data-model.html).
#[derive(Facet, Default, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GraphMLExporter {
    /// If set, add this ANNIS visualization configuration string to the corpus
    /// configuration. See
    /// <http://korpling.github.io/ANNIS/4.11/user-guide/import-and-config/visualizations.html>
    /// for a description of the possible visualization options of ANNIS.
    #[serde(default)]
    add_vis: Option<String>,
    /// Automatically generate visualization options for ANNIS based on the
    /// structure of the annotations, e.g. `Dominance` edges are indicators that
    /// a syntactic tree should be visualized.
    #[serde(default)]
    guess_vis: bool,
    /// Always generate the same order of nodes and edges in the output file.
    /// This is e.g. useful when comparing files in a versioning environment
    /// like git.
    /// **Attention: this is slower to generate.**
    #[serde(default)]
    stable_order: bool,
    /// Output a ZIP file that includes the GraphML file. Linked files (like
    /// e.g. audio files) are included if they have been referenced by a
    /// *relative* path. Since GraphML is easily compressed this can help with
    /// storage size. It also improves the IMPORT in the ANNIS frontend, which
    /// only accepts ZIP files.
    #[serde(default)]
    zip: bool,
    /// This path is used to help the exporter to resolve the path to physical copies of the linked files.
    /// As these are attempted to be resolved from the annatto runtime path, which can fail when the files
    /// are stored in subdirectory of depth higher than one or in an ancestral path. This attribute is only
    /// relevant, when the workflow contains a previous import step for linking files in the graph.
    ///
    /// Example:
    /// ```toml
    /// ...
    ///
    /// [[import]]
    /// format = "path"
    /// path = "configuration/visualizations/"
    ///
    /// ...
    ///
    /// [[export]]
    /// format = "graphml"
    /// path = "export/to/this/directory"
    ///
    /// [export.config]
    /// zip = true
    /// zip_copy_from = "configuration/"
    ///
    /// ```
    #[serde(default)]
    zip_copy_from: Option<PathBuf>, // we use an option here as a default path with value "" is irritating (serialization)
    #[serde(default)]
    partition_by_node_label: Option<AnnoKey>,
}

const DEFAULT_VIS_STR: &str = "# configure visualizations here";

#[derive(Serialize)]
struct Visualizer {
    element: String,
    layer: Option<String>,
    vis_type: String,
    display_name: String,
    visibility: String,
    mappings: Option<BTreeMap<String, String>>,
}

#[derive(Serialize)]
struct Visualization {
    visualizers: Vec<Visualizer>,
}

impl GraphMLExporter {
    /// Find all nodes of the type "file" and return an iterator
    /// over a tuple of the node name and path of the linked file as it is given in the annotation.
    fn get_linked_files<'a>(
        &'a self,
        graph: &'a AnnotationGraph,
    ) -> anyhow::Result<impl Iterator<Item = anyhow::Result<PathBuf>> + 'a> {
        let linked_file_key = AnnoKey {
            ns: ANNIS_NS.into(),
            name: "file".into(),
        };
        // Find all nodes of the type "file"
        let node_annos: &dyn NodeAnnotationStorage = graph.get_node_annos();
        let it = node_annos
            .exact_anno_search(Some(ANNIS_NS), NODE_TYPE, ValueSearch::Some("file"))
            // Get the linked file for this node
            .map(move |m| match m {
                Ok(m) => node_annos
                    .get_value_for_item(&m.node, &NODE_NAME_KEY)
                    .map(|node_name| (m, node_name)),
                Err(e) => Err(e),
            })
            .map(move |result| match result {
                Ok((m, _node_name)) => node_annos.get_value_for_item(&m.node, &linked_file_key),
                Err(e) => Err(e),
            })
            .filter_map_ok(move |file_path_value| {
                if let Some(file_path_value) = file_path_value {
                    return Some(PathBuf::from(file_path_value.as_ref()));
                }
                None
            })
            .map(|item| item.map_err(anyhow::Error::from));
        Ok(it)
    }

    fn write_graphml_file(
        &self,
        graph: &AnnotationGraph,
        output_file_path: &Path,
        zip_file: Option<&mut ZipWriter<File>>,
        vis_str: &str,
        reporter: &ProgressReporter,
    ) -> anyhow::Result<()> {
        let mut writer: Box<dyn std::io::Write> = if let Some(zip_file) = zip_file {
            Box::new(zip_file)
        } else {
            // Directly write to the output file
            let output_file = File::create(output_file_path)?;
            Box::new(output_file)
        };

        if self.stable_order {
            graphannis_core::graph::serialization::graphml::export_stable_order(
                graph,
                Some(vis_str),
                &mut writer,
                |msg| {
                    reporter.info(msg).expect("Could not send status message");
                },
            )?;
        } else {
            graphannis_core::graph::serialization::graphml::export(
                graph,
                Some(vis_str),
                &mut writer,
                |msg| {
                    reporter.info(msg).expect("Could not send status message");
                },
            )?;
        }
        Ok(())
    }
}

fn get_corpus_root(
    graph: &AnnotationGraph,
    output_path: &Path,
    step_id: StepID,
) -> Result<NodeID, Box<dyn std::error::Error>> {
    // Get the toplevel corpus name from the corpus structure
    let part_of_c = graph
        .get_all_components(Some(AnnotationComponentType::PartOf), None)
        .first()
        .cloned()
        .ok_or_else(|| AnnattoError::Export {
            reason: "Could not determine file name for graphML.".into(),
            exporter: step_id.module_name.clone(),
            path: output_path.to_path_buf(),
        })?;

    let corpus_nodes = graph.get_node_annos().exact_anno_search(
        Some(NODE_TYPE_KEY.ns.as_str()),
        NODE_TYPE_KEY.name.as_str(),
        ValueSearch::Some("corpus"),
    );
    let corpus_root_opt = if let Some(part_of_storage) = graph.get_graphstorage(&part_of_c) {
        corpus_nodes.into_iter().find(|n| {
            if let Ok(mtch) = n {
                !part_of_storage
                    .has_outgoing_edges(mtch.node)
                    .unwrap_or(true) // use true to not output unprobed node
            } else {
                false
            }
        })
    } else {
        None
    };
    let corpus_root = if let Some(corpus_root_r) = corpus_root_opt {
        corpus_root_r?.node
    } else {
        return Err(Box::new(AnnattoError::Export {
            reason: "No corpus root could be determined.".to_string(),
            exporter: step_id.module_name.to_string(),
            path: output_path.to_path_buf(),
        }));
    };
    Ok(corpus_root)
}

impl Exporter for GraphMLExporter {
    fn export_corpus(
        &self,
        graph: &AnnotationGraph,
        output_path: &Path,
        step_id: StepID,
        tx: Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let reporter = ProgressReporter::new_unknown_total_work(tx, step_id.clone())?;

        let corpus_root = get_corpus_root(graph, output_path, step_id)?;
        let toplevel_corpus_name = graph
            .get_node_annos()
            .get_value_for_item(&corpus_root, &NODE_NAME_KEY)?
            .unwrap_or(Cow::Borrowed("corpus"));

        if !output_path.exists() {
            create_dir_all(output_path)?;
        }

        // Use the corpus name to determine the file name
        let extension = self.file_extension();

        let zip_options =
            zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Deflated);
        let mut zip_file = if self.zip {
            // Create a ZIP file at the given location
            let file_name = format!("{toplevel_corpus_name}.{extension}");

            let output_file_path = output_path.join(file_name);

            let output_file = File::create(output_file_path.clone())?;
            let mut zip = zip::ZipWriter::new(output_file);

            // Create an entry in the ZIP file and write the GraphML to this file entry
            zip.start_file(format!("{toplevel_corpus_name}.graphml"), zip_options)?;
            Some(zip)
        } else {
            None
        };

        if let Some(partition_by) = &self.partition_by_node_label {
            let mut remaining_graph = AnnotationGraph::new(true)?;
            // Create new annotation graphs for each node that is the root of the partition
            let mut partitions: BTreeMap<NodeID, AnnotationGraph> = BTreeMap::new();

            for n in graph.get_node_annos().exact_anno_search(
                Some(&partition_by.ns),
                &partition_by.name,
                ValueSearch::Any,
            ) {
                let n = n?.node;
                partitions.insert(n, AnnotationGraph::new(true)?);
            }
            // TODO: merge partitions with a possible parent partition

            let mut copied_nodes: DiskMap<NodeID, NodeID> = DiskMap::default();

            let all_components = graph.get_all_components(None, None);
            let part_of_storages = graph
                .get_all_components(Some(AnnotationComponentType::PartOf), None)
                .iter()
                .filter_map(|c| graph.get_graphstorage(c))
                .collect_vec();
            let part_of_container = UnionEdgeContainer::new(
                part_of_storages
                    .iter()
                    .map(|gs| gs.as_edgecontainer())
                    .collect(),
            );

            for (partition_root, partition_graph) in &mut partitions {
                let dfs =
                    CycleSafeDFS::new_inverse(&part_of_container, *partition_root, 0, usize::MAX);
                for partition_node in dfs {
                    let partition_node = partition_node?.node;
                    copy_node(partition_node, &all_components, graph, partition_graph)?;
                    copied_nodes.insert(partition_node, *partition_root)?;
                }
            }
            // TODO: fill the remaining graph with all nodes not in any of the partitions

            todo!("Write out each partition to each file")
        } else {
            let file_name = format!("{toplevel_corpus_name}.{extension}");
            let output_file_path = output_path.join(file_name);

            let infered_vis = if self.guess_vis {
                Some(guess_vis::vis_from_graph(graph)?)
            } else {
                None
            };
            let vis_str = match self.add_vis {
                None => DEFAULT_VIS_STR.to_string(),
                Some(ref visualisations) => visualisations.to_string(),
            };
            let vis = if let Some(vis_cfg) = infered_vis {
                [vis_str, vis_cfg].join("\n\n")
            } else {
                vis_str
            };
            let vis_str = format!("\n{vis}\n");
            reporter.info(format!("Starting export to {}", output_file_path.display()).as_str())?;

            self.write_graphml_file(
                graph,
                &output_file_path,
                zip_file.as_mut(),
                &vis_str,
                &reporter,
            )?;
        }
        if let Some(mut zip_file) = zip_file {
            // Insert all linked files with a *relative* path into the ZIP file.
            // We can't rewrite the links in the GraphML at this point and have
            // to assume that when unpacking it again, the absolute file paths
            // should point to the original files. But when relative files are
            // used, we can store them in the ZIP file itself and the when
            // unpacked, the paths are still valid regardless of whether they
            // existed in the first place on the target system.
            for file_path in self.get_linked_files(graph)? {
                let original_path = self
                    .zip_copy_from
                    .clone()
                    .unwrap_or_default()
                    .join(file_path?);

                if original_path.is_relative() {
                    zip_file.start_file(original_path.to_string_lossy(), zip_options)?;
                }
                let file_to_copy = File::open(original_path)?;
                let mut reader = BufReader::new(file_to_copy);
                std::io::copy(&mut reader, &mut zip_file)?;
            }
        }
        Ok(())
    }

    fn file_extension(&self) -> &str {
        if self.zip { "zip" } else { "graphml" }
    }
}

fn copy_node(
    partition_node: NodeID,
    all_components: &[AnnotationComponent],
    graph: &AnnotationGraph,
    partition_graph: &mut AnnotationGraph,
) -> anyhow::Result<()> {
    // Copy all labels/annotations for this graph
    for anno in graph
        .get_node_annos()
        .get_annotations_for_item(&partition_node)?
    {
        partition_graph
            .get_node_annos_mut()
            .insert(partition_node, anno)?;
    }
    // Copy all outgoing edges for all components of this node
    for c in all_components {
        if let Some(gs) = graph.get_graphstorage_as_ref(c)
            && gs.has_outgoing_edges(partition_node)?
        {
            let partition_gs = partition_graph.get_or_create_writable(&c)?;
            for target in gs.get_outgoing_edges(partition_node) {
                let target = target?;
                let edge = Edge {
                    source: partition_node,
                    target,
                };

                partition_gs.add_edge(edge.clone())?;
                for anno in gs.get_anno_storage().get_annotations_for_item(&edge)? {
                    partition_gs.add_edge_annotation(edge.clone(), anno)?;
                }
            }
        }
    }
    Ok(())
}

mod guess_vis;
#[cfg(test)]
mod tests;
