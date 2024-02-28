//! Exports files as [GraphML](http://graphml.graphdrawing.org/) files which
//! conform to the [graphANNIS data
//! model](https://korpling.github.io/graphANNIS/docs/v2/data-model.html).
use std::{
    borrow::Cow,
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fs::{create_dir_all, File},
    io::BufReader,
    path::{Path, PathBuf},
};

use crate::{
    error::AnnattoError, exporter::Exporter, progress::ProgressReporter, workflow::StatusSender,
    Module, StepID,
};
use graphannis::AnnotationGraph;
use graphannis::{
    graph::AnnoKey,
    model::{AnnotationComponent, AnnotationComponentType},
};
use graphannis_core::{
    annostorage::{NodeAnnotationStorage, ValueSearch},
    dfs::CycleSafeDFS,
    graph::{ANNIS_NS, NODE_NAME_KEY, NODE_TYPE, NODE_TYPE_KEY},
    util::{join_qname, split_qname},
};
use itertools::Itertools;
use serde_derive::{Deserialize, Serialize};

pub const MODULE_NAME: &str = "export_graphml";

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct GraphMLExporter {
    add_vis: Option<String>,
    guess_vis: bool,
    zip: bool,
}

impl Module for GraphMLExporter {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
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

fn get_orderings(graph: &AnnotationGraph) -> Vec<AnnotationComponent> {
    let mut components = Vec::new();
    for c in graph.get_all_components(Some(AnnotationComponentType::Ordering), None) {
        let storage = graph.get_graphstorage(&c).unwrap();
        if storage.source_nodes().count() > 0 {
            // skip empty components (artifacts of previous processing)
            components.push(c);
        }
    }
    components
}

fn tree_vis(graph: &AnnotationGraph) -> Result<Vec<Visualizer>, Box<dyn std::error::Error>> {
    let mut visualizers = Vec::new();
    let node_annos = graph.get_node_annos();
    for c in graph.get_all_components(Some(AnnotationComponentType::Dominance), None) {
        if !c.name.is_empty() {
            let mut mappings = BTreeMap::new();
            let storage = graph.get_graphstorage(&c).unwrap();
            let random_struct = storage.source_nodes().last();
            {
                // determine terminal name
                if let Some(Ok(ref start_node)) = random_struct {
                    let dfs =
                        CycleSafeDFS::new(storage.as_edgecontainer(), *start_node, 1, usize::MAX);
                    let terminal = dfs
                        .into_iter()
                        .find(|nr| {
                            let n = nr.as_ref().unwrap().node;
                            let t = storage.has_outgoing_edges(n);
                            t.is_ok() && !t.unwrap()
                        })
                        .unwrap()?
                        .node;
                    let terminal_name = get_terminal_name(graph, terminal)?;
                    if !terminal_name.is_empty() {
                        mappings.insert("terminal_name".to_string(), terminal_name);
                    }
                } else {
                    // node nodes, no visualization required
                    continue;
                }
            }
            let all_keys = storage.get_anno_storage().annotation_keys()?;
            if let Some(first_key) = all_keys.first() {
                if !first_key.ns.is_empty() {
                    mappings.insert("edge_anno_ns".to_string(), first_key.ns.to_string());
                }
                mappings.insert("edge_key".to_string(), first_key.name.to_string());
            }
            mappings.insert("edge_type".to_string(), c.name.to_string());

            let mut node_names: BTreeMap<String, i32> = BTreeMap::new();
            for node_r in storage.source_nodes() {
                let node = node_r?;
                for k in node_annos.get_all_keys_for_item(&node, None, None)? {
                    let qname = join_qname(k.ns.as_str(), k.name.as_str());
                    node_names.entry(qname).and_modify(|e| *e += 1).or_insert(1);
                }
            }
            let (_, most_frequent_name) = itertools::max(
                node_names
                    .into_iter()
                    .map(|(name, count)| (count, name))
                    .collect_vec(),
            )
            .unwrap();
            let (ns_opt, name) = split_qname(most_frequent_name.as_str());
            if let Some(ns) = ns_opt {
                mappings.insert("node_anno_ns".to_string(), ns.to_string());
            }
            mappings.insert("node_key".to_string(), name.to_string());
            let layer = node_annos
                .get_value_for_item(
                    &random_struct.unwrap()?,
                    &AnnoKey {
                        ns: ANNIS_NS.into(),
                        name: "layer".into(),
                    },
                )?
                .map(|v| v.to_string());
            visualizers.push(Visualizer {
                element: "node".to_string(),
                layer,
                vis_type: "tree".to_string(),
                display_name: "dominance".to_string(),
                visibility: "hidden".to_string(),
                mappings: Some(mappings),
            });
        }
    }
    Ok(visualizers)
}

fn get_terminal_name(
    graph: &AnnotationGraph,
    probe_node: u64,
) -> Result<String, Box<dyn std::error::Error>> {
    let node_key = {
        let node_key_opt = graph
            .get_all_components(Some(AnnotationComponentType::Ordering), None)
            .into_iter()
            .filter(|component| {
                let st_opt = graph.get_graphstorage(component);
                if let Some(st) = st_opt {
                    st.get_ingoing_edges(probe_node).count() > 0
                        || st.has_outgoing_edges(probe_node).unwrap()
                } else {
                    false
                }
            })
            .map(|component| component.name.to_string())
            .last();
        match node_key_opt {
            None => "".to_string(),
            Some(v) => v,
        }
    };
    Ok(node_key)
}

fn arch_vis(graph: &AnnotationGraph) -> Result<Vec<Visualizer>, Box<dyn std::error::Error>> {
    let mut visualizers = Vec::new();
    let mut order_storages = BTreeMap::new();
    for component in get_orderings(graph) {
        order_storages.insert(
            component.name.to_string(),
            graph.get_graphstorage(&component).unwrap(),
        );
    }
    for c in graph.get_all_components(Some(AnnotationComponentType::Pointing), None) {
        let mut mappings = BTreeMap::new();
        let storage = graph.get_graphstorage(&c).unwrap();
        let probe_node = storage.source_nodes().find(|_| true).unwrap()?;
        let node_key = get_terminal_name(graph, probe_node)?;
        mappings.insert("node_key".to_string(), node_key);
        visualizers.push(Visualizer {
            element: "edge".to_string(),
            layer: if c.layer.is_empty() {
                None
            } else {
                Some(c.layer.to_string())
            },
            vis_type: "arch_dependency".to_string(),
            display_name: format!("pointing ({})", c.name),
            visibility: "hidden".to_string(),
            mappings: Some(mappings),
        });
    }
    Ok(visualizers)
}

fn media_vis(graph: &AnnotationGraph) -> Result<Vec<Visualizer>, Box<dyn std::error::Error>> {
    let mut vis = Vec::new();
    let node_annos = graph.get_node_annos();
    for match_r in node_annos.exact_anno_search(Some(ANNIS_NS), "file", ValueSearch::Any) {
        let m = match_r?;
        let path_opt = node_annos.get_value_for_item(&m.node, &m.anno_key)?;
        if let Some(path_s) = path_opt {
            match path_s.split('.').last() {
                None => {}
                Some(ending) => match ending {
                    "mp3" | "wav" => {
                        vis.push(Visualizer {
                            element: "node".to_string(),
                            layer: None,
                            vis_type: "audio".to_string(),
                            display_name: "audio".to_string(),
                            visibility: "hidden".to_string(),
                            mappings: None,
                        });
                    }
                    "mp4" | "avi" | "mov" => {
                        vis.push(Visualizer {
                            element: "node".to_string(),
                            layer: None,
                            vis_type: "video".to_string(),
                            display_name: "video".to_string(),
                            visibility: "hidden".to_string(),
                            mappings: None,
                        });
                    }
                    _ => {} // ...
                },
            };
        }
    }
    Ok(vis)
}

fn collect_qnames(
    graph: &AnnotationGraph,
    node_id: &u64,
) -> Result<BTreeSet<String>, Box<dyn std::error::Error>> {
    let mut key_set = BTreeSet::new();
    for key in graph
        .get_node_annos()
        .get_all_keys_for_item(node_id, None, None)?
    {
        key_set.insert(join_qname(&key.ns, &key.name));
    }
    Ok(key_set)
}

fn kwic_vis(graph: &AnnotationGraph) -> Result<Visualizer, Box<dyn std::error::Error>> {
    let mut segmentation_names: Vec<_> = get_orderings(graph)
        .into_iter()
        .filter(|c| !c.name.is_empty())
        .map(|c| c.name.to_string())
        .collect();
    segmentation_names.sort();

    let vis = if segmentation_names.is_empty() {
        Visualizer {
            element: "node".to_string(),
            layer: None,
            vis_type: "kwic".to_string(),
            display_name: "Key Word in Context".to_string(),
            visibility: "permanent".to_string(),
            mappings: None,
        }
    } else {
        let annos_value = segmentation_names
            .iter()
            .map(|name| format!("/{name}::{name}/"))
            .join(",");
        let mut mappings = BTreeMap::new();
        mappings.insert("annos".to_string(), annos_value);
        mappings.insert("hide_tok".to_string(), "true".to_string());
        Visualizer {
            element: "node".to_string(),
            layer: None,
            vis_type: "grid".to_string(),
            display_name: "Key Word in Context".to_string(),
            visibility: "permanent".to_string(),
            mappings: Some(mappings),
        }
    };
    Ok(vis)
}

fn node_annos_vis(graph: &AnnotationGraph) -> Result<Visualizer, Box<dyn std::error::Error>> {
    let order_names: Vec<_> = get_orderings(graph)
        .into_iter()
        .map(|c| c.name.to_string())
        .collect();
    let mut node_qnames = BTreeSet::new();
    let mut visited = BTreeSet::new();
    // gather all qnames that occur on nodes reachable through coverage edges (other annotations cannot be visualized in grid)
    for component in graph.get_all_components(Some(AnnotationComponentType::Coverage), None) {
        let storage = graph.get_graphstorage(&component).unwrap();
        for source_node in storage.source_nodes().flatten() {
            if !visited.contains(&source_node) {
                visited.insert(source_node);
                node_qnames.extend(collect_qnames(graph, &source_node)?);
            }
            let dfs = CycleSafeDFS::new(storage.as_edgecontainer(), source_node, 1, usize::MAX);
            for step_r in dfs {
                let step_node = step_r?.node;
                if !visited.contains(&step_node) {
                    visited.insert(step_node);
                    node_qnames.extend(collect_qnames(graph, &step_node)?);
                }
            }
        }
    }
    let mut sorted_node_qnames = node_qnames.into_iter().collect_vec();
    sorted_node_qnames.sort();
    let node_names = sorted_node_qnames
        .into_iter()
        .filter(|name| {
            !order_names.contains(name) && !name.starts_with(format!("{ANNIS_NS}::").as_str())
        })
        .map(|name| format!("/{name}/"))
        .join(",");
    let mut mappings = BTreeMap::new();
    mappings.insert("annos".to_string(), node_names);
    mappings.insert("escape_html".to_string(), "false".to_string());

    let more_than_one_ordering = order_names.len() > 1;
    let ordered_nodes_are_identical = {
        more_than_one_ordering && {
            let ordering_components =
                graph.get_all_components(Some(AnnotationComponentType::Ordering), None);
            let node_sets = ordering_components
                .iter()
                .map(|c| {
                    graph
                        .get_graphstorage(c)
                        .unwrap()
                        .source_nodes()
                        .map(|r| r.unwrap())
                        .collect::<BTreeSet<u64>>()
                })
                .collect_vec();
            let mut all_same = true;
            for i in 1..node_sets.len() {
                let a = node_sets.get(i - 1).unwrap();
                let b = node_sets.get(i).unwrap();
                all_same &= a.cmp(b) == Ordering::Equal;
            }
            all_same
        }
    };
    mappings.insert(
        "hide_tok".to_string(),
        (!ordered_nodes_are_identical).to_string(),
    );
    mappings.insert("show_ns".to_string(), "false".to_string());
    Ok(Visualizer {
        element: "node".to_string(),
        layer: None,
        vis_type: "grid".to_string(),
        display_name: "annotations".to_string(),
        visibility: "hidden".to_string(),
        mappings: Some(mappings),
    })
}

fn vis_from_graph(graph: &AnnotationGraph) -> Result<String, Box<dyn std::error::Error>> {
    let mut vis_list = Vec::new();
    // KWIC view/and or grid for segmentations
    vis_list.push(kwic_vis(graph)?);
    // edge annos
    vis_list.extend(tree_vis(graph)?);
    vis_list.extend(arch_vis(graph)?);
    // node annos
    vis_list.push(node_annos_vis(graph)?);
    vis_list.extend(media_vis(graph)?);
    let vis = toml::to_string(&Visualization {
        visualizers: vis_list,
    })?;
    Ok(vis)
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
}

impl Exporter for GraphMLExporter {
    fn export_corpus(
        &self,
        graph: &AnnotationGraph,
        output_path: &Path,
        step_id: StepID,
        tx: Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let reporter = ProgressReporter::new_unknown_total_work(tx, step_id)?;

        // Get the toplevel corpus name from the corpus structure
        let part_of_c = graph
            .get_all_components(Some(AnnotationComponentType::PartOf), None)
            .first()
            .cloned()
            .ok_or_else(|| AnnattoError::Export {
                reason: "Could not determine file name for graphML.".into(),
                exporter: self.module_name().to_string(),
                path: output_path.to_path_buf(),
            })?;

        let corpus_nodes = graph.get_node_annos().exact_anno_search(
            Some(NODE_TYPE_KEY.ns.as_str()),
            NODE_TYPE_KEY.name.as_str(),
            ValueSearch::Some("corpus"),
        );
        let part_of_storage = graph.get_graphstorage(&part_of_c).unwrap();
        let corpus_root = corpus_nodes
            .into_iter()
            .find(|n| {
                part_of_storage
                    .get_outgoing_edges((*n).as_ref().unwrap().node)
                    .count()
                    == 0
            })
            .unwrap()?
            .node;
        let toplevel_corpus_name = graph
            .get_node_annos()
            .get_value_for_item(&corpus_root, &NODE_NAME_KEY)?
            .unwrap_or_else(|| Cow::Borrowed("corpus"));

        // Use the corpus name to determine the file name
        let extension = self.file_extension();
        let file_name = format!("{toplevel_corpus_name}.{extension}");

        if !output_path.exists() {
            create_dir_all(output_path)?;
        }
        let output_file_path = output_path.join(file_name);
        let output_file = File::create(output_file_path.clone())?;

        let infered_vis = if self.guess_vis {
            Some(vis_from_graph(graph)?)
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
        reporter.info(format!("Starting export to {}", &output_file_path.display()).as_str())?;

        if self.zip {
            // Create a ZIP file at the given location
            let mut zip = zip::ZipWriter::new(output_file);

            // Create an entry in the ZIP file and write the GraphML to this file entry
            let options = zip::write::FileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated);

            zip.start_file(format!("{toplevel_corpus_name}.graphml"), options)?;

            graphannis_core::graph::serialization::graphml::export(
                graph,
                Some(format!("\n{vis}\n").as_str()),
                &mut zip,
                |msg| {
                    reporter.info(msg).expect("Could not send status message");
                },
            )?;
            // Insert all linked files with a *relative* path into the ZIP file.
            // We can't rewrite the links in the GraphML at this point and have
            // to assume that wen unpacking it again, the absolute file paths
            // should point to the original files. But when relative files are
            // used, we can store them in the ZIP file itself and the when
            // unpacked, the paths are still valid regardless of whether they
            // existed in the first place on the target system.
            for file in self.get_linked_files(graph)? {
                let original_path = file?;

                if original_path.is_relative() {
                    zip.start_file(original_path.to_string_lossy(), options)?;
                }
                let file_to_copy = File::open(original_path)?;
                let mut reader = BufReader::new(file_to_copy);
                std::io::copy(&mut reader, &mut zip)?;
            }
        } else {
            // Directly writhe the GraphML to the output file
            graphannis_core::graph::serialization::graphml::export(
                graph,
                Some(format!("\n{vis}\n").as_str()),
                output_file,
                |msg| {
                    reporter.info(msg).expect("Could not send status message");
                },
            )?;
        }

        Ok(())
    }

    fn file_extension(&self) -> &str {
        if self.zip {
            "zip"
        } else {
            "graphml"
        }
    }
}
