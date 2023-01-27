use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fs::{create_dir_all, File},
    path::Path,
};

use crate::{
    error::AnnattoError, exporter::Exporter, progress::ProgressReporter, workflow::StatusSender,
    Module,
};
use graphannis::AnnotationGraph;
use graphannis::{
    graph::AnnoKey,
    model::{AnnotationComponent, AnnotationComponentType},
};
use graphannis_core::{
    annostorage::ValueSearch,
    dfs::CycleSafeDFS,
    graph::{ANNIS_NS, NODE_NAME_KEY, NODE_TYPE_KEY},
    util::{join_qname, split_qname},
};
use itertools::Itertools;
use serde_derive::Serialize;

#[derive(Default)]
pub struct GraphMLExporter {}

impl Module for GraphMLExporter {
    fn module_name(&self) -> &str {
        "GraphMLExporter"
    }
}

pub const PROPERTY_VIS: &str = "add.vis";
const PROP_GUESS_VIS: &str = "guess.vis";
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

fn get_orderings(graph: &AnnotationGraph) -> Vec<String> {
    let mut names = Vec::new();
    for c in graph.get_all_components(Some(AnnotationComponentType::Ordering), None) {
        let storage = graph.get_graphstorage(&c).unwrap();
        if storage.source_nodes().count() > 0 {
            // skip empty components (artifacts of previous processing)
            names.push(c.name.to_string());
        }
    }
    names
}

fn tree_vis(graph: &AnnotationGraph) -> Result<Vec<Visualizer>, Box<dyn std::error::Error>> {
    let mut visualizers = Vec::new();
    let node_annos = graph.get_node_annos();
    for c in graph.get_all_components(Some(AnnotationComponentType::Dominance), None) {
        let mut mappings = BTreeMap::new();
        let storage = graph.get_graphstorage(&c).unwrap();
        let random_struct = storage.source_nodes().last();
        {
            // determine terminal name
            if let Some(Ok(ref start_node)) = random_struct {
                let dfs = CycleSafeDFS::new(storage.as_edgecontainer(), *start_node, 1, usize::MAX);
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
                mappings.insert("terminal_name".to_string(), terminal_name);
            } else {
                // node nodes, no visualization required
                continue;
            }
        }
        let all_keys = storage.get_anno_storage().annotation_keys()?;
        if let Some(first_key) = all_keys.get(0) {
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
        let layer = match node_annos.get_value_for_item(
            &random_struct.unwrap()?,
            &AnnoKey {
                ns: ANNIS_NS.into(),
                name: "layer".into(),
            },
        )? {
            None => None,
            Some(v) => Some(v.to_string()),
        };
        visualizers.push(Visualizer {
            element: "node".to_string(),
            layer: layer,
            vis_type: "tree".to_string(),
            display_name: "dominance".to_string(),
            visibility: "hidden".to_string(),
            mappings: Some(mappings),
        });
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
                if st_opt.is_none() {
                    false
                } else {
                    let st = st_opt.unwrap();
                    st.get_ingoing_edges(probe_node).count() > 0
                        || st.has_outgoing_edges(probe_node).unwrap()
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
    for order_name in get_orderings(graph) {
        let component = AnnotationComponent::new(
            AnnotationComponentType::Ordering,
            ANNIS_NS.into(),
            order_name.clone().into(),
        );
        order_storages.insert(order_name, graph.get_graphstorage(&component).unwrap());
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

fn node_annos_vis(graph: &AnnotationGraph) -> Result<Visualizer, Box<dyn std::error::Error>> {
    let order_names = get_orderings(graph);
    let orderings = order_names
        .iter()
        .filter(|s| !s.is_empty())
        .map(|s| format!("/{}/", s))
        .join(",");
    let mut node_qnames = BTreeSet::new();
    let mut visited = BTreeSet::new();
    // gather all qnames that occur on nodes reachable through coverage edges (other annotations cannot be visualized in grid)
    for component in graph.get_all_components(Some(AnnotationComponentType::Coverage), None) {
        let storage = graph.get_graphstorage(&component).unwrap();
        for source_node_r in storage.source_nodes() {
            if let Ok(source_node) = source_node_r {
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
    }
    let mut sorted_node_qnames = node_qnames.into_iter().collect_vec();
    sorted_node_qnames.sort();
    let node_names = sorted_node_qnames
        .into_iter()
        .filter(|name| {
            !order_names.contains(&name) && !name.starts_with(format!("{}::", ANNIS_NS).as_str())
        })
        .map(|name| format!("/{}/", name))
        .join(",");
    let mut mappings = BTreeMap::new();
    mappings.insert("annos".to_string(), [orderings, node_names].join(","));
    mappings.insert("escape_html".to_string(), "false".to_string());
    let more_than_one_ordering = !order_names.is_empty(); // reminder: order_names does not contain the unnamed ordering, therefore !is_empty is the way to go
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
                        .into_iter()
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

impl Exporter for GraphMLExporter {
    fn export_corpus(
        &self,
        graph: &AnnotationGraph,
        properties: &BTreeMap<String, String>,
        output_path: &Path,
        tx: Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let reporter = ProgressReporter::new(tx, self as &dyn Module, Some(output_path), 1)?;
        let file_name;
        if let Some(part_of_c) = graph
            .get_all_components(Some(AnnotationComponentType::PartOf), None)
            .first()
        {
            let corpus_nodes = graph.get_node_annos().exact_anno_search(
                Some(NODE_TYPE_KEY.ns.as_str()),
                NODE_TYPE_KEY.name.as_str(),
                ValueSearch::Some("corpus"),
            );
            let part_of_storage = graph.get_graphstorage(part_of_c).unwrap();
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
            file_name = format!(
                "{}.graphml",
                graph
                    .get_node_annos()
                    .get_value_for_item(&corpus_root, &NODE_NAME_KEY)?
                    .unwrap()
            );
        } else {
            let reason = String::from("Could not determine file name for graphML.");
            let err = AnnattoError::Export {
                reason,
                exporter: self.module_name().to_string(),
                path: output_path.to_path_buf(),
            };
            return Err(Box::new(err));
        }
        let output_file_path = match output_path.is_dir() {
            true => output_path.join(file_name),
            false => {
                create_dir_all(output_path)?;
                output_path.join(file_name)
            }
        };
        let output_file = File::create(output_file_path.clone())?;
        let infered_vis = match properties.get(&PROP_GUESS_VIS.to_string()) {
            None => None,
            Some(bool_str) => match bool_str.parse::<bool>()? {
                false => None,
                true => Some(vis_from_graph(graph)?),
            },
        };
        let vis_str = match properties.get(&PROPERTY_VIS.to_string()) {
            None => DEFAULT_VIS_STR.to_string(),
            Some(visualisations) => visualisations.to_string(),
        };
        let vis = if let Some(vis_cfg) = infered_vis {
            [vis_str, vis_cfg].join("\n\n")
        } else {
            vis_str
        };
        reporter.info(format!("Starting export to {}", &output_file_path.display()).as_str())?;
        graphannis_core::graph::serialization::graphml::export(
            graph,
            Some(format!("\n{}\n", vis).as_str()),
            output_file,
            |msg| {
                reporter.info(msg).expect("Could not send status message");
            },
        )?;
        reporter.worked(1)?;
        Ok(())
    }
}
