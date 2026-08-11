//! Heuristics to find suitable visualizations for graph.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use graphannis::{
    AnnotationGraph,
    graph::AnnoKey,
    model::{AnnotationComponent, AnnotationComponentType},
};
use graphannis_core::{
    annostorage::ValueSearch,
    dfs::CycleSafeDFS,
    graph::ANNIS_NS,
    util::{join_qname, split_qname},
};
use itertools::Itertools as _;

use crate::exporter::graphml::{Visualization, Visualizer};

pub(super) fn vis_from_graph(
    graph: &AnnotationGraph,
) -> Result<String, Box<dyn std::error::Error>> {
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
        if let Some(storage) = graph.get_graphstorage(&component) {
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

    let ordered_components_contain_identical_nodes = if order_names.len() > 1 {
        let ordering_components =
            graph.get_all_components(Some(AnnotationComponentType::Ordering), None);
        let node_sets = ordering_components
            .iter()
            .map(|c| {
                if let Some(strge) = graph.get_graphstorage(c) {
                    strge
                        .source_nodes()
                        .filter_map(|r| r.ok())
                        .collect::<BTreeSet<u64>>()
                } else {
                    BTreeSet::default()
                }
            })
            .collect_vec();
        let mut all_same = true;
        //for i in 1..node_sets.len()
        for (a, b) in node_sets.into_iter().tuple_windows() {
            all_same &= matches!(a.cmp(&b), Ordering::Equal);
        }
        all_same
    } else {
        // There is only one ordering component
        true
    };

    mappings.insert(
        "hide_tok".to_string(),
        (!ordered_components_contain_identical_nodes).to_string(),
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

fn tree_vis(graph: &AnnotationGraph) -> Result<Vec<Visualizer>, Box<dyn std::error::Error>> {
    let mut visualizers = Vec::new();
    let node_annos = graph.get_node_annos();
    for c in graph.get_all_components(Some(AnnotationComponentType::Dominance), None) {
        if !c.name.is_empty() {
            let mut mappings = BTreeMap::new();
            if let Some(storage) = graph.get_graphstorage(&c)
                && let Some(Ok(random_struct)) = storage.source_nodes().last()
            {
                // determine terminal name
                let dfs =
                    CycleSafeDFS::new(storage.as_edgecontainer(), random_struct, 1, usize::MAX);
                if let Some(terminal_opt) = dfs.into_iter().find(|nr| {
                    if let Ok(step) = nr {
                        storage.has_outgoing_edges(step.node).unwrap_or_default()
                    } else {
                        false
                    }
                }) {
                    let terminal = terminal_opt?.node;
                    let terminal_name = get_terminal_name(graph, terminal)?.unwrap_or_default();
                    if !terminal_name.is_empty() {
                        mappings.insert("terminal_name".to_string(), terminal_name);
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
                if let Some((_, most_frequent_name)) = itertools::max(
                    node_names
                        .into_iter()
                        .map(|(name, count)| (count, name))
                        .collect_vec(),
                ) {
                    let (ns_opt, name) = split_qname(most_frequent_name.as_str());
                    if let Some(ns) = ns_opt {
                        mappings.insert("node_anno_ns".to_string(), ns.to_string());
                    }
                    mappings.insert("node_key".to_string(), name.to_string());
                    let layer = node_annos
                        .get_value_for_item(
                            &random_struct,
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
        }
    }
    Ok(visualizers)
}

fn arch_vis(graph: &AnnotationGraph) -> Result<Vec<Visualizer>, Box<dyn std::error::Error>> {
    let mut visualizers = Vec::new();
    let mut order_storages = BTreeMap::new();
    for component in get_orderings(graph) {
        if let Some(storage) = graph.get_graphstorage(&component) {
            order_storages.insert(component.name.to_string(), storage);
        }
    }
    for c in graph.get_all_components(Some(AnnotationComponentType::Pointing), None) {
        let mut mappings = BTreeMap::new();
        if let Some(storage) = graph.get_graphstorage(&c)
            && let Some(Ok(probe_node)) = storage.source_nodes().last()
        {
            if let Some(node_key) = get_terminal_name(graph, probe_node)? {
                mappings.insert("node_key".to_string(), node_key);
            }
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
            match path_s.split('.').next_back() {
                None => {}
                Some(ending) => match ending {
                    "mp3" | "wav" => {
                        vis.push(Visualizer {
                            element: "node".to_string(),
                            layer: None,
                            vis_type: "audio".to_string(),
                            display_name: "audio".to_string(),
                            visibility: "preloaded".to_string(),
                            mappings: None,
                        });
                    }
                    "mp4" | "avi" | "mov" | "webm" => {
                        vis.push(Visualizer {
                            element: "node".to_string(),
                            layer: None,
                            vis_type: "video".to_string(),
                            display_name: "video".to_string(),
                            visibility: "preloaded".to_string(),
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

fn get_orderings(graph: &AnnotationGraph) -> Vec<AnnotationComponent> {
    let mut components = Vec::new();
    for c in graph.get_all_components(Some(AnnotationComponentType::Ordering), None) {
        if let Some(storage) = graph.get_graphstorage(&c)
            && storage.source_nodes().count() > 0
        // skip empty components (artifacts of previous processing)
        {
            components.push(c);
        }
    }
    components
}

fn get_terminal_name(
    graph: &AnnotationGraph,
    probe_node: u64,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let node_key_opt = graph
        .get_all_components(Some(AnnotationComponentType::Ordering), None)
        .into_iter()
        .filter(|component| {
            let st_opt = graph.get_graphstorage(component);
            if let Some(st) = st_opt {
                st.get_ingoing_edges(probe_node).count() > 0
                    || st.has_outgoing_edges(probe_node).unwrap_or_default()
            } else {
                false
            }
        })
        .map(|component| {
            if component.name.is_empty() {
                None
            } else {
                Some(component.name.to_string())
            }
        })
        .next_back();
    Ok(node_key_opt.unwrap_or_default())
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
