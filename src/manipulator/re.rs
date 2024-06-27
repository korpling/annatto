use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use anyhow::anyhow;
use graphannis::{
    graph::{AnnoKey, Edge, Match},
    model::{AnnotationComponent, AnnotationComponentType},
    update::{GraphUpdate, UpdateEvent},
    AnnotationGraph,
};
use graphannis_core::{annostorage::NodeAnnotationStorage, util::split_qname};
use graphannis_core::{
    annostorage::ValueSearch,
    dfs::CycleSafeDFS,
    graph::{ANNIS_NS, NODE_NAME_KEY},
};
use itertools::Itertools;
use serde_derive::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use crate::{
    error::{AnnattoError, StandardErrorResult},
    progress::ProgressReporter,
    Manipulator, StepID,
};
use documented::{Documented, DocumentedFields};

/// Manipulate annotations, like deleting or renaming them.
#[derive(Default, Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(default, deny_unknown_fields)]
pub struct Revise {
    /// A map of nodes to rename, usually useful for corpus nodes. If the target name exists,
    /// the operation will fail with an error. If the target name is empty, the node will be
    /// deleted.
    node_names: BTreeMap<String, String>,
    /// a list of names of nodes to be removed
    remove_nodes: Vec<String>,
    /// also move annotations to other host nodes determined by namespace
    move_node_annos: bool,
    /// rename node annotation
    node_annos: BTreeMap<String, String>,
    /// rename edge annotations
    edge_annos: BTreeMap<String, String>,
    /// rename or erase namespaces
    namespaces: BTreeMap<String, String>,
    /// rename or erase components
    components: BTreeMap<String, String>,
    /// The given node names and all ingoing paths (incl. nodes) in PartOf/annis/ will be removed
    remove_subgraph: Vec<String>,
}

const DELIMITER: &str = "::";

fn remove_subgraph(
    graph: &AnnotationGraph,
    update: &mut GraphUpdate,
    from_node: &str,
) -> Result<(), anyhow::Error> {
    update.add_event(UpdateEvent::DeleteNode {
        node_name: from_node.to_string(),
    })?;
    if let Some(part_of_storage) = graph.get_graphstorage(&AnnotationComponent::new(
        AnnotationComponentType::PartOf,
        ANNIS_NS.into(),
        "".into(),
    )) {
        let node_annos = graph.get_node_annos();
        let nid = node_annos.get_node_id_from_name(from_node)?;
        if let Some(node_id) = nid {
            for n in CycleSafeDFS::new_inverse(
                part_of_storage.as_edgecontainer(),
                node_id,
                1,
                usize::MAX,
            ) {
                let node = n?.node;
                if let Some(node_name) = node_annos.get_value_for_item(&node, &NODE_NAME_KEY)? {
                    update.add_event(UpdateEvent::DeleteNode {
                        node_name: node_name.to_string(),
                    })?;
                }
            }
        }
    }
    Ok(())
}

fn parse_component_string(value: &str) -> Result<Option<AnnotationComponent>, anyhow::Error> {
    if value.trim().is_empty() {
        return Ok(None);
    }
    if let Some((ctype_str, layer, name)) = value.splitn(3, DELIMITER).collect_tuple() {
        let ctype = match ctype_str.to_lowercase().as_str() {
            "partof" => AnnotationComponentType::PartOf,
            "ordering" => AnnotationComponentType::Ordering,
            "coverage" => AnnotationComponentType::Coverage,
            "dominance" => AnnotationComponentType::Dominance,
            "pointing" => AnnotationComponentType::Dominance,
            "l" => AnnotationComponentType::LeftToken,
            "r" => AnnotationComponentType::RightToken,
            _ => return Err(anyhow!("Could not map component configuration `{value}`")),
        };
        Ok(Some(AnnotationComponent::new(
            ctype,
            layer.into(),
            name.into(),
        )))
    } else {
        Err(anyhow!("Could not map component configuration `{value}`"))
    }
}

fn to_component_map(
    str_map: &BTreeMap<String, String>,
) -> Result<BTreeMap<AnnotationComponent, Option<AnnotationComponent>>, anyhow::Error> {
    let mut component_map = BTreeMap::new();
    for (old, new) in str_map {
        if let Some(source) = parse_component_string(old)? {
            component_map.insert(source, parse_component_string(new)?);
        } else {
            return Err(anyhow!("Could not parse source component: {old}"));
        }
    }
    Ok(component_map)
}

fn revise_components(
    graph: &AnnotationGraph,
    component_config: &BTreeMap<String, String>,
    update: &mut GraphUpdate,
    progress_reporter: &ProgressReporter,
) -> Result<(), anyhow::Error> {
    let component_map = to_component_map(component_config)?;
    for (source, target) in component_map {
        revise_component(graph, source, target, update, progress_reporter)?;
    }
    Ok(())
}

fn node_name(
    node_id: &u64,
    node_annos: &dyn NodeAnnotationStorage,
) -> Result<String, AnnattoError> {
    if let Some(name) = node_annos.get_value_for_item(node_id, &NODE_NAME_KEY)? {
        Ok(name.to_string())
    } else {
        Err(AnnattoError::Manipulator {
            reason: "Could not determine node name in component revision".to_string(),
            manipulator: "revise".to_string(),
        })
    }
}

fn revise_component(
    graph: &AnnotationGraph,
    source_component: AnnotationComponent,
    target_component: Option<AnnotationComponent>,
    update: &mut GraphUpdate,
    progress_reporter: &ProgressReporter,
) -> Result<(), AnnattoError> {
    if let Some(source_storage) = graph.get_graphstorage(&source_component) {
        let node_annos = graph.get_node_annos();
        let edge_anno_storage = source_storage.get_anno_storage();
        for node in source_storage.as_edgecontainer().source_nodes() {
            if let Ok(node_id) = node {
                let source_node_name = node_name(&node_id, node_annos)?;
                for reachable_target in
                    source_storage.find_connected(node_id, 1, std::ops::Bound::Included(1))
                {
                    if let Ok(target_id) = reachable_target {
                        let edge = Edge {
                            source: node_id,
                            target: target_id,
                        };
                        let target_node_name = node_name(&target_id, node_annos)?;
                        update.add_event(UpdateEvent::DeleteEdge {
                            source_node: source_node_name.to_string(),
                            target_node: target_node_name.to_string(),
                            layer: source_component.layer.to_string(),
                            component_type: source_component.get_type().to_string(),
                            component_name: source_component.name.to_string(),
                        })?;
                        if let Some(target_c) = &target_component {
                            update.add_event(UpdateEvent::AddEdge {
                                source_node: source_node_name.to_string(),
                                target_node: target_node_name.to_string(),
                                layer: target_c.layer.to_string(),
                                component_type: target_c.get_type().to_string(),
                                component_name: target_c.name.to_string(),
                            })?;
                            for anno_key in
                                edge_anno_storage.get_all_keys_for_item(&edge, None, None)?
                            {
                                if anno_key.ns == ANNIS_NS {
                                    continue;
                                }
                                if let Some(edge_anno_value) =
                                    edge_anno_storage.get_value_for_item(&edge, &anno_key)?
                                {
                                    update.add_event(UpdateEvent::AddEdgeLabel {
                                        source_node: source_node_name.to_string(),
                                        target_node: target_node_name.to_string(),
                                        layer: target_c.layer.to_string(),
                                        component_type: target_c.get_type().to_string(),
                                        component_name: target_c.name.to_string(),
                                        anno_ns: anno_key.ns.to_string(),
                                        anno_name: anno_key.name.to_string(),
                                        anno_value: edge_anno_value.to_string(),
                                    })?;
                                }
                            }
                        }
                    } else {
                        progress_reporter.warn(
                            format!(
                                "Could not retrieve target node for source node in component {}",
                                source_component
                            )
                            .as_str(),
                        )?;
                    }
                }
            } else {
                progress_reporter.warn(
                    format!(
                        "Could not obtain node from source nodes in component {}.",
                        source_component
                    )
                    .as_str(),
                )?;
            }
        }
    } else {
        progress_reporter.warn(
            format!(
                "Component {} does not exist and will not be mapped",
                source_component
            )
            .as_str(),
        )?;
    }
    Ok(())
}

fn remove_nodes(
    update: &mut GraphUpdate,
    names: &Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    for name in names {
        update.add_event(UpdateEvent::DeleteNode {
            node_name: name.to_string(),
        })?;
    }
    Ok(())
}

fn place_at_new_target(
    graph: &AnnotationGraph,
    update: &mut GraphUpdate,
    m: &Match,
    target_key: &AnnoKey,
) -> Result<(), Box<dyn std::error::Error>> {
    let coverage_component = AnnotationComponent::new(
        AnnotationComponentType::Coverage,
        ANNIS_NS.into(),
        "".into(),
    );
    let coverage_storage = if let Some(strg) = graph.get_graphstorage(&coverage_component) {
        strg
    } else {
        return Err(anyhow!("Could not obtain storage of coverage component.").into());
    };
    let order_component = AnnotationComponent::new(
        AnnotationComponentType::Ordering,
        ANNIS_NS.to_string().into(),
        target_key.ns.clone(),
    );
    let order_storage = if let Some(strg) = graph.get_graphstorage(&order_component) {
        strg
    } else {
        return Err(anyhow!("Could not obtain storage of ordering component.").into());
    };
    let source_node = m.node;
    let mut covered_terminal_nodes = Vec::new();
    CycleSafeDFS::new(
        coverage_storage.as_edgecontainer(),
        source_node,
        1,
        usize::MAX,
    )
    .filter_map(|sr| {
        if let Ok(step) = sr {
            let n = step.node;
            if !coverage_storage.has_outgoing_edges(n).unwrap_or_default() {
                Some(n)
            } else {
                None
            }
        } else {
            None
        }
    })
    .for_each(|n| covered_terminal_nodes.push(n));
    let mut covering_nodes = BTreeSet::new();
    for terminal in covered_terminal_nodes {
        for reachable in
            CycleSafeDFS::new_inverse(coverage_storage.as_edgecontainer(), terminal, 1, usize::MAX)
        {
            let covering_node = reachable?.node;
            let is_part_of_ordering = order_storage.has_outgoing_edges(covering_node)?
                || order_storage.get_ingoing_edges(covering_node).count() > 0;
            if is_part_of_ordering {
                covering_nodes.insert(covering_node);
            }
        }
    }
    let node_annos = graph.get_node_annos();
    if let Some(anno_value) = node_annos.get_value_for_item(&m.node, &m.anno_key)? {
        let probe_node = if let Some(nid) = covering_nodes.pop_last() {
            nid
        } else {
            return Err(anyhow!(
                "Could not gather any covered nodes for name `{}`",
                target_key.ns
            )
            .into());
        };
        if covering_nodes.is_empty() {
            let target_name = node_name(&probe_node, node_annos)?;
            update.add_event(UpdateEvent::DeleteNodeLabel {
                node_name: target_name.to_string(),
                anno_ns: target_key.ns.to_string(),
                anno_name: target_key.name.to_string(),
            })?; // safety delete in case of multiple annotations
            update.add_event(UpdateEvent::AddNodeLabel {
                node_name: target_name.to_string(),
                anno_ns: target_key.ns.to_string(),
                anno_name: target_key.name.to_string(),
                anno_value: anno_value.to_string(),
            })?;
        } else {
            // create new span first (we could also check for an exiting one, but it sounds expensive and not promising)
            let old_name = node_name(&probe_node, node_annos)?;
            let name_pref = old_name.trim_end_matches(char::is_numeric);
            covering_nodes.insert(probe_node);
            let existing = node_annos
                .get_all_values(&NODE_NAME_KEY, false)?
                .iter()
                .filter(|v| v.starts_with(name_pref))
                .collect_vec()
                .len();
            let span_name = format!("{name_pref}{}", existing + 1);
            update.add_event(UpdateEvent::AddNode {
                node_name: span_name.clone(),
                node_type: "node".to_string(),
            })?;
            update.add_event(UpdateEvent::DeleteNodeLabel {
                node_name: span_name.to_string(),
                anno_ns: target_key.ns.to_string(),
                anno_name: target_key.name.to_string(),
            })?; // safety delete in case of multiple annotations
            update.add_event(UpdateEvent::AddNodeLabel {
                node_name: span_name.clone(),
                anno_ns: target_key.ns.to_string(),
                anno_name: target_key.name.to_string(),
                anno_value: anno_value.to_string(),
            })?;
            for member in covering_nodes {
                let member_name = node_name(&member, node_annos)?;
                update.add_event(UpdateEvent::AddEdge {
                    source_node: span_name.clone(),
                    target_node: member_name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Coverage.to_string(),
                    component_name: "".to_string(),
                })?;
            }
        };
    }
    Ok(())
}

fn replace_node_annos(
    graph: &mut AnnotationGraph,
    update: &mut GraphUpdate,
    anno_keys: Vec<(AnnoKey, Option<AnnoKey>)>,
    move_by_ns: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let annos = graph.get_node_annos();
    for (old_key, new_key_opt) in anno_keys.into_iter() {
        for r in annos.exact_anno_search(
            ns_from_key(&old_key),
            old_key.name.as_str(),
            ValueSearch::Any,
        ) {
            let m = r?;
            let node_name = node_name(&m.node, annos)?;
            update.add_event(UpdateEvent::DeleteNodeLabel {
                node_name: node_name.to_string(),
                anno_ns: old_key.ns.to_string(),
                anno_name: old_key.name.to_string(),
            })?;
            if let Some(ref new_key) = new_key_opt {
                if move_by_ns {
                    place_at_new_target(graph, update, &m, new_key)?;
                } else if let Some(value) = annos.get_value_for_item(&m.node, &old_key)? {
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: node_name.to_string(),
                        anno_ns: new_key.ns.to_string(),
                        anno_name: new_key.name.to_string(),
                        anno_value: value.to_string(),
                    })?;
                }
            }
        }
    }
    Ok(())
}

fn replace_edge_annos(
    graph: &mut AnnotationGraph,
    update: &mut GraphUpdate,
    anno_keys: Vec<(AnnoKey, Option<AnnoKey>)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let node_annos = graph.get_node_annos();
    for (old_key, new_key_opt) in anno_keys {
        for component in graph.get_all_components(None, None) {
            let component_storage = graph.get_graphstorage(&component).unwrap();
            let edge_annos = component_storage.get_anno_storage();
            for r in edge_annos.exact_anno_search(
                ns_from_key(&old_key),
                old_key.name.as_str(),
                ValueSearch::Any,
            ) {
                let m = r?;
                let source_node = m.node;
                let source_node_name = node_name(&source_node, node_annos)?;
                for out_edge_opt in component_storage.get_outgoing_edges(source_node) {
                    let target_node = out_edge_opt?;
                    let target_node_name = node_name(&target_node, node_annos)?;
                    update.add_event(UpdateEvent::DeleteEdgeLabel {
                        source_node: source_node_name.to_string(),
                        target_node: target_node_name.to_string(),
                        layer: component.layer.to_string(),
                        component_type: component.get_type().to_string(),
                        component_name: component.name.to_string(),
                        anno_ns: m.anno_key.ns.to_string(),
                        anno_name: old_key.name.to_string(),
                    })?;
                    if let Some(ref new_key) = new_key_opt {
                        if let Some(value) = edge_annos.get_value_for_item(
                            &Edge {
                                source: source_node,
                                target: target_node,
                            },
                            &m.anno_key,
                        )? {
                            update.add_event(UpdateEvent::AddEdgeLabel {
                                source_node: source_node_name.to_string(),
                                target_node: target_node_name.to_string(),
                                layer: component.layer.to_string(),
                                component_type: component.get_type().to_string(),
                                component_name: component.name.to_string(),
                                anno_ns: new_key.ns.to_string(),
                                anno_name: new_key.name.to_string(),
                                anno_value: value.to_string(),
                            })?;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn replace_namespaces(
    graph: &AnnotationGraph,
    update: &mut GraphUpdate,
    renamings: Vec<(String, Option<String>)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let node_annos = graph.get_node_annos();
    // for node annotations
    for (old_namespace, new_namespace_opt) in renamings.iter() {
        let new_ns = match new_namespace_opt {
            None => "".to_string(),
            Some(v) => v.to_string(),
        };
        for ak in node_annos
            .annotation_keys()?
            .into_iter()
            .filter(|k| k.ns.as_str() == old_namespace)
        {
            for m_r in node_annos.exact_anno_search(
                Some(old_namespace.as_str()),
                ak.name.as_str(),
                ValueSearch::Any,
            ) {
                let m = m_r?;
                let node_name = node_name(&m.node, node_annos)?;
                if let Some(value) = node_annos.get_value_for_item(&m.node, &m.anno_key)? {
                    update.add_event(UpdateEvent::DeleteNodeLabel {
                        node_name: node_name.to_string(),
                        anno_ns: m.anno_key.ns.to_string(),
                        anno_name: m.anno_key.name.to_string(),
                    })?;
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: node_name.to_string(),
                        anno_ns: new_ns.to_string(),
                        anno_name: m.anno_key.name.to_string(),
                        anno_value: value.to_string(),
                    })?;
                }
            }
        }
    }
    // for edge annotations
    for component in graph.get_all_components(None, None) {
        let storage = if let Some(strg) = graph.get_graphstorage(&component) {
            strg
        } else {
            return Err(anyhow!("Could not obtain component storage: {}", &component).into());
        };
        for (old_namespace, new_namespace_opt) in renamings.iter() {
            let new_ns = match new_namespace_opt {
                None => "".to_string(),
                Some(v) => v.to_string(),
            };
            for ak in storage
                .get_anno_storage()
                .annotation_keys()?
                .into_iter()
                .filter(|k| k.ns.as_str() == old_namespace)
            {
                for m_r in storage.get_anno_storage().exact_anno_search(
                    Some(old_namespace.as_str()),
                    ak.name.as_str(),
                    ValueSearch::Any,
                ) {
                    let m = m_r?;
                    let source_node = m.node;
                    let source_node_name = node_name(&source_node, node_annos)?;
                    for target_r in storage.get_outgoing_edges(source_node) {
                        let target_node = target_r?;
                        if let Some(value) = storage.get_anno_storage().get_value_for_item(
                            &Edge {
                                source: source_node,
                                target: target_node,
                            },
                            &m.anno_key,
                        )? {
                            let target_node_name = node_name(&target_node, node_annos)?;
                            update.add_event(UpdateEvent::DeleteEdgeLabel {
                                source_node: source_node_name.to_string(),
                                target_node: target_node_name.to_string(),
                                layer: component.layer.to_string(),
                                component_type: component.get_type().to_string(),
                                component_name: component.name.to_string(),
                                anno_ns: m.anno_key.ns.to_string(),
                                anno_name: m.anno_key.name.to_string(),
                            })?;
                            update.add_event(UpdateEvent::AddEdgeLabel {
                                source_node: source_node_name.to_string(),
                                target_node: target_node_name.to_string(),
                                layer: component.layer.to_string(),
                                component_type: component.get_type().to_string(),
                                component_name: component.name.to_string(),
                                anno_ns: new_ns.to_string(),
                                anno_name: m.anno_key.name.to_string(),
                                anno_value: value.to_string(),
                            })?;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn key_from_qname(qname: &str) -> AnnoKey {
    let (ns, name) = split_qname(qname);
    match ns {
        None => AnnoKey {
            ns: "".into(),
            name: name.into(),
        },
        Some(ns_val) => AnnoKey {
            ns: ns_val.into(),
            name: name.into(),
        },
    }
}

fn ns_from_key(anno_key: &AnnoKey) -> Option<&str> {
    if anno_key.ns.is_empty() {
        None
    } else {
        Some(anno_key.ns.as_str())
    }
}

fn read_replace_property_value(
    value: &BTreeMap<String, String>,
) -> StandardErrorResult<Vec<(AnnoKey, Option<AnnoKey>)>> {
    let mut names = Vec::new();
    for (source_name, target_name) in value {
        let src_key = key_from_qname(source_name);
        let tgt_key = if target_name.trim().is_empty() {
            None
        } else {
            Some(key_from_qname(target_name))
        };
        names.push((src_key, tgt_key));
    }
    Ok(names)
}

fn rename_nodes(
    graph: &AnnotationGraph,
    update: &mut GraphUpdate,
    old_name: &str,
    new_name: &str,
    step_id: &StepID,
) -> Result<(), Box<dyn std::error::Error>> {
    let node_annos = graph.get_node_annos();
    let trimmed_old_name = old_name.trim();
    if node_annos.has_node_name(trimmed_old_name)? {
        let trimmed_new_name = new_name.trim();
        if trimmed_new_name.is_empty() {
            // deletion by rename
            update.add_event(UpdateEvent::DeleteNode {
                node_name: trimmed_old_name.to_string(),
            })?;
        } else {
            update.add_event(UpdateEvent::AddNodeLabel {
                node_name: trimmed_old_name.to_string(),
                anno_ns: NODE_NAME_KEY.ns.to_string(),
                anno_name: NODE_NAME_KEY.name.to_string(),
                anno_value: trimmed_new_name.to_string(),
            })?;
        }
        if node_annos.has_node_name(trimmed_new_name)? {
            // this will also be triggered when old and new name are identical (which is fine)
            Err(Box::new(AnnattoError::Manipulator {
                reason: format!("New node name {trimmed_new_name} is already in use"),
                manipulator: step_id.module_name.to_string(),
            }))
        } else {
            Ok(())
        }
    } else {
        Err(Box::new(AnnattoError::Manipulator {
            reason: format!("No such node to be renamed: {trimmed_old_name}"),
            manipulator: step_id.module_name.to_string(),
        }))
    }
}

impl Manipulator for Revise {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        _workflow_directory: &Path,
        step_id: StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let progress_reporter = ProgressReporter::new(tx, step_id.clone(), 7)?;
        let mut update = GraphUpdate::default();
        for (old_name, new_name) in &self.node_names {
            rename_nodes(graph, &mut update, old_name, new_name, &step_id)?;
        }
        progress_reporter.worked(1)?;
        let move_by_ns = self.move_node_annos;
        if !self.remove_nodes.is_empty() {
            remove_nodes(&mut update, &self.remove_nodes)?;
        }
        progress_reporter.worked(1)?;
        if !self.node_annos.is_empty() {
            let node_annos = read_replace_property_value(&self.node_annos)?;
            replace_node_annos(graph, &mut update, node_annos, move_by_ns)?;
        }
        progress_reporter.worked(1)?;
        if !self.edge_annos.is_empty() {
            let edge_annos = read_replace_property_value(&self.edge_annos)?;
            replace_edge_annos(graph, &mut update, edge_annos)?;
        }
        progress_reporter.worked(1)?;
        if !self.namespaces.is_empty() {
            let namespaces = read_replace_property_value(&self.namespaces)?;
            let replacements = namespaces
                .into_iter()
                .map(|(k, k_opt)| {
                    let old_namespace = k.name.to_string();
                    let new_namespace = match k_opt {
                        None => None,
                        Some(v) => Some(v.name.to_string()),
                    };
                    (old_namespace, new_namespace)
                })
                .collect_vec();
            replace_namespaces(graph, &mut update, replacements)?;
        }
        progress_reporter.worked(1)?;
        if !self.components.is_empty() {
            revise_components(graph, &self.components, &mut update, &progress_reporter)?;
        }
        progress_reporter.worked(1)?;
        if !self.remove_subgraph.is_empty() {
            for node_name in &self.remove_subgraph {
                remove_subgraph(graph, &mut update, node_name)?;
            }
        }
        graph.apply_update(&mut update, |_| {})?;
        progress_reporter.worked(1)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::fs;
    use std::path::Path;

    use crate::exporter::graphml::GraphMLExporter;
    use crate::manipulator::re::{parse_component_string, to_component_map, Revise};
    use crate::manipulator::Manipulator;
    use crate::progress::ProgressReporter;
    use crate::test_util::export_to_string;
    use crate::{Result, StepID};

    use graphannis::corpusstorage::{QueryLanguage, ResultOrder, SearchQuery};
    use graphannis::model::{AnnotationComponent, AnnotationComponentType};
    use graphannis::update::{GraphUpdate, UpdateEvent};
    use graphannis::{AnnotationGraph, CorpusStorage};
    use graphannis_core::annostorage::ValueSearch;
    use graphannis_core::graph::{ANNIS_NS, NODE_NAME_KEY, NODE_TYPE_KEY};
    use insta::assert_snapshot;
    use itertools::Itertools;
    use tempfile::{tempdir, tempfile};

    use super::revise_components;

    #[test]
    fn test_remove_in_mem() {
        let r = core_test(false, false);
        assert_eq!(r.is_ok(), true, "Probing core test result {:?}", r);
    }

    #[test]
    fn test_remove_on_disk() {
        let r = core_test(true, false);
        assert_eq!(r.is_ok(), true, "Probing core test result {:?}", r);
    }

    #[test]
    fn test_rename_in_mem() {
        let r = core_test(false, true);
        assert_eq!(r.is_ok(), true, "Probing core test result {:?}", r);
    }

    #[test]
    fn test_rename_on_disk() {
        let r = core_test(true, true);
        assert_eq!(r.is_ok(), true, "Probing core test result {:?}", r);
    }

    fn core_test(on_disk: bool, rename: bool) -> Result<()> {
        let mut g = input_graph(on_disk, false)?;
        let (node_anno_prop_val, edge_anno_prop_val) = if rename {
            ("pos = \"upos\"", "deprel = \"func\"")
        } else {
            ("pos = \"\"", "deprel = \"\"")
        };
        let node_map: BTreeMap<String, String> = toml::from_str(node_anno_prop_val)?;
        let edge_map: BTreeMap<String, String> = toml::from_str(edge_anno_prop_val)?;
        let replace = Revise {
            node_names: BTreeMap::default(),
            remove_nodes: vec![],
            move_node_annos: false,
            node_annos: node_map,
            edge_annos: edge_map,
            namespaces: BTreeMap::default(),
            components: BTreeMap::default(),
            remove_subgraph: vec![],
        };
        let step_id = StepID {
            module_name: "replace".to_string(),
            path: None,
        };
        let result = replace.manipulate_corpus(&mut g, tempdir()?.path(), step_id, None);
        assert_eq!(result.is_ok(), true, "Probing merge result {:?}", &result);
        let mut e_g = if rename {
            input_graph(on_disk, true)?
        } else {
            expected_output_graph(on_disk)?
        };
        // corpus nodes
        let e_corpus_nodes: BTreeSet<String> = e_g
            .get_node_annos()
            .exact_anno_search(
                Some(&NODE_TYPE_KEY.ns),
                &NODE_TYPE_KEY.name,
                ValueSearch::Some("corpus"),
            )
            .into_iter()
            .map(|r| r.unwrap().node)
            .map(|id_| {
                e_g.get_node_annos()
                    .get_value_for_item(&id_, &NODE_NAME_KEY)
                    .unwrap()
                    .unwrap()
                    .to_string()
            })
            .collect();
        let g_corpus_nodes: BTreeSet<String> = g
            .get_node_annos()
            .exact_anno_search(
                Some(&NODE_TYPE_KEY.ns),
                &NODE_TYPE_KEY.name,
                ValueSearch::Some("corpus"),
            )
            .into_iter()
            .map(|r| r.unwrap().node)
            .map(|id_| {
                g.get_node_annos()
                    .get_value_for_item(&id_, &NODE_NAME_KEY)
                    .unwrap()
                    .unwrap()
                    .to_string()
            })
            .collect();
        assert_eq!(e_corpus_nodes, g_corpus_nodes); //TODO clarify: Delegate or assertion?
                                                    // test by components
        let e_c_list = e_g
            .get_all_components(None, None)
            .into_iter()
            .filter(|c| e_g.get_graphstorage(c).unwrap().source_nodes().count() > 0)
            .collect_vec();
        let g_c_list = g
            .get_all_components(None, None)
            .into_iter()
            .filter(|c| g.get_graphstorage(c).unwrap().source_nodes().count() > 0) // graph might contain empty components after merge
            .collect_vec();
        assert_eq!(
            e_c_list.len(),
            g_c_list.len(),
            "components expected:\n{:?};\ncomponents are:\n{:?}",
            &e_c_list,
            &g_c_list
        );
        for c in e_c_list {
            let candidates = g.get_all_components(Some(c.get_type()), Some(c.name.as_str()));
            assert_eq!(candidates.len(), 1);
            let c_o = candidates.get(0);
            assert_eq!(&c, c_o.unwrap());
        }
        // test with queries
        let queries = [
            "tok",
            "text",
            "lemma",
            "pos",
            "upos",
            "node ->dep node",
            "node ->dep[deprel=/.+/] node",
            "node ->dep[func=/.+/] node",
        ];
        let corpus_name = "current";
        let tmp_dir_e = tempdir()?;
        let tmp_dir_g = tempdir()?;
        e_g.save_to(&tmp_dir_e.path().join(corpus_name))?;
        g.save_to(&tmp_dir_g.path().join(corpus_name))?;
        let cs_e = CorpusStorage::with_auto_cache_size(&tmp_dir_e.path(), true)?;
        let cs_g = CorpusStorage::with_auto_cache_size(&tmp_dir_g.path(), true)?;
        for query_s in queries {
            let query = SearchQuery {
                corpus_names: &[corpus_name],
                query: query_s,
                query_language: QueryLanguage::AQL,
                timeout: None,
            };
            let matches_e = cs_e.find(query.clone(), 0, None, ResultOrder::Normal)?;
            let matches_g = cs_g.find(query, 0, None, ResultOrder::Normal)?;
            assert_eq!(
                matches_e.len(),
                matches_g.len(),
                "Failed with query: {}",
                query_s
            );
            for (m_e, m_g) in matches_e.into_iter().zip(matches_g.into_iter()) {
                assert_eq!(m_e, m_g);
            }
        }
        Ok(())
    }

    #[test]
    fn test_move_on_disk() {
        let r = move_test(true);
        assert_eq!(r.is_ok(), true, "Probing move test result {:?}", r);
    }

    #[test]
    fn test_move_in_mem() {
        let r = move_test(false);
        assert_eq!(r.is_ok(), true, "Probing move test result {:?}", r);
    }

    fn move_test(on_disk: bool) -> Result<()> {
        let mut g = input_graph_for_move(on_disk)?;
        let node_map: BTreeMap<String, String> = toml::from_str(
            r#"
            "norm::pos" = "dipl::derived_pos"
        "#,
        )?;
        let replace = Revise {
            node_names: BTreeMap::default(),
            move_node_annos: true,
            namespaces: BTreeMap::default(),
            node_annos: node_map,
            edge_annos: BTreeMap::default(),
            remove_nodes: vec![],
            components: BTreeMap::default(),
            remove_subgraph: vec![],
        };
        let step_id = StepID {
            module_name: "replace".to_string(),
            path: None,
        };
        let result = replace.manipulate_corpus(&mut g, tempdir()?.path(), step_id, None);
        assert_eq!(result.is_ok(), true, "Probing merge result {:?}", &result);
        let mut e_g = expected_output_for_move(on_disk)?;
        // corpus nodes
        let e_corpus_nodes: BTreeSet<String> = e_g
            .get_node_annos()
            .exact_anno_search(
                Some(&NODE_TYPE_KEY.ns),
                &NODE_TYPE_KEY.name,
                ValueSearch::Some("corpus"),
            )
            .into_iter()
            .map(|r| r.unwrap().node)
            .map(|id_| {
                e_g.get_node_annos()
                    .get_value_for_item(&id_, &NODE_NAME_KEY)
                    .unwrap()
                    .unwrap()
                    .to_string()
            })
            .collect();
        let g_corpus_nodes: BTreeSet<String> = g
            .get_node_annos()
            .exact_anno_search(
                Some(&NODE_TYPE_KEY.ns),
                &NODE_TYPE_KEY.name,
                ValueSearch::Some("corpus"),
            )
            .into_iter()
            .map(|r| r.unwrap().node)
            .map(|id_| {
                g.get_node_annos()
                    .get_value_for_item(&id_, &NODE_NAME_KEY)
                    .unwrap()
                    .unwrap()
                    .to_string()
            })
            .collect();
        assert_eq!(e_corpus_nodes, g_corpus_nodes); //TODO clarify: Delegate or assertion?
                                                    // test by components
        let e_c_list = e_g
            .get_all_components(None, None)
            .into_iter()
            .filter(|c| e_g.get_graphstorage(c).unwrap().source_nodes().count() > 0)
            .collect_vec();
        let g_c_list = g
            .get_all_components(None, None)
            .into_iter()
            .filter(|c| g.get_graphstorage(c).unwrap().source_nodes().count() > 0) // graph might contain empty components after merge
            .collect_vec();
        assert_eq!(
            e_c_list.len(),
            g_c_list.len(),
            "components expected:\n{:?};\ncomponents are:\n{:?}",
            &e_c_list,
            &g_c_list
        );
        for c in e_c_list {
            let candidates = g.get_all_components(Some(c.get_type()), Some(c.name.as_str()));
            assert_eq!(candidates.len(), 1);
            let c_o = candidates.get(0);
            assert_eq!(&c, c_o.unwrap());
        }
        // test with queries
        let queries = ["tok", "pos", "derived_pos"];
        let corpus_name = "current";
        let tmp_dir_e = tempdir()?;
        let tmp_dir_g = tempdir()?;
        e_g.save_to(&tmp_dir_e.path().join(corpus_name))?;
        g.save_to(&tmp_dir_g.path().join(corpus_name))?;
        let cs_e = CorpusStorage::with_auto_cache_size(&tmp_dir_e.path(), true)?;
        let cs_g = CorpusStorage::with_auto_cache_size(&tmp_dir_g.path(), true)?;
        for query_s in queries {
            let query = SearchQuery {
                corpus_names: &[corpus_name],
                query: query_s,
                query_language: QueryLanguage::AQL,
                timeout: None,
            };
            let matches_e = cs_e.find(query.clone(), 0, None, ResultOrder::Normal)?;
            let matches_g = cs_g.find(query, 0, None, ResultOrder::Normal)?;
            assert_eq!(
                matches_e.len(),
                matches_g.len(),
                "Failed with query: {}",
                query_s
            );
            for (m_e, m_g) in matches_e
                .into_iter()
                .sorted()
                .zip(matches_g.into_iter().sorted())
            {
                assert_eq!(m_e, m_g);
            }
        }
        Ok(())
    }

    #[test]
    fn test_export_mem() {
        let export = export_test(false);
        assert_eq!(
            export.is_ok(),
            true,
            "Export test ends with Err: {:?}",
            &export
        );
    }

    #[test]
    fn test_export_disk() {
        let export = export_test(true);
        assert_eq!(
            export.is_ok(),
            true,
            "Export test ends with Err: {:?}",
            &export
        );
    }

    fn export_test(on_disk: bool) -> Result<()> {
        let mut g = input_graph(on_disk, false)?;
        let node_map: BTreeMap<String, String> = toml::from_str(
            r#"
            pos = ""
        "#,
        )?;
        let edge_map: BTreeMap<String, String> = toml::from_str(
            r#"
            deprel = ""
        "#,
        )?;
        let replace = Revise {
            node_names: BTreeMap::default(),
            move_node_annos: true,
            namespaces: BTreeMap::default(),
            node_annos: node_map,
            edge_annos: edge_map,
            remove_nodes: vec![],
            components: BTreeMap::default(),
            remove_subgraph: vec![],
        };
        let step_id = StepID {
            module_name: "replace".to_string(),
            path: None,
        };
        assert_eq!(
            replace
                .manipulate_corpus(&mut g, tempdir()?.path(), step_id, None)
                .is_ok(),
            true
        );
        let tmp_file = tempfile()?;
        let export =
            graphannis_core::graph::serialization::graphml::export(&g, None, tmp_file, |_| {});
        assert_eq!(export.is_ok(), true, "Export fails: {:?}", &export);
        Ok(())
    }

    #[test]
    fn test_export_move_result_mem() {
        let export = export_test_move_result(false);
        assert_eq!(
            export.is_ok(),
            true,
            "Testing export of move result ends with Err: {:?}",
            &export
        );
    }

    #[test]
    fn test_export_move_result_disk() {
        let export = export_test_move_result(true);
        assert_eq!(
            export.is_ok(),
            true,
            "Testing export of move result ends with Err: {:?}",
            &export
        );
    }

    fn export_test_move_result(on_disk: bool) -> Result<()> {
        let mut g = input_graph_for_move(on_disk)?;
        let node_map: BTreeMap<String, String> = toml::from_str(
            r#"
            "norm::pos" = "dipl::derived_pos"
        "#,
        )?;
        let edge_map: BTreeMap<String, String> = toml::from_str(
            r#"
            deprel = ""
        "#,
        )?;
        let replace = Revise {
            node_names: BTreeMap::default(),
            move_node_annos: true,
            namespaces: BTreeMap::default(),
            node_annos: node_map,
            edge_annos: edge_map,
            remove_nodes: vec![],
            components: BTreeMap::default(),
            remove_subgraph: vec![],
        };
        let step_id = StepID {
            module_name: "replace".to_string(),
            path: None,
        };
        assert_eq!(
            replace
                .manipulate_corpus(&mut g, tempdir()?.path(), step_id, None)
                .is_ok(),
            true
        );
        let tmp_file = tempfile()?;
        let export =
            graphannis_core::graph::serialization::graphml::export(&g, None, tmp_file, |_| {});
        assert_eq!(export.is_ok(), true, "Export fails: {:?}", &export);
        Ok(())
    }

    #[test]
    fn namespace_test_in_mem() {
        let r = namespace_test(false);
        assert_eq!(r.is_ok(), true, "Failed with: {:?}", &r);
    }

    #[test]
    fn namespace_test_on_disk() {
        let r = namespace_test(true);
        assert_eq!(r.is_ok(), true, "Failed with: {:?}", &r);
    }

    fn namespace_test(on_disk: bool) -> Result<()> {
        let mut g = namespace_test_graph(on_disk, false)?;
        let ns_map: BTreeMap<String, String> = toml::from_str(
            r#"
            ud = "default_ns"
            "" = "default_ns"
        "#,
        )?;
        let replace = Revise {
            node_names: BTreeMap::default(),
            remove_nodes: vec![],
            move_node_annos: false,
            node_annos: BTreeMap::default(),
            edge_annos: BTreeMap::default(),
            namespaces: ns_map,
            components: BTreeMap::default(),
            remove_subgraph: vec![],
        };
        let step_id = StepID {
            module_name: "replace".to_string(),
            path: None,
        };
        let op_result = replace.manipulate_corpus(&mut g, tempdir()?.path(), step_id, None);
        assert_eq!(
            op_result.is_ok(),
            true,
            "Replacing namespaces failed: {:?}",
            &op_result
        );
        let mut e_g = namespace_test_graph(on_disk, true)?;
        // corpus nodes
        let e_corpus_nodes: BTreeSet<String> = e_g
            .get_node_annos()
            .exact_anno_search(
                Some(&NODE_TYPE_KEY.ns),
                &NODE_TYPE_KEY.name,
                ValueSearch::Some("corpus"),
            )
            .into_iter()
            .map(|r| r.unwrap().node)
            .map(|id_| {
                e_g.get_node_annos()
                    .get_value_for_item(&id_, &NODE_NAME_KEY)
                    .unwrap()
                    .unwrap()
                    .to_string()
            })
            .collect();
        let g_corpus_nodes: BTreeSet<String> = g
            .get_node_annos()
            .exact_anno_search(
                Some(&NODE_TYPE_KEY.ns),
                &NODE_TYPE_KEY.name,
                ValueSearch::Some("corpus"),
            )
            .into_iter()
            .map(|r| r.unwrap().node)
            .map(|id_| {
                g.get_node_annos()
                    .get_value_for_item(&id_, &NODE_NAME_KEY)
                    .unwrap()
                    .unwrap()
                    .to_string()
            })
            .collect();
        assert_eq!(e_corpus_nodes, g_corpus_nodes); //TODO clarify: Delegate or assertion?
                                                    // test by components
        let e_c_list = e_g
            .get_all_components(None, None)
            .into_iter()
            .filter(|c| e_g.get_graphstorage(c).unwrap().source_nodes().count() > 0)
            .collect_vec();
        let g_c_list = g
            .get_all_components(None, None)
            .into_iter()
            .filter(|c| g.get_graphstorage(c).unwrap().source_nodes().count() > 0) // graph might contain empty components after merge
            .collect_vec();
        assert_eq!(
            e_c_list.len(),
            g_c_list.len(),
            "components expected:\n{:?};\ncomponents are:\n{:?}",
            &e_c_list,
            &g_c_list
        );
        for c in e_c_list {
            let candidates = g.get_all_components(Some(c.get_type()), Some(c.name.as_str()));
            assert_eq!(candidates.len(), 1);
            let c_o = candidates.get(0);
            assert_eq!(&c, c_o.unwrap());
        }
        // test with queries
        let queries = [
            "tok",
            "pos",
            "ud:pos",
            "default_ns:pos",
            "lemma",
            "default_ns:lemma",
            "node ->dep[func=/.*/] node",
            "node ->dep[ud:func=/.*/] node",
            "node ->dep[default_ns:func=/.*/] node",
        ];
        let corpus_name = "current";
        let tmp_dir_e = tempdir()?;
        let tmp_dir_g = tempdir()?;
        e_g.save_to(&tmp_dir_e.path().join(corpus_name))?;
        g.save_to(&tmp_dir_g.path().join(corpus_name))?;
        let cs_e = CorpusStorage::with_auto_cache_size(&tmp_dir_e.path(), true)?;
        let cs_g = CorpusStorage::with_auto_cache_size(&tmp_dir_g.path(), true)?;
        for query_s in queries {
            let query = SearchQuery {
                corpus_names: &[corpus_name],
                query: query_s,
                query_language: QueryLanguage::AQL,
                timeout: None,
            };
            let matches_e = cs_e.find(query.clone(), 0, None, ResultOrder::Normal)?;
            let matches_g = cs_g.find(query, 0, None, ResultOrder::Normal)?;
            assert_eq!(
                matches_e.len(),
                matches_g.len(),
                "Failed with query: {}",
                query_s
            );
            for (m_e, m_g) in matches_e.into_iter().zip(matches_g.into_iter()) {
                assert_eq!(m_e, m_g);
            }
        }
        Ok(())
    }

    fn input_graph(on_disk: bool, new_names: bool) -> Result<AnnotationGraph> {
        let mut g = AnnotationGraph::with_default_graphstorages(on_disk)?;
        let mut u = GraphUpdate::default();
        u.add_event(UpdateEvent::AddNode {
            node_name: "root".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "root/b".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "root/b".to_string(),
            target_node: "root".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "root/b/doc".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "root/b/doc".to_string(),
            target_node: "root/b".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        let pos_name = if new_names { "upos" } else { "pos" };
        for (ii, (txt, lemma_label, pos_label)) in [
            ("I", "I", "PRON"),
            ("am", "be", "VERB"),
            ("in", "in", "ADP"),
            ("Berlin", "Berlin", "PROPN"),
        ]
        .iter()
        .enumerate()
        {
            let i = ii + 1;
            let name = format!("root/b/doc#t{}", i);
            u.add_event(UpdateEvent::AddNode {
                node_name: name.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: txt.to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: "".to_string(),
                anno_name: "text".to_string(),
                anno_value: txt.to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: "".to_string(),
                anno_name: "lemma".to_string(),
                anno_value: lemma_label.to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: "".to_string(),
                anno_name: pos_name.to_string(),
                anno_value: pos_label.to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdge {
                source_node: name.to_string(),
                target_node: "root/b/doc".to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string(),
            })?;
            if i > 1 {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("root/b/doc#t{}", i - 1),
                    target_node: name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "".to_string(),
                })?;
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("root/b/doc#t{}", i - 1),
                    target_node: name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "text".to_string(),
                })?;
            }
        }
        let dep_layer_name = "syntax";
        let dep_comp_name = "dep";
        let deprel_name = if new_names { "func" } else { "deprel" };
        for (source, target, label) in
            [(2, 1, "subj"), (2, 3, "comp:pred"), (3, 4, "comp:obj")].iter()
        {
            let source_name = format!("root/b/doc#t{}", source);
            let target_name = format!("root/b/doc#t{}", target);
            u.add_event(UpdateEvent::AddEdge {
                source_node: source_name.to_string(),
                target_node: target_name.to_string(),
                layer: dep_layer_name.to_string(),
                component_type: AnnotationComponentType::Pointing.to_string(),
                component_name: dep_comp_name.to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdgeLabel {
                source_node: source_name,
                target_node: target_name,
                layer: dep_layer_name.to_string(),
                component_type: AnnotationComponentType::Pointing.to_string(),
                component_name: dep_comp_name.to_string(),
                anno_ns: "".to_string(),
                anno_name: deprel_name.to_string(),
                anno_value: label.to_string(),
            })?;
        }
        g.apply_update(&mut u, |_msg| {})?;
        Ok(g)
    }

    fn expected_output_graph(on_disk: bool) -> Result<AnnotationGraph> {
        let mut g = AnnotationGraph::with_default_graphstorages(on_disk)?;
        let mut u = GraphUpdate::default();
        u.add_event(UpdateEvent::AddNode {
            node_name: "root".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "root/b".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "root/b".to_string(),
            target_node: "root".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "root/b/doc".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "root/b/doc".to_string(),
            target_node: "root/b".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        for (ii, (txt, lemma_label)) in
            [("I", "I"), ("am", "be"), ("in", "in"), ("Berlin", "Berlin")]
                .iter()
                .enumerate()
        {
            let i = ii + 1;
            let name = format!("root/b/doc#t{}", i);
            u.add_event(UpdateEvent::AddNode {
                node_name: name.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: txt.to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: "".to_string(),
                anno_name: "text".to_string(),
                anno_value: txt.to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: "".to_string(),
                anno_name: "lemma".to_string(),
                anno_value: lemma_label.to_string(),
            })?;
            if i > 1 {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("root/b/doc#t{}", i - 1),
                    target_node: name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "".to_string(),
                })?;
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("root/b/doc#t{}", i - 1),
                    target_node: name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "text".to_string(),
                })?;
            }
        }
        let dep_layer_name = "syntax";
        let dep_comp_name = "dep";
        for (source, target) in [(2, 1), (2, 3), (3, 4)].iter() {
            let source_name = format!("root/b/doc#t{}", source);
            let target_name = format!("root/b/doc#t{}", target);
            u.add_event(UpdateEvent::AddEdge {
                source_node: source_name.to_string(),
                target_node: target_name.to_string(),
                layer: dep_layer_name.to_string(),
                component_type: AnnotationComponentType::Pointing.to_string(),
                component_name: dep_comp_name.to_string(),
            })?;
        }
        g.apply_update(&mut u, |_msg| {})?;
        Ok(g)
    }

    fn input_graph_for_move(on_disk: bool) -> Result<AnnotationGraph> {
        let mut g = AnnotationGraph::with_default_graphstorages(on_disk)?;
        let mut u = GraphUpdate::default();
        u.add_event(UpdateEvent::AddNode {
            node_name: "root".to_string(),
            node_type: "corpus".to_string(),
        })?;
        // import 1
        u.add_event(UpdateEvent::AddNode {
            node_name: "root/a".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "root/a".to_string(),
            target_node: "root".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "root/a/doc".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "root/a/doc".to_string(),
            target_node: "root/a".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        for i in 0..5 {
            u.add_event(UpdateEvent::AddNode {
                node_name: format!("root/a/doc#t{}", i),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: format!("root/a/doc#t{}", i),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: " ".to_string(),
            })?;
            if i > 1 {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("root/a/doc#t{}", i - 1),
                    target_node: format!("root/a/doc#t{}", i),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "".to_string(),
                })?;
            }
        }
        // fake-tok 1
        let sentence_span_name = "root/a/doc#s0";
        u.add_event(UpdateEvent::AddNode {
            node_name: sentence_span_name.to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: sentence_span_name.to_string(),
            anno_ns: "dipl".to_string(),
            anno_name: "sentence".to_string(),
            anno_value: "1".to_string(),
        })?;
        for (ii, (txt, start, end)) in [("I'm", 0, 2), ("in", 2, 3), ("New", 3, 4), ("York", 4, 5)]
            .iter()
            .enumerate()
        {
            let i = ii + 1;
            let name = format!("root/a/doc#s{}", i);
            u.add_event(UpdateEvent::AddNode {
                node_name: name.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: txt.to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: "".to_string(),
                anno_name: "dipl".to_string(),
                anno_value: txt.to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdge {
                source_node: sentence_span_name.to_string(),
                target_node: name.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::Coverage.to_string(),
                component_name: "".to_string(),
            })?;
            for j in *start..*end {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: name.to_string(),
                    target_node: format!("root/a/doc#t{}", j),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Coverage.to_string(),
                    component_name: "".to_string(),
                })?;
            }
            if i > 1 {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("root/a/doc#s{}", i - 1),
                    target_node: name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "dipl".to_string(),
                })?;
            }
        }
        // fake-tok 2
        for (ii, (txt, start, end, pos_label)) in [
            ("I", 0, 1, "PRON"),
            ("am", 1, 2, "VERB"),
            ("in", 2, 3, "ADP"),
            ("New York", 3, 5, "PROPN"),
        ]
        .iter()
        .enumerate()
        {
            let i = ii + 5;
            let name = format!("root/a/doc#s{}", i);
            u.add_event(UpdateEvent::AddNode {
                node_name: name.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: txt.to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: "".to_string(),
                anno_name: "norm".to_string(),
                anno_value: txt.to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: "norm".to_string(),
                anno_name: "pos".to_string(),
                anno_value: pos_label.to_string(),
            })?;
            for j in *start..*end {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: name.to_string(),
                    target_node: format!("root/a/doc#t{}", j),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Coverage.to_string(),
                    component_name: "".to_string(),
                })?;
            }
            if ii > 0 {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("root/a/doc#s{}", i - 1),
                    target_node: name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "norm".to_string(),
                })?;
            }
        }
        g.apply_update(&mut u, |_msg| {})?;
        Ok(g)
    }

    fn expected_output_for_move(on_disk: bool) -> Result<AnnotationGraph> {
        let mut g = AnnotationGraph::with_default_graphstorages(on_disk)?;
        let mut u = GraphUpdate::default();
        u.add_event(UpdateEvent::AddNode {
            node_name: "root".to_string(),
            node_type: "corpus".to_string(),
        })?;
        // import 1
        u.add_event(UpdateEvent::AddNode {
            node_name: "root/a".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "root/a".to_string(),
            target_node: "root".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "root/a/doc".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "root/a/doc".to_string(),
            target_node: "root/a".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        for i in 0..5 {
            u.add_event(UpdateEvent::AddNode {
                node_name: format!("root/a/doc#t{}", i),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: format!("root/a/doc#t{}", i),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: " ".to_string(),
            })?;
            if i > 1 {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("root/a/doc#t{}", i - 1),
                    target_node: format!("root/a/doc#t{}", i),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "".to_string(),
                })?;
            }
        }
        // fake-tok 1
        let sentence_span_name = "root/a/doc#s0";
        u.add_event(UpdateEvent::AddNode {
            node_name: sentence_span_name.to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: sentence_span_name.to_string(),
            anno_ns: "dipl".to_string(),
            anno_name: "sentence".to_string(),
            anno_value: "1".to_string(),
        })?;
        for (ii, (txt, start, end, pos_label)) in [
            ("I'm", 0, 2, Some("VERB")),
            ("in", 2, 3, Some("ADP")),
            ("New", 3, 4, None),
            ("York", 4, 5, None),
        ]
        .iter()
        .enumerate()
        {
            let i = ii + 1;
            let name = format!("root/a/doc#s{}", i);
            u.add_event(UpdateEvent::AddNode {
                node_name: name.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: txt.to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: "".to_string(),
                anno_name: "dipl".to_string(),
                anno_value: txt.to_string(),
            })?;
            if let Some(v) = pos_label {
                u.add_event(UpdateEvent::AddNodeLabel {
                    node_name: name.to_string(),
                    anno_ns: "dipl".to_string(),
                    anno_name: "derived_pos".to_string(),
                    anno_value: v.to_string(),
                })?;
            }
            u.add_event(UpdateEvent::AddEdge {
                source_node: sentence_span_name.to_string(),
                target_node: name.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::Coverage.to_string(),
                component_name: "".to_string(),
            })?;
            for j in *start..*end {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: name.to_string(),
                    target_node: format!("root/a/doc#t{}", j),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Coverage.to_string(),
                    component_name: "".to_string(),
                })?;
            }
            if i > 1 {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("root/a/doc#s{}", i - 1),
                    target_node: name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "dipl".to_string(),
                })?;
            }
        }
        let span_name = "root/a/doc#s10";
        u.add_event(UpdateEvent::AddNode {
            node_name: span_name.to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: span_name.to_string(),
            anno_ns: "dipl".to_string(),
            anno_name: "derived_pos".to_string(),
            anno_value: "PROPN".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: span_name.to_string(),
            target_node: "root/a/doc#s3".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::Coverage.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: span_name.to_string(),
            target_node: "root/a/doc#s4".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::Coverage.to_string(),
            component_name: "".to_string(),
        })?;
        // fake-tok 2
        for (ii, (txt, start, end)) in [("I", 0, 1), ("am", 1, 2), ("in", 2, 3), ("New York", 3, 5)]
            .iter()
            .enumerate()
        {
            let i = ii + 6;
            let name = format!("root/a/doc#s{}", i);
            u.add_event(UpdateEvent::AddNode {
                node_name: name.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: txt.to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: name.to_string(),
                anno_ns: "".to_string(),
                anno_name: "norm".to_string(),
                anno_value: txt.to_string(),
            })?;
            for j in *start..*end {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: name.to_string(),
                    target_node: format!("root/a/doc#t{}", j),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Coverage.to_string(),
                    component_name: "".to_string(),
                })?;
            }
            if ii > 0 {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("root/a/doc#s{}", i - 1),
                    target_node: name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "norm".to_string(),
                })?;
            }
        }
        g.apply_update(&mut u, |_msg| {})?;
        Ok(g)
    }

    fn namespace_test_graph(on_disk: bool, after: bool) -> Result<AnnotationGraph> {
        let mut g = AnnotationGraph::with_default_graphstorages(on_disk)?;
        let mut u = GraphUpdate::default();
        let corpus_type = "corpus";
        let node_type = "node";
        let doc_path = "root/subnode/doc";
        u.add_event(UpdateEvent::AddNode {
            node_name: "root".to_string(),
            node_type: corpus_type.to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "root/subnode".to_string(),
            node_type: corpus_type.to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: doc_path.to_string(),
            node_type: corpus_type.to_string(),
        })?;
        let default_ns = "default_ns";
        let pos_ns = if after { default_ns } else { "ud" };
        let pos_name = "pos";
        let lemma_ns = if after { default_ns } else { "" };
        let lemma_name = "lemma";
        for (i, (text, pos_value, lemma_value)) in [
            ("This", "PRON", "this"),
            ("is", "VERB", "be"),
            ("a", "DET", "a"),
            ("test", "NOUN", "test"),
        ]
        .iter()
        .enumerate()
        {
            let tok_name = format!("{}#t{}", doc_path, &(i + 1));
            u.add_event(UpdateEvent::AddNode {
                node_name: tok_name.to_string(),
                node_type: node_type.to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: tok_name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: text.to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: tok_name.to_string(),
                anno_ns: lemma_ns.to_string(),
                anno_name: lemma_name.to_string(),
                anno_value: lemma_value.to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: tok_name.to_string(),
                anno_ns: pos_ns.to_string(),
                anno_name: pos_name.to_string(),
                anno_value: pos_value.to_string(),
            })?;
            if i.gt(&0) {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("{}#t{}", doc_path, &i),
                    target_node: tok_name,
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "".to_string(),
                })?;
            }
        }
        let func_name = "func";
        let func_ns = if after { default_ns } else { "ud" };
        for (source_i, target_i, func_value) in [(4, 1, "subj"), (4, 2, "cop"), (4, 3, "det")] {
            let source_name = format!("{}#t{}", doc_path, &source_i);
            let target_name = format!("{}#t{}", doc_path, &target_i);
            u.add_event(UpdateEvent::AddEdge {
                source_node: source_name.to_string(),
                target_node: target_name.to_string(),
                layer: "".to_string(),
                component_type: AnnotationComponentType::Pointing.to_string(),
                component_name: "dep".to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdgeLabel {
                source_node: source_name.to_string(),
                target_node: target_name.to_string(),
                layer: "".to_string(),
                component_type: AnnotationComponentType::Pointing.to_string(),
                component_name: "dep".to_string(),
                anno_ns: func_ns.to_string(),
                anno_name: func_name.to_string(),
                anno_value: func_value.to_string(),
            })?;
        }
        g.apply_update(&mut u, |_| {})?;
        Ok(g)
    }

    #[test]
    fn test_deserialize_map_components() {
        let path = Path::new("./tests/data/graph_op/re/map_component.toml");
        let toml_string = fs::read_to_string(path);
        assert!(toml_string.is_ok(), "Could not read test file: {:?}", path);
        let r: std::result::Result<BTreeMap<String, String>, toml::de::Error> =
            toml::from_str(toml_string.unwrap().as_str());
        assert!(r.is_ok(), "Could not parse test file: {:?}", &r.err());
    }

    #[test]
    fn test_modify_component_in_mem() {
        let r = test_modify_component(false);
        assert!(r.is_ok(), "Error occured: {:?}", r.err());
    }

    #[test]
    fn test_modify_component_on_disk() {
        let r = test_modify_component(true);
        assert!(r.is_ok(), "Error occured: {:?}", r.err());
    }

    fn test_modify_component(on_disk: bool) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut g = input_graph_for_move(on_disk)
            .map_err(|_| assert!(false))
            .unwrap();
        let previous_components = g.get_all_components(None, None);
        let mut erased_components = Vec::new();
        let mut new_components = Vec::new();
        let mut component_mod_config = BTreeMap::new();
        for existing_component in previous_components {
            let (old_ctype_str, new_ctype_str, new_ctype) = match existing_component.get_type() {
                AnnotationComponentType::Coverage => continue,
                AnnotationComponentType::Dominance => {
                    ("dominance", "pointing", AnnotationComponentType::Pointing)
                }
                AnnotationComponentType::Pointing => {
                    ("pointing", "dominance", AnnotationComponentType::Dominance)
                }
                AnnotationComponentType::Ordering => {
                    ("ordering", "ordering", AnnotationComponentType::Ordering)
                }
                AnnotationComponentType::LeftToken => continue,
                AnnotationComponentType::RightToken => continue,
                AnnotationComponentType::PartOf => continue,
            };
            let key_str = format!(
                "{old_ctype_str}::{}::{}",
                existing_component.layer.to_string(),
                existing_component.name.to_string()
            );
            let val_str = format!(
                "{new_ctype_str}::::moved_{}",
                existing_component.name.to_string()
            );
            component_mod_config.insert(key_str, val_str);
            let new_c = AnnotationComponent::new(
                new_ctype,
                "".into(),
                format!("moved_{}", existing_component.name).into(),
            );
            new_components.push(new_c);
            erased_components.push(existing_component);
        }
        let op = Revise {
            node_names: BTreeMap::default(),
            remove_nodes: vec![],
            move_node_annos: false,
            node_annos: BTreeMap::default(),
            edge_annos: BTreeMap::default(),
            namespaces: BTreeMap::default(),
            components: component_mod_config,
            remove_subgraph: vec![],
        };
        let step_id = StepID {
            module_name: "replace".to_string(),
            path: None,
        };
        let r = op.manipulate_corpus(&mut g, Path::new("./"), step_id, None);
        assert!(r.is_ok(), "graph op returned error: {:?}", r.err());
        let current_components = g.get_all_components(None, None);
        for ec in erased_components {
            let storage = g.get_graphstorage(&ec);
            assert!(
                !current_components.contains(&ec)
                    || (storage.is_some() && storage.unwrap().source_nodes().count() == 0)
            );
        }
        for nc in new_components {
            assert!(current_components.contains(&nc));
        }
        Ok(())
    }

    #[test]
    fn deserialization_test() {
        let toml_str = fs::read_to_string("tests/data/graph_op/re/deser_test.toml")
            .map_err(|_| assert!(false))
            .unwrap();
        let revise: Revise = toml::from_str(toml_str.as_str())
            .map_err(|e| assert!(false, "{:?}", e))
            .unwrap();
        assert_eq!(
            vec!["any_weird_node_address".to_string()],
            revise.remove_nodes
        );
        assert!(revise.move_node_annos);
        let mut name_map = BTreeMap::new();
        name_map.insert("norm::pos".to_string(), "norm::POS".to_string());
        name_map.insert("norm::lemma".to_string(), "norm::LEMMA".to_string());
        assert_eq!(name_map, revise.node_annos);
        name_map = BTreeMap::new();
        name_map.insert("deprel".to_string(), "func".to_string());
        assert_eq!(name_map, revise.edge_annos);
        name_map = BTreeMap::new();
        name_map.insert("default_ns".to_string(), "".to_string());
        assert_eq!(name_map, revise.namespaces);
        name_map = BTreeMap::new();
        name_map.insert(
            "ordering::annis::text".to_string(),
            "ordering::default_ns::text".to_string(),
        );
        name_map.insert(
            "ordering::annis::".to_string(),
            "ordering::::default_ordering".to_string(),
        );
        name_map.insert("dominance::annis::syntax".to_string(), "".to_string());
        assert_eq!(name_map, revise.components);
        let component_map = to_component_map(&revise.components)
            .map_err(|_| assert!(false))
            .unwrap();
        let mut target_map = BTreeMap::new();
        target_map.insert(
            AnnotationComponent::new(
                AnnotationComponentType::Ordering,
                ANNIS_NS.into(),
                "text".into(),
            ),
            Some(AnnotationComponent::new(
                AnnotationComponentType::Ordering,
                "default_ns".into(),
                "text".into(),
            )),
        );
        target_map.insert(
            AnnotationComponent::new(
                AnnotationComponentType::Ordering,
                ANNIS_NS.into(),
                "".into(),
            ),
            Some(AnnotationComponent::new(
                AnnotationComponentType::Ordering,
                "".into(),
                "default_ordering".into(),
            )),
        );
        target_map.insert(
            AnnotationComponent::new(
                AnnotationComponentType::Dominance,
                ANNIS_NS.into(),
                "syntax".into(),
            ),
            None,
        );
        assert_eq!(target_map, component_map);
    }

    #[test]
    fn test_parse_component_string() {
        assert!(parse_component_string("no_delimiter").is_err());
        assert!(parse_component_string("invalid_type::layer::name").is_err());
        assert!(parse_component_string("partof::layer::name").is_ok());
        assert!(parse_component_string("ordering::layer::name").is_ok());
        assert!(parse_component_string("coverage::layer::name").is_ok());
        assert!(parse_component_string("dominance::layer::name").is_ok());
        assert!(parse_component_string("pointing::layer::name").is_ok());
        assert!(parse_component_string("l::layer::name").is_ok());
        assert!(parse_component_string("r::layer::name").is_ok());
        assert!(parse_component_string("partof::::").is_ok());
    }

    #[test]
    fn test_component_updates_in_mem() {
        let r = test_component_updates(false);
        assert!(r.is_ok());
    }

    #[test]
    fn test_component_updates_on_disk() {
        let r = test_component_updates(true);
        assert!(r.is_ok());
    }

    fn test_component_updates(
        on_disk: bool,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let source_node_name = "node_a";
        let target_node_name = "node_b";
        let source_component = AnnotationComponent::new(
            AnnotationComponentType::Ordering,
            ANNIS_NS.into(),
            "".into(),
        );
        let target_component = AnnotationComponent::new(
            AnnotationComponentType::Ordering,
            "".into(),
            "default_ordering".into(),
        );
        let mut build_update = GraphUpdate::default();
        build_update.add_event(UpdateEvent::AddNode {
            node_name: "document".to_string(),
            node_type: "corpus".to_string(),
        })?;
        build_update.add_event(UpdateEvent::AddNode {
            node_name: source_node_name.to_string(),
            node_type: "node".to_string(),
        })?;
        build_update.add_event(UpdateEvent::AddNode {
            node_name: target_node_name.to_string(),
            node_type: "node".to_string(),
        })?;
        build_update.add_event(UpdateEvent::AddEdge {
            source_node: source_node_name.to_string(),
            target_node: "document".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        build_update.add_event(UpdateEvent::AddEdge {
            source_node: target_node_name.to_string(),
            target_node: "document".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        build_update.add_event(UpdateEvent::AddEdge {
            source_node: source_node_name.to_string(),
            target_node: target_node_name.to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::Ordering.to_string(),
            component_name: "".to_string(),
        })?;
        build_update.add_event(UpdateEvent::AddEdgeLabel {
            source_node: source_node_name.to_string(),
            target_node: target_node_name.to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::Ordering.to_string(),
            component_name: "".to_string(),
            anno_ns: "".to_string(),
            anno_name: "info".to_string(),
            anno_value: "note this info".to_string(),
        })?;
        let mut g = AnnotationGraph::with_default_graphstorages(on_disk)?;
        g.apply_update(&mut build_update, |_| {})?;
        let mut expected_update = GraphUpdate::default();
        expected_update.add_event(UpdateEvent::DeleteEdge {
            source_node: source_node_name.to_string(),
            target_node: target_node_name.to_string(),
            layer: source_component.layer.to_string(),
            component_type: source_component.get_type().to_string(),
            component_name: source_component.name.to_string(),
        })?;
        expected_update.add_event(UpdateEvent::AddEdge {
            source_node: source_node_name.to_string(),
            target_node: target_node_name.to_string(),
            layer: target_component.layer.to_string(),
            component_type: target_component.get_type().to_string(),
            component_name: target_component.name.to_string(),
        })?;
        expected_update.add_event(UpdateEvent::AddEdgeLabel {
            source_node: source_node_name.to_string(),
            target_node: target_node_name.to_string(),
            layer: target_component.layer.to_string(),
            component_type: target_component.get_type().to_string(),
            component_name: target_component.name.to_string(),
            anno_ns: "".to_string(),
            anno_name: "info".to_string(),
            anno_value: "note this info".to_string(),
        })?;
        let mut test_update = GraphUpdate::default();
        let pg = ProgressReporter::new(
            None,
            StepID {
                module_name: "test_revise".to_string(),
                path: None,
            },
            1,
        )?;
        let mut component_config = BTreeMap::new();
        component_config.insert(
            "ordering::annis::".to_string(),
            "ordering::::default_ordering".to_string(),
        );
        revise_components(&g, &component_config, &mut test_update, &pg)?;
        let mut ti = test_update.iter()?;
        for e in expected_update.iter()? {
            let (_, ue) = e?;
            let (_, ue_) = ti.next().unwrap()?;
            match ue {
                UpdateEvent::AddEdge { .. } => assert!(matches!(ue_, UpdateEvent::AddEdge { .. })),
                UpdateEvent::DeleteEdge { .. } => {
                    assert!(matches!(ue_, UpdateEvent::DeleteEdge { .. }))
                }
                UpdateEvent::AddEdgeLabel { .. } => {
                    assert!(matches!(ue_, UpdateEvent::AddEdgeLabel { .. }))
                }
                _ => assert!(false),
            };
        }
        Ok(())
    }

    #[test]
    fn delete_subgraph() {
        let g = input_graph(true, false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let tmp = tempdir();
        assert!(tmp.is_ok());
        let manipulation = Revise {
            remove_subgraph: vec!["root/b".to_string()],
            ..Default::default()
        }
        .manipulate_corpus(
            &mut graph,
            tmp.unwrap().path(),
            StepID {
                module_name: "test_revise".to_string(),
                path: None,
            },
            None,
        );
        assert!(manipulation.is_ok());
        let gs = export_to_string(&graph, GraphMLExporter::default());
        assert!(gs.is_ok());
        let graphml = gs.unwrap();
        assert_snapshot!(graphml);
    }

    #[test]
    fn rename_corpus_node() {
        let g = input_graph(true, false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let tmp = tempdir();
        assert!(tmp.is_ok());
        let node_names = [("root/b", "corpus/subcorpus"), ("root", "corpus")]
            .iter()
            .map(|(old, new)| (old.to_string(), new.to_string()))
            .collect();
        let manipulation = Revise {
            node_names,
            ..Default::default()
        }
        .manipulate_corpus(
            &mut graph,
            tmp.unwrap().path(),
            StepID {
                module_name: "test_rename_nodes".to_string(),
                path: None,
            },
            None,
        );
        assert!(manipulation.is_ok());
        let gs = export_to_string(&graph, GraphMLExporter::default());
        assert!(gs.is_ok());
        let graphml = gs.unwrap();
        assert_snapshot!(graphml);
    }

    #[test]
    fn rename_node_fail_in_use() {
        let g = input_graph(true, false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let tmp = tempdir();
        assert!(tmp.is_ok());
        let node_names = [("root/b", "root/b/doc")]
            .iter()
            .map(|(old, new)| (old.to_string(), new.to_string()))
            .collect();
        let manipulation = Revise {
            node_names,
            ..Default::default()
        }
        .manipulate_corpus(
            &mut graph,
            tmp.unwrap().path(),
            StepID {
                module_name: "test_rename_nodes_fail_in_use".to_string(),
                path: None,
            },
            None,
        );
        assert!(manipulation.is_err());
    }

    #[test]
    fn deletion_by_rename() {
        let g = input_graph(true, false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let tmp = tempdir();
        assert!(tmp.is_ok());
        let node_names = [("root/b", "")]
            .iter()
            .map(|(old, new)| (old.to_string(), new.to_string()))
            .collect();
        let manipulation = Revise {
            node_names,
            ..Default::default()
        }
        .manipulate_corpus(
            &mut graph,
            tmp.unwrap().path(),
            StepID {
                module_name: "test_deletion_by_rename".to_string(),
                path: None,
            },
            None,
        );
        assert!(manipulation.is_ok());
        let gs = export_to_string(&graph, GraphMLExporter::default());
        assert!(gs.is_ok());
        let graphml = gs.unwrap();
        assert_snapshot!(graphml);
    }
}
