use std::{cmp::Ordering, collections::BTreeSet};

use graphannis::{AnnotationGraph, update::{GraphUpdate, UpdateEvent}, graph::{Edge, AnnoKey, Match}, model::{AnnotationComponentType, AnnotationComponent}};
use graphannis_core::{annostorage::ValueSearch, graph::{NODE_NAME_KEY, ANNIS_NS}, dfs::CycleSafeDFS};
use graphannis_core::util::split_qname;
use itertools::Itertools;

use crate::{Manipulator, Module, error::AnnattoError};

pub struct Replace {}

pub const MODULE_NAME: &str = "replace";

impl Default for Replace {
    fn default() -> Self {
        Replace {}
    }
}

impl Module for Replace {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}

const PROP_NODE_ANNOS: &str = "node.annos";
const PROP_NODE_NAMES: &str = "node.names";
const PROP_EDGE_ANNOS: &str = "edge.annos";
const PROP_MOVE: &str = "move.node.annos";
const PROPVAL_SEP: &str = ",";
const PROPVAL_OLD_NEW_SEP: &str = ":=";


fn remove_nodes(graph: &mut AnnotationGraph, names: Vec<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let mut update = GraphUpdate::default();
    for name in names {
        update.add_event(UpdateEvent::DeleteNode { node_name: name.to_string() })?;
    }
    graph.apply_update(&mut update, |_| {})?;
    Ok(())
}


fn label_with_new_target(graph: &AnnotationGraph,
                         update: &mut GraphUpdate,
                         m: &Match,
                         target_key: &AnnoKey) -> Result<(), Box<dyn std::error::Error>> {                            
    let coverage_component = AnnotationComponent::new(AnnotationComponentType::Coverage, ANNIS_NS.into(), "".into());
    let coverage_storage = graph.get_graphstorage(&coverage_component).unwrap();    
    let order_component = AnnotationComponent::new(AnnotationComponentType::Ordering, ANNIS_NS.to_string().into(), target_key.ns.clone());
    let order_storage = graph.get_graphstorage(&order_component).unwrap();
    let node = m.node;
    let mut covered_terminal_nodes = Vec::new();
    CycleSafeDFS::new(coverage_storage.as_edgecontainer(), node, 1, usize::MAX)
    .into_iter()
    .map(|r| r.unwrap().node.clone())
    .filter(|n| !coverage_storage.has_outgoing_edges(*n).unwrap())
    .for_each(|n|covered_terminal_nodes.push(n));
    let mut covering_nodes = BTreeSet::new();
    for terminal in covered_terminal_nodes {
        for reachable in CycleSafeDFS::new_inverse(coverage_storage.as_edgecontainer(), terminal, 1, usize::MAX) {
            let covering_node = reachable?.node;            
            let is_part_of_ordering = order_storage.has_outgoing_edges(covering_node)? || order_storage.get_ingoing_edges(covering_node).count() > 0;
            if is_part_of_ordering {                       
                covering_nodes.insert(covering_node);
            }
        }
    }
    let node_annos = graph.get_node_annos();
    let anno_value = node_annos.get_value_for_item(&m.node, &m.anno_key)?.unwrap();    
    match covering_nodes.len().partial_cmp(&1) {
        Some(Ordering::Equal) => {
            let target_name = node_annos.get_value_for_item(&covering_nodes.pop_last().unwrap(), &NODE_NAME_KEY)?.unwrap();
            update.add_event(UpdateEvent::AddNodeLabel { node_name: target_name.to_string(), anno_ns: target_key.ns.to_string(), anno_name: target_key.name.to_string(), anno_value: anno_value.to_string() })?;
        },
        Some(Ordering::Greater) => {
            // create new span first (we could also check for an exiting one, but it sounds expensive and not promising)    
            let probe_node = covering_nodes.pop_last().unwrap();
            let doc_name = node_annos.get_value_for_item(&probe_node, &NODE_NAME_KEY)?.unwrap().rsplit_once("#").unwrap().0.to_string();
            covering_nodes.insert(probe_node);
            let node_name_pref = format!("{}#sSpan", doc_name);
            let existing = node_annos.get_all_values(&NODE_NAME_KEY, false)?
                                    .iter()
                                    .filter(|v| v.starts_with(node_name_pref.as_str()))
                                    .collect_vec().len();
            let span_name = format!("{}{}", node_name_pref, existing + 1);
            update.add_event(UpdateEvent::AddNode { node_name: span_name.clone(), node_type: "node".to_string() })?;
            update.add_event(UpdateEvent::AddNodeLabel { node_name: span_name.clone(), anno_ns: target_key.ns.to_string(), anno_name: target_key.name.to_string(), anno_value: anno_value.to_string() })?;            
            for member in covering_nodes {
                let member_name = node_annos.get_value_for_item(&member, &NODE_NAME_KEY)?.unwrap();
                update.add_event(UpdateEvent::AddEdge { source_node: span_name.clone(), 
                                                        target_node: member_name.to_string(), 
                                                        layer: ANNIS_NS.to_string(), 
                                                        component_type: AnnotationComponentType::Coverage.to_string(), 
                                                        component_name: "".to_string() })?;
            }        
        },
        _ => {
            let message = format!("Could not gather any covered nodes for name `{}`", target_key.ns);
            let err = AnnattoError::Manipulator { reason: message, manipulator: MODULE_NAME.to_string() };
            return Err(Box::new(err));
        },
    };
    Ok(())
}


fn replace_node_annos(graph: &mut AnnotationGraph, 
                      anno_keys: Vec<(AnnoKey, Option<AnnoKey>)>, 
                      move_by_ns: bool) -> Result<(), Box<dyn std::error::Error>> {
    let mut update = GraphUpdate::default();
    let annos = graph.get_node_annos();
    for (old_key, new_key_opt) in anno_keys.into_iter() {
        for r in annos.exact_anno_search(ns_from_key(&old_key), old_key.name.as_str(), ValueSearch::Any) {
            let m = r?;
            let node_name = annos.get_value_for_item(&m.node, &NODE_NAME_KEY)?.unwrap();
            update.add_event(UpdateEvent::DeleteNodeLabel { node_name: node_name.to_string(), 
                                                            anno_ns: old_key.ns.to_string(), 
                                                            anno_name: old_key.name.to_string() })?;
            if let Some(ref new_key) = new_key_opt {
                if move_by_ns {
                    label_with_new_target(graph, &mut update, &m, new_key)?;
                } else {
                    let value = annos.get_value_for_item(&m.node, &old_key)?.unwrap();
                    update.add_event(UpdateEvent::AddNodeLabel { node_name: node_name.to_string(), 
                                                                 anno_ns: new_key.ns.to_string(), 
                                                                 anno_name: new_key.name.to_string(), 
                                                                 anno_value: value.to_string() })?;
                }
            }
        }
    }
    graph.apply_update(&mut update, |_| {})?;
    Ok(())
}

fn replace_edge_annos(graph: &mut AnnotationGraph, 
                      anno_keys: Vec<(AnnoKey, Option<AnnoKey>)>) -> Result<(), Box<dyn std::error::Error>> {
    let mut update = GraphUpdate::default();
    let node_annos = graph.get_node_annos();
    for (old_key, new_key_opt) in anno_keys {
        for component in graph.get_all_components(None, None) {
            let component_storage = graph.get_graphstorage(&component).unwrap();            
            let edge_annos = component_storage.get_anno_storage();           
            for r in edge_annos.exact_anno_search(ns_from_key(&old_key), old_key.name.as_str(), ValueSearch::Any) {
                let m = r?;                
                let source_node = m.node;
                let source_node_name = node_annos.get_value_for_item(&source_node, &NODE_NAME_KEY)?.unwrap();
                for out_edge_opt in component_storage.get_outgoing_edges(source_node) {                    
                    let target_node = out_edge_opt?;
                    let target_node_name = node_annos.get_value_for_item(&target_node, &NODE_NAME_KEY)?.unwrap();
                    update.add_event(UpdateEvent::DeleteEdgeLabel { source_node: source_node_name.to_string(), 
                                                                    target_node: target_node_name.to_string(), 
                                                                    layer: component.layer.to_string(), 
                                                                    component_type: component.get_type().to_string(), 
                                                                    component_name: component.name.to_string(), 
                                                                    anno_ns: m.anno_key.ns.to_string(), 
                                                                    anno_name: old_key.name.to_string() })?;
                    if let Some(ref new_key) = new_key_opt {
                        let value = edge_annos.get_value_for_item(&Edge { source: source_node, target: target_node}, &m.anno_key)?.unwrap();
                        update.add_event(UpdateEvent::AddEdgeLabel { source_node: source_node_name.to_string(), 
                                                                     target_node: target_node_name.to_string(), 
                                                                     layer: component.layer.to_string(), 
                                                                     component_type: component.get_type().to_string(),
                                                                     component_name: component.name.to_string(), 
                                                                     anno_ns: new_key.ns.to_string(), 
                                                                     anno_name: new_key.name.to_string(), 
                                                                     anno_value: value.to_string() })?;
                    }
                }
            }
        }
    }
    graph.apply_update(&mut update, |_| {})?;
    Ok(())
}


fn key_from_qname(qname: &str) -> AnnoKey {
    let (ns, name) = split_qname(qname);
    match ns {
        None => AnnoKey { ns: "".into(), name: name.into() },
        Some(ns_val) => AnnoKey {ns: ns_val.into(), name: name.into() }
    }
}

fn ns_from_key<'a>(anno_key: &'a AnnoKey) -> Option<&'a str> {
    if anno_key.ns.is_empty() {
        None
    } else {
        Some(anno_key.ns.as_str())
    }
}


fn read_property(value: &str) -> Result<Vec<(AnnoKey, Option<AnnoKey>)>, Box<dyn std::error::Error>> {
    let mut names = Vec::new();
    for entry in value.split(PROPVAL_SEP) {
        let old_new = entry.split_once(PROPVAL_OLD_NEW_SEP);
        let key_and_opt = match old_new {
            None => {
                // only old name, i. e. remove
                (key_from_qname(entry), None)  
            },
            Some(tpl) => {
                // new name specified, too
                (key_from_qname(tpl.0), Some(key_from_qname(tpl.1)))
            }
        };
        names.push(key_and_opt);
    }
    Ok(names)
}


impl Manipulator for Replace {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        properties: &std::collections::BTreeMap<String, String>,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let move_by_ns = match properties.get(&PROP_MOVE.to_string()) {
            None => false,
            Some(v) => v.parse::<bool>()?
        };
        if let Some(node_name_s ) = properties.get(&PROP_NODE_NAMES.to_string()) {
            let node_names = node_name_s.split(PROPVAL_SEP).collect_vec();
            remove_nodes(graph, node_names)?;
        }
        if let Some(anno_name_s ) = properties.get(&PROP_NODE_ANNOS.to_string()) {
            let node_annos = read_property(anno_name_s)?;
            replace_node_annos(graph, node_annos, move_by_ns)?;
        }
        if let Some(edge_name_s) = properties.get(&PROP_EDGE_ANNOS.to_string()) {
            let edge_annos = read_property(edge_name_s)?;
            replace_edge_annos(graph, edge_annos)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::env::temp_dir;

    use crate::Result;
    use crate::manipulator::Manipulator;
    use crate::manipulator::re::Replace;

    use graphannis::{AnnotationGraph,CorpusStorage};
    use graphannis::corpusstorage::{QueryLanguage,ResultOrder,SearchQuery};
    use graphannis_core::annostorage::ValueSearch;
    use graphannis::model::AnnotationComponentType;
    use graphannis::update::{GraphUpdate,UpdateEvent};
    use graphannis_core::graph::{ANNIS_NS, NODE_TYPE_KEY, NODE_NAME_KEY};
    use itertools::Itertools;
    use tempfile::{tempfile, tempdir_in};

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
        let mut properties = BTreeMap::new();
        let (node_anno_prop_val, edge_anno_prop_val) = if rename {
            ("pos:=upos".to_string(), "deprel:=func".to_string())
        } else {
            ("pos".to_string(), "deprel".to_string())
        };
        properties.insert("node.annos".to_string(), node_anno_prop_val);
        properties.insert("edge.annos".to_string(), edge_anno_prop_val);
        let replace = Replace::default();
        let result = replace.manipulate_corpus(&mut g, &properties, None);
        assert_eq!(result.is_ok(), true, "Probing merge result {:?}", &result);
        let mut e_g = if rename {
            input_graph(on_disk, true)?
        } else {
            expected_output_graph(on_disk)?
        };
        // corpus nodes
        let e_corpus_nodes: BTreeSet<String> = e_g.get_node_annos()
                                        .exact_anno_search(Some(&NODE_TYPE_KEY.ns), &NODE_TYPE_KEY.name, ValueSearch::Some("corpus"))
                                        .into_iter()
                                        .map(|r| r.unwrap().node)
                                        .map(|id_| e_g.get_node_annos().get_value_for_item(&id_, &NODE_NAME_KEY).unwrap().unwrap().to_string())
                                        .collect();
        let g_corpus_nodes: BTreeSet<String> = g.get_node_annos()
                                        .exact_anno_search(Some(&NODE_TYPE_KEY.ns), &NODE_TYPE_KEY.name, ValueSearch::Some("corpus"))
                                        .into_iter()
                                        .map(|r| r.unwrap().node)
                                        .map(|id_| g.get_node_annos().get_value_for_item(&id_, &NODE_NAME_KEY).unwrap().unwrap().to_string())
                                        .collect();
        assert_eq!(e_corpus_nodes, g_corpus_nodes);  //TODO clarify: Delegate or assertion?
        // test by components
        let e_c_list = e_g.get_all_components(None, None)
                                            .into_iter()
                                            .filter(|c| e_g.get_graphstorage(c).unwrap().source_nodes().count() > 0)
                                            .collect_vec();
        let g_c_list = g.get_all_components(None, None)
                                            .into_iter()
                                            .filter(|c| g.get_graphstorage(c).unwrap().source_nodes().count() > 0)  // graph might contain empty components after merge
                                            .collect_vec();
        assert_eq!(e_c_list.len(), g_c_list.len(), "components expected:\n{:?};\ncomponents are:\n{:?}", &e_c_list, &g_c_list);
        for c in e_c_list {
            let candidates = g.get_all_components(Some(c.get_type()), Some(c.name.as_str()));
            assert_eq!(candidates.len(), 1);
            let c_o  = candidates.get(0);
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
            "node ->dep[func=/.+/] node"
        ];
        let corpus_name = "current";
        let tmp_dir_e = tempdir_in(temp_dir())?;
        let tmp_dir_g = tempdir_in(temp_dir())?;        
        e_g.save_to(&tmp_dir_e.path().join(corpus_name))?;
        g.save_to(&tmp_dir_g.path().join(corpus_name))?;        
        let cs_e = CorpusStorage::with_auto_cache_size(&tmp_dir_e.path(), true)?;
        let cs_g = CorpusStorage::with_auto_cache_size(&tmp_dir_g.path(), true)?;
        for query_s in queries {
            let query = SearchQuery {
                corpus_names: &[corpus_name],
                query: query_s,
                query_language: QueryLanguage::AQL,
                timeout: None
            };
            let matches_e = cs_e.find(query.clone(), 0, None, ResultOrder::Normal)?;
            let matches_g = cs_g.find(query, 0, None, ResultOrder::Normal)?;
            assert_eq!(matches_e.len(), matches_g.len(), "Failed with query: {}", query_s);
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
        let mut properties = BTreeMap::new();
        properties.insert("node.annos".to_string(), "norm::pos:=dipl::derived_pos".to_string());
        properties.insert("move.node.annos".to_string(), "true".to_string());
        let replace = Replace::default();
        let result = replace.manipulate_corpus(&mut g, &properties, None);
        assert_eq!(result.is_ok(), true, "Probing merge result {:?}", &result);
        let mut e_g = expected_output_for_move(on_disk)?;
        // corpus nodes
        let e_corpus_nodes: BTreeSet<String> = e_g.get_node_annos()
                                        .exact_anno_search(Some(&NODE_TYPE_KEY.ns), &NODE_TYPE_KEY.name, ValueSearch::Some("corpus"))
                                        .into_iter()
                                        .map(|r| r.unwrap().node)
                                        .map(|id_| e_g.get_node_annos().get_value_for_item(&id_, &NODE_NAME_KEY).unwrap().unwrap().to_string())
                                        .collect();
        let g_corpus_nodes: BTreeSet<String> = g.get_node_annos()
                                        .exact_anno_search(Some(&NODE_TYPE_KEY.ns), &NODE_TYPE_KEY.name, ValueSearch::Some("corpus"))
                                        .into_iter()
                                        .map(|r| r.unwrap().node)
                                        .map(|id_| g.get_node_annos().get_value_for_item(&id_, &NODE_NAME_KEY).unwrap().unwrap().to_string())
                                        .collect();
        assert_eq!(e_corpus_nodes, g_corpus_nodes);  //TODO clarify: Delegate or assertion?
        // test by components
        let e_c_list = e_g.get_all_components(None, None)
                                            .into_iter()
                                            .filter(|c| e_g.get_graphstorage(c).unwrap().source_nodes().count() > 0)
                                            .collect_vec();
        let g_c_list = g.get_all_components(None, None)
                                            .into_iter()
                                            .filter(|c| g.get_graphstorage(c).unwrap().source_nodes().count() > 0)  // graph might contain empty components after merge
                                            .collect_vec();
        assert_eq!(e_c_list.len(), g_c_list.len(), "components expected:\n{:?};\ncomponents are:\n{:?}", &e_c_list, &g_c_list);
        for c in e_c_list {
            let candidates = g.get_all_components(Some(c.get_type()), Some(c.name.as_str()));
            assert_eq!(candidates.len(), 1);
            let c_o  = candidates.get(0);
            assert_eq!(&c, c_o.unwrap());
        }
        // test with queries
        let queries = [
            "tok",
            "pos",
            "derived_pos"
        ];
        let corpus_name = "current";
        let tmp_dir_e = tempdir_in(temp_dir())?;
        let tmp_dir_g = tempdir_in(temp_dir())?;        
        e_g.save_to(&tmp_dir_e.path().join(corpus_name))?;
        g.save_to(&tmp_dir_g.path().join(corpus_name))?;        
        let cs_e = CorpusStorage::with_auto_cache_size(&tmp_dir_e.path(), true)?;
        let cs_g = CorpusStorage::with_auto_cache_size(&tmp_dir_g.path(), true)?;
        for query_s in queries {
            let query = SearchQuery {
                corpus_names: &[corpus_name],
                query: query_s,
                query_language: QueryLanguage::AQL,
                timeout: None
            };
            let matches_e = cs_e.find(query.clone(), 0, None, ResultOrder::Normal)?;
            let matches_g = cs_g.find(query, 0, None, ResultOrder::Normal)?;    
            assert_eq!(matches_e.len(), matches_g.len(), "Failed with query: {}", query_s);
            for (m_e, m_g) in matches_e.into_iter().zip(matches_g.into_iter()) {
                assert_eq!(m_e, m_g);
            }
        }
        Ok(())
    }

    #[test]
    fn test_export_mem() {
        let export = export_test(false);
        assert_eq!(export.is_ok(), true, "Export test ends with Err: {:?}", &export);
    }

    #[test]
    fn test_export_disk() {
        let export = export_test(true);
        assert_eq!(export.is_ok(), true, "Export test ends with Err: {:?}", &export);
    }

    fn export_test(on_disk: bool) -> Result<()> {
        let mut g = input_graph(on_disk, false)?;
        let mut properties = BTreeMap::new();
        properties.insert("edge.annos".to_string(), "deprel".to_string());
        properties.insert("node.annos".to_string(), "pos".to_string());
        let replace = Replace::default();
        assert_eq!(replace.manipulate_corpus(&mut g, &properties, None).is_ok(), true);
        let tmp_file = tempfile()?;
        let export = graphannis_core::graph::serialization::graphml::export(&g, None, tmp_file, |_| {});
        assert_eq!(export.is_ok(), true, "Export fails: {:?}", &export);
        Ok(())
    }

    #[test]
    fn test_export_move_result_mem() {
        let export = export_test_move_result(false);
        assert_eq!(export.is_ok(), true, "Testing export of move result ends with Err: {:?}", &export);
    }

    #[test]
    fn test_export_move_result_disk() {
        let export = export_test_move_result(true);
        assert_eq!(export.is_ok(), true, "Testing export of move result ends with Err: {:?}", &export);
    }

    fn export_test_move_result(on_disk: bool) -> Result<()> {
        let mut g = input_graph_for_move(on_disk)?;
        let mut properties = BTreeMap::new();
        properties.insert("node.annos".to_string(), "norm::pos:=dipl::derived_pos".to_string());
        properties.insert("move.node.annos".to_string(), "true".to_string());
        let replace = Replace::default();
        assert_eq!(replace.manipulate_corpus(&mut g, &properties, None).is_ok(), true);
        let tmp_file = tempfile()?;
        let export = graphannis_core::graph::serialization::graphml::export(&g, None, tmp_file, |_| {});
        assert_eq!(export.is_ok(), true, "Export fails: {:?}", &export);
        Ok(())
    }    

    fn input_graph(on_disk: bool, new_names: bool) -> Result<AnnotationGraph> {
        let mut g = AnnotationGraph::new(on_disk)?;
        let mut u = GraphUpdate::default();
        u.add_event(UpdateEvent::AddNode { node_name: "root".to_string(), node_type: "corpus".to_string() })?;        
        u.add_event(UpdateEvent::AddNode { node_name: "root/b".to_string(), node_type: "corpus".to_string() })?;
        u.add_event(UpdateEvent::AddEdge { source_node: "root/b".to_string(), 
                                           target_node: "root".to_string(), 
                                           layer: ANNIS_NS.to_string(), 
                                           component_type: AnnotationComponentType::PartOf.to_string(), 
                                           component_name: "".to_string() })?;
        u.add_event(UpdateEvent::AddNode { node_name: "root/b/doc".to_string(), node_type: "corpus".to_string() })?;
        u.add_event(UpdateEvent::AddEdge { source_node: "root/b/doc".to_string(), 
                                           target_node: "root/b".to_string(), 
                                           layer: ANNIS_NS.to_string(), 
                                           component_type: AnnotationComponentType::PartOf.to_string(), 
                                           component_name: "".to_string() })?;
        let pos_name = if new_names {
            "upos"
        } else {
            "pos"
        };
        for (ii, (txt, lemma_label, pos_label)) in [("I", "I", "PRON"),
                                                  ("am", "be", "VERB"), 
                                                  ("in", "in", "ADP"), 
                                                  ("Berlin", "Berlin", "PROPN")].iter().enumerate() {
            let i = ii + 1;
            let name = format!("root/b/doc#t{}", i);
            u.add_event(UpdateEvent::AddNode { node_name: name.to_string(), node_type: "node".to_string() })?;
            u.add_event(UpdateEvent::AddNodeLabel { node_name: name.to_string(), 
                                                    anno_ns: ANNIS_NS.to_string(), 
                                                    anno_name: "tok".to_string(), 
                                                    anno_value: txt.to_string() })?;
            u.add_event(UpdateEvent::AddNodeLabel { node_name: name.to_string(), 
                                                    anno_ns: "".to_string(), 
                                                    anno_name: "text".to_string(), 
                                                    anno_value: txt.to_string() })?;
            u.add_event(UpdateEvent::AddNodeLabel { node_name: name.to_string(), 
                                                    anno_ns: "".to_string(), 
                                                    anno_name: "lemma".to_string(), 
                                                    anno_value: lemma_label.to_string() })?;
            u.add_event(UpdateEvent::AddNodeLabel { node_name: name.to_string(), 
                                                    anno_ns: "".to_string(), 
                                                    anno_name: pos_name.to_string(), 
                                                    anno_value: pos_label.to_string() })?;
            if i > 1 {
                u.add_event(UpdateEvent::AddEdge { source_node: format!("root/b/doc#t{}", i - 1), 
                                                   target_node: name.to_string(), 
                                                   layer: ANNIS_NS.to_string(), 
                                                   component_type: AnnotationComponentType::Ordering.to_string(), 
                                                   component_name: "".to_string() })?;
                u.add_event(UpdateEvent::AddEdge { source_node: format!("root/b/doc#t{}", i - 1), 
                                                   target_node: name.to_string(), 
                                                   layer: ANNIS_NS.to_string(), 
                                                   component_type: AnnotationComponentType::Ordering.to_string(), 
                                                   component_name: "text".to_string() })?;
            }
        }
        let dep_layer_name = "syntax";
        let dep_comp_name = "dep";
        let deprel_name = if new_names {
            "func"
        } else {
            "deprel"
        };
        for (source, target, label) in [(2, 1, "subj"),
                                      (2, 3, "comp:pred"),
                                      (3, 4, "comp:obj")].iter() {
            let source_name = format!("root/b/doc#t{}", source);
            let target_name = format!("root/b/doc#t{}", target);
            u.add_event(UpdateEvent::AddEdge { source_node: source_name.to_string(), 
                                               target_node: target_name.to_string(), 
                                               layer: dep_layer_name.to_string(), 
                                               component_type: AnnotationComponentType::Pointing.to_string(), 
                                               component_name: dep_comp_name.to_string() })?;
            u.add_event(UpdateEvent::AddEdgeLabel { source_node: source_name, 
                                                    target_node: target_name, 
                                                    layer: dep_layer_name.to_string(), 
                                                    component_type: AnnotationComponentType::Pointing.to_string(), 
                                                    component_name: dep_comp_name.to_string(), 
                                                    anno_ns: "".to_string(), 
                                                    anno_name: deprel_name.to_string(), 
                                                    anno_value: label.to_string() })?;
        }        
        g.apply_update(&mut u, |_msg| {})?;
        Ok(g)
    }

    fn expected_output_graph(on_disk: bool) -> Result<AnnotationGraph> {
        let mut g = AnnotationGraph::new(on_disk)?;
        let mut u = GraphUpdate::default();
        u.add_event(UpdateEvent::AddNode { node_name: "root".to_string(), node_type: "corpus".to_string() })?;        
        u.add_event(UpdateEvent::AddNode { node_name: "root/b".to_string(), node_type: "corpus".to_string() })?;
        u.add_event(UpdateEvent::AddEdge { source_node: "root/b".to_string(), 
                                           target_node: "root".to_string(), 
                                           layer: ANNIS_NS.to_string(), 
                                           component_type: AnnotationComponentType::PartOf.to_string(), 
                                           component_name: "".to_string() })?;
        u.add_event(UpdateEvent::AddNode { node_name: "root/b/doc".to_string(), node_type: "corpus".to_string() })?;
        u.add_event(UpdateEvent::AddEdge { source_node: "root/b/doc".to_string(), 
                                           target_node: "root/b".to_string(), 
                                           layer: ANNIS_NS.to_string(), 
                                           component_type: AnnotationComponentType::PartOf.to_string(), 
                                           component_name: "".to_string() })?;
        for (ii, (txt, lemma_label)) in [("I", "I"),
                                                  ("am", "be"), 
                                                  ("in", "in"), 
                                                  ("Berlin", "Berlin")].iter().enumerate() {
            let i = ii + 1;
            let name = format!("root/b/doc#t{}", i);
            u.add_event(UpdateEvent::AddNode { node_name: name.to_string(), node_type: "node".to_string() })?;
            u.add_event(UpdateEvent::AddNodeLabel { node_name: name.to_string(), 
                                                    anno_ns: ANNIS_NS.to_string(), 
                                                    anno_name: "tok".to_string(), 
                                                    anno_value: txt.to_string() })?;
            u.add_event(UpdateEvent::AddNodeLabel { node_name: name.to_string(), 
                                                    anno_ns: "".to_string(), 
                                                    anno_name: "text".to_string(), 
                                                    anno_value: txt.to_string() })?;
            u.add_event(UpdateEvent::AddNodeLabel { node_name: name.to_string(), 
                                                    anno_ns: "".to_string(), 
                                                    anno_name: "lemma".to_string(), 
                                                    anno_value: lemma_label.to_string() })?;
            if i > 1 {
                u.add_event(UpdateEvent::AddEdge { source_node: format!("root/b/doc#t{}", i - 1), 
                                                   target_node: name.to_string(), 
                                                   layer: ANNIS_NS.to_string(), 
                                                   component_type: AnnotationComponentType::Ordering.to_string(), 
                                                   component_name: "".to_string() })?;
                u.add_event(UpdateEvent::AddEdge { source_node: format!("root/b/doc#t{}", i - 1), 
                                                   target_node: name.to_string(), 
                                                   layer: ANNIS_NS.to_string(), 
                                                   component_type: AnnotationComponentType::Ordering.to_string(), 
                                                   component_name: "text".to_string() })?;
            }
        }
        let dep_layer_name = "syntax";
        let dep_comp_name = "dep";
        for (source, target) in [(2, 1), (2, 3), (3, 4)].iter() {
            let source_name = format!("root/b/doc#t{}", source);
            let target_name = format!("root/b/doc#t{}", target);
            u.add_event(UpdateEvent::AddEdge { source_node: source_name.to_string(), 
                                               target_node: target_name.to_string(), 
                                               layer: dep_layer_name.to_string(), 
                                               component_type: AnnotationComponentType::Pointing.to_string(), 
                                               component_name: dep_comp_name.to_string() })?;
        }
        g.apply_update(&mut u, |_msg| {})?;
        Ok(g)
    }

    fn input_graph_for_move(on_disk: bool) -> Result<AnnotationGraph> {
        let mut g = AnnotationGraph::new(on_disk)?;
        let mut u = GraphUpdate::default();
        u.add_event(UpdateEvent::AddNode { node_name: "root".to_string(), node_type: "corpus".to_string() })?;
        // import 1
        u.add_event(UpdateEvent::AddNode { node_name: "root/a".to_string(), node_type: "corpus".to_string() })?;
        u.add_event(UpdateEvent::AddEdge { source_node: "root/a".to_string(), 
                                           target_node: "root".to_string(), 
                                           layer: ANNIS_NS.to_string(), 
                                           component_type: AnnotationComponentType::PartOf.to_string(), 
                                           component_name: "".to_string() })?;
        u.add_event(UpdateEvent::AddNode { node_name: "root/a/doc".to_string(), node_type: "corpus".to_string() })?;
        u.add_event(UpdateEvent::AddEdge { source_node: "root/a/doc".to_string(), 
                                           target_node: "root/a".to_string(), 
                                           layer: ANNIS_NS.to_string(), 
                                           component_type: AnnotationComponentType::PartOf.to_string(), 
                                           component_name: "".to_string() })?;
        for i in 0 .. 5 {
            u.add_event(UpdateEvent::AddNode { node_name: format!("root/a/doc#t{}", i), node_type: "node".to_string() })?;
            u.add_event(UpdateEvent::AddNodeLabel { node_name: format!("root/a/doc#t{}", i), 
                                                    anno_ns: ANNIS_NS.to_string(), 
                                                    anno_name: "tok".to_string(), 
                                                    anno_value: " ".to_string() })?;
            if i > 1 {
                u.add_event(UpdateEvent::AddEdge { source_node: format!("root/a/doc#t{}", i - 1), 
                                                   target_node: format!("root/a/doc#t{}", i), 
                                                   layer: ANNIS_NS.to_string(), 
                                                   component_type: AnnotationComponentType::Ordering.to_string(), 
                                                   component_name: "".to_string() })?;
            }
        }
        // fake-tok 1
        let sentence_span_name = "root/a/doc#s0";
        u.add_event(UpdateEvent::AddNode { node_name: sentence_span_name.to_string(), node_type: "node".to_string() })?;
        u.add_event(UpdateEvent::AddNodeLabel { node_name: sentence_span_name.to_string(), 
                                                anno_ns: "dipl".to_string(),
                                                anno_name: "sentence".to_string(), 
                                                anno_value: "1".to_string() })?;
        for (ii, (txt, start, end)) in [("I'm", 0, 2), 
                                                  ("in", 2, 3), 
                                                  ("New", 3, 4), 
                                                  ("York", 4, 5)].iter().enumerate() {
            let i = ii + 1;
            let name = format!("root/a/doc#s{}", i);
            u.add_event(UpdateEvent::AddNode { node_name: name.to_string(), node_type: "node".to_string() })?;
            u.add_event(UpdateEvent::AddNodeLabel { node_name: name.to_string(), 
                                                    anno_ns: ANNIS_NS.to_string(), 
                                                    anno_name: "tok".to_string(), 
                                                    anno_value: txt.to_string() })?;
            u.add_event(UpdateEvent::AddNodeLabel { node_name: name.to_string(), 
                                                    anno_ns: "".to_string(), 
                                                    anno_name: "dipl".to_string(), 
                                                    anno_value: txt.to_string() })?;
            u.add_event(UpdateEvent::AddEdge { source_node: sentence_span_name.to_string(),
                                               target_node: name.to_string(),
                                               layer: ANNIS_NS.to_string(), 
                                               component_type: AnnotationComponentType::Coverage.to_string(), 
                                               component_name: "".to_string() })?;
            for j in *start .. *end {
                u.add_event(UpdateEvent::AddEdge { source_node: name.to_string(), 
                                                   target_node: format!("root/a/doc#t{}", j), 
                                                   layer: ANNIS_NS.to_string(), 
                                                   component_type: AnnotationComponentType::Coverage.to_string(), 
                                                   component_name: "".to_string() })?;
            }
            if i > 1 {
                u.add_event(UpdateEvent::AddEdge { source_node: format!("root/a/doc#s{}", i - 1), 
                                                   target_node: name.to_string(), 
                                                   layer: ANNIS_NS.to_string(), 
                                                   component_type: AnnotationComponentType::Ordering.to_string(), 
                                                   component_name: "dipl".to_string() })?;
            }
        }
        // fake-tok 2
        for (ii, (txt, start, end, pos_label)) in [("I", 0, 1, "PRON"), 
                                                  ("am", 1, 2, "VERB"), 
                                                  ("in", 2, 3, "ADP"), 
                                                  ("New York", 3, 5, "PROPN")].iter().enumerate() {
            let i = ii + 5;
            let name = format!("root/a/doc#s{}", i);
            u.add_event(UpdateEvent::AddNode { node_name: name.to_string(), node_type: "node".to_string() })?;
            u.add_event(UpdateEvent::AddNodeLabel { node_name: name.to_string(), 
                                                    anno_ns: ANNIS_NS.to_string(), 
                                                    anno_name: "tok".to_string(), 
                                                    anno_value: txt.to_string() })?;
            u.add_event(UpdateEvent::AddNodeLabel { node_name: name.to_string(), 
                                                    anno_ns: "".to_string(), 
                                                    anno_name: "norm".to_string(), 
                                                    anno_value: txt.to_string() })?;
            u.add_event(UpdateEvent::AddNodeLabel { node_name: name.to_string(), 
                                                    anno_ns: "norm".to_string(), 
                                                    anno_name: "pos".to_string(), 
                                                    anno_value: pos_label.to_string() })?;
            for j in *start .. *end {
                u.add_event(UpdateEvent::AddEdge { source_node: name.to_string(), 
                                                   target_node: format!("root/a/doc#t{}", j), 
                                                   layer: ANNIS_NS.to_string(), 
                                                   component_type: AnnotationComponentType::Coverage.to_string(), 
                                                   component_name: "".to_string() })?;
            }
            if ii > 0 {
                u.add_event(UpdateEvent::AddEdge { source_node: format!("root/a/doc#s{}", i - 1), 
                                                   target_node: name.to_string(), 
                                                   layer: ANNIS_NS.to_string(), 
                                                   component_type: AnnotationComponentType::Ordering.to_string(), 
                                                   component_name: "norm".to_string() })?;
            }
        }        
        g.apply_update(&mut u, |_msg| {})?;
        Ok(g)
    }

    fn expected_output_for_move(on_disk: bool) -> Result<AnnotationGraph> {
        let mut g = AnnotationGraph::new(on_disk)?;
        let mut u = GraphUpdate::default();
        u.add_event(UpdateEvent::AddNode { node_name: "root".to_string(), node_type: "corpus".to_string() })?;
        // import 1
        u.add_event(UpdateEvent::AddNode { node_name: "root/a".to_string(), node_type: "corpus".to_string() })?;
        u.add_event(UpdateEvent::AddEdge { source_node: "root/a".to_string(), 
                                           target_node: "root".to_string(), 
                                           layer: ANNIS_NS.to_string(), 
                                           component_type: AnnotationComponentType::PartOf.to_string(), 
                                           component_name: "".to_string() })?;
        u.add_event(UpdateEvent::AddNode { node_name: "root/a/doc".to_string(), node_type: "corpus".to_string() })?;
        u.add_event(UpdateEvent::AddEdge { source_node: "root/a/doc".to_string(), 
                                           target_node: "root/a".to_string(), 
                                           layer: ANNIS_NS.to_string(), 
                                           component_type: AnnotationComponentType::PartOf.to_string(), 
                                           component_name: "".to_string() })?;
        for i in 0 .. 5 {
            u.add_event(UpdateEvent::AddNode { node_name: format!("root/a/doc#t{}", i), node_type: "node".to_string() })?;
            u.add_event(UpdateEvent::AddNodeLabel { node_name: format!("root/a/doc#t{}", i), 
                                                    anno_ns: ANNIS_NS.to_string(), 
                                                    anno_name: "tok".to_string(), 
                                                    anno_value: " ".to_string() })?;
            if i > 1 {
                u.add_event(UpdateEvent::AddEdge { source_node: format!("root/a/doc#t{}", i - 1), 
                                                   target_node: format!("root/a/doc#t{}", i), 
                                                   layer: ANNIS_NS.to_string(), 
                                                   component_type: AnnotationComponentType::Ordering.to_string(), 
                                                   component_name: "".to_string() })?;
            }
        }
        // fake-tok 1
        let sentence_span_name = "root/a/doc#s0";
        u.add_event(UpdateEvent::AddNode { node_name: sentence_span_name.to_string(), node_type: "node".to_string() })?;
        u.add_event(UpdateEvent::AddNodeLabel { node_name: sentence_span_name.to_string(), 
                                                anno_ns: "dipl".to_string(),
                                                anno_name: "sentence".to_string(), 
                                                anno_value: "1".to_string() })?;
        for (ii, (txt, start, end, pos_label )) in [("I'm", 0, 2, Some("VERB")), 
                                                  ("in", 2, 3, Some("ADP")), 
                                                  ("New", 3, 4, None), 
                                                  ("York", 4, 5, None)].iter().enumerate() {
            let i = ii + 1;
            let name = format!("root/a/doc#s{}", i);
            u.add_event(UpdateEvent::AddNode { node_name: name.to_string(), node_type: "node".to_string() })?;
            u.add_event(UpdateEvent::AddNodeLabel { node_name: name.to_string(), 
                                                    anno_ns: ANNIS_NS.to_string(), 
                                                    anno_name: "tok".to_string(), 
                                                    anno_value: txt.to_string() })?;
            u.add_event(UpdateEvent::AddNodeLabel { node_name: name.to_string(), 
                                                    anno_ns: "".to_string(), 
                                                    anno_name: "dipl".to_string(), 
                                                    anno_value: txt.to_string() })?;
            if let Some(v) = pos_label {
                u.add_event(UpdateEvent::AddNodeLabel { node_name: name.to_string(), 
                                                        anno_ns: "dipl".to_string(), 
                                                        anno_name: "derived_pos".to_string(), 
                                                        anno_value: v.to_string() })?;
            }
            u.add_event(UpdateEvent::AddEdge { source_node: sentence_span_name.to_string(),
                                               target_node: name.to_string(),
                                               layer: ANNIS_NS.to_string(), 
                                               component_type: AnnotationComponentType::Coverage.to_string(), 
                                               component_name: "".to_string() })?;
            for j in *start .. *end {
                u.add_event(UpdateEvent::AddEdge { source_node: name.to_string(), 
                                                   target_node: format!("root/a/doc#t{}", j), 
                                                   layer: ANNIS_NS.to_string(), 
                                                   component_type: AnnotationComponentType::Coverage.to_string(), 
                                                   component_name: "".to_string() })?;
            }
            if i > 1 {
                u.add_event(UpdateEvent::AddEdge { source_node: format!("root/a/doc#s{}", i - 1), 
                                                   target_node: name.to_string(), 
                                                   layer: ANNIS_NS.to_string(), 
                                                   component_type: AnnotationComponentType::Ordering.to_string(), 
                                                   component_name: "dipl".to_string() })?;
            }
        }
        let span_name = "root/a/doc#sSpan1";
        u.add_event(UpdateEvent::AddNode { node_name: span_name.to_string(), node_type: "node".to_string() })?;
        u.add_event(UpdateEvent::AddNodeLabel { node_name: span_name.to_string(), 
                                                anno_ns: "dipl".to_string(), 
                                                anno_name: "derived_pos".to_string(), 
                                                anno_value: "PROPN".to_string() })?;
        u.add_event(UpdateEvent::AddEdge { source_node: span_name.to_string(), 
                                           target_node: "root/a/doc#s3".to_string(), 
                                           layer: ANNIS_NS.to_string(), 
                                           component_type: AnnotationComponentType::Coverage.to_string(), 
                                           component_name: "".to_string() })?;
        u.add_event(UpdateEvent::AddEdge { source_node: span_name.to_string(), 
                                            target_node: "root/a/doc#s4".to_string(), 
                                            layer: ANNIS_NS.to_string(), 
                                            component_type: AnnotationComponentType::Coverage.to_string(), 
                                            component_name: "".to_string() })?;
        // fake-tok 2
        for (ii, (txt, start, end)) in [("I", 0, 1), 
                                                  ("am", 1, 2), 
                                                  ("in", 2, 3), 
                                                  ("New York", 3, 5)].iter().enumerate() {
            let i = ii + 5;
            let name = format!("root/a/doc#s{}", i);
            u.add_event(UpdateEvent::AddNode { node_name: name.to_string(), node_type: "node".to_string() })?;
            u.add_event(UpdateEvent::AddNodeLabel { node_name: name.to_string(), 
                                                    anno_ns: ANNIS_NS.to_string(), 
                                                    anno_name: "tok".to_string(), 
                                                    anno_value: txt.to_string() })?;
            u.add_event(UpdateEvent::AddNodeLabel { node_name: name.to_string(), 
                                                    anno_ns: "".to_string(), 
                                                    anno_name: "norm".to_string(), 
                                                    anno_value: txt.to_string() })?;
            for j in *start .. *end {
                u.add_event(UpdateEvent::AddEdge { source_node: name.to_string(), 
                                                   target_node: format!("root/a/doc#t{}", j), 
                                                   layer: ANNIS_NS.to_string(), 
                                                   component_type: AnnotationComponentType::Coverage.to_string(), 
                                                   component_name: "".to_string() })?;
            }
            if ii > 0 {
                u.add_event(UpdateEvent::AddEdge { source_node: format!("root/a/doc#s{}", i - 1), 
                                                   target_node: name.to_string(), 
                                                   layer: ANNIS_NS.to_string(), 
                                                   component_type: AnnotationComponentType::Ordering.to_string(), 
                                                   component_name: "norm".to_string() })?;
            }
        }        
        g.apply_update(&mut u, |_msg| {})?;
        Ok(g)
    }

}