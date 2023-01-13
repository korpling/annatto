use graphannis::{AnnotationGraph, update::{GraphUpdate, UpdateEvent}};
use graphannis_core::{annostorage::ValueSearch, graph::NODE_NAME_KEY};
use graphannis_core::util::split_qname;
use itertools::Itertools;

use crate::{Manipulator, Module};

pub struct Rename {}
pub struct Remove {}

const RENAME_ID: &str = "rename";
const REMOVE_ID: &str = "remove";

impl Default for Rename {
    fn default() -> Self {
        Rename {}
    }
}

impl Default for Remove {
    fn default() -> Self {
        Remove {}
    }
}

impl Module for Rename {
    fn module_name(&self) -> &str {
        RENAME_ID
    }
}

impl Module for Remove {
    fn module_name(&self) -> &str {
        REMOVE_ID
    }
}

const PROP_NODE_ANNOS: &str = "node.annos";
const PROP_NODE_NAMES: &str = "node.names";
const PROP_EDGE_ANNOS: &str = "edge.annos";
const PROPVAL_SEP: &str = ",";

fn remove_nodes(graph: &mut AnnotationGraph, names: Vec<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let mut update = GraphUpdate::default();
    for name in names {
        update.add_event(UpdateEvent::DeleteNode { node_name: name.to_string() })?;
    }
    graph.apply_update(&mut update, |_| {})?;
    Ok(())
}

fn remove_node_annos(graph: &mut AnnotationGraph, names: Vec<(Option<&str>, &str)>) -> Result<(), Box<dyn std::error::Error>> {
    let mut update = GraphUpdate::default();
    let annos = graph.get_node_annos();
    for (ns, name) in names.into_iter() {
        for r in annos.exact_anno_search(ns, name, ValueSearch::Any) {
            let m = r?;
            let node_name = annos.get_value_for_item(&m.node, &NODE_NAME_KEY)?.unwrap();
            let del_ns = match ns {
                None => "".to_string(),
                Some(v) => v.to_string()
            };
            update.add_event(UpdateEvent::DeleteNodeLabel { node_name: node_name.to_string(), 
                                                            anno_ns: del_ns, 
                                                            anno_name: name.to_string() })?;
        }
    }
    graph.apply_update(&mut update, |_| {})?;
    Ok(())
}

fn remove_edge_annos(graph: &mut AnnotationGraph, names: Vec<(Option<&str>, &str)>) -> Result<(), Box<dyn std::error::Error>> {
    let mut update = GraphUpdate::default();
    let node_annos = graph.get_node_annos();
    for (ns, name) in names {
        for component in graph.get_all_components(None, None) {
            let component_storage = graph.get_graphstorage(&component).unwrap();            
            let edge_annos = component_storage.get_anno_storage();           
            for r in edge_annos.exact_anno_search(ns, name, ValueSearch::Any) {
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
                                                                    anno_name: name.to_string() })?;
                }
            }
        }
    }
    graph.apply_update(&mut update, |_| {})?;
    Ok(())
}

impl Manipulator for Remove {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        properties: &std::collections::BTreeMap<String, String>,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(node_name_s ) = properties.get(&PROP_NODE_NAMES.to_string()) {
            let node_names = node_name_s.split(PROPVAL_SEP).collect_vec();
            remove_nodes(graph, node_names)?;
        }
        if let Some(node_name_s ) = properties.get(&PROP_NODE_ANNOS.to_string()) {
            let node_annos = node_name_s.split(PROPVAL_SEP).map(|s| split_qname(s)).collect_vec();
            remove_node_annos(graph, node_annos)?;
        }
        if let Some(edge_name_s) = properties.get(&PROP_EDGE_ANNOS.to_string()) {
            let edge_annos = edge_name_s.split(PROPVAL_SEP).map(|s| split_qname(s)).collect_vec();
            remove_edge_annos(graph, edge_annos)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashSet};
    use std::env::temp_dir;

    use crate::Result;
    use crate::manipulator::Manipulator;
    use crate::manipulator::re::Remove;

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
        let r = core_test(false); 
        assert_eq!(r.is_ok(), true, "Probing core test result {:?}", r);
    }

    #[test]
    fn test_remove_on_disk() {
        let r = core_test(true); 
        assert_eq!(r.is_ok(), true, "Probing core test result {:?}", r);
    }

    fn core_test(on_disk: bool) -> Result<()> {
        let mut g = input_graph(on_disk)?;
        let mut properties = BTreeMap::new();
        properties.insert("edge.annos".to_string(), "deprel".to_string());
        properties.insert("node.annos".to_string(), "pos".to_string());
        let remove = Remove::default();
        let result = remove.manipulate_corpus(&mut g, &properties, None);
        assert_eq!(result.is_ok(), true, "Probing merge result {:?}", &result);
        let mut e_g = expected_output_graph(on_disk)?;
        // corpus nodes
        let e_corpus_nodes: HashSet<String> = e_g.get_node_annos()
                                        .exact_anno_search(Some(&NODE_TYPE_KEY.ns), &NODE_TYPE_KEY.name, ValueSearch::Some("corpus"))
                                        .into_iter()
                                        .map(|r| r.unwrap().node)
                                        .map(|id_| e_g.get_node_annos().get_value_for_item(&id_, &NODE_NAME_KEY).unwrap().unwrap().to_string())
                                        .collect();
        let g_corpus_nodes: HashSet<String> = g.get_node_annos()
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
            "node ->dep node",
            "node ->dep[deprel=/.+/] node"
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
        let mut g = input_graph(on_disk)?;
        let mut properties = BTreeMap::new();
        properties.insert("edge.annos".to_string(), "deprel".to_string());
        properties.insert("node.annos".to_string(), "pos".to_string());
        let remover = Remove::default();
        assert_eq!(remover.manipulate_corpus(&mut g, &properties, None).is_ok(), true);
        let tmp_file = tempfile()?;
        let export = graphannis_core::graph::serialization::graphml::export(&g, None, tmp_file, |_| {});
        assert_eq!(export.is_ok(), true, "Export fails: {:?}", &export);
        Ok(())
    }

    fn input_graph(on_disk: bool) -> Result<AnnotationGraph> {
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
                                                    anno_name: "pos".to_string(), 
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
        let deprel_name = "deprel";
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

}