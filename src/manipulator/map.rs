use std::{
    env::temp_dir,
    fs,
    path::{Path, PathBuf},
};

use crate::{workflow::StatusSender, StepID};
use documented::{Documented, DocumentedFields};
use graphannis::{
    corpusstorage::{QueryLanguage, SearchQuery},
    update::{GraphUpdate, UpdateEvent},
    AnnotationGraph, CorpusStorage,
};
use itertools::Itertools;
use serde_derive::Deserialize;
use tempfile::tempdir_in;

use super::Manipulator;

/// Creates new annotations based on existing annotation values.
#[derive(Deserialize, Documented, DocumentedFields)]
#[serde(deny_unknown_fields)]
pub struct MapAnnos {
    rule_file: PathBuf,
}

impl Manipulator for MapAnnos {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        workflow_directory: &std::path::Path,
        _step_id: StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let read_from_path = {
            let p = Path::new(&self.rule_file).to_path_buf();
            if p.is_relative() {
                workflow_directory.join(p)
            } else {
                p
            }
        };
        let mapping = read_config(read_from_path.as_path())?;
        self.run(graph, mapping, &tx)?;
        Ok(())
    }
}

impl MapAnnos {
    fn run(
        &self,
        graph: &mut AnnotationGraph,
        mapping: Mapping,
        _tx: &Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let corpus_name = "current";
        let tmp_dir = tempdir_in(temp_dir())?;
        graph.save_to(&tmp_dir.path().join(corpus_name))?;
        let cs = CorpusStorage::with_auto_cache_size(tmp_dir.path(), true)?;
        for rule in mapping.rules {
            let query = SearchQuery {
                corpus_names: &["current"],
                query: rule.query.as_str(),
                query_language: QueryLanguage::AQL,
                timeout: None,
            };
            let search_results = cs.find(
                query,
                0,
                None,
                graphannis::corpusstorage::ResultOrder::NotSorted,
            )?;
            for m in search_results {
                let matching_nodes = m
                    .split(' ')
                    .filter_map(|s| s.split("::").last())
                    .collect_vec();
                let target = rule.target - 1;
                if let Some(node_name) = matching_nodes.get(target) {
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: node_name.to_string(),
                        anno_ns: rule.ns.to_string(),
                        anno_name: rule.name.to_string(),
                        anno_value: rule.value.to_string(),
                    })?;
                }
            }
        }
        graph.apply_update(&mut update, |_| {})?;
        Ok(())
    }
}

fn read_config(path: &Path) -> Result<Mapping, Box<dyn std::error::Error>> {
    let config_string = fs::read_to_string(path)?;
    let m: Mapping = toml::from_str(config_string.as_str())?;
    Ok(m)
}

#[derive(Deserialize)]
struct Mapping {
    rules: Vec<Rule>,
}

#[derive(Deserialize)]
struct Rule {
    query: String,
    target: usize,
    ns: String,
    name: String,
    value: String,
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, env::temp_dir, sync::mpsc};

    use graphannis::{
        corpusstorage::{QueryLanguage, ResultOrder, SearchQuery},
        model::AnnotationComponentType,
        update::{GraphUpdate, UpdateEvent},
        AnnotationGraph, CorpusStorage,
    };
    use graphannis_core::{
        annostorage::ValueSearch,
        graph::{ANNIS_NS, NODE_NAME_KEY, NODE_TYPE_KEY},
        util::join_qname,
    };
    use itertools::Itertools;
    use tempfile::tempdir_in;

    use super::{MapAnnos, Mapping};

    #[test]
    fn test_map_annos_in_mem() {
        let r = main_test(false);
        assert!(r.is_ok(), "Error: {:?}", r.err());
    }

    #[test]
    fn test_map_annos_on_disk() {
        let r = main_test(true);
        assert!(r.is_ok(), "Error: {:?}", r.err());
    }

    fn main_test(on_disk: bool) -> Result<(), Box<dyn std::error::Error>> {
        let config = r#"
            [mapping]

            [[rules]]            
            query = "tok=/I/"
            target = 1
            ns = ""
            name = "pos"
            value = "PRON"            
            
            [[rules]]
            query = "tok=/am/"
            target = 1
            ns = ""
            name = "pos"
            value = "VERB"            
            
            [[rules]]
            query = "tok=/in/"
            target = 1
            ns = ""
            name = "pos"
            value = "ADP"            
            
            [[rules]]
            query = "tok=/New York/"
            target = 1
            ns = ""
            name = "pos"
            value = "PROPN"
        "#;
        let mapping: Mapping = toml::from_str(config)?;
        let mapper = MapAnnos {
            rule_file: temp_dir().join("rule_file_test.toml"), // dummy path
        };
        let (sender, _receiver) = mpsc::channel();
        let mut g = source_graph(on_disk)?;
        let tx = Some(sender);
        mapper.run(&mut g, mapping, &tx)?;
        let mut e_g = target_graph(on_disk)?;
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
        assert_eq!(e_corpus_nodes, g_corpus_nodes);
        // anno names
        let e_anno_names = e_g.get_node_annos().annotation_keys()?;
        let g_anno_names = g.get_node_annos().annotation_keys()?;
        let e_name_iter = e_anno_names
            .iter()
            .sorted_by(|a, b| join_qname(&a.ns, &a.name).cmp(&join_qname(&b.ns, &b.name)));
        let g_name_iter = g_anno_names
            .iter()
            .sorted_by(|a, b| join_qname(&a.ns, &a.name).cmp(&join_qname(&b.ns, &b.name)));
        for (e_qname, g_qname) in e_name_iter.zip(g_name_iter) {
            assert_eq!(
                e_qname, g_qname,
                "Differing annotation keys between expected and generated graph: `{:?}` vs. `{:?}`",
                e_qname, g_qname
            );
        }
        assert_eq!(
            e_anno_names.len(),
            g_anno_names.len(),
            "Expected graph and generated graph do not contain the same number of annotation keys."
        );
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
        //test with queries
        let queries = [
            ("tok=/I/ _=_ pos=/PRON/", 1),
            ("tok=/am/ _=_ pos=/VERB/", 1),
            ("tok=/in/ _=_ pos=/ADP/", 1),
            ("tok=/New York/ _=_ pos=/PROPN/", 1),
        ];
        let corpus_name = "current";
        let tmp_dir_e = tempdir_in(temp_dir())?;
        let tmp_dir_g = tempdir_in(temp_dir())?;
        e_g.save_to(&tmp_dir_e.path().join(corpus_name))?;
        g.save_to(&tmp_dir_g.path().join(corpus_name))?;
        let cs_e = CorpusStorage::with_auto_cache_size(&tmp_dir_e.path(), true)?;
        let cs_g = CorpusStorage::with_auto_cache_size(&tmp_dir_g.path(), true)?;
        for (query_s, expected_n) in queries {
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
        expected_n,
        "Number of results for query `{}` does not match for expected graph. Expected:{} vs. Is:{}",
        query_s,
        expected_n,
        matches_e.len()
    );
            assert_eq!(
                matches_e.len(),
                matches_g.len(),
                "Failed with query: {}",
                query_s
            );
        }
        Ok(())
    }

    fn source_graph(on_disk: bool) -> Result<AnnotationGraph, Box<dyn std::error::Error>> {
        let mut g = AnnotationGraph::new(on_disk)?;
        let mut u = GraphUpdate::default();
        u.add_event(UpdateEvent::AddNode {
            node_name: "doc".to_string(),
            node_type: "corpus".to_string(),
        })?;
        for (i, text) in ["I", "am", "in", "New York"].iter().enumerate() {
            let node_name = format!("doc#t{}", &i + &1);
            u.add_event(UpdateEvent::AddNode {
                node_name: node_name.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: text.to_string(),
            })?;
            if i > 0 {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("doc#t{i}"),
                    target_node: node_name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "".to_string(),
                })?;
            }
        }
        g.apply_update(&mut u, |_| {})?;
        Ok(g)
    }

    fn target_graph(on_disk: bool) -> Result<AnnotationGraph, Box<dyn std::error::Error>> {
        let mut g = source_graph(on_disk)?;
        let mut u = GraphUpdate::default();
        for (i, pos_val) in ["PRON", "VERB", "ADP", "PROPN"].iter().enumerate() {
            let node_name = format!("doc#t{}", &i + &1);
            u.add_event(UpdateEvent::AddNode {
                node_name: node_name.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_name.to_string(),
                anno_ns: "".to_string(),
                anno_name: "pos".to_string(),
                anno_value: pos_val.to_string(),
            })?;
        }
        g.apply_update(&mut u, |_| {})?;
        Ok(g)
    }
}
