//! Created edges between nodes based on their annotation value.
use super::Manipulator;
use crate::{
    deserialize::deserialize_annotation_component, error::AnnattoError, progress::ProgressReporter,
    workflow::StatusSender, StepID,
};
use anyhow::anyhow;
use documented::{Documented, DocumentedFields};
use graphannis::{
    corpusstorage::{QueryLanguage, ResultOrder, SearchQuery},
    model::AnnotationComponent,
    update::{GraphUpdate, UpdateEvent},
    AnnotationGraph, CorpusStorage,
};
use graphannis_core::{types::AnnoKey, util::split_qname};
use itertools::Itertools;
use serde_derive::Deserialize;
use std::{collections::BTreeMap, env::temp_dir};
use struct_field_names_as_array::FieldNamesAsSlice;
use tempfile::tempdir_in;

/// Link nodes within a graph. Source and target of a link are determined via
/// queries; type, layer, and name of the link component can be configured.
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(deny_unknown_fields)]
pub struct LinkNodes {
    /// The AQL query to find all source node annotations. Source and target nodes are then paired by equal value for their query match.
    source_query: String,
    /// The 1-based index selecting the value providing node in the AQL source query.    
    source_node: usize,
    /// Contains one or multiple 1-based indexes, from which (in order of mentioning) the value for mapping source and target will be concatenated.
    source_value: Vec<usize>,
    /// The AQL query to find all target node annotations.
    target_query: String,
    /// The 1-based index selecting the value providing node in the AQL target query.    
    target_node: usize,
    /// Contains one or multiple 1-based indexes, from which (in order of mentioning) the value for mapping source and target will be concatenated.
    target_value: Vec<usize>,
    /// The edge component to be built.
    #[serde(deserialize_with = "deserialize_annotation_component")]
    component: AnnotationComponent,
    /// In case of multiple `source_values` or `target_values` this delimiter (default empty string) will be used for value concatenation.
    #[serde(default)]
    value_sep: String,
}

impl Manipulator for LinkNodes {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        _workflow_directory: &std::path::Path,
        step_id: StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let db_dir = tempdir_in(temp_dir())?;
        graph.save_to(db_dir.path().join("current").as_path())?;
        let cs = CorpusStorage::with_auto_cache_size(db_dir.path(), true)?;
        let link_sources = gather_link_data(
            graph,
            &cs,
            self.source_query.to_string(),
            self.source_node,
            &self.source_value,
            &self.value_sep,
            &step_id,
        )?;
        let link_targets = gather_link_data(
            graph,
            &cs,
            self.target_query.to_string(),
            self.target_node,
            &self.target_value,
            &self.value_sep,
            &step_id,
        )?;
        let mut update = self.link_nodes(link_sources, link_targets, tx, step_id)?;
        graph.apply_update(&mut update, |_| {})?;
        Ok(())
    }
}

type NodeBundle = Vec<(Option<AnnoKey>, String)>;

/// This function executes a single query and returns bundled results or an error.
/// A bundled result is the annotation key the node has a match for and the matching node itself.
fn retrieve_nodes_with_values(
    cs: &CorpusStorage,
    query: String,
) -> Result<Vec<NodeBundle>, Box<dyn std::error::Error>> {
    let mut node_bundles = Vec::new();
    for m in cs.find(
        SearchQuery {
            corpus_names: &["current"],
            query: query.as_str(),
            query_language: QueryLanguage::AQL,
            timeout: None,
        },
        0,
        None,
        ResultOrder::Normal,
    )? {
        node_bundles.push(
            m.split(' ')
                .map(|match_member| {
                    match_member
                        .rsplit_once("::")
                        .map_or((None, match_member.to_string()), |(pref, suff)| {
                            (Some(anno_key(pref)), suff.to_string())
                        })
                })
                .collect_vec(),
        );
    }
    Ok(node_bundles)
}

fn anno_key(qname: &str) -> AnnoKey {
    let (ns, name) = split_qname(qname);
    AnnoKey {
        name: name.into(),
        ns: match ns {
            None => "".into(),
            Some(v) => v.into(),
        },
    }
}

/// This function queries the corpus graph and returns the relevant match data.
/// The returned data maps an annotation value or a joint value (value) to the nodes holding said value.
fn gather_link_data(
    graph: &AnnotationGraph,
    cs: &CorpusStorage,
    query: String,
    node_index: usize,
    value_indices: &[usize],
    sep: &str,
    step_id: &StepID,
) -> Result<BTreeMap<String, Vec<String>>, Box<dyn std::error::Error>> {
    let mut data: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let node_annos = graph.get_node_annos();
    for group_of_bundles in retrieve_nodes_with_values(cs, query.to_string())? {
        if let Some((_, link_node_name)) = group_of_bundles.get(node_index - 1) {
            let mut target_data = Vec::new();
            let mut value_segments = Vec::new();
            for value_index in value_indices {
                if let Some((Some(anno_key), carrying_node_name)) =
                    group_of_bundles.get(*value_index - 1)
                {
                    let value_node_id = if let Some(node_id) =
                        node_annos.get_node_id_from_name(carrying_node_name)?
                    {
                        node_id
                    } else {
                        return Err(anyhow!(
                            "Could not determine node id from name {}",
                            carrying_node_name
                        )
                        .into());
                    };
                    if let Some(anno_value) =
                        node_annos.get_value_for_item(&value_node_id, anno_key)?
                    {
                        value_segments.push(anno_value.trim().to_lowercase()); // simply concatenate values
                    }
                } else {
                    return Err(AnnattoError::Manipulator {
                        reason: format!(
                            "Could not extract node with value index {value_index} from query `{}`",
                            &query
                        ),
                        manipulator: step_id.module_name.to_string(),
                    }
                    .into());
                }
                target_data.push(link_node_name.to_string());
            }
            let joint_value = value_segments.join(sep);
            if let Some(nodes_with_value) = data.get_mut(&joint_value) {
                nodes_with_value.extend(target_data);
            } else {
                data.insert(joint_value, target_data);
            }
        } else {
            return Err(AnnattoError::Manipulator {
                reason: format!(
                    "Could not extract node with node index {node_index} from query `{}`",
                    &query
                ),
                manipulator: step_id.module_name.to_string(),
            }
            .into());
        }
    }
    Ok(data)
}

impl LinkNodes {
    fn link_nodes(
        &self,
        sources: BTreeMap<String, Vec<String>>,
        targets: BTreeMap<String, Vec<String>>,
        tx: Option<StatusSender>,
        step_id: StepID,
    ) -> Result<GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let progress = ProgressReporter::new(tx, step_id, sources.len())?;
        for (anno_value, node_list) in sources {
            if let Some(target_node_list) = targets.get(&anno_value) {
                for (source, target) in node_list.iter().cartesian_product(target_node_list) {
                    update.add_event(UpdateEvent::AddEdge {
                        source_node: source.to_string(),
                        target_node: target.to_string(),
                        layer: self.component.layer.to_string(),
                        component_type: self.component.get_type().to_string(),
                        component_name: self.component.name.to_string(),
                    })?;
                }
            }
            progress.worked(1)?;
        }
        Ok(update)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::mpsc,
    };

    use graphannis::{
        corpusstorage::{QueryLanguage, ResultOrder, SearchQuery},
        model::{AnnotationComponent, AnnotationComponentType},
        update::{GraphUpdate, UpdateEvent},
        AnnotationGraph, CorpusStorage,
    };
    use graphannis_core::{
        annostorage::ValueSearch,
        graph::{ANNIS_NS, NODE_NAME_KEY, NODE_TYPE_KEY},
        util::join_qname,
    };
    use itertools::Itertools;
    use tempfile::{tempdir, TempDir};

    use crate::{
        manipulator::{
            link::{gather_link_data, retrieve_nodes_with_values, LinkNodes},
            Manipulator,
        },
        workflow::StatusMessage,
        StepID,
    };

    #[test]
    fn test_linker_on_disk() {
        let r = main_test(true);
        assert!(r.is_ok(), "Error in main test: {:?}", r.err());
    }

    #[test]
    fn test_linker_in_mem() {
        let r = main_test(false);
        assert!(r.is_ok(), "Error in main test: {:?}", r.err());
    }

    fn main_test(on_disk: bool) -> Result<(), Box<dyn std::error::Error>> {
        let mut e_g = target_graph(on_disk)?;
        let (sender, receiver) = mpsc::channel();
        let mut g = source_graph(on_disk)?;
        let linker = LinkNodes {
            source_query: "norm _=_ lemma".to_string(),
            source_node: 1,
            source_value: vec![2],
            target_query: "morph & node? !> #1".to_string(),
            target_node: 1,
            target_value: vec![1],
            component: AnnotationComponent::new(
                AnnotationComponentType::Pointing,
                "".into(),
                "morphology".into(),
            ),
            value_sep: "".to_string(),
        };
        let dummy_dir = tempdir()?;
        let dummy_path = dummy_dir.path();
        let step_id = StepID {
            module_name: "linler".to_string(),
            path: None,
        };
        let link = linker.manipulate_corpus(&mut g, dummy_path, step_id, Some(sender));
        assert!(
            link.is_ok(),
            "Importer update failed with error: {:?}",
            link.err()
        );
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
            ("norm:norm ->morphology node", 2),
            ("node ->morphology node", 2),
            ("node ->morphology morph", 2),
            ("norm:norm ->morphology morph=/New York/", 1),
            ("norm:norm ->morphology morph=/New York/ > node", 2),
            ("norm:norm ->morphology morph=/New York/ > morph=/New/", 1),
            ("norm:norm ->morphology morph=/New York/ > morph=/York/", 1),
            ("norm:norm ->morphology morph=/I/", 1),
        ];
        let corpus_name = "current";
        let (cs_e, _tmpe) = store_corpus(&mut e_g, corpus_name)?;
        let (cs_g, _tmpg) = store_corpus(&mut g, corpus_name)?;
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
            for (match_g, match_e) in matches_g.iter().zip(&matches_e) {
                assert_eq!(match_g, match_e);
            }
        }
        let message_count = receiver
            .into_iter()
            .filter(|m| !matches!(m, &StatusMessage::Progress { .. }))
            .count();
        assert_eq!(0, message_count);
        Ok(())
    }

    fn store_corpus(
        graph: &mut AnnotationGraph,
        corpus_name: &str,
    ) -> Result<(CorpusStorage, TempDir), Box<dyn std::error::Error>> {
        let tmp_dir = tempdir()?;
        graph.save_to(&tmp_dir.path().join(corpus_name))?;
        Ok((
            CorpusStorage::with_auto_cache_size(&tmp_dir.path(), true)?,
            tmp_dir,
        ))
    }

    #[test]
    fn test_retrieve_nodes_with_values() {
        let g = source_graph(false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let corpus_name = "current";
        let cs_r = store_corpus(&mut graph, corpus_name);
        assert!(cs_r.is_ok());
        let (cs, _tmp) = cs_r.unwrap();
        // 1
        let r1 = retrieve_nodes_with_values(&cs, "tok=/.*/".to_string());
        assert!(r1.is_ok(), "not Ok: {:?}", r1.err());
        let results_1 = r1.unwrap();
        assert_eq!(6, results_1.len());
        for match_v in results_1 {
            assert_eq!(1, match_v.len());
            assert!(match_v.get(0).unwrap().0.is_none());
        }
        // 2
        let r2 = retrieve_nodes_with_values(&cs, "norm _=_ pos".to_string());
        assert!(r2.is_ok(), "not Ok: {:?}", r2.err());
        let results_2 = r2.unwrap();
        assert_eq!(4, results_2.len());
        for match_v in results_2 {
            assert_eq!(2, match_v.len());
            assert!(match_v.get(0).unwrap().0.is_some());
        }
    }

    #[test]
    fn test_gather_link_data() {
        let g = source_graph(false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let corpus_name = "current";
        let cs_r = store_corpus(&mut graph, corpus_name);
        assert!(cs_r.is_ok());
        let (cs, _tmp) = cs_r.unwrap();
        let ldr = gather_link_data(
            &graph,
            &cs,
            "norm _=_ pos".to_string(),
            1,
            &[1, 2],
            &" ".to_string(),
            &StepID {
                module_name: "link".to_string(),
                path: None,
            },
        );
        assert!(ldr.is_ok(), "not Ok: {:?}", ldr.err());
        let link_data = ldr.unwrap();
        let expected_link_data: BTreeMap<String, Vec<String>> = vec![
            (
                "i pron".to_string(),
                vec![
                    "import/exmaralda/test_doc#t_norm_T286-T0".to_string(),
                    "import/exmaralda/test_doc#t_norm_T286-T0".to_string(),
                ],
            ),
            (
                "am verb".to_string(),
                vec![
                    "import/exmaralda/test_doc#t_norm_T0-T1".to_string(),
                    "import/exmaralda/test_doc#t_norm_T0-T1".to_string(),
                ],
            ),
            (
                "in adp".to_string(),
                vec![
                    "import/exmaralda/test_doc#t_norm_T1-T2".to_string(),
                    "import/exmaralda/test_doc#t_norm_T1-T2".to_string(),
                ],
            ),
            (
                "new york propn".to_string(),
                vec![
                    "import/exmaralda/test_doc#t_norm_T2-T4".to_string(),
                    "import/exmaralda/test_doc#t_norm_T2-T4".to_string(),
                ],
            ),
        ]
        .into_iter()
        .collect();
        assert_eq!(expected_link_data, link_data);
    }

    #[test]
    fn test_link_nodes() {
        let g = source_graph(false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let linker = LinkNodes {
            source_query: "dummy query -- value not used".to_string(),
            source_node: 1,        // dummy value
            source_value: vec![1], // dummy value
            target_query: "dummy query -- value not used".to_string(),
            target_node: 1,        // dummy value
            target_value: vec![1], // dummy value
            component: AnnotationComponent::new(
                AnnotationComponentType::Pointing,
                "".into(),
                "link".into(),
            ),
            value_sep: "dummy value".to_string(),
        };
        let source_map = vec![
            (
                "i_am_im".to_string(),
                vec!["import/exmaralda/test_doc#t_dipl_T286-T1".to_string()],
            ),
            (
                "New_York".to_string(),
                vec![
                    "import/exmaralda/test_doc#t_dipl_T2-T3".to_string(),
                    "import/exmaralda/test_doc#t_dipl_T3-T4".to_string(),
                ],
            ),
        ]
        .into_iter()
        .collect();
        let target_map = vec![
            (
                "i_am_im".to_string(),
                vec![
                    "import/exmaralda/test_doc#t_norm_T286-T0".to_string(),
                    "import/exmaralda/test_doc#t_norm_T0-T1".to_string(),
                ],
            ),
            (
                "New_York".to_string(),
                vec!["import/exmaralda/test_doc#t_norm_T2-T4".to_string()],
            ),
        ]
        .into_iter()
        .collect();
        let r = linker.link_nodes(
            source_map,
            target_map,
            None,
            StepID {
                module_name: "test".to_string(),
                path: None,
            },
        );
        assert!(r.is_ok());
        let mut u = r.unwrap();
        assert!(graph.apply_update(&mut u, |_| {}).is_ok());
        let storage_bundle = store_corpus(&mut graph, "current");
        assert!(storage_bundle.is_ok());
        let (cs, _tmp) = storage_bundle.unwrap();
        let queries_with_results = [
            ("dipl=/I'm/ ->link norm=/I/ . norm=/am/ & #1 ->link #3", 1),
            (
                "dipl=/New/ . dipl=/York/ & #1 ->link norm=/New York/ & #2 ->link #3",
                1,
            ),
        ];
        for (q, n) in queries_with_results {
            let query = SearchQuery {
                corpus_names: &["current"],
                query: q,
                query_language: QueryLanguage::AQL,
                timeout: None,
            };
            let c = cs.count(query);
            assert!(c.is_ok());
            assert_eq!(n, c.unwrap());
        }
    }

    fn source_graph(on_disk: bool) -> Result<AnnotationGraph, Box<dyn std::error::Error>> {
        // copied this from exmaralda test
        let mut graph = AnnotationGraph::with_default_graphstorages(on_disk)?;
        let mut u = GraphUpdate::default();
        u.add_event(UpdateEvent::AddNode {
            node_name: "import".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "import/exmaralda".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "import/exmaralda".to_string(),
            target_node: "import".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "import/exmaralda/test_doc".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "import/exmaralda/test_doc".to_string(),
            target_node: "import/exmaralda".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        let tlis = ["T286", "T0", "T1", "T2", "T3", "T4"];
        let times = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0];
        for tli in tlis {
            let node_name = format!("import/exmaralda/test_doc#{}", tli);
            u.add_event(UpdateEvent::AddNode {
                node_name: node_name.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: " ".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "layer".to_string(),
                anno_value: "default_layer".to_string(),
            })?;
        }
        for window in tlis.windows(2) {
            let tli0 = window[0];
            let tli1 = window[1];
            let source = format!("import/exmaralda/test_doc#{}", tli0);
            let target = format!("import/exmaralda/test_doc#{}", tli1);
            u.add_event(UpdateEvent::AddEdge {
                source_node: source,
                target_node: target,
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::Ordering.to_string(),
                component_name: "".to_string(),
            })?;
        }
        let mut prev: Option<String> = None;
        for (tpe, spk, name, value, start, end, reset_after) in [
            ("t", "dipl", "dipl", "I'm", 0, 2, false),
            ("t", "dipl", "dipl", "in", 2, 3, false),
            ("t", "dipl", "dipl", "New", 3, 4, false),
            ("t", "dipl", "dipl", "York", 4, 5, true),
            ("a", "dipl", "sentence", "1", 0, 5, true),
            ("t", "norm", "norm", "I", 0, 1, false),
            ("t", "norm", "norm", "am", 1, 2, false),
            ("t", "norm", "norm", "in", 2, 3, false),
            ("t", "norm", "norm", "New York", 3, 5, true),
            ("a", "norm", "lemma", "I", 0, 1, true),
            ("a", "norm", "lemma", "be", 1, 2, true),
            ("a", "norm", "lemma", "in", 2, 3, true),
            ("a", "norm", "lemma", "New York", 3, 5, true),
            ("a", "norm", "pos", "PRON", 0, 1, true),
            ("a", "norm", "pos", "VERB", 1, 2, true),
            ("a", "norm", "pos", "ADP", 2, 3, true),
            ("a", "norm", "pos", "PROPN", 3, 5, true),
        ] {
            let node_name = format!(
                "{}#{}_{}_{}-{}",
                "import/exmaralda/test_doc", tpe, spk, tlis[start], tlis[end]
            );
            let start_time = times[start];
            let end_time = times[end];
            u.add_event(UpdateEvent::AddNode {
                node_name: node_name.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "time".to_string(),
                anno_value: format!("{}-{}", start_time, end_time),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "layer".to_string(),
                anno_value: spk.to_string(),
            })?;
            if tpe == "t" {
                u.add_event(UpdateEvent::AddNodeLabel {
                    node_name: node_name.to_string(),
                    anno_ns: ANNIS_NS.to_string(),
                    anno_name: "tok".to_string(),
                    anno_value: value.to_string(),
                })?;
                u.add_event(UpdateEvent::AddEdge {
                    source_node: node_name.to_string(),
                    target_node: "import/exmaralda/test_doc".to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::PartOf.to_string(),
                    component_name: "".to_string(),
                })?;
                if let Some(other_name) = prev {
                    u.add_event(UpdateEvent::AddEdge {
                        source_node: other_name,
                        target_node: node_name.to_string(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::Ordering.to_string(),
                        component_name: spk.to_string(),
                    })?;
                }
                prev = if reset_after {
                    None
                } else {
                    Some(node_name.to_string())
                }
            }
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_name.to_string(),
                anno_ns: spk.to_string(),
                anno_name: name.to_string(),
                anno_value: value.to_string(),
            })?;
            for i in start..end {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: node_name.to_string(),
                    target_node: format!("import/exmaralda/test_doc#{}", tlis[i]),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Coverage.to_string(),
                    component_name: "".to_string(),
                })?;
            }
        }
        // add unlinked corpus nodes
        u.add_event(UpdateEvent::AddNode {
            node_name: "import/lex".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "import/lex/new_york".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "import/lex/i".to_string(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "import/lex".to_string(),
            target_node: "import".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "import/lex/new_york".to_string(),
            target_node: "import/lex".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "import/lex/i".to_string(),
            target_node: "import/lex".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        // add unlinked data nodes
        u.add_event(UpdateEvent::AddNode {
            node_name: "import/lex/new_york#root".to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "import/lex/new_york#root".to_string(),
            anno_ns: "".to_string(),
            anno_name: "morph".to_string(),
            anno_value: "New York".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "import/lex/new_york#root".to_string(),
            target_node: "import/lex/new_york".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "import/lex/new_york#m1".to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "import/lex/new_york#m1".to_string(),
            anno_ns: "".to_string(),
            anno_name: "morph".to_string(),
            anno_value: "New".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "import/lex/new_york#m2".to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "import/lex/new_york#m2".to_string(),
            anno_ns: "".to_string(),
            anno_name: "morph".to_string(),
            anno_value: "York".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "import/lex/new_york#root".to_string(),
            target_node: "import/lex/new_york#m1".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::Dominance.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "import/lex/new_york#root".to_string(),
            target_node: "import/lex/new_york#m2".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::Dominance.to_string(),
            component_name: "".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNode {
            node_name: "import/lex/i#root".to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: "import/lex/i#root".to_string(),
            anno_ns: "".to_string(),
            anno_name: "morph".to_string(),
            anno_value: "I".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "import/lex/i#root".to_string(),
            target_node: "import/lex/i".to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        graph.apply_update(&mut u, |_| {})?;
        Ok(graph)
    }

    fn target_graph(on_disk: bool) -> Result<AnnotationGraph, Box<dyn std::error::Error>> {
        let mut u = GraphUpdate::default();
        u.add_event(UpdateEvent::AddEdge {
            source_node: "import/exmaralda/test_doc#t_norm_T2-T4".to_string(),
            target_node: "import/lex/new_york#root".to_string(),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "morphology".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: "import/exmaralda/test_doc#t_norm_T286-T0".to_string(),
            target_node: "import/lex/i#root".to_string(),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "morphology".to_string(),
        })?;
        let mut g = source_graph(on_disk)?;
        g.apply_update(&mut u, |_| {})?;
        Ok(g)
    }
}
