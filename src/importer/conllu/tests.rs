use std::{
    collections::{BTreeMap, BTreeSet},
    env::temp_dir,
    path::Path,
    sync::mpsc,
};

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

use crate::{importer::Importer, workflow::StatusMessage};

use super::ImportCoNLLU;

const TEST_PATH: &str = "tests/data/import/conll/valid";

#[test]
fn test_conll_fail_invalid() {
    let import = ImportCoNLLU::default();
    let import_path = Path::new("tests/data/import/conll/invalid");
    let job = import.import_corpus(import_path, &BTreeMap::new(), None);
    assert!(job.is_ok());
    let mut u = GraphUpdate::default();
    assert!(import
        .import_document(
            &mut u,
            import_path.join("test_file.conllu").as_path(),
            import_path.join("test_file").to_str().unwrap().to_string(),
            &None
        )
        .is_err());
}

#[test]
fn test_conll_fail_invalid_heads() {
    let import = ImportCoNLLU::default();
    let import_path = Path::new("tests/data/import/conll/invalid-heads/");
    let (sender, receiver) = mpsc::channel();
    let job = import.import_corpus(import_path, &BTreeMap::new(), Some(sender));
    assert!(job.is_ok());
    let fail_msgs = receiver.into_iter().filter(|s| match *s {
        StatusMessage::Failed(_) => true,
        _ => false,
    });
    assert!(fail_msgs.count() > 0);
}

#[test]
fn test_conll_fail_cyclic() -> Result<(), Box<dyn std::error::Error>> {
    let import = ImportCoNLLU::default();
    let import_path = Path::new("tests/data/import/conll/cyclic-deps/");
    let job = import.import_corpus(import_path, &BTreeMap::new(), None);
    assert!(job.is_ok());
    Ok(())
}

#[test]
fn test_conll_in_mem() {
    assert!(basic_test(false).is_ok());
}

#[test]
fn test_conll_on_disk() {
    assert!(basic_test(true).is_ok());
}

fn basic_test(on_disk: bool) -> Result<(), Box<dyn std::error::Error>> {
    let mut e_g = target_graph(on_disk)?;
    let import = ImportCoNLLU::default();
    let import_path = Path::new(TEST_PATH);
    let job = import.import_corpus(import_path, &BTreeMap::new(), None);
    assert!(job.is_ok());
    let mut u = job.unwrap();
    let mut g = AnnotationGraph::new(on_disk)?;
    g.apply_update(&mut u, |_| {})?;
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
        ("tok", 11),
        ("sent_id", 2),
        ("text", 2),
        ("sent_id _o_ tok", 11),
        ("text _o_ tok", 11),
        ("sent_id=/1/", 1),
        ("sent_id=/2/", 1),
        ("text=/They buy and sell books./", 1),
        ("text=/I have no clue./", 1),
        ("node ->dep node", 9),
        ("node ->dep[deprel=/.+/] node", 9),
        ("node ->dep[deprel=/nsubj/] node", 2),
        ("node ->dep[deprel=/cc/] node", 1),
        ("node ->dep[deprel=/conj/] node", 1),
        ("node ->dep[deprel=/obj/] node", 2),
        ("node ->dep[deprel=/punct/] node", 2),
        ("node ->dep[deprel=/det/] node", 1),
        ("node ->dep[deprel!=/det|punct|obj|conj|cc|nsubj/] node", 0),
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

fn target_graph(on_disk: bool) -> Result<AnnotationGraph, Box<dyn std::error::Error>> {
    let mut graph = AnnotationGraph::new(on_disk)?;
    let mut u = GraphUpdate::default();
    let case = "Case";
    let number = "Number";
    let person = "Person";
    let pron_type = "PronType";
    let tense = "Tense";
    let space_after = "SpaceAfter";
    let lemma = "lemma";
    let upos = "upos";
    let xpos = "xpos";
    let deprel = "deprel";
    let sent_id = "sent_id";
    let text = "text";
    // corpus nodes
    u.add_event(UpdateEvent::AddNode {
        node_name: "valid".to_string(),
        node_type: "corpus".to_string(),
    })?;
    u.add_event(UpdateEvent::AddNode {
        node_name: "valid/website_example".to_string(),
        node_type: "corpus".to_string(),
    })?;
    u.add_event(UpdateEvent::AddNodeLabel {
        node_name: "valid/website_example".to_string(),
        anno_ns: ANNIS_NS.to_string(),
        anno_name: "doc".to_string(),
        anno_value: "website_example".to_string(),
    })?;
    u.add_event(UpdateEvent::AddEdge {
        source_node: "valid/website_example".to_string(),
        target_node: "valid".to_string(),
        layer: ANNIS_NS.to_string(),
        component_type: AnnotationComponentType::PartOf.to_string(),
        component_name: "".to_string(),
    })?;
    // sentence_spans
    u.add_event(UpdateEvent::AddNode {
        node_name: "valid/website_example#s1".to_string(),
        node_type: "node".to_string(),
    })?;
    u.add_event(UpdateEvent::AddNodeLabel {
        node_name: "valid/website_example#s1".to_string(),
        anno_ns: "".to_string(),
        anno_name: sent_id.to_string(),
        anno_value: "1".to_string(),
    })?;
    u.add_event(UpdateEvent::AddNodeLabel {
        node_name: "valid/website_example#s1".to_string(),
        anno_ns: "".to_string(),
        anno_name: text.to_string(),
        anno_value: "They buy and sell books.".to_string(),
    })?;
    u.add_event(UpdateEvent::AddNode {
        node_name: "valid/website_example#s2".to_string(),
        node_type: "node".to_string(),
    })?;
    u.add_event(UpdateEvent::AddNodeLabel {
        node_name: "valid/website_example#s2".to_string(),
        anno_ns: "".to_string(),
        anno_name: sent_id.to_string(),
        anno_value: "2".to_string(),
    })?;
    u.add_event(UpdateEvent::AddNodeLabel {
        node_name: "valid/website_example#s2".to_string(),
        anno_ns: "".to_string(),
        anno_name: text.to_string(),
        anno_value: "I have no clue.".to_string(),
    })?;
    // tokens with annotations
    for (
        j,
        (
            form,
            lemma_val,
            upos_val,
            xpos_val,
            case_val,
            number_val,
            person_val,
            tense_val,
            space_val,
            pron_type_val,
            s_id,
        ),
    ) in [
        (
            "They",
            "they",
            "PRON",
            "PRP",
            Some("Nom"),
            Some("Plur"),
            None,
            None,
            None,
            None,
            1,
        ),
        (
            "buy",
            "buy",
            "VERB",
            "VBP",
            None,
            Some("Plur"),
            Some("3"),
            Some("Pres"),
            None,
            None,
            1,
        ),
        (
            "and", "and", "CONJ", "CC", None, None, None, None, None, None, 1,
        ),
        (
            "sell",
            "sell",
            "VERB",
            "VBP",
            None,
            Some("Plur"),
            Some("3"),
            Some("Pres"),
            None,
            None,
            1,
        ),
        (
            "books",
            "book",
            "NOUN",
            "NNS",
            None,
            Some("Plur"),
            None,
            None,
            Some("No"),
            None,
            1,
        ),
        (
            ".", ".", "PUNCT", ".", None, None, None, None, None, None, 1,
        ),
        (
            "I",
            "I",
            "PRON",
            "PRP",
            Some("Nom"),
            Some("Sing"),
            Some("1"),
            None,
            None,
            None,
            2,
        ),
        (
            "have",
            "have",
            "VERB",
            "VBP",
            None,
            Some("Sing"),
            Some("1"),
            Some("Pres"),
            None,
            None,
            2,
        ),
        (
            "no",
            "no",
            "NOUN",
            "NN",
            None,
            None,
            None,
            None,
            None,
            Some("Neg"),
            2,
        ),
        (
            "clue",
            "clue",
            "NOUN",
            "NN",
            None,
            Some("Sing"),
            None,
            None,
            Some("No"),
            None,
            2,
        ),
        (
            ".", ".", "PUNCT", ".", None, None, None, None, None, None, 2,
        ),
    ]
    .iter()
    .enumerate()
    {
        let i = j + 1;
        let node_name = format!("valid/website_example#t{i}");
        u.add_event(UpdateEvent::AddNode {
            node_name: node_name.to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: node_name.to_string(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "tok".to_string(),
            anno_value: form.to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: node_name.to_string(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "layer".to_string(),
            anno_value: "default_layer".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: node_name.to_string(),
            anno_ns: "".to_string(),
            anno_name: lemma.to_string(),
            anno_value: lemma_val.to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: node_name.to_string(),
            anno_ns: "".to_string(),
            anno_name: upos.to_string(),
            anno_value: upos_val.to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: node_name.to_string(),
            anno_ns: "".to_string(),
            anno_name: xpos.to_string(),
            anno_value: xpos_val.to_string(),
        })?;
        for (anno_name, anno_val) in [case, number, person, tense, pron_type, space_after]
            .iter()
            .zip([
                case_val,
                number_val,
                person_val,
                tense_val,
                pron_type_val,
                space_val,
            ])
        {
            if let Some(val) = anno_val {
                u.add_event(UpdateEvent::AddNodeLabel {
                    node_name: node_name.to_string(),
                    anno_ns: "".to_string(),
                    anno_name: anno_name.to_string(),
                    anno_value: val.to_string(),
                })?;
            }
        }
        if j > 0 {
            u.add_event(UpdateEvent::AddEdge {
                source_node: format!("valid/website_example#t{j}"),
                target_node: node_name.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::Ordering.to_string(),
                component_name: "".to_string(),
            })?;
        }
        let span_name = format!("valid/website_example#s{s_id}");
        u.add_event(UpdateEvent::AddEdge {
            source_node: span_name,
            target_node: node_name.to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::Coverage.to_string(),
            component_name: "".to_string(),
        })?;
    }
    // dependencies
    for (governor, dependent, deprel_value) in [
        (2, 1, "nsubj"),
        (4, 3, "cc"),
        (2, 4, "conj"),
        (2, 5, "obj"),
        (2, 6, "punct"),
        (8, 7, "nsubj"),
        (10, 9, "det"),
        (8, 10, "obj"),
        (8, 11, "punct"),
    ] {
        let source_node = format!("valid/website_example#t{governor}");
        let target_node = format!("valid/website_example#t{dependent}");
        u.add_event(UpdateEvent::AddEdge {
            source_node: source_node.to_string(),
            target_node: target_node.to_string(),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdgeLabel {
            source_node: source_node.to_string(),
            target_node: target_node.to_string(),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: "dep".to_string(),
            anno_ns: "".to_string(),
            anno_name: deprel.to_string(),
            anno_value: deprel_value.to_string(),
        })?;
    }
    graph.apply_update(&mut u, |_| {})?;
    Ok(graph)
}
