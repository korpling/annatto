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
use tempfile::tempdir_in;

use crate::{importer::Importer, util::graphupdate::map_audio_source};

use super::ImportEXMARaLDA;

use itertools::Itertools;

#[test]
fn test_exb_fail_for_timeline() {
    let import = ImportEXMARaLDA::default();
    let import_path = "./tests/data/import/exmaralda/fail-corrupt_timeline/import/";
    let (sender, receiver) = mpsc::channel();
    let r = import.import_corpus(Path::new(import_path), &BTreeMap::new(), Some(sender));
    assert!(r.is_ok());
    assert!(receiver.into_iter().count() > 0);
    let document_path = "./tests/data/import/exmaralda/fail-corrupt_timeline/import/test_doc.exb";
    let mut u = GraphUpdate::default();
    assert!(import
        .import_document(
            Path::new(import_path),
            Path::new(document_path),
            &mut u,
            &None
        )
        .is_err());
}

#[test]
fn test_exb_fail_for_no_category() {
    let import = ImportEXMARaLDA::default();
    let import_path = "./tests/data/import/exmaralda/fail-no_category/";
    let (sender, receiver) = mpsc::channel();
    let r = import.import_corpus(Path::new(import_path), &BTreeMap::new(), Some(sender));
    assert!(r.is_ok());
    assert!(receiver.into_iter().count() > 0);
    let document_path = Path::new(import_path).join("test_doc.exb");
    let mut u = GraphUpdate::default();
    assert!(import
        .import_document(
            Path::new(import_path),
            document_path.as_path(),
            &mut u,
            &None
        )
        .is_err());
}

#[test]
fn test_exb_fail_for_no_speaker() {
    let import = ImportEXMARaLDA::default();
    let import_path = "./tests/data/import/exmaralda/fail-no_speaker/";
    let (sender, receiver) = mpsc::channel();
    let r = import.import_corpus(Path::new(import_path), &BTreeMap::new(), Some(sender));
    assert!(r.is_ok());
    assert!(receiver.into_iter().count() > 0);
    let document_path = Path::new(import_path).join("test_doc.exb");
    let mut u = GraphUpdate::default();
    assert!(import
        .import_document(
            Path::new(import_path),
            document_path.as_path(),
            &mut u,
            &None
        )
        .is_err());
}

#[test]
fn test_exb_fail_for_undefined_speaker() {
    let import = ImportEXMARaLDA::default();
    let import_path = "./tests/data/import/exmaralda/fail-undefined_speaker/";
    let (sender, receiver) = mpsc::channel();
    let r = import.import_corpus(Path::new(import_path), &BTreeMap::new(), Some(sender));
    assert!(r.is_ok());
    assert!(receiver.into_iter().count() > 0);
    let document_path = Path::new(import_path).join("test_doc.exb");
    let mut u = GraphUpdate::default();
    assert!(import
        .import_document(
            Path::new(import_path),
            document_path.as_path(),
            &mut u,
            &None
        )
        .is_err());
}

#[test]
fn test_fail_invalid() {
    let import = ImportEXMARaLDA::default();
    let import_path = "./tests/data/import/exmaralda/fail-invalid/import/";
    let (sender, receiver) = mpsc::channel();
    let r = import.import_corpus(Path::new(import_path), &BTreeMap::new(), Some(sender));
    assert!(r.is_ok());
    assert!(receiver.into_iter().count() > 0);
    let document_path = "./tests/data/import/exmaralda/fail-invalid/import/test_doc_invalid.exb";
    let mut u = GraphUpdate::default();
    assert!(import
        .import_document(
            Path::new(import_path),
            Path::new(document_path),
            &mut u,
            &None
        )
        .is_err());
}

#[test]
fn test_exb_in_mem() {
    let r = test_exb(false, true);
    assert_eq!(r.is_ok(), true, "Probing core test result {:?}", r);
}

#[test]
fn test_exb_on_disk() {
    let r = test_exb(true, true);
    assert_eq!(r.is_ok(), true, "Probing core test result {:?}", r);
}

#[test]
fn test_exb_broken_audio_in_mem() {
    let r = test_exb(false, false);
    assert_eq!(r.is_ok(), true, "Probing core test result {:?}", r);
}

#[test]
fn test_exb_broken_audio_on_disk() {
    let r = test_exb(true, false);
    assert_eq!(r.is_ok(), true, "Probing core test result {:?}", r);
}

fn test_exb(on_disk: bool, with_audio: bool) -> Result<(), Box<dyn std::error::Error>> {
    let mut e_g = target_graph(on_disk, with_audio)?;
    let import = ImportEXMARaLDA::default();
    let import_path = if with_audio {
        "./tests/data/import/exmaralda/clean/import/"
    } else {
        "./tests/data/import/exmaralda/broken_audio/import/"
    };
    let (sender, receiver) = mpsc::channel();
    let mut update =
        import.import_corpus(Path::new(import_path), &BTreeMap::new(), Some(sender))?;
    let mut g = AnnotationGraph::new(on_disk)?;
    let update_app = g.apply_update(&mut update, |_| {});
    assert!(
        update_app.is_ok(),
        "Importer update failed with error: {:?}",
        update_app.err()
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
        ("norm:norm", 4),
        ("dipl:dipl", 4),
        ("norm:norm _o_ dipl:dipl", 5),
        ("dipl:sentence _o_ norm:norm", 4),
        ("dipl:sentence _o_ dipl:dipl", 4),
        ("dipl:sentence", 1),
        ("dipl:dipl . dipl:dipl", 3),
        ("norm:norm . norm:norm", 3),
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
        for (match_g, match_e) in matches_g.iter().zip(&matches_e) {
            assert_eq!(match_g, match_e);
        }
    }
    // if with_audio is false, it means we are testing with a broken audio link
    // which is supposed to be reported and leave a trace in the receiver
    let message_count = receiver.into_iter().count();
    assert_eq!(!with_audio, message_count > 0);
    // also, there should be no warnings for the regular test case
    assert_eq!(with_audio, message_count == 0);
    Ok(())
}

fn target_graph(
    on_disk: bool,
    with_audio: bool,
) -> Result<AnnotationGraph, Box<dyn std::error::Error>> {
    let mut graph = AnnotationGraph::new(on_disk)?;
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
    if with_audio {
        map_audio_source(
            &mut u,
            Path::new("./import/exmaralda/test_file.wav"),
            "import/exmaralda",
            "import/exmaralda/test_doc",
        )?;
    }
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
                anno_value: "value".to_string(),
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
    graph.apply_update(&mut u, |_| {})?;
    Ok(graph)
}
