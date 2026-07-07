use core::f64;
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::mpsc,
    usize,
};

use graphannis::{
    AnnotationGraph,
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::ANNIS_NS;
use insta::assert_snapshot;
use itertools::Itertools;
use tempfile::tempdir;
use toml;

use crate::{
    StepID,
    manipulator::{
        Manipulator,
        check::{AQLTest, FailurePolicy, QueryResult, ReportLevel, TestResult},
    },
    util::example_generator,
    util::update_graph_silent,
    workflow::StatusMessage,
};

use super::{Check, Test};

#[test]
fn serialize_custom() {
    let module = Check {
            policy: FailurePolicy::Warn,
            tests: vec![
                Test::QueryTest {
                    query: "tok @* doc=/largest-doc/".into(),
                    expected: QueryResult::SemiOpenInterval(1, f64::INFINITY),
                    description: "I expect a lot of tokens".to_string(),
                    policy: None
                },
                Test::QueryTest {
                    query: "pos".into(),
                    expected: QueryResult::ClosedQueryInterval(
                        "norm".into(),
                        "tok".into(),
                    ),
                    description: "Plausible number of pos annotations.".to_string(),
                    policy: None
                },
                Test::QueryTest {
                    query: "sentence".into(),
                    expected: QueryResult::ClosedLQueryInterval("doc".into(), 400),
                    description: "Plausible distribution of sentence annotations.".to_string(),
                    policy: None
                },
                Test::QueryTest {
                    query: "doc _ident_ author=/William Shakespeare/".into(),
                    expected: QueryResult::ClosedRQueryInterval(1, "doc".into()),
                    description: "At least one document in the corpus was written by Shakespeare, hopefully all of them!".to_string(),
                    policy: None
                },
                Test::QueryTest {
                    query: "lemma=/hello/".into(),
                    expected: QueryResult::SemiOpenQueryInterval("doc".into(), f64::INFINITY),
                    description: "There are at least as many hellos as there are documents.".to_string(),
                    policy: None
                },
                Test::LayerTest {
                    layers: vec![(
                        "Reflexive".to_string(),
                        vec!["yes".to_string(), "no".to_string()]
                            .into_iter()
                            .collect(),
                    )]
                    .into_iter()
                    .collect(),
                    edge: None,
                    optional: true
                },
            ],
            report: Some(ReportLevel::List),
            failed_only: true,
            save: Some(PathBuf::from("this/is/a/non-existing/path.log")),
            overwrite: false,
        };
    let serialization = toml::to_string(&module);
    assert!(
        serialization.is_ok(),
        "Serialization failed: {:?}",
        serialization.err()
    );
    assert_snapshot!(serialization.unwrap());
}

#[test]
fn fail_deserialization_with_bad_query() {
    let test: Result<Test, _> = toml::from_str(
        r#"
        query = "annis:tok @* doc"
        expected = [1, "tok"]
        description = "Test nothing."
        "#,
    );
    assert!(test.is_err());
    assert_snapshot!(test.err().unwrap());
}

#[test]
fn fail_deserialization_with_bad_query_in_expected_result() {
    let test: Result<Test, _> = toml::from_str(
        r#"
        query = "tok @* doc"
        expected = [1, "annis:tok"]
        description = "Test nothing."
        "#,
    );
    assert!(test.is_err());
    assert_snapshot!(test.err().unwrap());
}

#[test]
fn graph_statistics() {
    let g = AnnotationGraph::with_default_graphstorages(false);
    assert!(g.is_ok());
    let mut graph = g.unwrap();
    let mut u = GraphUpdate::default();
    example_generator::create_corpus_structure_simple(&mut u);
    assert!(update_graph_silent(&mut graph, &mut u).is_ok());
    let check: Check = Check {
        tests: vec![],
        report: None,
        failed_only: false,
        policy: FailurePolicy::Warn,
        save: None,
        overwrite: false,
    };
    assert!(
        check
            .validate_graph(
                &mut graph,
                StepID {
                    module_name: "test".to_string(),
                    path: None
                },
                None
            )
            .is_ok()
    );
    assert!(graph.global_statistics.is_some());
}

#[test]
fn test_check_on_disk() {
    let r = test(true);
    assert!(r.is_ok(), "Error when testing on disk: {:?}", r.err());
}

#[test]
fn test_check_in_mem() {
    let r = test(false);
    assert!(r.is_ok(), "Error when testing in memory: {:?}", r.err());
}

#[test]
fn test_failing_checks_on_disk() {
    let r = test_failing_checks(true, false);
    assert!(r.is_ok(), "Error when testing on disk: {:?}", r.err());
}

#[test]
fn test_failing_checks_in_mem() {
    let r = test_failing_checks(true, false);
    assert!(r.is_ok(), "Error when testing in memory: {:?}", r.err());
}

#[test]
fn test_failing_checks_with_nodes_on_disk() {
    let r = test_failing_checks(true, true);
    assert!(r.is_ok(), "Error when testing on disk: {:?}", r.err());
}

#[test]
fn test_failing_checks_with_nodes_in_mem() {
    let r = test_failing_checks(true, true);
    assert!(r.is_ok(), "Error when testing in memory: {:?}", r.err());
}

fn test(on_disk: bool) -> Result<(), Box<dyn std::error::Error>> {
    let serialized_data = fs::read_to_string("./tests/data/graph_op/check/serialized_check.toml")?;
    let mut check: Check = toml::from_str(serialized_data.as_str())?;
    let tmp_report_dir = tempdir()?;
    let report_path = if on_disk {
        // only test on disk
        Some(tmp_report_dir.as_ref().join("test_check_report.txt"))
    } else {
        None
    };
    check.save = report_path;
    let mut g = input_graph(on_disk, "corpus")?;

    let step_id = StepID {
        module_name: "check".to_string(),
        path: None,
    };

    let (sender, receiver) = mpsc::channel();
    check.manipulate_corpus(&mut g, tempdir()?.path(), step_id, Some(sender))?;
    assert!(check.report.is_some()); // if deserialization worked properly, `check` should be set to report
    assert!(matches!(check.report.as_ref().unwrap(), &ReportLevel::List));
    assert!(receiver.iter().count() > 0); // there should be a status report
    if let Some(path) = &check.save {
        let written_report = fs::read_to_string(path)?;
        assert_snapshot!(written_report);
    }
    Ok(())
}

#[test]
fn test_policy_hierarchy() {
    let serialized_data =
        fs::read_to_string("./tests/data/graph_op/check/competing_policies.toml").unwrap();
    let check: Check = toml::from_str(serialized_data.as_str()).unwrap();
    let mut g = input_graph(true, "corpus").unwrap();
    assert!(
        check
            .manipulate_corpus(
                &mut g,
                Path::new("./"),
                StepID {
                    module_name: "test_check_policies".to_string(),
                    path: None
                },
                None
            )
            .is_ok()
    );
}

#[test]
fn test_policy_hierarchy_fail() {
    let serialized_data =
        fs::read_to_string("./tests/data/graph_op/check/competing_policies_fail.toml").unwrap();
    let check: Check = toml::from_str(serialized_data.as_str()).unwrap();
    let mut g = input_graph(true, "corpus").unwrap();
    let run = check.manipulate_corpus(
        &mut g,
        Path::new("./"),
        StepID {
            module_name: "test_check_policies".to_string(),
            path: None,
        },
        None,
    );
    assert!(run.is_err());
    assert_snapshot!(run.err().unwrap());
}

fn test_failing_checks(on_disk: bool, with_nodes: bool) -> Result<(), Box<dyn std::error::Error>> {
    let toml_path = if with_nodes {
        "./tests/data/graph_op/check/serialized_check_failing_with_nodes.toml"
    } else {
        "./tests/data/graph_op/check/serialized_check_failing.toml"
    };
    let serialized_data = fs::read_to_string(toml_path)?;
    let check: Check = toml::from_str(serialized_data.as_str())?;
    let mut g = input_graph(on_disk, "corpus")?;
    let (sender, _receiver) = mpsc::channel();

    let step_id = StepID {
        module_name: "check".to_string(),
        path: None,
    };
    let result = check.manipulate_corpus(&mut g, tempdir()?.path(), step_id, Some(sender));
    assert!(result.is_err());
    assert!(check.report.is_some());
    if with_nodes {
        assert!(matches!(
            check.report.as_ref().unwrap(),
            ReportLevel::Verbose
        ));
    } else {
        assert!(matches!(check.report.as_ref().unwrap(), ReportLevel::List));
    }

    let (r, _) = check.run_tests(&mut g)?;
    assert!(
        r.iter()
            .map(|(_, tr)| match tr {
                TestResult::Failed { is, .. } => is.len(),
                TestResult::ProcessingError { .. } => 1,
                _ => 0,
            })
            .sum::<usize>()
            > 0
    );
    if with_nodes {
        assert!(
            r.iter()
                .any(|(_, tr)| matches!(tr, TestResult::Failed { .. }))
        );
    }
    Ok(())
}

#[test]
fn test_layer_check_in_mem() {
    let r = test_layer_check(false);
    assert!(r.is_ok(), "{:?}", r.err());
}

#[test]
fn test_layer_check_on_disk() {
    let r = test_layer_check(true);
    assert!(r.is_ok(), "{:?}", r.err());
}

#[test]
fn test_layer_check_fail_in_mem() {
    let r = test_layer_check_fail(false);
    assert!(r.is_ok(), "{:?}", r.err());
}

#[test]
fn test_layer_check_fail_on_disk() {
    let r = test_layer_check_fail(true);
    assert!(r.is_ok(), "{:?}", r.err());
}

fn test_layer_check(on_disk: bool) -> Result<(), Box<dyn std::error::Error>> {
    let mut g = input_graph(on_disk, "corpus")?;
    let toml_path = "./tests/data/graph_op/check/serialized_layer_check.toml";
    let s = fs::read_to_string(toml_path)?;
    let check: Check = toml::from_str(s.as_str())?;
    let (results, _) = check.run_tests(&mut g)?;
    let all_pass = results
        .iter()
        .all(|(_, tr)| matches!(tr, TestResult::Passed));
    if !all_pass {
        let table_string = Check::results_to_table(&results, &ReportLevel::Verbose, false);
        println!("{}", table_string);
    }
    assert!(all_pass);
    Ok(())
}

fn test_layer_check_fail(on_disk: bool) -> Result<(), Box<dyn std::error::Error>> {
    let mut g = input_graph(on_disk, "corpus")?;
    let toml_path = "./tests/data/graph_op/check/serialized_layer_check_failing.toml";
    let s = fs::read_to_string(toml_path)?;
    let check: Check = toml::from_str(s.as_str())?;
    let (results, _) = check.run_tests(&mut g)?;
    let failing = results
        .iter()
        .filter(|(_, tr)| matches!(tr, TestResult::Failed { .. }))
        .count();
    let passing = results
        .iter()
        .filter(|(_, tr)| matches!(tr, TestResult::Passed))
        .count();
    if passing != failing {
        let table_string = Check::results_to_table(&results, &ReportLevel::Verbose, false);
        println!("{}", table_string);
    }
    assert_eq!(passing, failing);
    Ok(())
}

#[test]
fn test_layer_check_fail_policy_warn() {
    let gr = input_graph(false, "corpus");
    assert!(gr.is_ok());
    let mut g = gr.unwrap();
    let toml_path = "./tests/data/graph_op/check/serialized_layer_check_failing_warn.toml";
    if let Ok(s) = fs::read_to_string(toml_path) {
        let processor_opt: Result<Check, _> = toml::from_str(s.as_str());
        assert!(processor_opt.is_ok());
        let check = processor_opt.unwrap();
        let (sender, receiver) = mpsc::channel();
        let tmp = tempdir();
        assert!(tmp.is_ok());
        let dummy_value = tmp.unwrap();

        let step_id = StepID {
            module_name: "check".to_string(),
            path: None,
        };
        let run = check.manipulate_corpus(&mut g, dummy_value.path(), step_id, Some(sender));
        assert!(run.is_ok());
        assert_eq!(
            receiver
                .into_iter()
                .filter(|msg| matches!(msg, StatusMessage::Warning { .. }))
                .count(),
            1
        );
    }
    let toml_path_fail = "./tests/data/graph_op/check/serialized_layer_check_failing.toml";
    if let Ok(s) = fs::read_to_string(toml_path_fail) {
        let processor_opt: Result<Check, _> = toml::from_str(s.as_str());
        assert!(processor_opt.is_ok());
        let check = processor_opt.unwrap();
        let (sender, _receiver) = mpsc::channel();
        let tmp = tempdir();
        assert!(tmp.is_ok());
        let dummy_value = tmp.unwrap();

        let step_id = StepID {
            module_name: "check".to_string(),
            path: None,
        };
        let run = check.manipulate_corpus(&mut g, dummy_value.path(), step_id, Some(sender));
        assert!(run.is_err());
        assert_snapshot!(run.err().unwrap().to_string());
    }
}

#[test]
fn test_layer_test_to_aql_test() {
    let mut layers = BTreeMap::new();
    layers.insert(
        "layer1".to_string(),
        vec!["v1".to_string(), "v2".to_string(), "v3".to_string()],
    );
    layers.insert(
        "layer2".to_string(),
        vec!["v1".to_string(), "v2".to_string(), "v3".to_string()],
    );
    layers.insert(
        "layer3".to_string(),
        vec!["v1".to_string(), "v2".to_string(), "v3".to_string()],
    );
    let aql_tests: Vec<AQLTest> = (&Test::LayerTest {
        layers,
        edge: None,
        optional: false,
    })
        .into();
    assert_eq!(aql_tests.len(), 6);
}

#[test]
fn test_append_report() {
    let g = input_graph(false, "corpus");
    assert!(g.is_ok());
    let mut graph = g.unwrap();
    let tests = vec![Test::QueryTest {
        query: "tok".into(),
        expected: QueryResult::Numeric(4),
        description: "Correct number of tokens".to_string(),
        policy: None,
    }];
    let tmp = tempdir();
    assert!(tmp.is_ok());
    let tmp_dir = tmp.unwrap();
    let report_path = tmp_dir.path().join("annatto_test_report_out_append.txt");
    let check = Check {
        policy: FailurePolicy::Fail,
        tests,
        report: Some(ReportLevel::List),
        failed_only: false,
        save: Some(report_path.clone()),
        overwrite: false,
    };

    let step_id = StepID {
        module_name: "check".to_string(),
        path: None,
    };
    let run = check.manipulate_corpus(&mut graph, tmp_dir.path(), step_id.clone(), None);
    assert!(run.is_ok(), "Error writing report: {:?}", run.err());
    assert!(report_path.exists());
    let another_check = Check {
        policy: FailurePolicy::Fail,
        tests: vec![Test::QueryTest {
            query: "tok".into(),
            expected: QueryResult::Numeric(4),
            description: "Correct number of tokens".to_string(),
            policy: None,
        }],
        report: None,
        failed_only: false,
        save: Some(report_path.clone()),
        overwrite: false,
    };
    let (sender, receiver) = mpsc::channel();
    let application =
        another_check.manipulate_corpus(&mut graph, tmp_dir.path(), step_id, Some(sender));
    assert!(application.is_ok(), "Error: {:?}", application.err());
    let log_contents = fs::read_to_string(report_path);
    assert_snapshot!(log_contents.unwrap());
    let mut log_message = receiver
        .into_iter()
        .map(|m| match m {
            StatusMessage::Info(msg) => msg.to_string(),
            _ => "".to_string(),
        })
        .join("\n");
    log_message.replace_range(
        log_message.find("/").unwrap_or_default()..log_message.rfind("/").unwrap_or_default(),
        "<tmp-dir>",
    );
    assert_snapshot!("log_message_append", log_message);
}

#[test]
fn test_overwrite_report() {
    let g = input_graph(false, "corpus");
    assert!(g.is_ok());
    let mut graph = g.unwrap();
    let tests = vec![Test::QueryTest {
        query: "tok".into(),
        expected: QueryResult::Numeric(4),
        description: "Correct number of tokens".to_string(),
        policy: None,
    }];
    let tmp = tempdir();
    assert!(tmp.is_ok());
    let tmp_dir = tmp.unwrap();
    let report_path = tmp_dir.path().join("annatto_test_report_out_overwrite.txt");
    let check = Check {
        policy: FailurePolicy::Fail,
        tests,
        report: Some(ReportLevel::List),
        failed_only: false,
        save: Some(report_path.clone()),
        overwrite: true,
    };

    let step_id = StepID {
        module_name: "check".to_string(),
        path: None,
    };
    let run = check.manipulate_corpus(&mut graph, tmp_dir.path(), step_id.clone(), None);
    assert!(run.is_ok(), "Error writing report: {:?}", run.err());
    assert!(report_path.exists());
    let another_check = Check {
        policy: FailurePolicy::Fail,
        tests: vec![Test::QueryTest {
            query: "tok".into(),
            expected: QueryResult::Numeric(4),
            description: "Correct number of tokens".to_string(),
            policy: None,
        }],
        report: None,
        failed_only: false,
        save: Some(report_path.clone()),
        overwrite: true,
    };
    let (sender, receiver) = mpsc::channel();
    let application =
        another_check.manipulate_corpus(&mut graph, tmp_dir.path(), step_id, Some(sender));
    assert!(application.is_ok(), "Error: {:?}", application.err());
    let log_contents = fs::read_to_string(report_path);
    assert_snapshot!(log_contents.unwrap());
    let mut log_message = receiver
        .into_iter()
        .map(|m| match m {
            StatusMessage::Info(msg) => msg.to_string(),
            _ => "".to_string(),
        })
        .join("\n");
    log_message.replace_range(
        log_message.find("/").unwrap_or_default()..log_message.rfind("/").unwrap_or_default(),
        "<tmp-dir>",
    );
    assert_snapshot!("log_message_overwrite", log_message);
}

#[test]
fn test_write_report_verbose() {
    let g = input_graph(true, "corpus");
    assert!(g.is_ok());
    let mut graph = g.unwrap();
    let tests = vec![
        Test::QueryTest {
            query: "tok".into(),
            expected: QueryResult::Numeric(4),
            description: "Correct number of tokens is 4".to_string(),
            policy: None,
        },
        Test::QueryTest {
            query: "tok".into(),
            expected: QueryResult::Numeric(2),
            description: "Correct number of tokens is 2".to_string(),
            policy: None,
        },
        Test::QueryTest {
            query: "tok".into(),
            expected: QueryResult::Numeric(3),
            description: "Correct number of tokens is 3".to_string(),
            policy: None,
        },
        Test::QueryTest {
            query: "tok".into(),
            expected: QueryResult::Numeric(1),
            description: "Correct number of tokens is 1".to_string(),
            policy: None,
        },
        Test::LayerTest {
            layers: vec![(
                "pos".to_string(),
                vec!["DET".to_string(), "NOUN".to_string()],
            )]
            .into_iter()
            .collect(),
            edge: None,
            optional: false,
        },
    ];
    let tmp = tempdir();
    assert!(tmp.is_ok());
    let tmp_dir = tmp.unwrap();
    let report_path = tmp_dir.path().join("annatto_test_report_out_verbose.txt");
    let check = Check {
        policy: FailurePolicy::Warn,
        tests,
        report: Some(ReportLevel::Verbose),
        failed_only: false,
        save: Some(report_path.clone()),
        overwrite: false,
    };

    let step_id = StepID {
        module_name: "check_verbose".to_string(),
        path: None,
    };
    let run = check.manipulate_corpus(&mut graph, tmp_dir.path(), step_id.clone(), None);
    assert!(run.is_ok(), "Error writing report: {:?}", run.err());
    assert!(report_path.exists());
    let log_contents = fs::read_to_string(report_path);
    assert_snapshot!(log_contents.unwrap());
}

#[test]
fn with_external_corpus() {
    let g = input_graph(true, "new-corpus");
    assert!(g.is_ok());
    let mut graph = g.unwrap();
    let query = "/This/ _ident_ pos=/PRON/ . /is/ _ident_ pos=/VERB/ . /a/ _ident_ pos=/DET/ . /test/ _ident_ pos=/NOUN/";
    let check = Check {
        policy: FailurePolicy::Fail,
        tests: vec![
            Test::QueryTest {
                query: query.into(),
                expected: QueryResult::Numeric(1),
                description: "Control test to make sure the query actually works".to_string(),
                policy: None,
            },
            Test::QueryTest {
                description: "Query sequence.".to_string(),
                query: query.into(),
                expected: QueryResult::CorpusQuery(
                    Path::new("tests/data/graph_op/check/external_db/").to_path_buf(),
                    "corpus".to_string(),
                    query.into(),
                ),
                policy: None,
            },
            Test::QueryTest {
                description: "Query nodes.".to_string(),
                query: "node".into(),
                expected: QueryResult::CorpusQuery(
                    Path::new("tests/data/graph_op/check/external_db/").to_path_buf(),
                    "corpus".to_string(),
                    "node".into(),
                ),
                policy: None,
            },
        ],
        report: None,
        failed_only: false,
        save: None,
        overwrite: false,
    };
    let result = check.run_tests(&mut graph);
    assert!(result.is_ok(), "{:?}", result.err());

    let step_id = StepID {
        module_name: "check".to_string(),
        path: None,
    };
    let manip = check.manipulate_corpus(&mut graph, Path::new("./"), step_id, None);
    assert!(manip.is_ok(), "{:?}", manip.err());
}

#[test]
fn failed_only() {
    let g = input_graph(true, "corpus");
    let check = Check {
        failed_only: true,
        report: Some(ReportLevel::Verbose),
        tests: vec![
            Test::QueryTest {
                query: "tok".into(),
                expected: QueryResult::SemiOpenInterval(1, f64::INFINITY),
                description: "gimme some tokens, please".to_string(),
                policy: None,
            },
            Test::QueryTest {
                query: "weird_anno_name".into(),
                expected: QueryResult::Numeric(1),
                description: "I want that".to_string(),
                policy: None,
            },
        ],
        policy: FailurePolicy::Warn,
        save: None,
        overwrite: false,
    };
    assert!(g.is_ok());
    let mut graph = g.unwrap();
    let (tx, rx) = mpsc::channel();
    let run = check.manipulate_corpus(
        &mut graph,
        Path::new("./"),
        StepID {
            module_name: "test".to_string(),
            path: None,
        },
        Some(tx),
    );
    assert!(run.is_ok(), "Error: {:?}", run.err());
    let output = rx
        .into_iter()
        .map(|m| match m {
            StatusMessage::Info(msg) => msg,
            _ => "".to_string(),
        })
        .join("\n");
    assert_snapshot!(output);
}

fn input_graph(
    on_disk: bool,
    root_name: &str,
) -> Result<AnnotationGraph, Box<dyn std::error::Error>> {
    let mut g = AnnotationGraph::with_default_graphstorages(on_disk)?;
    let mut u = GraphUpdate::default();
    let root_corpus = root_name;
    let doc_name = "doc";
    let doc_node = format!("{root_corpus}/{doc_name}");
    u.add_event(UpdateEvent::AddNode {
        node_name: root_corpus.to_string(),
        node_type: "corpus".to_string(),
    })?;
    u.add_event(UpdateEvent::AddNode {
        node_name: doc_node.to_string(),
        node_type: "corpus".to_string(),
    })?;
    u.add_event(UpdateEvent::AddEdge {
        source_node: doc_node.to_string(),
        target_node: root_corpus.to_string(),
        layer: ANNIS_NS.to_string(),
        component_type: AnnotationComponentType::PartOf.to_string(),
        component_name: "".to_string(),
    })?;
    let s_node = format!("{doc_node}#s1");
    u.add_event(UpdateEvent::AddNode {
        node_name: s_node.to_string(),
        node_type: "node".to_string(),
    })?;
    u.add_event(UpdateEvent::AddNodeLabel {
        node_name: s_node.to_string(),
        anno_ns: "".to_string(),
        anno_name: "sentence".to_string(),
        anno_value: "1".to_string(),
    })?;
    for (i, (text_value, pos_value)) in [
        ("This", "PRON"),
        ("is", "VERB"),
        ("a", "DET"),
        ("test", "NOUN"),
    ]
    .iter()
    .enumerate()
    {
        let tok_node = format!("{doc_node}#t{}", &i + &1);
        u.add_event(UpdateEvent::AddNode {
            node_name: tok_node.to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: tok_node.to_string(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "tok".to_string(),
            anno_value: text_value.to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: tok_node.to_string(),
            anno_ns: "".to_string(),
            anno_name: "pos".to_string(),
            anno_value: pos_value.to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: s_node.to_string(),
            target_node: tok_node.to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::Coverage.to_string(),
            component_name: "".to_string(),
        })?;
        if i > 0 {
            u.add_event(UpdateEvent::AddEdge {
                source_node: format!("{doc_node}#t{}", &i),
                target_node: tok_node.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::Ordering.to_string(),
                component_name: "".to_string(),
            })?;
        }
    }
    // dependencies
    let dep = "dep";
    let deprel = "deprel";
    for (source_id, target_id, label) in [(4, 1, "nsubj"), (4, 2, "cop"), (4, 3, "det")] {
        let source_node = format!("{doc_node}#t{}", source_id);
        let target_node = format!("{doc_node}#t{}", target_id);
        u.add_event(UpdateEvent::AddEdge {
            source_node: source_node.to_string(),
            target_node: target_node.to_string(),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: dep.to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdgeLabel {
            source_node: source_node.to_string(),
            target_node: target_node.to_string(),
            layer: "".to_string(),
            component_type: AnnotationComponentType::Pointing.to_string(),
            component_name: dep.to_string(),
            anno_ns: "".to_string(),
            anno_name: deprel.to_string(),
            anno_value: label.to_string(),
        })?;
    }
    let cat = "cat";
    let func = "func";
    for (members, name, category) in [
        (vec![("t1", None)], "n1", "DP"),
        (vec![("t3", Some("head")), ("t4", None)], "n2", "DP"),
        (vec![("t2", Some("head")), ("n2", None)], "n3", "IP"),
        (vec![("n1", Some("head")), ("n3", None)], "n4", "CP"),
    ] {
        let node_name = format!("{doc_node}#{name}");
        u.add_event(UpdateEvent::AddNode {
            node_name: node_name.to_string(),
            node_type: "node".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: node_name.to_string(),
            anno_ns: "".to_string(),
            anno_name: cat.to_string(),
            anno_value: category.to_string(),
        })?;
        for (member, function_opt) in members {
            let target_name = format!("{doc_node}#{member}");
            u.add_event(UpdateEvent::AddEdge {
                source_node: node_name.to_string(),
                target_node: target_name.to_string(),
                layer: "".to_string(),
                component_type: AnnotationComponentType::Dominance.to_string(),
                component_name: "".to_string(),
            })?;
            if let Some(function) = function_opt {
                u.add_event(UpdateEvent::AddEdgeLabel {
                    source_node: node_name.to_string(),
                    target_node: target_name.to_string(),
                    layer: "".to_string(),
                    component_type: AnnotationComponentType::Dominance.to_string(),
                    component_name: "".to_string(),
                    anno_ns: "".to_string(),
                    anno_name: func.to_string(),
                    anno_value: function.to_string(),
                })?;
            }
        }
    }
    g.apply_update(&mut u, |_| {})?;
    Ok(g)
}
