//! Runs AQL queries on the corpus and checks for constraints on the result.
// Can fail the workflow when one of the checks fail
use std::{collections::BTreeMap, env::temp_dir, path::Path};

use graphannis::{
    corpusstorage::{QueryLanguage, ResultOrder, SearchQuery},
    AnnotationGraph, CorpusStorage,
};
use itertools::Itertools;
use serde_derive::Deserialize;
use tabled::{Table, Tabled};
use tempfile::tempdir_in;

use crate::{
    error::AnnattoError,
    workflow::{StatusMessage, StatusSender},
    Manipulator, Module,
};

pub const MODULE_NAME: &str = "check";

#[derive(Deserialize)]
pub struct Check {
    tests: Vec<Test>,
    report: Option<ReportLevel>,
    #[serde(default)]
    policy: FailurePolicy,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FailurePolicy {
    Warn,
    #[default]
    Fail,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ReportLevel {
    List,
    Verbose,
}

impl Module for Check {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}

impl Manipulator for Check {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        _workflow_directory: &Path,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let r = self.run_tests(graph)?;
        if self.report.is_some() && tx.is_some() {
            self.print_report(self.report.as_ref().unwrap(), &r, tx.as_ref().unwrap())?;
        }
        let failed_checks = r
            .into_iter()
            .filter(|(_, r)| !matches!(r, TestResult::Passed))
            .map(|(d, _)| d)
            .collect_vec();
        if !failed_checks.is_empty() {
            let msg = match self.policy {
                FailurePolicy::Warn => StatusMessage::Warning(format!(
                    "One or more checks failed:\n{}",
                    failed_checks.join("\n")
                )),
                FailurePolicy::Fail => {
                    StatusMessage::Failed(AnnattoError::ChecksFailed { failed_checks })
                }
            };
            if let Some(ref sender) = tx {
                sender.send(msg)?;
            }
        }
        Ok(())
    }
}

impl Check {
    fn result_to_table_entry(
        description: &String,
        result: &TestResult,
        level: &ReportLevel,
    ) -> TestTableEntry {
        match level {
            ReportLevel::List => TestTableEntry {
                description: description.to_string(),
                result: result.to_string(),
                details: "".to_string(),
            },
            ReportLevel::Verbose => {
                let verbose_desc = match result {
                    TestResult::Failed { query, .. } => {
                        [description.to_string(), query.to_string()].join("\n")
                    }
                    _ => description.to_string(),
                };
                let verbose_details = match result {
                    TestResult::Passed => "".to_string(),
                    TestResult::Failed { matches, .. } => matches.join("\n"),
                    TestResult::ProcessingError { error } => error.to_string(),
                };
                TestTableEntry {
                    description: verbose_desc,
                    result: result.to_string(),
                    details: verbose_details,
                }
            }
        }
    }

    fn results_to_table(results: &[(String, TestResult)], level: &ReportLevel) -> String {
        let table_data = results
            .iter()
            .map(|r| Check::result_to_table_entry(&r.0, &r.1, level))
            .collect_vec();
        Table::new(table_data).to_string()
    }

    fn print_report(
        &self,
        level: &ReportLevel,
        results: &[(String, TestResult)],
        sender: &StatusSender,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let table = Check::results_to_table(results, level);
        sender.send(StatusMessage::Info(table))?;
        Ok(())
    }

    fn run_tests(
        &self,
        graph: &mut AnnotationGraph,
    ) -> Result<Vec<(String, TestResult)>, Box<dyn std::error::Error>> {
        let corpus_name = "current";
        let tmp_dir = tempdir_in(temp_dir())?;
        graph.save_to(&tmp_dir.path().join(corpus_name))?;
        let cs = CorpusStorage::with_auto_cache_size(tmp_dir.path(), true)?;
        let mut results = Vec::new();
        for test in &self.tests {
            let aql_tests: Vec<AQLTest> = test.into();
            for aql_test in aql_tests {
                results.push((
                    aql_test.description.to_string(),
                    Check::run_test(&cs, &aql_test),
                ));
            }
        }
        Ok(results)
    }

    fn run_test(cs: &CorpusStorage, test: &AQLTest) -> TestResult {
        let query_s = &test.query[..];
        let expected_result = &test.expected;
        let result = Check::run_query(cs, query_s);
        if let Ok(r) = result {
            let n = r.len();
            let passes = match expected_result {
                ExpectedQueryResult::Numeric(n_is) => &n == n_is,
                ExpectedQueryResult::Query(alt_query) => {
                    let alt_result = Check::run_query(cs, &alt_query[..]);
                    alt_result.is_ok() && alt_result.unwrap().len() == n
                }
                ExpectedQueryResult::ClosedInterval(lower, upper) => n.ge(lower) && n.le(upper),
                ExpectedQueryResult::SemiOpenInterval(lower, upper) => {
                    if upper.is_infinite() || upper.is_nan() {
                        n.ge(lower)
                    } else {
                        let u = upper.abs().ceil() as usize;
                        n.ge(lower) && u.gt(&n)
                    }
                }
            };
            if passes {
                TestResult::Passed
            } else {
                TestResult::Failed {
                    query: test.query.to_string(),
                    matches: r,
                }
            }
        } else {
            TestResult::ProcessingError {
                error: result.err().unwrap(),
            }
        }
    }

    fn run_query(
        storage: &CorpusStorage,
        query_s: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let query = SearchQuery {
            corpus_names: &["current"],
            query: query_s,
            query_language: QueryLanguage::AQL,
            timeout: None,
        };
        let results = storage.find(query, 0, None, ResultOrder::Normal)?;
        Ok(results)
    }
}

struct AQLTest {
    query: String,
    expected: ExpectedQueryResult,
    description: String,
}

impl From<&Test> for Vec<AQLTest> {
    fn from(value: &Test) -> Self {
        match value {
            Test::QueryTest {
                query,
                expected,
                description,
            } => vec![AQLTest {
                query: query.to_string(),
                expected: match expected {
                    ExpectedQueryResult::Numeric(n) => ExpectedQueryResult::Numeric(*n),
                    ExpectedQueryResult::Query(q) => ExpectedQueryResult::Query(q.to_string()),
                    ExpectedQueryResult::ClosedInterval(a, b) => {
                        ExpectedQueryResult::ClosedInterval(*a, *b)
                    }
                    ExpectedQueryResult::SemiOpenInterval(a, b) => {
                        ExpectedQueryResult::SemiOpenInterval(*a, *b)
                    }
                },
                description: description.to_string(),
            }],
            Test::LayerTest {
                layers,
                edge: target,
            } => {
                let mut tests = Vec::new();
                for (anno_qname, list_of_values) in layers {
                    let joint_values = list_of_values.join("|");
                    let inner_query_frag = format!("{anno_qname}!=/{joint_values}/");
                    let (value_query, exist_query) = if let Some(edge_spec) = target {
                        (
                            format!("node {edge_spec}[{inner_query_frag}] node"),
                            format!("node {edge_spec}[{anno_qname}=/.*/] node"),
                        )
                    } else {
                        (inner_query_frag, anno_qname.to_string())
                    };
                    tests.push(AQLTest {
                        // each layer needs to be tested for existence as well to be able to properly interpret a 0-result for the joint test
                        query: exist_query,
                        expected: ExpectedQueryResult::SemiOpenInterval(1, f64::INFINITY),
                        description: format!("Layer `{anno_qname}` exists"),
                    });
                    tests.push(AQLTest {
                        query: value_query,
                        expected: ExpectedQueryResult::Numeric(0),
                        description: format!("Check layer `{anno_qname}` for invalid values."),
                    })
                }
                tests
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Test {
    QueryTest {
        query: String,
        expected: ExpectedQueryResult,
        description: String,
    },
    LayerTest {
        layers: BTreeMap<String, Vec<String>>,
        edge: Option<String>,
    },
}

enum TestResult {
    Passed,
    Failed { query: String, matches: Vec<String> },
    ProcessingError { error: Box<dyn std::error::Error> },
}

impl ToString for TestResult {
    fn to_string(&self) -> String {
        match self {
            TestResult::Passed => format!(
                "{}+{}",
                ansi_term::Color::Green.prefix(),
                ansi_term::Color::Green.suffix()
            ),
            TestResult::Failed { matches, .. } => format!(
                "{}{}{}",
                ansi_term::Color::Red.prefix(),
                matches.len(),
                ansi_term::Color::Red.suffix()
            ),
            TestResult::ProcessingError { .. } => format!(
                "{}(bad){}",
                ansi_term::Color::Purple.prefix(),
                ansi_term::Color::Purple.suffix()
            ),
        }
    }
}

#[derive(Tabled)]
struct TestTableEntry {
    description: String,
    result: String,
    details: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ExpectedQueryResult {
    Numeric(usize),
    Query(String),
    ClosedInterval(usize, usize),
    SemiOpenInterval(usize, f64),
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, env::temp_dir, fs, sync::mpsc};

    use graphannis::{
        model::AnnotationComponentType,
        update::{GraphUpdate, UpdateEvent},
        AnnotationGraph,
    };
    use graphannis_core::graph::ANNIS_NS;
    use toml;

    use crate::{
        manipulator::{
            check::{AQLTest, ReportLevel, TestResult},
            Manipulator,
        },
        workflow::StatusMessage,
    };

    use super::{Check, Test};

    #[test]
    fn test_check_on_disk() {
        let r = test(true);
        assert!(r.is_ok(), "Error when testing on disk: {:?}", r.err());
    }

    #[test]
    fn test_check_in_mem() {
        let r = test(true);
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
        let serialized_data =
            fs::read_to_string("./tests/data/graph_op/check/serialized_check.toml")?;
        let check: Check = toml::from_str(serialized_data.as_str())?;
        let mut g = input_graph(on_disk)?;
        let (sender, receiver) = mpsc::channel();
        check.manipulate_corpus(&mut g, temp_dir().as_path(), Some(sender))?;
        assert!(check.report.is_some()); // if deserialization worked properly, `check` should be set to report
        assert!(matches!(check.report.as_ref().unwrap(), &ReportLevel::List));
        assert!(receiver.iter().count() > 0); // there should be a status report
        Ok(())
    }

    fn test_failing_checks(
        on_disk: bool,
        with_nodes: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let toml_path = if with_nodes {
            "./tests/data/graph_op/check/serialized_check_failing_with_nodes.toml"
        } else {
            "./tests/data/graph_op/check/serialized_check_failing.toml"
        };
        let serialized_data = fs::read_to_string(toml_path)?;
        let check: Check = toml::from_str(serialized_data.as_str())?;
        let mut g = input_graph(on_disk)?;
        let (sender, receiver) = mpsc::channel();
        check.manipulate_corpus(&mut g, temp_dir().as_path(), Some(sender))?;
        assert!(check.report.is_some());
        if with_nodes {
            assert!(matches!(
                check.report.as_ref().unwrap(),
                ReportLevel::Verbose
            ));
        } else {
            assert!(matches!(check.report.as_ref().unwrap(), ReportLevel::List));
        }
        assert!(
            receiver
                .iter()
                .map(|m| matches!(m, StatusMessage::Failed(_)))
                .count()
                > 0
        ); // there should be a report of a failure
        let r = check.run_tests(&mut g)?;
        assert!(
            r.iter()
                .map(|(_, tr)| match tr {
                    TestResult::Failed { matches, .. } => matches.len(),
                    TestResult::ProcessingError { .. } => 1,
                    _ => 0,
                })
                .sum::<usize>()
                > 0
        );
        if with_nodes {
            assert!(r
                .iter()
                .any(|(_, tr)| matches!(tr, TestResult::Failed { .. })));
            assert!(r
                .iter()
                .any(|(_, tr)| matches!(tr, TestResult::ProcessingError { .. })));
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
        let mut g = input_graph(on_disk)?;
        let toml_path = "./tests/data/graph_op/check/serialized_layer_check.toml";
        let s = fs::read_to_string(toml_path)?;
        let check: Check = toml::from_str(s.as_str())?;
        let results = check.run_tests(&mut g)?;
        let all_pass = results
            .iter()
            .all(|(_, tr)| matches!(tr, TestResult::Passed));
        if !all_pass {
            let table_string = Check::results_to_table(&results, &ReportLevel::Verbose);
            println!("{}", table_string);
        }
        assert!(all_pass);
        Ok(())
    }

    fn test_layer_check_fail(on_disk: bool) -> Result<(), Box<dyn std::error::Error>> {
        let mut g = input_graph(on_disk)?;
        let toml_path = "./tests/data/graph_op/check/serialized_layer_check_failing.toml";
        let s = fs::read_to_string(toml_path)?;
        let check: Check = toml::from_str(s.as_str())?;
        let results = check.run_tests(&mut g)?;
        let failing = results
            .iter()
            .filter(|(_, tr)| matches!(tr, TestResult::Failed { .. }))
            .count();
        let passing = results
            .iter()
            .filter(|(_, tr)| matches!(tr, TestResult::Passed))
            .count();
        if passing != failing {
            let table_string = Check::results_to_table(&results, &ReportLevel::Verbose);
            println!("{}", table_string);
        }
        assert_eq!(passing, failing);
        Ok(())
    }

    #[test]
    fn test_layer_check_fail_policy_warn() {
        let gr = input_graph(false);
        assert!(gr.is_ok());
        let mut g = gr.unwrap();
        let toml_path = "./tests/data/graph_op/check/serialized_layer_check_failing_warn.toml";
        if let Ok(s) = fs::read_to_string(toml_path) {
            let processor_opt: Result<Check, _> = toml::from_str(s.as_str());
            assert!(processor_opt.is_ok());
            let check = processor_opt.unwrap();
            let (sender, receiver) = mpsc::channel();
            let dummy_value = temp_dir();
            let run = check.manipulate_corpus(&mut g, dummy_value.as_path(), Some(sender));
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
            let (sender, receiver) = mpsc::channel();
            let dummy_value = temp_dir();
            let run = check.manipulate_corpus(&mut g, dummy_value.as_path(), Some(sender));
            assert!(run.is_ok());
            assert_eq!(
                receiver
                    .into_iter()
                    .filter(|msg| matches!(msg, StatusMessage::Failed { .. }))
                    .count(),
                1
            );
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
        let aql_tests: Vec<AQLTest> = (&Test::LayerTest { layers, edge: None }).into();
        assert_eq!(aql_tests.len(), 6);
    }

    fn input_graph(on_disk: bool) -> Result<AnnotationGraph, Box<dyn std::error::Error>> {
        let mut g = AnnotationGraph::new(on_disk)?;
        let mut u = GraphUpdate::default();
        let root_corpus = "corpus";
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
}
