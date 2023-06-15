use std::{env::temp_dir, path::Path};

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
    #[serde(default)] // allows to drop report field when report is not required
    report: Option<ReportLevel>,
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
            let msg = StatusMessage::Failed(AnnattoError::ChecksFailed { failed_checks });
            if let Some(ref sender) = tx {
                sender.send(msg)?;
            }
        }
        Ok(())
    }
}

impl Check {
    fn print_report(
        &self,
        level: &ReportLevel,
        results: &[(String, TestResult)],
        sender: &StatusSender,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let table_data = results
            .iter()
            .map(|r| TestTableEntry {
                description: r.0.to_string(),
                result: r.1.to_string(),
                details: match level {
                    ReportLevel::List => "".to_string(),
                    ReportLevel::Verbose => match &r.1 {
                        TestResult::Passed => "".to_string(),
                        TestResult::Failed(v) => v.join("\n"),
                        TestResult::ProcessingError(e) => e.to_string(),
                    },
                },
            })
            .collect_vec();
        let table = Table::new(table_data).to_string();
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
            results.push((test.description.to_string(), Check::run_test(&cs, test)));
        }
        Ok(results)
    }

    fn run_test(cs: &CorpusStorage, test: &Test) -> TestResult {
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
                TestResult::Failed(r)
            }
        } else {
            TestResult::ProcessingError(result.err().unwrap())
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

#[derive(Deserialize)]
struct Test {
    query: String,
    expected: ExpectedQueryResult,
    description: String,
}

enum TestResult {
    Passed,
    Failed(Vec<String>),
    ProcessingError(Box<dyn std::error::Error>),
}

impl ToString for TestResult {
    fn to_string(&self) -> String {
        match self {
            TestResult::Passed => format!(
                "{}+{}",
                ansi_term::Color::Green.prefix(),
                ansi_term::Color::Green.suffix()
            ),
            TestResult::Failed(v) => format!(
                "{}{}{}",
                ansi_term::Color::Red.prefix(),
                v.len(),
                ansi_term::Color::Red.suffix()
            ),
            TestResult::ProcessingError(_) => format!(
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
    use std::{env::temp_dir, fs, sync::mpsc};

    use graphannis::{
        model::AnnotationComponentType,
        update::{GraphUpdate, UpdateEvent},
        AnnotationGraph,
    };
    use graphannis_core::graph::ANNIS_NS;
    use toml;

    use crate::{
        manipulator::{
            check::{ReportLevel, TestResult},
            Manipulator,
        },
        workflow::StatusMessage,
    };

    use super::Check;

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
                    TestResult::Failed(v) => v.len(),
                    TestResult::ProcessingError(_) => 1,
                    _ => 0,
                })
                .sum::<usize>()
                > 0
        );
        if with_nodes {
            assert!(r.iter().any(|(_, tr)| matches!(tr, TestResult::Failed(_))));
            assert!(r
                .iter()
                .any(|(_, tr)| matches!(tr, TestResult::ProcessingError(_))));
        }
        Ok(())
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
        g.apply_update(&mut u, |_| {})?;
        Ok(g)
    }
}
