use std::{env::temp_dir, path::Path};

use graphannis::{
    corpusstorage::{QueryLanguage, SearchQuery},
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
    #[serde(default)]
    report: bool,
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
        if self.report && tx.is_some() {
            self.print_report(&r, tx.as_ref().unwrap())?;
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
        results: &[(String, TestResult)],
        sender: &StatusSender,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let table_data = results
            .iter()
            .map(|r| TestTableEntry {
                test_description: r.0.to_string(),
                test_result: r.1.to_string(),
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
        if let Ok(n) = result {
            let passes = match expected_result {
                ExpectedQueryResult::Numeric(n_is) => &(n as usize) == n_is,
                ExpectedQueryResult::Query(alt_query) => {
                    let alt_result = Check::run_query(cs, &alt_query[..]);
                    alt_result.is_ok() && alt_result.unwrap() == n
                }
                ExpectedQueryResult::Interval(min_value, max_value) => {
                    min_value.le(&(n as usize))
                        && (max_value.is_none() || max_value.unwrap().gt(&(n as usize)))
                }
            };
            if passes {
                TestResult::Passed
            } else {
                TestResult::Failed
            }
        } else {
            TestResult::ProcessingError
        }
    }

    fn run_query(
        storage: &CorpusStorage,
        query_s: &str,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let query = SearchQuery {
            corpus_names: &["current"],
            query: query_s,
            query_language: QueryLanguage::AQL,
            timeout: None,
        };
        let c = storage.count(query)?;
        Ok(c)
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
    Failed,
    ProcessingError,
}

impl ToString for TestResult {
    fn to_string(&self) -> String {
        match self {
            TestResult::Passed => r"\e[0;32m+\e[0m".to_string(),
            TestResult::Failed => r"\e[0;31m-\e[0m".to_string(),
            TestResult::ProcessingError => r"\e[0;35m(bad test)\e[0m".to_string(),
        }
    }
}

#[derive(Tabled)]
struct TestTableEntry {
    test_description: String,
    test_result: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ExpectedQueryResult {
    Numeric(usize),
    Query(String),
    Interval(usize, Option<usize>),
}

#[cfg(test)]
mod tests {
    use std::{env::temp_dir, sync::mpsc};

    use graphannis::{
        model::AnnotationComponentType,
        update::{GraphUpdate, UpdateEvent},
        AnnotationGraph,
    };
    use graphannis_core::graph::ANNIS_NS;
    use toml;

    use crate::manipulator::Manipulator;

    use super::Check;

    fn test(on_disk: bool) -> Result<(), Box<dyn std::error::Error>> {
        let check: Check = toml::from_str(
            r#"
            [check]
            report = true

            [[check.tests]]
            query = "tok"
            expected = (1, )
            description = "There has to be at least one token"

            [[check.tests]]
            query = "pos"
            expected = "tok"
            description = "There has to be the same number of pos annotations and tokens"

            [[check.tests]]
            query = "pos _=_ tok"
            expected = "tok"
            description = "Every token has a part of speech annotation"

            [[check.tests]]
            query = "sentence"
            expected = 1
            description = "There is only one sentence"
        "#,
        )?;
        let mut g = input_graph(on_disk)?;
        let (sender, receiver) = mpsc::channel();
        let r = check.manipulate_corpus(&mut g, temp_dir().as_path(), Some(sender));
        assert!(
            r.is_ok(),
            "Could not test `check`, there was an error: {:?}",
            r.err()
        ); // all tests should pass w/o any error
        assert!(receiver.iter().count() > 0); // there should be a status report
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
        .into_iter()
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
