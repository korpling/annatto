use std::{
    collections::{btree_map::Entry, BTreeMap},
    fmt::Display,
    fs,
    path::{Path, PathBuf},
    sync::mpsc,
};

use anyhow::anyhow;
use documented::{Documented, DocumentedFields};
use graphannis::{aql, AnnotationGraph};
use graphannis_core::graph::{ANNIS_NS, NODE_NAME_KEY, NODE_TYPE};
use itertools::Itertools;
use serde_derive::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;
use tabled::{Table, Tabled};

use crate::{
    error::AnnattoError,
    workflow::{StatusMessage, StatusSender},
    Manipulator, StepID,
};

/// Runs AQL queries on the corpus and checks for constraints on the result.
/// Can fail the workflow when one of the checks fail
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(deny_unknown_fields)]
pub struct Check {
    /// The tests to run on the current graph.
    tests: Vec<Test>,
    /// Optional level of report. No value means no printed report. Values are `list` or `verbose`.
    report: Option<ReportLevel>, // default is None, not default report level
    /// This policy if the process interrupts on a test failure (`fail`) or throws a warning (`warn`).
    #[serde(default)]
    policy: FailurePolicy,
    /// Provide a path to a file containing the test report. The verbosity is defined by the report attribute.
    #[serde(default)]
    save: Option<PathBuf>,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FailurePolicy {
    Warn,
    #[default]
    Fail,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "snake_case")]
enum ReportLevel {
    #[default] // default report level is required for save option
    List,
    Verbose,
}

impl Manipulator for Check {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        workflow_directory: &Path,
        _step_id: StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let r = self.run_tests(graph)?;
        if let (Some(level), Some(sender)) = (&self.report, &tx) {
            self.print_report(level, &r[..], sender)?;
        }
        if let Some(path) = &self.save {
            let (sender, receiver) = mpsc::channel();
            self.print_report(
                self.report.as_ref().unwrap_or(&ReportLevel::default()),
                &r[..],
                &sender,
            )?;
            if let Some(StatusMessage::Info(msg)) = receiver.into_iter().next() {
                let target_path = if path.is_absolute() {
                    path.to_path_buf()
                } else {
                    workflow_directory.join(path)
                };
                fs::write(target_path, msg)?;
            }
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
                    return Err(AnnattoError::ChecksFailed { failed_checks }.into());
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
                    TestResult::Failed { is: matches, .. } => matches.join("\n"),
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
        let mut results = Vec::with_capacity(self.tests.len());
        let mut graph_cache = BTreeMap::default();
        for test in &self.tests {
            let aql_tests: Vec<AQLTest> = test.into();
            for aql_test in aql_tests {
                results.push((
                    aql_test.description.to_string(),
                    Check::run_test(graph, &aql_test, &mut graph_cache),
                ));
            }
        }
        Ok(results)
    }

    fn run_test(
        g: &AnnotationGraph,
        test: &AQLTest,
        graph_cache: &mut BTreeMap<String, AnnotationGraph>,
    ) -> TestResult {
        let query_s = &test.query[..];
        let expected_result = &test.expected;
        let result = Check::run_query(g, query_s);
        match result {
            Ok(r) => {
                let n = r.len();
                let (passes, expected_r) = match expected_result {
                    QueryResult::Numeric(n_exp) => (&n == n_exp, QueryResult::Numeric(*n_exp)),
                    QueryResult::Query(alt_query) => {
                        let alt_result = Check::run_query(g, &alt_query[..]);
                        if let Ok(alt_matches) = alt_result {
                            (
                                alt_matches.len() == n,
                                QueryResult::Numeric(alt_matches.len()),
                            )
                        } else {
                            return TestResult::ProcessingError {
                                error: anyhow!("Could not compute expected result from query")
                                    .into(),
                            };
                        }
                    }
                    QueryResult::ClosedInterval(lower, upper) => (
                        n.ge(lower) && n.le(upper),
                        QueryResult::ClosedInterval(*lower, *upper),
                    ),
                    QueryResult::SemiOpenInterval(lower, upper) => {
                        let forward_r = QueryResult::SemiOpenInterval(*lower, *upper);
                        if upper.is_infinite() || upper.is_nan() {
                            (n.ge(lower), forward_r)
                        } else {
                            let u = upper.abs().ceil() as usize;
                            (n.ge(lower) && u.gt(&n), forward_r)
                        }
                    }
                    QueryResult::CorpusQuery(db_dir, corpus_name, query) => {
                        let path = db_dir.join(corpus_name);
                        let path_string = path.to_string_lossy().to_string();
                        let entry = graph_cache.entry(path_string.to_string());
                        let external_g = match entry {
                            Entry::Vacant(e) => {
                                let eg = AnnotationGraph::with_default_graphstorages(false);
                                match eg {
                                    Err(err) => {
                                        return TestResult::ProcessingError {
                                            error: Box::new(err),
                                        };
                                    }
                                    Ok(mut external_g) => {
                                        if let Err(e) = external_g.open(&db_dir.join(corpus_name)) {
                                            return TestResult::ProcessingError {
                                                error: Box::new(e),
                                            };
                                        }
                                        e.insert(external_g)
                                    }
                                }
                            }
                            Entry::Occupied(e) => e.into_mut(),
                        };
                        if external_g.ensure_loaded_all().is_err() {
                            return TestResult::ProcessingError {
                                error: anyhow!("Could not load corpus entirely.").into(),
                            };
                        }
                        let e_n = Check::run_query(external_g, query);
                        if let Ok(v) = e_n {
                            (v.len() == n, QueryResult::Numeric(v.len()))
                        } else {
                            return TestResult::ProcessingError {
                                error: anyhow!("Could not compute expected result from query on external corpus")
                                    .into(),
                            };
                        }
                    }
                };
                if passes {
                    TestResult::Passed
                } else {
                    TestResult::Failed {
                        query: test.query.to_string(),
                        expected: expected_r,
                        is: r,
                    }
                }
            }
            Err(e) => TestResult::ProcessingError { error: e },
        }
    }

    fn run_query(
        g: &AnnotationGraph,
        query_s: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let parsed_query = aql::parse(query_s, false)?;
        let it = aql::execute_query_on_graph(g, &parsed_query, true, None)?;
        let mut result = Vec::with_capacity(it.size_hint().0);
        for m in it {
            let m = m?;
            let mut match_desc = String::new();

            for (i, singlematch) in m.iter().enumerate() {
                // check if query node actually should be included

                if i > 0 {
                    match_desc.push(' ');
                }

                let singlematch_anno_key = &singlematch.anno_key;
                if singlematch_anno_key.ns != ANNIS_NS || singlematch_anno_key.name != NODE_TYPE {
                    if !singlematch_anno_key.ns.is_empty() {
                        match_desc.push_str(&singlematch_anno_key.ns);
                        match_desc.push_str("::");
                    }
                    match_desc.push_str(&singlematch_anno_key.name);
                    match_desc.push_str("::");
                }

                if let Some(node_name) = g
                    .get_node_annos()
                    .get_value_for_item(&singlematch.node, &NODE_NAME_KEY)?
                {
                    match_desc.push_str(&node_name);
                }
            }
            result.push(match_desc);
        }
        Ok(result)
    }
}

struct AQLTest {
    query: String,
    expected: QueryResult,
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
                    QueryResult::Numeric(n) => QueryResult::Numeric(*n),
                    QueryResult::Query(q) => QueryResult::Query(q.to_string()),
                    QueryResult::ClosedInterval(a, b) => QueryResult::ClosedInterval(*a, *b),
                    QueryResult::SemiOpenInterval(a, b) => QueryResult::SemiOpenInterval(*a, *b),
                    QueryResult::CorpusQuery(db, c, q) => {
                        QueryResult::CorpusQuery(db.to_path_buf(), c.to_string(), q.to_string())
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
                        expected: QueryResult::SemiOpenInterval(1, f64::INFINITY),
                        description: format!("Layer `{anno_qname}` exists"),
                    });
                    tests.push(AQLTest {
                        query: value_query,
                        expected: QueryResult::Numeric(0),
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
        expected: QueryResult,
        description: String,
    },
    LayerTest {
        layers: BTreeMap<String, Vec<String>>,
        edge: Option<String>,
    },
}

enum TestResult {
    Passed,
    Failed {
        query: String,
        expected: QueryResult,
        is: Vec<String>,
    },
    ProcessingError {
        error: Box<dyn std::error::Error>,
    },
}

impl Display for TestResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            TestResult::Passed => format!(
                "{}+{}",
                ansi_term::Color::Green.prefix(),
                ansi_term::Color::Green.suffix()
            ),
            TestResult::Failed {
                is: matches,
                expected,
                ..
            } => {
                let exp = match expected {
                    QueryResult::Numeric(n) => format!("{n} ≠ "),
                    QueryResult::ClosedInterval(l, u) => format!("[{l}, {u}] ∌ "),
                    QueryResult::SemiOpenInterval(l, u) => format!("[{l}, {u}] ∌ "),
                    _ => "".to_string(),
                };
                format!(
                    "{}{exp}{}{}",
                    ansi_term::Color::Red.prefix(),
                    matches.len(),
                    ansi_term::Color::Red.suffix()
                )
            }
            TestResult::ProcessingError { .. } => format!(
                "{}(bad){}",
                ansi_term::Color::Purple.prefix(),
                ansi_term::Color::Purple.suffix()
            ),
        };
        write!(f, "{s}")
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
enum QueryResult {
    Numeric(usize),
    Query(String),
    ClosedInterval(usize, usize),
    SemiOpenInterval(usize, f64),
    CorpusQuery(PathBuf, String, String), // db_dir, corpus name, query
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs, path::Path, sync::mpsc};

    use graphannis::{
        model::AnnotationComponentType,
        update::{GraphUpdate, UpdateEvent},
        AnnotationGraph,
    };
    use graphannis_core::graph::ANNIS_NS;
    use insta::assert_snapshot;
    use tempfile::tempdir;
    use toml;

    use crate::{
        manipulator::{
            check::{AQLTest, FailurePolicy, QueryResult, ReportLevel, TestResult},
            Manipulator,
        },
        workflow::StatusMessage,
        StepID,
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

        let r = check.run_tests(&mut g)?;
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
        let mut g = input_graph(on_disk, "corpus")?;
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
        let mut g = input_graph(on_disk, "corpus")?;
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
        let aql_tests: Vec<AQLTest> = (&Test::LayerTest { layers, edge: None }).into();
        assert_eq!(aql_tests.len(), 6);
    }

    #[test]
    fn test_write_report() {
        let g = input_graph(false, "corpus");
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let tests = vec![Test::QueryTest {
            query: "tok".to_string(),
            expected: QueryResult::Numeric(4),
            description: "Correct number of tokens".to_string(),
        }];
        let tmp = tempdir();
        assert!(tmp.is_ok());
        let tmp_dir = tmp.unwrap();
        let report_path = tmp_dir.path().join("annatto_test_report_out.txt");
        let check = Check {
            policy: FailurePolicy::Fail,
            tests,
            report: Some(ReportLevel::List),
            save: Some(report_path.clone()),
        };

        let step_id = StepID {
            module_name: "check".to_string(),
            path: None,
        };
        assert!(check
            .manipulate_corpus(&mut graph, tmp_dir.path(), step_id, None)
            .is_ok());
        assert!(report_path.exists());
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
                    query: query.to_string(),
                    expected: QueryResult::Numeric(1),
                    description: "Control test to make sure the query actually works".to_string(),
                },
                Test::QueryTest {
                    description: "Query sequence.".to_string(),
                    query: query.to_string(),
                    expected: QueryResult::CorpusQuery(
                        Path::new("tests/data/graph_op/check/external_db/").to_path_buf(),
                        "corpus".to_string(),
                        query.to_string(),
                    ),
                },
                Test::QueryTest {
                    description: "Query nodes.".to_string(),
                    query: "node".to_string(),
                    expected: QueryResult::CorpusQuery(
                        Path::new("tests/data/graph_op/check/external_db/").to_path_buf(),
                        "corpus".to_string(),
                        "node".to_string(),
                    ),
                },
            ],
            report: None,
            save: None,
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
}
