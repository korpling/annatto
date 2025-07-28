use std::{
    collections::{BTreeMap, btree_map::Entry},
    fmt::Display,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::mpsc,
};

use anyhow::anyhow;
use documented::{Documented, DocumentedFields};
use graphannis::{AnnotationGraph, aql, errors::GraphAnnisError};
use graphannis_core::{
    errors::GraphAnnisCoreError,
    graph::{ANNIS_NS, NODE_NAME_KEY, NODE_TYPE},
};
use itertools::Itertools;
use serde::Serialize;
use serde_derive::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;
use tabled::{Table, Tabled};

use crate::{
    Manipulator, StepID,
    error::AnnattoError,
    workflow::{StatusMessage, StatusSender},
};

/// Runs AQL queries on the corpus and checks for constraints on the result.
/// Can fail the workflow when one of the checks fail.
///
/// There are general attributes to control this modules behaviour:
///
/// `policy`: Values are either `warn` or `fail`. The former will only output
/// a warning, while the latter will stop the conversion process after the
/// check module has completed all tests. The default policy is `fail`.
///
/// `report`: If set to `list`, the results will be printed to as a table, if
/// set to `verbose`, each failed test will be followed by a short appendix
/// listing all matches to help you debug your data. If nothing is set, no report
/// will be shown.
///
/// `failed_only`: If set to true, a report will only contain results of failed tests.
///
/// `save`: If you provide a file path (the file can exist already), the report
/// is additionally saved to disk.
///
/// `overwrite`: If set to `true`, an existing log file will be overwritten. If set
/// to `false`, an existing log file will be appended to. Default is `false`.
///
/// Example:
///
/// ```toml
/// [[graph_op]]
/// action = "check"
///
/// [graph_op.config]
/// report = "list"
/// save = "report.log"
/// overwrite = false
/// policy = "warn"
/// ```
///
/// There are several ways to configure tests. The default test type is defined
/// by a query, that is run on the current corpus graph, an expected result, and
/// a description to ensure meaningful output. E. g.:
///
/// ```toml
/// [[graph_op.config.tests]]
/// query = "lemma=/stone/ _=_ pos=/VERB/"
/// expected = 0
/// description = "No stone is labeled as a verb."
/// ```
/// A test can be given its own failure policy. This only makes sense if your global
/// failure policy is `fail` and you do not want a specific test to cause a failure.
/// A `warn` will always outrank a fail, i. e. whenever the global policy is `warn`,
/// an individual test's policy `fail` will have no effect.
///
/// Example:
///
/// ```toml
/// [[graph_op.config.tests]]
/// query = "lemma=/stone/ _=_ pos=/VERB/"
/// expected = 0
/// description = "No stone is labeled as a verb."
/// policy = "warn"
/// ```
///
/// The expected value can be given in one of the following ways:
///
/// + exact numeric value (s. example above)
/// + closed numeric interval
///
/// ```toml
/// [[graph_op.config.tests]]
/// query = "lemma=/stone/"
/// expected = [0, 20]
/// description = "The lemma stone occurs at most and 20 times in the corpus"
/// ```
///
/// + numeric interval with an open right boundary:
///
/// ```toml
/// [[graph_op.config.tests]]
/// query = "lemma=/stone/"
/// expected = [1, inf]
/// description = "The lemma stone occurs at least once."
/// ```
///
/// + a query that should have the same amount of results:
///
/// ```toml
/// [[graph_op.config.tests]]
/// query = "lemma=/stone/"
/// expected = "pos=/NOUN/"
/// description = "There are as many lemmas `stone` as there are nouns."
/// ```
///
/// + an interval defined by numbers and/or queries:
///
/// ```toml
/// [[graph_op.config.tests]]
/// query = "lemma=/stone/"
/// expected = [1, "pos=/NOUN/"]
/// description = "There is at least one mention of a stone, but not more than there are nouns."
/// ```
///
/// ```toml
/// [[graph_op.config.tests]]
/// query = "lemma=/stone/"
/// expected = ["sentence", inf]
/// description = "There are at least as many lemmas `stone` as there are sentences."
/// ```
///
/// ```toml
/// [[graph_op.config.tests]]
/// query = "lemma=/stone/"
/// expected = ["sentence", "tok"]
/// description = "There are at least as many lemmas `stone` as there are sentences, at most as there are tokens."
/// ```
///
/// + or a query on a corpus loaded from an external GraphANNIS data base:
///
/// ```toml
/// [[graph_op.config.tests]]
/// query = "lemma=/stone/"
/// expected = ["~/.annis/v4", "SameCorpus_vOlderVersion", "lemma=/stone/"]
/// description = "The frequency of lemma `stone` is stable between the current graph and the previous version."
/// ```
///
/// There is also a second test type, that can be used to check closed class annotation layers' annotation values:
///
/// ```toml
/// [[graph_op.config.tests]]
/// [graph_op.config.tests.layers]
/// number = ["sg", "pl"]
/// person = ["1", "2", "3"]
/// voice = ["active", "passive"]
/// ```
///
/// For each defined layer two tests are derived, an existence test of the annotation
/// layer and a test, that no other value has been used. So the entry for `number`
/// above is equivalent to the following tests, that are derived internally:
///
/// ```toml
/// [[graph_op.config.tests]]
/// query = "number"
/// expected = [1, inf]
/// description = "Layer `number` exists."
///
/// [[graph_op.config.tests]]
/// query = "number!=/sg|pl/"
/// expected = 0
/// description = "Check layer `number` for invalid values."
/// ```
///
/// A layer test can be defined as optional, i. e. the existence check is
/// allowed to fail, but not the value check (unless the global policy is `warn`):
///
/// ```toml
/// [[graph_op.config.tests]]
/// optional = true
/// [graph_op.config.tests.layers]
/// number = ["sg", "pl"]
/// person = ["1", "2", "3"]
/// voice = ["active", "passive"]
/// ```
///
/// A layer test can also be applied to edge annotations. Assume there are
/// pointing relations in the tested corpus for annotating reference and
/// an edge annotation `ref_type` can take values "a" and "k". The edge
/// name is `ref`. If in GraphANNIS you want to query such relations, one
/// would use a query such as `node ->ref[ref_type="k"] node`. For testing
/// `ref_type` with a layer test, you would use a configuration like this:
///
/// ```toml
/// [[graph_op.config.tests]]
/// edge = "->ref"
/// [graph.config.tests.layers]
/// ref_type = ["a", "k"]
/// ```
///
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Check {
    /// The tests to run on the current graph.
    tests: Vec<Test>, // does not default as a check without any tests makes no sense, also there is no default test with a default expected result, at least not a reasonable one
    /// Optional level of report. No value means no printed report. Values are `list` or `verbose`.
    #[serde(default)]
    report: Option<ReportLevel>, // default is None, not default report level
    /// By setting this to `true`, only results of failed tests will be listed in the report (only works if a report level is set).
    #[serde(default)]
    failed_only: bool,
    /// This policy if the process interrupts on a test failure (`fail`) or throws a warning (`warn`).
    #[serde(default)]
    policy: FailurePolicy,
    /// Provide a path to a file containing the test report. The verbosity is defined by the report attribute.
    #[serde(default)]
    save: Option<PathBuf>,
    /// If a path is provided to option `save`, the file is appended to by default. If you prefer to overwrite,
    /// set this attribute to `true`.
    #[serde(default)]
    overwrite: bool,
}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum FailurePolicy {
    Warn,
    #[default]
    Fail,
}

#[derive(Deserialize, Default, Serialize, Clone)]
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
        let (r, policies) = self.run_tests(graph)?;
        if let (Some(level), Some(sender)) = (&self.report, &tx) {
            self.print_report(level, &r, sender)?;
        }
        if let Some(path) = &self.save {
            let (sender, receiver) = mpsc::channel();
            self.print_report(
                self.report.as_ref().unwrap_or(&ReportLevel::default()),
                &r,
                &sender,
            )?;
            if let Some(StatusMessage::Info(msg)) = receiver.into_iter().next() {
                let target_path = if path.is_absolute() {
                    path.to_path_buf()
                } else {
                    workflow_directory.join(path)
                };
                let color_free = msg
                    .replace(&ansi_term::Color::Red.prefix().to_string(), "")
                    .replace(&ansi_term::Color::Red.suffix().to_string(), "")
                    .replace(&ansi_term::Color::Green.prefix().to_string(), "")
                    .replace(&ansi_term::Color::Green.suffix().to_string(), "")
                    .replace(&ansi_term::Color::Purple.prefix().to_string(), "")
                    .replace(&ansi_term::Color::Purple.suffix().to_string(), "");
                if target_path.exists() {
                    if let Some(sender) = &tx {
                        sender.send(StatusMessage::Info(format!(
                            "{} check log to file {} ...",
                            if self.overwrite {
                                "Writing"
                            } else {
                                "Appending"
                            },
                            target_path.to_string_lossy()
                        )))?;
                    }
                    let mut f = if self.overwrite {
                        fs::remove_file(target_path.as_path())?;
                        fs::File::create(target_path)?
                    } else {
                        fs::OpenOptions::new().append(true).open(target_path)?
                    };
                    f.write_all("\n\n".as_bytes())?;
                    f.write_all(color_free.as_bytes())?;
                    f.flush()?;
                } else {
                    fs::write(target_path, color_free)?;
                }
            }
        }
        let failed_checks = r
            .iter()
            .filter(|(_, r)| !matches!(r, TestResult::Passed))
            .map(|(d, _)| d.to_string())
            .collect_vec();
        if !failed_checks.is_empty() {
            let global_demands_fail = matches!(&self.policy, FailurePolicy::Fail);
            let critical = r.iter().zip(policies).any(|((_, tr), tp)| {
                if !matches!(tr, TestResult::Passed) {
                    if let Some(fp) = &tp {
                        matches!(fp, FailurePolicy::Fail) && global_demands_fail
                    } else {
                        global_demands_fail
                    }
                } else {
                    false
                }
            });
            if critical {
                return Err(AnnattoError::ChecksFailed { failed_checks }.into());
            }
            if let Some(sender) = &tx {
                let msg = StatusMessage::Warning(format!(
                    "One or more checks failed:\n{}",
                    failed_checks.join("\n")
                ));
                sender.send(msg)?;
            }
        }
        Ok(())
    }

    fn requires_statistics(&self) -> bool {
        true
    }
}

type NamedResults = Vec<(String, TestResult)>;
type Policies = Vec<Option<FailurePolicy>>;

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
                appendix: None,
            },
            ReportLevel::Verbose => {
                let verbose_desc = match result {
                    TestResult::Failed { query, .. } => {
                        [description.to_string(), query.to_string()].join("\n")
                    }
                    _ => description.to_string(),
                };
                let appendix = match result {
                    TestResult::Passed => None,
                    TestResult::Failed { is, .. } => {
                        if is.is_empty() {
                            None // an empty appendix is useless (and not particularly pretty)
                        } else {
                            let mut v = Vec::with_capacity(is.len() + 1);
                            v.push(format!("Matches for query of test `{description}`:"));
                            v.extend(is.iter().map(|ms| ms.to_string()).sorted_unstable());
                            Some(v.join("\n"))
                        }
                    }
                    TestResult::ProcessingError { error } => Some(error.to_string()),
                };
                TestTableEntry {
                    description: verbose_desc,
                    result: result.to_string(),
                    appendix,
                }
            }
        }
    }

    fn results_to_table(
        results: &[(String, TestResult)],
        level: &ReportLevel,
        failed_only: bool,
    ) -> String {
        let table_data = results
            .iter()
            .filter_map(|(d, r)| {
                if !failed_only || !matches!(r, TestResult::Passed) {
                    Some(Check::result_to_table_entry(d, r, level))
                } else {
                    None
                }
            })
            .collect_vec();
        let mut output = String::default();
        let mut table_buffer = Vec::new();
        for entry in table_data {
            table_buffer.push(entry);
            let appendix = &table_buffer[table_buffer.len() - 1].appendix;
            if let Some(bottom_details) = appendix {
                output.push_str(&Table::new(&table_buffer).to_string());
                output.push('\n');
                output.push_str(bottom_details);
                output.push_str("\n\n");
                table_buffer = Vec::default();
            }
        }
        if !table_buffer.is_empty() {
            output.push_str(&Table::new(&table_buffer).to_string());
        }
        output
    }

    fn print_report(
        &self,
        level: &ReportLevel,
        results: &[(String, TestResult)],
        sender: &StatusSender,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let table = Check::results_to_table(results, level, self.failed_only);
        sender.send(StatusMessage::Info(table))?;
        Ok(())
    }

    fn run_tests(
        &self,
        graph: &mut AnnotationGraph,
    ) -> Result<(NamedResults, Policies), Box<dyn std::error::Error>> {
        let mut results = Vec::with_capacity(self.tests.len());
        let mut policies = Vec::with_capacity(self.tests.len());
        let mut graph_cache = BTreeMap::default();
        for test in &self.tests {
            let aql_tests: Vec<AQLTest> = test.into();
            for aql_test in aql_tests {
                results.push((
                    aql_test.description.to_string(),
                    Check::run_test(graph, &aql_test, &mut graph_cache),
                ));
                policies.push(aql_test.policy);
            }
        }
        Ok((results, policies))
    }

    fn run_test(
        g: &AnnotationGraph,
        test: &AQLTest,
        graph_cache: &mut BTreeMap<String, AnnotationGraph>,
    ) -> TestResult {
        let query_s = test.query.as_str();
        let expected_result = &test.expected;
        let result = Check::run_query(g, query_s);
        match result {
            Ok(r) => {
                let n = r.len();
                let (passes, expected_r) = match expected_result {
                    QueryResult::Numeric(n_exp) => (&n == n_exp, QueryResult::Numeric(*n_exp)),
                    QueryResult::Query(alt_query) => {
                        let alt_result = Check::run_query(g, &alt_query[..]);
                        match alt_result {
                            Ok(alt_matches) => (
                                alt_matches.len() == n,
                                QueryResult::Numeric(alt_matches.len()),
                            ),
                            Err(err) => {
                                return TestResult::ProcessingError { error: err };
                            }
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
                                        return TestResult::ProcessingError { error: err.into() };
                                    }
                                    Ok(mut external_g) => {
                                        if let Err(err) = external_g.open(&db_dir.join(corpus_name))
                                        {
                                            return TestResult::ProcessingError {
                                                error: err.into(),
                                            };
                                        }
                                        e.insert(external_g)
                                    }
                                }
                            }
                            Entry::Occupied(e) => e.into_mut(),
                        };
                        if let Err(err) = external_g.ensure_loaded_all() {
                            return TestResult::ProcessingError { error: err.into() };
                        }
                        let e_n = Check::run_query(external_g, query);
                        match e_n {
                            Ok(v) => (v.len() == n, QueryResult::Numeric(v.len())),
                            Err(err) => {
                                return TestResult::ProcessingError { error: err };
                            }
                        }
                    }
                    QueryResult::ClosedLQueryInterval(query, upper) => {
                        let lower = Check::run_query(g, query);
                        match lower {
                            Ok(v) => (
                                v.len().le(&n) && upper.ge(&n),
                                QueryResult::ClosedInterval(v.len(), *upper),
                            ),
                            Err(error) => return TestResult::ProcessingError { error },
                        }
                    }
                    QueryResult::ClosedRQueryInterval(lower, query) => {
                        let upper = Check::run_query(g, query);
                        match upper {
                            Ok(v) => (
                                lower.le(&n) && v.len().ge(&n),
                                QueryResult::ClosedInterval(*lower, v.len()),
                            ),
                            Err(error) => return TestResult::ProcessingError { error },
                        }
                    }
                    QueryResult::ClosedQueryInterval(query_l, query_r) => {
                        let lower = Check::run_query(g, query_l);
                        let upper = Check::run_query(g, query_r);
                        if let Ok(l) = &lower
                            && let Ok(u) = &upper
                        {
                            (
                                l.len().le(&n) && u.len().ge(&n),
                                QueryResult::ClosedInterval(l.len(), u.len()),
                            )
                        } else if let Err(error) = lower {
                            return TestResult::ProcessingError { error };
                        } else {
                            return TestResult::ProcessingError {
                                error: upper.err().unwrap_or(
                                    GraphAnnisCoreError::Other(
                                        anyhow!(
                                            "Something went wrong determining the upper bound."
                                        )
                                        .into(),
                                    )
                                    .into(),
                                ),
                            };
                        }
                    }
                    QueryResult::SemiOpenQueryInterval(query, upper) => {
                        let lower = Check::run_query(g, query);
                        match lower {
                            Ok(v) => {
                                let l = v.len();
                                (
                                    l.le(&n) && (!upper.is_normal() || upper.ge(&(n as f64))),
                                    if upper.is_normal() {
                                        QueryResult::ClosedInterval(l, *upper as usize)
                                    } else {
                                        QueryResult::SemiOpenInterval(l, *upper)
                                    },
                                )
                            }
                            Err(error) => return TestResult::ProcessingError { error },
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

    fn run_query(g: &AnnotationGraph, query_s: &str) -> Result<Vec<String>, GraphAnnisError> {
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
    policy: Option<FailurePolicy>, // is option such that the global policy is used when None is given which would not work for the default policy
}

impl From<&Test> for Vec<AQLTest> {
    fn from(value: &Test) -> Self {
        match value {
            Test::QueryTest {
                query,
                expected,
                description,
                policy,
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
                    QueryResult::ClosedLQueryInterval(q, n) => {
                        QueryResult::ClosedLQueryInterval(q.to_string(), *n)
                    }
                    QueryResult::ClosedRQueryInterval(n, q) => {
                        QueryResult::ClosedRQueryInterval(*n, q.to_string())
                    }
                    QueryResult::ClosedQueryInterval(ql, qr) => {
                        QueryResult::ClosedQueryInterval(ql.to_string(), qr.to_string())
                    }
                    QueryResult::SemiOpenQueryInterval(q, b) => {
                        QueryResult::SemiOpenQueryInterval(q.to_string(), *b)
                    }
                },
                description: description.to_string(),
                policy: (*policy).clone(),
            }],
            Test::LayerTest {
                layers,
                edge: target,
                optional,
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
                    let existence_policy = if *optional {
                        Some(FailurePolicy::Warn)
                    } else {
                        Some(FailurePolicy::Fail)
                    };
                    tests.push(AQLTest {
                        // each layer needs to be tested for existence as well to be able to properly interpret a 0-result for the joint test
                        query: exist_query,
                        expected: QueryResult::SemiOpenInterval(1, f64::INFINITY),
                        description: format!("Layer `{anno_qname}` exists"),
                        policy: existence_policy, // a demand for warn here ranks higher than the global demand for fail
                    });
                    tests.push(AQLTest {
                        query: value_query,
                        expected: QueryResult::Numeric(0),
                        description: format!("Check layer `{anno_qname}` for invalid values."),
                        policy: None, // here the global policy is decisive, i. e. a local warn cannot outrank the global demand for fail
                    })
                }
                tests
            }
        }
    }
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(untagged, deny_unknown_fields)]
enum Test {
    QueryTest {
        query: String,
        expected: QueryResult,
        description: String,
        #[serde(default)]
        policy: Option<FailurePolicy>, // is option, such that the global policy can override the local
    },
    LayerTest {
        layers: BTreeMap<String, Vec<String>>,
        #[serde(default)]
        edge: Option<String>,
        #[serde(default)]
        optional: bool,
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
        error: GraphAnnisError,
    },
}

impl Display for TestResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            TestResult::Passed => format!(
                "{}passed{}",
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
            TestResult::ProcessingError { error } => format!(
                "{}invalid: {}{}",
                ansi_term::Color::Purple.prefix(),
                error,
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
    #[tabled(skip)]
    appendix: Option<String>,
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(untagged)]
enum QueryResult {
    Numeric(usize),
    Query(String),
    ClosedInterval(usize, usize),
    ClosedLQueryInterval(String, usize),
    ClosedRQueryInterval(usize, String),
    ClosedQueryInterval(String, String),
    SemiOpenInterval(usize, f64),
    SemiOpenQueryInterval(String, f64),
    CorpusQuery(PathBuf, String, String), // db_dir, corpus name, query
}

#[cfg(test)]
mod tests {
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
        core::update_graph_silent,
        manipulator::{
            Manipulator,
            check::{AQLTest, FailurePolicy, QueryResult, ReportLevel, TestResult},
        },
        util::example_generator,
        workflow::StatusMessage,
    };

    use super::{Check, Test};

    #[test]
    fn serialize_custom() {
        let module = Check {
            policy: FailurePolicy::Warn,
            tests: vec![
                Test::QueryTest {
                    query: "tok @* doc=/largest-doc/".to_string(),
                    expected: QueryResult::SemiOpenInterval(1, f64::INFINITY),
                    description: "I expect a lot of tokens".to_string(),
                    policy: None
                },
                Test::QueryTest {
                    query: "pos".to_string(),
                    expected: QueryResult::ClosedQueryInterval(
                        "norm".to_string(),
                        "tok".to_string(),
                    ),
                    description: "Plausible number of pos annotations.".to_string(),
                    policy: None
                },
                Test::QueryTest {
                    query: "sentence".to_string(),
                    expected: QueryResult::ClosedLQueryInterval("doc".to_string(), 400),
                    description: "Plausible distribution of sentence annotations.".to_string(),
                    policy: None
                },
                Test::QueryTest {
                    query: "doc _ident_ author=/William Shakespeare/".to_string(),
                    expected: QueryResult::ClosedRQueryInterval(1, "doc".to_string()),
                    description: "At least one document in the corpus was written by Shakespeare, hopefully all of them!".to_string(),
                    policy: None
                },
                Test::QueryTest {
                    query: "lemma=/hello/".to_string(),
                    expected: QueryResult::SemiOpenQueryInterval("doc".to_string(), f64::INFINITY),
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
        let serialized_data =
            fs::read_to_string("./tests/data/graph_op/check/serialized_check.toml")?;
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
            assert!(
                r.iter()
                    .any(|(_, tr)| matches!(tr, TestResult::ProcessingError { .. }))
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
            query: "tok".to_string(),
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
                query: "tok".to_string(),
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
            query: "tok".to_string(),
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
                query: "tok".to_string(),
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
                query: "tok".to_string(),
                expected: QueryResult::Numeric(4),
                description: "Correct number of tokens is 4".to_string(),
                policy: None,
            },
            Test::QueryTest {
                query: "tok".to_string(),
                expected: QueryResult::Numeric(2),
                description: "Correct number of tokens is 2".to_string(),
                policy: None,
            },
            Test::QueryTest {
                query: "tok".to_string(),
                expected: QueryResult::Numeric(3),
                description: "Correct number of tokens is 3".to_string(),
                policy: None,
            },
            Test::QueryTest {
                query: "tok".to_string(),
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
                    query: query.to_string(),
                    expected: QueryResult::Numeric(1),
                    description: "Control test to make sure the query actually works".to_string(),
                    policy: None,
                },
                Test::QueryTest {
                    description: "Query sequence.".to_string(),
                    query: query.to_string(),
                    expected: QueryResult::CorpusQuery(
                        Path::new("tests/data/graph_op/check/external_db/").to_path_buf(),
                        "corpus".to_string(),
                        query.to_string(),
                    ),
                    policy: None,
                },
                Test::QueryTest {
                    description: "Query nodes.".to_string(),
                    query: "node".to_string(),
                    expected: QueryResult::CorpusQuery(
                        Path::new("tests/data/graph_op/check/external_db/").to_path_buf(),
                        "corpus".to_string(),
                        "node".to_string(),
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
                    query: "tok".to_string(),
                    expected: QueryResult::SemiOpenInterval(1, f64::INFINITY),
                    description: "gimme some tokens, please".to_string(),
                    policy: None,
                },
                Test::QueryTest {
                    query: "weird_anno_name".to_string(),
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
}
