use std::{
    collections::{BTreeMap, btree_map::Entry},
    fmt::Display,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::mpsc,
};

use anyhow::anyhow;
use facet::Facet;
use graphannis::{AnnotationGraph, aql, errors::GraphAnnisError};
use graphannis_core::{
    errors::GraphAnnisCoreError,
    graph::{ANNIS_NS, NODE_NAME_KEY, NODE_TYPE},
};
use itertools::Itertools;
use serde::Serialize;
use serde_derive::Deserialize;
use tabled::{Table, Tabled};

use crate::{
    Manipulator, StepID,
    error::AnnattoError,
    estarde::query::check_deserialized_query,
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
#[derive(Facet, Deserialize, Serialize, Clone, PartialEq)]
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

#[derive(Facet, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
enum FailurePolicy {
    Warn,
    #[default]
    Fail,
}

#[derive(Facet, Deserialize, Default, Serialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
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
                        let alt_result = Check::run_query(g, alt_query.as_ref());
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
                        let e_n = Check::run_query(external_g, query.as_ref());
                        match e_n {
                            Ok(v) => (v.len() == n, QueryResult::Numeric(v.len())),
                            Err(err) => {
                                return TestResult::ProcessingError { error: err };
                            }
                        }
                    }
                    QueryResult::ClosedLQueryInterval(query, upper) => {
                        let lower = Check::run_query(g, query.as_ref());
                        match lower {
                            Ok(v) => (
                                v.len().le(&n) && upper.ge(&n),
                                QueryResult::ClosedInterval(v.len(), *upper),
                            ),
                            Err(error) => return TestResult::ProcessingError { error },
                        }
                    }
                    QueryResult::ClosedRQueryInterval(lower, query) => {
                        let upper = Check::run_query(g, query.as_ref());
                        match upper {
                            Ok(v) => (
                                lower.le(&n) && v.len().ge(&n),
                                QueryResult::ClosedInterval(*lower, v.len()),
                            ),
                            Err(error) => return TestResult::ProcessingError { error },
                        }
                    }
                    QueryResult::ClosedQueryInterval(query_l, query_r) => {
                        let lower = Check::run_query(g, query_l.as_ref());
                        let upper = Check::run_query(g, query_r.as_ref());
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
                        let lower = Check::run_query(g, query.as_ref());
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
                    QueryResult::Query(q) => QueryResult::Query(q.clone()),
                    QueryResult::ClosedInterval(a, b) => QueryResult::ClosedInterval(*a, *b),
                    QueryResult::SemiOpenInterval(a, b) => QueryResult::SemiOpenInterval(*a, *b),
                    QueryResult::CorpusQuery(db, c, q) => {
                        QueryResult::CorpusQuery(db.to_path_buf(), c.to_string(), q.clone())
                    }
                    QueryResult::ClosedLQueryInterval(q, n) => {
                        QueryResult::ClosedLQueryInterval(q.clone(), *n)
                    }
                    QueryResult::ClosedRQueryInterval(n, q) => {
                        QueryResult::ClosedRQueryInterval(*n, q.clone())
                    }
                    QueryResult::ClosedQueryInterval(ql, qr) => {
                        QueryResult::ClosedQueryInterval(ql.clone(), qr.clone())
                    }
                    QueryResult::SemiOpenQueryInterval(q, b) => {
                        QueryResult::SemiOpenQueryInterval(q.clone(), *b)
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

// This type is needed for deserializing `Test` structs and properly evaluating
// queries for parse errors. As `Test` is an untagged enum, raising an error regarding
// the syntax or semantics of a query directly in deserialization of the respective `Test`
// variant's field will be interpreted as "this is not the right variant" by serde, as there
// is no other clue due to the missing tag (which we need to be that way for more human-readable workflows).
#[derive(Deserialize)]
#[serde(untagged, deny_unknown_fields)]
enum UncheckedTest {
    QueryTest {
        query: String,
        expected: UncheckedQueryResult,
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

impl TryFrom<UncheckedTest> for Test {
    type Error = AnnattoError;

    fn try_from(value: UncheckedTest) -> Result<Self, Self::Error> {
        match value {
            UncheckedTest::QueryTest {
                query,
                expected,
                description,
                policy,
            } => {
                check_deserialized_query(&query)?;
                Ok(Test::QueryTest {
                    query,
                    expected: QueryResult::try_from(expected)?,
                    description,
                    policy,
                })
            }
            UncheckedTest::LayerTest {
                layers,
                edge,
                optional,
            } => Ok(Test::LayerTest {
                layers,
                edge,
                optional,
            }),
        }
    }
}

#[derive(Deserialize, Facet, Clone, PartialEq, Serialize)]
#[serde(untagged, deny_unknown_fields, try_from = "UncheckedTest")]
#[repr(u8)]
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
    // This is less likely to occur, as queries are now parsed on deserialization.
    // Nevertheless, there might be a rare incident that graphANNIS itself has an
    // internal bug or a corpus has invalid structure(s), such that a GraphANNISError
    // might occur
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

#[derive(Deserialize)]
#[serde(untagged, deny_unknown_fields)]
enum UncheckedQueryResult {
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

impl TryFrom<UncheckedQueryResult> for QueryResult {
    type Error = AnnattoError;

    fn try_from(value: UncheckedQueryResult) -> Result<Self, Self::Error> {
        match value {
            UncheckedQueryResult::Numeric(n) => Ok(QueryResult::Numeric(n)),
            UncheckedQueryResult::Query(q) => Ok(QueryResult::Query(q)),
            UncheckedQueryResult::ClosedInterval(l, r) => Ok(QueryResult::ClosedInterval(l, r)),
            UncheckedQueryResult::ClosedLQueryInterval(q, r) => {
                check_deserialized_query(&q)?;
                Ok(QueryResult::ClosedLQueryInterval(q, r))
            }
            UncheckedQueryResult::ClosedRQueryInterval(l, q) => {
                check_deserialized_query(&q)?;
                Ok(QueryResult::ClosedRQueryInterval(l, q))
            }
            UncheckedQueryResult::ClosedQueryInterval(ql, qr) => {
                check_deserialized_query(&ql)?;
                check_deserialized_query(&qr)?;
                Ok(QueryResult::ClosedQueryInterval(ql, qr))
            }
            UncheckedQueryResult::SemiOpenInterval(l, r) => Ok(QueryResult::SemiOpenInterval(l, r)),
            UncheckedQueryResult::SemiOpenQueryInterval(q, r) => {
                check_deserialized_query(&q)?;
                Ok(QueryResult::SemiOpenQueryInterval(q, r))
            }
            UncheckedQueryResult::CorpusQuery(path_buf, c, q) => {
                check_deserialized_query(&q)?;
                Ok(QueryResult::CorpusQuery(path_buf, c, q))
            }
        }
    }
}

#[derive(Facet, Serialize, Clone, PartialEq)]
#[serde(untagged)]
#[repr(u8)]
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
mod tests;
