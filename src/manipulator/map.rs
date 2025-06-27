use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::mpsc,
};

use super::Manipulator;
use crate::{
    StepID,
    core::{update_graph, update_graph_silent},
    progress::ProgressReporter,
    util::{
        CorpusGraphHelper,
        token_helper::{TOKEN_KEY, TokenHelper},
    },
};
use anyhow::{Context, anyhow};
use documented::{Documented, DocumentedFields};
use graphannis::{
    AnnotationGraph,
    graph::{AnnoKey, EdgeContainer, Match},
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::{ANNIS_NS, DEFAULT_NS, NODE_NAME_KEY, NODE_TYPE_KEY};
use regex::Regex;
use serde::Serialize;
use serde_derive::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

/// Creates new or updates annotations based on existing annotation values.
///
/// The module is configured with TOML files that contains a list of mapping
/// `rules`. Each rule contains `query` field which describes the nodes the
/// annotation are added to. The `target` field defines which node of the query
/// the annotation should be added to. The annotation itself is defined by the
/// `ns` (namespace), `name` and `value` fields.
///
/// ```toml
/// [[rules]]
/// query = "clean _o_ pos_lang=/(APPR)?ART/ _=_ lemma!=/[Dd](ie|as|er|ies)?/"
/// target = 1
/// anno = "indef"
/// value = ""
/// ```
///
/// A `target` can also be a list. In this case, a new span is created that
/// covers the same token as the referenced nodes of the match.
/// ```toml
/// [[rules]]
/// query = "tok=/more/ . tok"
/// target = [1,2]
/// anno = "mapper::form"
/// value = "comparison"
/// ```
///
/// Instead of a fixed value, you can also use an existing annotation value
/// from the matched nodes copy the value.
/// ```toml
/// [[rules]]
/// query = "tok=\"complicated\""
/// target = 1
/// anno = "newtok"
/// value = {copy = 1}
/// ```
///
/// It is also possible to replace all occurences in the original value that
/// match a regular expression with a replacement value.
/// The `replacements` parameter is a list of pairs where the left part is the
/// search string and the right part the replacement string.
/// ```toml
/// [[rules]]
/// query = "tok=\"complicated\""
/// target = 1
/// anno = "newtok"
/// value = {target = 1, replacements = [["cat", "dog"]]}
/// ```
/// This would add a new annotation value "complidoged" to any token with the value "complicated".
/// You can define more
///
/// The `replacements` values can contain back references to the regular
/// expression (e.g. "${0}" for the whole match or "${1}" for the first match
/// group).
/// ```toml
/// [[rules]]
/// query = "tok=\"New York\""
/// target = 1
/// anno = "abbr"
/// value = {target = 1, replacements = [["([A-Z])[a-z]+ ([A-Z])[a-z]+", "${1}${2}"]]}
/// ```
/// This example would add an annotation with the value "NY".
///
/// The `copy` and `target` fields in the value description can also refer
/// to more than one copy of the query by using arrays instead of a single
/// number. In this case, the node values are concatenated using a space as
/// seperator.
///
/// You can also apply a set of rules repeatedly. The standard is to only
/// executed it once. But you can configure
/// ```toml
/// repetition = {Fixed = {n = 3}}
///
/// [[rules]]
/// # ...
/// ```
/// at the beginning to set the fixed number of repetitions (in this case `3`).
/// An even more advanced usage is to apply the changes until none of the
/// queries in the rules matches anymore.
/// ```toml
/// repetition = "UntilUnchanged"
///
/// [[rules]]
/// # ...
/// ```
/// Make sure that the updates in the rules actually change the condition of the
/// rule, otherwise you might get an endless loop and the workflow will never
/// finish!
///
/// If you want to delete an existing annotation while mapping, you can use `delete`, which accepts a list
/// of query node indices. This will not delete nodes, but the annotation described in the query. The given
/// example queries for annotations of name "norm", creates an annotation "normalisation" with the same value
/// at the same node and then deletes the "norm" annotation:
///
/// ```toml
/// [[rules]]
/// query = "norm"
/// target = 1
/// anno = "normalisation"
/// value = { copy = 1 }
/// delete = [1]
/// ```
#[derive(Default, Deserialize, Documented, DocumentedFields, FieldNamesAsSlice, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MapAnnos {
    #[serde(default)]
    /// The path of the TOML file containing an array of mapping rules.
    /// Use rule files when you want to apply a lot of rules to not blow
    /// up the main configuration file.
    rule_file: Option<PathBuf>,
    /// This mechanism can be used to provide rules inline instead of in a
    /// separate file. Also, both mechanisms can be combined.
    ///
    /// Example:
    /// ```toml
    /// [[graph_op]]
    /// action = "map"
    ///
    /// [graph_op.config.mapping]  # this part is optional and can be dropped for default values
    /// repetition = "UntilUnchanged"
    ///
    /// [[graph_op.config.mapping.rules]]
    /// query = "norm"
    /// target = 1
    /// anno = "default_ns::normalisation"
    /// value = { copy = 1 }
    /// delete = [1]
    /// ```
    #[serde(default)]
    mapping: Option<Mapping>,
    /// If you wish for detailled output about the match count of each rule,
    /// set this to `true`. Default is `false`, so no output.
    ///
    /// Example:
    /// ```toml
    /// [[graph_op]]
    /// action = "map"
    ///
    /// [graph_op.config]
    /// rule_file = "mapping-rules.toml"
    /// debug = true
    /// ```
    #[serde(default)]
    debug: bool,
}

impl Manipulator for MapAnnos {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        workflow_directory: &std::path::Path,
        step_id: StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let progress = ProgressReporter::new_unknown_total_work(tx.clone(), step_id.clone())?;
        let config_from_file = if let Some(path) = &self.rule_file {
            let read_from_path = {
                let p = path.as_path().to_path_buf();
                if p.is_relative() {
                    workflow_directory.join(p)
                } else {
                    p
                }
            };
            Some(read_config(read_from_path.as_path())?)
        } else {
            None
        };
        progress.info("Ensure all graph storages are loaded.")?;
        graph.ensure_loaded_all()?;
        if self.mapping.is_none() && self.rule_file.is_none() {
            progress.warn("Neither a rule file was provided nor are there any inline mapping definitions. This step will thus not modify the annotation graph.")?;
        }

        if let Some(config) = config_from_file {
            progress.info("Starting application of rules from rule file ...")?;
            let mut map_impl = MapperImpl {
                config,
                added_spans: 0,
                progress: { if self.debug { Some(progress) } else { None } },
            };
            map_impl.run(graph)?;
        }
        if let Some(inline_config) = &self.mapping {
            let inline_progress = ProgressReporter::new_unknown_total_work(tx, step_id)?;
            inline_progress.info("Starting application of inline rules ...")?;
            let mut map_impl = MapperImpl {
                config: inline_config.clone(),
                added_spans: 0,
                progress: {
                    if self.debug {
                        Some(inline_progress)
                    } else {
                        None
                    }
                },
            };
            map_impl.run(graph)?;
        }
        Ok(())
    }

    fn requires_statistics(&self) -> bool {
        true
    }
}

fn read_config(path: &Path) -> Result<Mapping, Box<dyn std::error::Error>> {
    let config_string = fs::read_to_string(path)?;
    let m: Mapping = toml::from_str(config_string.as_str())?;
    Ok(m)
}

#[derive(Debug, Deserialize, Clone, Serialize)]
enum RepetitionMode {
    /// Repeat applying the rules n times.
    Fixed { n: usize },
    /// Repeat applying the rules until no changes are made.
    UntilUnchanged,
}

impl Default for RepetitionMode {
    fn default() -> Self {
        Self::Fixed { n: 1 }
    }
}

#[derive(Deserialize, Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
struct Mapping {
    rules: Vec<Rule>,
    #[serde(default)]
    repetition: RepetitionMode,
}

#[derive(Clone, Deserialize, Debug, Serialize)]
#[serde(untagged)]
enum TargetRef {
    Node(usize),
    Span(Vec<usize>),
}

impl TargetRef {
    fn resolve_value(
        &self,
        graph: &AnnotationGraph,
        mg: &[Match],
        sep: char,
    ) -> anyhow::Result<String> {
        let targets: Vec<usize> = match self {
            TargetRef::Node(n) => vec![*n],
            TargetRef::Span(t) => t.clone(),
        };
        let mut result = String::new();
        for target_node in targets {
            let m = mg
                .get(target_node - 1)
                .with_context(|| format!("target {target_node} does not exist in result"))?;
            let anno_key = if m.anno_key.as_ref() == NODE_TYPE_KEY.as_ref() {
                TOKEN_KEY.clone()
            } else {
                m.anno_key.clone()
            };
            // Extract the annotation value for this match
            let orig_val = graph
                .get_node_annos()
                .get_value_for_item(&m.node, &anno_key)?
                .unwrap_or_default();
            if !result.is_empty() {
                result.push(sep);
            }
            result.push_str(&orig_val);
        }
        Ok(result)
    }
}

#[derive(Clone, Deserialize, Debug, Serialize)]
#[serde(untagged)]
enum Value {
    Fixed(String),
    Copy {
        /// The target node(s) of the query the annotation is fetched from. If
        /// more than one target is given, the strings a separated with a space
        /// character.
        copy: TargetRef,
    },
    Replace {
        /// The target node(s) of the query the annotation is fetched from. If
        /// more than one target is given, the strings a separated with a space
        /// character.
        target: TargetRef,
        /// Pairs of regular expression that is used to find parts of the string to be
        /// replaced and the fixed strings the matches are replaced with.
        replacements: Vec<(String, String)>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Rule {
    query: String,
    target: TargetRef,
    #[serde(with = "crate::estarde::anno_key")]
    anno: AnnoKey,
    value: Value,
    #[serde(default)]
    delete: Vec<usize>,
}

impl Rule {
    fn resolve_value(&self, graph: &AnnotationGraph, mg: &[Match]) -> anyhow::Result<String> {
        match &self.value {
            Value::Fixed(val) => Ok(val.clone()),
            Value::Copy { copy } => copy.resolve_value(graph, mg, ' '),
            Value::Replace {
                target,
                replacements,
            } => {
                let mut val = target.resolve_value(graph, mg, ' ')?;
                for (search, replace) in replacements {
                    // replace all occurences of the value
                    let search = Regex::new(search)?;
                    val = search.replace_all(&val, replace).to_string();
                }
                Ok(val)
            }
        }
    }
}

struct MapperImpl {
    config: Mapping,
    added_spans: usize,

    progress: Option<ProgressReporter>,
}

impl MapperImpl {
    fn run(&mut self, graph: &mut AnnotationGraph) -> anyhow::Result<()> {
        match self.config.repetition {
            RepetitionMode::Fixed { n } => {
                for i in 0..n {
                    if let Some(p) = &self.progress {
                        p.info(&format!(
                            "Applying rule set of `map` module run {}/{n}",
                            i + 1
                        ))?;
                    }
                    self.apply_ruleset(graph)?;
                    if i < n - 1 {
                        graph.calculate_all_statistics()?;
                    }
                }
            }
            RepetitionMode::UntilUnchanged => {
                let mut run_nr = 1;
                loop {
                    if let Some(p) = &self.progress {
                        p.info(&format!("Applying rule set of `map` module run {run_nr}"))?;
                    }
                    let new_update_size = self.apply_ruleset(graph)?;
                    if new_update_size > 0 {
                        if let Some(p) = &self.progress {
                            p.info(&format!("Added {new_update_size} updates because of rules, repeating to apply all rules until no updates are generated."))?;
                        }
                        run_nr += 1;
                        graph.calculate_all_statistics()?;
                    } else {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    fn apply_ruleset(&mut self, graph: &mut AnnotationGraph) -> anyhow::Result<usize> {
        let mut updates = GraphUpdate::default();
        for rule in self.config.rules.clone() {
            let query = graphannis::aql::parse(&rule.query, false)
                .with_context(|| format!("could not parse query '{}'", &rule.query))?;
            let result_it = graphannis::aql::execute_query_on_graph(graph, &query, true, None)?;
            let mut n = 0;
            for match_group in result_it {
                let match_group = match_group?;
                match rule.target {
                    TargetRef::Node(target) => {
                        self.map_single_node(&rule, target, &match_group, graph, &mut updates)?;
                    }
                    TargetRef::Span(ref all_targets) => {
                        self.map_span(&rule, all_targets, &match_group, graph, &mut updates)?;
                    }
                }
                self.delete_existing_annotations(&rule, &match_group, graph, &mut updates)?;
                n += 1;
            }
            if let Some(p) = &self.progress {
                p.info(&format!(
                    "Rule with query `{}` matched {n} time(s).",
                    &rule.query
                ))?;
            }
        }
        let number_of_updates = updates.len()?;
        if number_of_updates > 0 {
            let tx_rx = if self.progress.is_some() {
                Some(mpsc::channel())
            } else {
                None
            };
            if let Some((sender, _receiver)) = tx_rx {
                update_graph(
                    graph,
                    &mut updates,
                    Some(StepID {
                        module_name: "map".to_string(),
                        path: None,
                    }),
                    Some(sender),
                )?;
            } else {
                update_graph_silent(graph, &mut updates)?;
            }
        }
        Ok(number_of_updates)
    }
    fn map_single_node(
        &self,
        rule: &Rule,
        target: usize,
        match_group: &[Match],
        graph: &AnnotationGraph,
        update: &mut GraphUpdate,
    ) -> anyhow::Result<()> {
        if let Some(m) = match_group.get(target - 1) {
            let match_node_name = graph
                .get_node_annos()
                .get_value_for_item(&m.node, &NODE_NAME_KEY)?
                .context("Missing node name for matched node")?;
            update.add_event(UpdateEvent::AddNodeLabel {
                node_name: match_node_name.to_string(),
                anno_ns: rule.anno.ns.to_string(),
                anno_name: rule.anno.name.to_string(),
                anno_value: rule.resolve_value(graph, match_group)?,
            })?;
        }
        Ok(())
    }

    fn map_span(
        &mut self,
        rule: &Rule,
        targets: &[usize],
        match_group: &[Match],
        graph: &AnnotationGraph,
        update: &mut GraphUpdate,
    ) -> anyhow::Result<()> {
        let tok_helper = TokenHelper::new(graph)?;
        let corpusgraph_helper = CorpusGraphHelper::new(graph);
        if let Some(first_match) = targets
            .first()
            .copied()
            .and_then(|t| match_group.get(t - 1))
        {
            // Calculate all token that should be covered by the newly create span
            let mut covered_token = BTreeSet::new();
            for t in targets {
                if let Some(n) = match_group.get(t - 1) {
                    if tok_helper.is_token(n.node)? {
                        covered_token.insert(n.node);
                    } else {
                        covered_token.extend(tok_helper.covered_token(n.node)?);
                    }
                }
            }

            // Determine the new node name by extending the node name of the first target
            let first_node_name = graph
                .get_node_annos()
                .get_value_for_item(&first_match.node, &NODE_NAME_KEY)?
                .context("Missing node name")?;
            let new_node_name = format!("{first_node_name}_map_{}", self.added_spans);
            self.added_spans += 1;

            // Add the node and the annotation value according to the rule
            update.add_event(UpdateEvent::AddNode {
                node_name: new_node_name.clone(),
                node_type: "node".to_string(),
            })?;
            update.add_event(UpdateEvent::AddNodeLabel {
                node_name: new_node_name.clone(),
                anno_ns: rule.anno.ns.to_string(),
                anno_name: rule.anno.name.to_string(),
                anno_value: rule.resolve_value(graph, match_group)?,
            })?;

            // Add the new node to the common parent
            if let Some(parent_node) = corpusgraph_helper
                .get_outgoing_edges(first_match.node)
                .next()
            {
                let parent_node = parent_node?;
                let parent_node_name = graph
                    .get_node_annos()
                    .get_value_for_item(&parent_node, &NODE_NAME_KEY)?
                    .context("Missing node name for parent node")?;
                update.add_event(UpdateEvent::AddEdge {
                    source_node: new_node_name.clone(),
                    target_node: parent_node_name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::PartOf.to_string(),
                    component_name: "".to_string(),
                })?;
            }
            // Add the coverage edges to the covered tokens
            for t in covered_token {
                let token_node_name = graph
                    .get_node_annos()
                    .get_value_for_item(&t, &NODE_NAME_KEY)?
                    .context("Missing node name for covered token")?;
                update.add_event(UpdateEvent::AddEdge {
                    source_node: new_node_name.clone(),
                    target_node: token_node_name.to_string(),
                    layer: DEFAULT_NS.to_string(),
                    component_type: AnnotationComponentType::Coverage.to_string(),
                    component_name: "".to_string(),
                })?;
            }
        }

        Ok(())
    }

    fn delete_existing_annotations(
        &mut self,
        rule: &Rule,
        match_group: &[Match],
        graph: &AnnotationGraph,
        update: &mut GraphUpdate,
    ) -> anyhow::Result<()> {
        for query_index in &rule.delete {
            if let Some(m) = match_group.get(*query_index - 1) {
                let delete_from_node = graph
                    .get_node_annos()
                    .get_value_for_item(&m.node, &NODE_NAME_KEY)?
                    .ok_or(anyhow!("Node has no node name."))?;
                update.add_event(UpdateEvent::DeleteNodeLabel {
                    node_name: delete_from_node.to_string(),
                    anno_ns: m.anno_key.ns.to_string(),
                    anno_name: m.anno_key.name.to_string(),
                })?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use graphannis::{
        AnnotationGraph, aql,
        model::AnnotationComponentType,
        update::{GraphUpdate, UpdateEvent},
    };
    use graphannis_core::{annostorage::ValueSearch, graph::ANNIS_NS};

    use insta::assert_snapshot;
    use pretty_assertions::assert_eq;
    use tempfile::NamedTempFile;
    use tests::test_util::export_to_string;

    use crate::{
        StepID, exporter::graphml::GraphMLExporter, manipulator::Manipulator, test_util,
        util::example_generator,
    };

    use super::*;

    #[test]
    fn serialize() {
        let module = MapAnnos::default();
        let serialization = toml::to_string(&module);
        assert!(
            serialization.is_ok(),
            "Serialization failed: {:?}",
            serialization.err()
        );
        assert_snapshot!(serialization.unwrap());
    }

    #[test]
    fn serialize_custom() {
        let module = MapAnnos {
            rule_file: Some(PathBuf::from("external/file.toml")),
            mapping: Some(Mapping {
                rules: vec![Rule {
                    query: "pos=/NN/".to_string(),
                    target: TargetRef::Span(vec![1]),
                    anno: AnnoKey {
                        name: "upos".into(),
                        ns: "ud".into(),
                    },
                    value: Value::Fixed("NOUN".to_string()),
                    delete: vec![1],
                }],
                repetition: RepetitionMode::UntilUnchanged,
            }),
            debug: true,
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
        let module = MapAnnos {
            rule_file: Some(PathBuf::from("./any_file.toml")),
            mapping: None,
            debug: false,
        };
        assert!(
            module
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
    fn inline_rules() {
        let mut updates = GraphUpdate::new();
        example_generator::create_corpus_structure_simple(&mut updates);
        example_generator::create_tokens(&mut updates, Some("root/doc1"));
        let mut g = AnnotationGraph::with_default_graphstorages(false).unwrap();
        g.apply_update(&mut updates, |_msg| {}).unwrap();

        let config = r#"
        [[mapping.rules]]
        query = "tok"
        target = 1
        anno = "test_ns::test"
        value = {copy = 1}
        "#;

        let m: Result<MapAnnos, _> = toml::from_str(config);
        assert!(m.is_ok(), "Error deserializing mapper: {:?}", m.err());
        let mapper = m.unwrap();
        assert!(
            mapper
                .manipulate_corpus(
                    &mut g,
                    Path::new("./"),
                    StepID {
                        module_name: "test_map_inline".to_string(),
                        path: None,
                    },
                    None,
                )
                .is_ok()
        );

        let tok_match = g
            .get_node_annos()
            .exact_anno_search(Some("annis"), "tok", ValueSearch::Some("complicated"))
            .next()
            .unwrap()
            .unwrap();

        let resolved = mapper.mapping.unwrap().rules[0]
            .resolve_value(&g, &vec![tok_match])
            .unwrap();
        assert_eq!("complicated", resolved);
    }

    #[test]
    fn test_delete() {
        let mut updates = GraphUpdate::new();
        example_generator::create_corpus_structure_simple(&mut updates);
        example_generator::create_multiple_segmentations(&mut updates, "root/doc1");
        let mut g = AnnotationGraph::with_default_graphstorages(true).unwrap();
        g.apply_update(&mut updates, |_msg| {}).unwrap();

        let map_with_deletion = Rule {
            query: "b".to_string(),
            target: super::TargetRef::Node(1),
            anno: AnnoKey {
                name: "c".into(),
                ns: "".into(),
            },
            value: Value::Copy {
                copy: TargetRef::Node(1),
            },
            delete: vec![1],
        };

        let mapping = Mapping {
            repetition: super::RepetitionMode::Fixed { n: 1 },
            rules: vec![map_with_deletion],
        };
        let mut mapper = super::MapperImpl {
            config: mapping,
            added_spans: 0,
            progress: None,
        };

        assert!(mapper.apply_ruleset(&mut g).is_ok());

        let actual = export_to_string(&g, GraphMLExporter::default());
        assert!(actual.is_ok());
        assert_snapshot!(actual.unwrap());
    }

    #[test]
    fn test_resolve_value_fixed() {
        let mut updates = GraphUpdate::new();
        example_generator::create_corpus_structure_simple(&mut updates);
        example_generator::create_tokens(&mut updates, Some("root/doc1"));
        let mut g = AnnotationGraph::with_default_graphstorages(false).unwrap();
        g.apply_update(&mut updates, |_msg| {}).unwrap();

        let fixed_value = Rule {
            query: "tok".to_string(),
            target: super::TargetRef::Node(1),
            anno: AnnoKey {
                ns: "test_ns".into(),
                name: "test".into(),
            },
            value: Value::Fixed("myvalue".to_string()),
            delete: vec![],
        };

        let resolved = fixed_value.resolve_value(&g, &vec![]).unwrap();
        assert_eq!("myvalue", resolved);
    }

    #[test]
    fn test_resolve_value_copy() {
        let mut updates = GraphUpdate::new();
        example_generator::create_corpus_structure_simple(&mut updates);
        example_generator::create_tokens(&mut updates, Some("root/doc1"));
        let mut g = AnnotationGraph::with_default_graphstorages(false).unwrap();
        g.apply_update(&mut updates, |_msg| {}).unwrap();

        let config = r#"
        [[rules]]
        query = "tok"
        target = 1
        anno = "test_ns::test"
        value = {copy = 1}
        "#;

        let m: Mapping = toml::from_str(config).unwrap();

        let tok_match = g
            .get_node_annos()
            .exact_anno_search(Some("annis"), "tok", ValueSearch::Some("complicated"))
            .next()
            .unwrap()
            .unwrap();

        let resolved = m.rules[0].resolve_value(&g, &vec![tok_match]).unwrap();
        assert_eq!("complicated", resolved);
    }

    #[test]
    fn test_resolve_value_replace_simple() {
        let mut updates = GraphUpdate::new();
        example_generator::create_corpus_structure_simple(&mut updates);
        example_generator::create_tokens(&mut updates, Some("root/doc1"));
        let mut g = AnnotationGraph::with_default_graphstorages(false).unwrap();
        g.apply_update(&mut updates, |_msg| {}).unwrap();

        let fixed_value = Rule {
            query: "tok".to_string(),
            target: super::TargetRef::Node(1),
            anno: AnnoKey {
                name: "test".into(),
                ns: "test_ns".into(),
            },
            value: Value::Replace {
                target: TargetRef::Node(1),
                replacements: vec![("cat".to_string(), "dog".to_string())],
            },
            delete: vec![],
        };

        let tok_match = g
            .get_node_annos()
            .exact_anno_search(Some("annis"), "tok", ValueSearch::Some("complicated"))
            .next()
            .unwrap()
            .unwrap();

        let resolved = fixed_value.resolve_value(&g, &vec![tok_match]).unwrap();
        assert_eq!("complidoged", resolved);
    }

    #[test]
    fn test_resolve_value_replace_with_backreference() {
        let mut updates = GraphUpdate::new();
        example_generator::create_corpus_structure_simple(&mut updates);
        example_generator::create_tokens(&mut updates, Some("root/doc1"));
        let mut g = AnnotationGraph::with_default_graphstorages(false).unwrap();
        g.apply_update(&mut updates, |_msg| {}).unwrap();

        let fixed_value = Rule {
            query: "tok".to_string(),
            target: super::TargetRef::Node(1),
            anno: AnnoKey {
                name: "test".into(),
                ns: "test_ns".into(),
            },
            value: Value::Replace {
                target: TargetRef::Node(1),
                replacements: vec![("cat.*".to_string(), "$0$0".to_string())],
            },
            delete: vec![],
        };

        let tok_match = g
            .get_node_annos()
            .exact_anno_search(Some("annis"), "tok", ValueSearch::Some("complicated"))
            .next()
            .unwrap()
            .unwrap();

        let resolved = fixed_value.resolve_value(&g, &vec![tok_match]).unwrap();
        assert_eq!("complicatedcated", resolved);
    }

    #[test]
    fn test_parse_complicated_replace() {
        let config = r#"
[[rules]]
query = "tok=\"New York\""
target = 1
anno = "abbr"

[rules.value]
target = 1
replacements = [["([A-Z])[a-z]+ ([A-Z])[a-z]+", "${1}${2}"]]
"#;

        let m: Mapping = toml::from_str(config).unwrap();

        let g = source_graph(false).unwrap();

        let newyork_match = g
            .get_node_annos()
            .exact_anno_search(Some("annis"), "tok", ValueSearch::Some("New York"))
            .next()
            .unwrap()
            .unwrap();

        let result = m.rules[0].resolve_value(&g, &[newyork_match]).unwrap();
        assert_eq!("NY", result);
    }

    #[test]
    fn test_ridges_clean_resolver() {
        let config = r#"
[[rules]]
query = "tok"
target = 1
anno = "test::clean"

[rules.value]
target = 1
replacements = [
    ['ð', 'der'],
    ['(.*)(.)\u0304(.*)', '$1$2/MACRON_M/$3|$1$2/MACRON_N/$3'],
    ['([^|]*)([^|])\u0304([^|]*)', '$1$2/MACRON_M/$3|$1$2/MACRON_N/$3'],
    ['/MACRON_M/', 'm'],
	['/MACRON_N/', 'n'],
]
"#;

        let m: Mapping = toml::from_str(config).unwrap();

        let g = tokens_with_macrons().unwrap();

        let singlemacron = g
            .get_node_annos()
            .exact_anno_search(Some("annis"), "tok", ValueSearch::Some("anðthalbē"))
            .next()
            .unwrap()
            .unwrap();

        let result = m.rules[0].resolve_value(&g, &[singlemacron]).unwrap();
        assert_eq!("anderthalbem|anderthalben", result);

        let multiple_macron = g
            .get_node_annos()
            .exact_anno_search(Some("annis"), "tok", ValueSearch::Some("ellēbogē"))
            .next()
            .unwrap()
            .unwrap();

        let result = m.rules[0].resolve_value(&g, &[multiple_macron]).unwrap();
        assert_eq!("ellembogem|ellenbogem|ellembogen|ellenbogen", result);
    }

    #[test]
    fn repeat_mapping_fixed() {
        let config = r#"
repetition = {Fixed = {n = 3}}

[[rules]]
query = "tok"
target = 1
anno = "annis::tok"

[rules.value]
target = 1
# Only replace the last character of each token.
replacements = [
    ['(\w\u0304?)X*$', 'X'],
]
        "#;
        let mut g = tokens_with_macrons().unwrap();

        let tmp = NamedTempFile::new().unwrap();

        std::fs::write(tmp.path(), config).unwrap();
        let mapper = MapAnnos {
            rule_file: Some(tmp.path().to_path_buf()),
            mapping: None,
            debug: true,
        };
        let step_id = StepID {
            module_name: "test_map".to_string(),
            path: None,
        };
        mapper
            .manipulate_corpus(&mut g, tmp.path().parent().unwrap(), step_id, None)
            .unwrap();

        let th = TokenHelper::new(&g).unwrap();

        let tokens = th.get_ordered_token("doc", None).unwrap();
        let text = th.spanned_text(&tokens).unwrap();

        // The rule is applied three times, to the last 3 characters of each
        // token should have been replaced.
        assert_eq!("X krX wechX etX anðthaX ellēbX hX", text);
    }

    #[test]
    fn repeat_mapping_until_unchanged() {
        let config = r#"
repetition = "UntilUnchanged"

[[rules]]
query = 'tok!="X"'
target = 1
anno = "annis::tok"

[rules.value]
target = 1
replacements = [
    ['[^X]X*$', 'X'],
]
        "#;
        let mut g = tokens_with_macrons().unwrap();

        let tmp = NamedTempFile::new().unwrap();

        std::fs::write(tmp.path(), config).unwrap();
        let mapper = MapAnnos {
            rule_file: Some(tmp.path().to_path_buf()),
            mapping: None,
            debug: true,
        };
        let step_id = StepID {
            module_name: "test_map".to_string(),
            path: None,
        };
        mapper
            .manipulate_corpus(&mut g, tmp.path().parent().unwrap(), step_id, None)
            .unwrap();

        let th = TokenHelper::new(&g).unwrap();

        let tokens = th.get_ordered_token("doc", None).unwrap();
        let text = th.spanned_text(&tokens).unwrap();

        // The rule is applied until all characters have been replaced.
        assert_eq!("X X X X X X X", text);
    }

    #[test]
    fn test_map_spans() {
        let mut updates = GraphUpdate::new();
        example_generator::create_corpus_structure_simple(&mut updates);
        example_generator::create_tokens(&mut updates, Some("root/doc1"));
        let mut g = AnnotationGraph::with_default_graphstorages(false).unwrap();
        g.apply_update(&mut updates, |_msg| {}).unwrap();

        let config = r#"
[[rules]]
query = "tok=/more/ . tok"
target = [1,2]
anno = "mapper::form"
value = "comparison"
        "#;
        let tmp = NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), config).unwrap();
        let mapper = MapAnnos {
            rule_file: Some(tmp.path().to_path_buf()),
            mapping: None,
            debug: true,
        };
        let step_id = StepID {
            module_name: "test_map".to_string(),
            path: None,
        };
        mapper
            .validate_graph(
                &mut g,
                StepID {
                    module_name: "test".to_string(),
                    path: None,
                },
                None,
            )
            .unwrap();
        mapper
            .manipulate_corpus(&mut g, tmp.path().parent().unwrap(), step_id, None)
            .unwrap();
        g.calculate_all_statistics().unwrap();
        let query = aql::parse(
            "mapper:form=\"comparison\" & \"more\" . \"complicated\" & #1 _l_ #2 & #1 _r_ #3",
            false,
        )
        .unwrap();
        let result: Vec<_> = aql::execute_query_on_graph(&g, &query, true, None)
            .unwrap()
            .collect();
        assert_eq!(1, result.len(), "Results are: {:?}", result);
        assert_eq!(true, result[0].is_ok());
    }

    #[test]
    fn test_map_annos_in_mem() {
        let r = main_test(false);
        assert!(r.is_ok(), "Error: {:?}", r.err());
    }

    #[test]
    fn test_map_annos_on_disk() {
        let r = main_test(true);
        assert!(r.is_ok(), "Error: {:?}", r.err());
    }

    fn main_test(on_disk: bool) -> Result<(), Box<dyn std::error::Error>> {
        let config = r#"
[[rules]]
query = "tok=/I/"
target = 1
anno = "pos"
value = "PRON"

[[rules]]
query = "tok=/am/"
target = 1
anno = "pos"
value = "VERB"

[[rules]]
query = "tok=/in/"
target = 1
anno = "pos"
value = "ADP"

[[rules]]
query = "tok=/New York/"
target = 1
anno = "pos"
value = "PROPN"
        "#;
        let tmp = NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), config).unwrap();
        let mapper = MapAnnos {
            rule_file: Some(tmp.path().to_path_buf()),
            mapping: None,
            debug: true,
        };
        let mut g = source_graph(on_disk)?;
        let (sender, _receiver) = mpsc::channel();
        let tx = Some(sender);
        let step_id = StepID {
            module_name: "test_map".to_string(),
            path: None,
        };
        mapper
            .manipulate_corpus(&mut g, tmp.path().parent().unwrap(), step_id, tx)
            .unwrap();

        let e_g = target_graph(on_disk)?;

        test_util::compare_graphs(&g, &e_g);

        //test with queries
        let queries = [
            ("tok=/I/ _=_ pos=/PRON/", 1),
            ("tok=/am/ _=_ pos=/VERB/", 1),
            ("tok=/in/ _=_ pos=/ADP/", 1),
            ("tok=/New York/ _=_ pos=/PROPN/", 1),
        ];

        for (query_s, expected_n) in queries {
            let query = graphannis::aql::parse(&query_s, false).unwrap();
            let matches_e: Result<Vec<_>, graphannis::errors::GraphAnnisError> =
                graphannis::aql::execute_query_on_graph(&e_g, &query, false, None)?.collect();
            let matches_g: Result<Vec<_>, graphannis::errors::GraphAnnisError> =
                graphannis::aql::execute_query_on_graph(&g, &query, false, None)?.collect();

            let mut matches_e = matches_e.unwrap();
            let mut matches_g = matches_g.unwrap();

            assert_eq!(
                matches_e.len(),
                expected_n,
                "Number of results for query `{}` does not match for expected graph. Expected:{} vs. Is:{}",
                query_s,
                expected_n,
                matches_e.len()
            );
            matches_e.sort();
            matches_g.sort();
            assert_eq!(
                matches_e, matches_g,
                "Matches for query '{query_s}' are not equal. {matches_e:?} != {matches_g:?}"
            );
        }
        Ok(())
    }

    fn source_graph(on_disk: bool) -> Result<AnnotationGraph, Box<dyn std::error::Error>> {
        let mut g = AnnotationGraph::with_default_graphstorages(on_disk)?;
        let mut u = GraphUpdate::default();
        u.add_event(UpdateEvent::AddNode {
            node_name: "doc".to_string(),
            node_type: "corpus".to_string(),
        })?;
        for (i, text) in ["I", "am", "in", "New York"].iter().enumerate() {
            let node_name = format!("doc#t{}", &i + &1);
            u.add_event(UpdateEvent::AddNode {
                node_name: node_name.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: text.to_string(),
            })?;
            if i > 0 {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("doc#t{i}"),
                    target_node: node_name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "".to_string(),
                })?;
            }
        }
        g.apply_update(&mut u, |_| {})?;
        Ok(g)
    }

    /// Create tokens "ein kraut wechſzt etwan anðthalbē ellēbogē hoch".
    fn tokens_with_macrons() -> Result<AnnotationGraph, Box<dyn std::error::Error>> {
        let mut g = AnnotationGraph::with_default_graphstorages(true)?;
        let mut u = GraphUpdate::default();
        u.add_event(UpdateEvent::AddNode {
            node_name: "doc".to_string(),
            node_type: "corpus".to_string(),
        })?;
        for (i, text) in [
            "ein",
            "kraut",
            "wechſzt",
            "etwan",
            "anðthalbē",
            "ellēbogē",
            "hoch",
        ]
        .iter()
        .enumerate()
        {
            let node_name = format!("doc#t{}", &i + &1);
            u.add_event(UpdateEvent::AddNode {
                node_name: node_name.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: text.to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdge {
                source_node: format!("doc#t{i}"),
                target_node: "doc".to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string(),
            })?;
            if i > 0 {
                u.add_event(UpdateEvent::AddEdge {
                    source_node: format!("doc#t{i}"),
                    target_node: node_name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "".to_string(),
                })?;
            }
        }
        g.apply_update(&mut u, |_| {})?;
        Ok(g)
    }

    fn target_graph(on_disk: bool) -> Result<AnnotationGraph, Box<dyn std::error::Error>> {
        let mut g = source_graph(on_disk)?;
        let mut u = GraphUpdate::default();
        for (i, pos_val) in ["PRON", "VERB", "ADP", "PROPN"].iter().enumerate() {
            let node_name = format!("doc#t{}", &i + &1);
            u.add_event(UpdateEvent::AddNode {
                node_name: node_name.to_string(),
                node_type: "node".to_string(),
            })?;
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_name.to_string(),
                anno_ns: "".to_string(),
                anno_name: "pos".to_string(),
                anno_value: pos_val.to_string(),
            })?;
        }
        g.apply_update(&mut u, |_| {})?;
        Ok(g)
    }
}
