use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use crate::{
    progress::ProgressReporter,
    util::token_helper::{TokenHelper, TOKEN_KEY},
    StepID,
};
use anyhow::Context;
use documented::{Documented, DocumentedFields};
use graphannis::{
    graph::{EdgeContainer, Match},
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
    AnnotationGraph,
};
use graphannis_core::graph::{
    storage::union::UnionEdgeContainer, ANNIS_NS, DEFAULT_NS, NODE_NAME_KEY, NODE_TYPE_KEY,
};
use regex::Regex;
use serde_derive::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use super::Manipulator;

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
/// ns = ""
/// name = "indef"
/// value = ""
/// ```
///
/// A `target` can also be a list. In this case, a new span is created that
/// covers the same token as the referenced nodes of the match.
/// ```toml
/// [[rules]]    
/// query = "tok=/more/ . tok"
/// target = [1,2]
/// ns = "mapper"
/// name = "form"
/// value = "comparison"
/// ```
///
/// Instead of a fixed value, you can also use an existing annotation value
/// from the matched nodes copy the value.
/// ```toml
/// [[rules]]    
/// query = "tok=\"complicated\""
/// target = 1
/// ns = ""
/// name = "newtok"
/// value = {copy = 1}
/// ```
///
/// It is also possible to replace all occurences in the original value that
/// match a `search` regular expression with a `replacement` value.
/// ```toml
/// [[rules]]    
/// query = "tok=\"complicated\""
/// target = 1
/// ns = ""
/// name = "newtok"
/// value = {target = 1, search = "cat", replacement = "dog"}
/// ```
/// This would add a new annotation value "complidoged" to any token with the value "complicated".
///
/// The `replacement` value can contain back references to the regular
/// expression (e.g. "${0}" for the whole match or "${1}" for the first match
/// group).
/// ```toml
/// [[rules]]    
/// query = "tok=\"New York\""
/// target = 1
/// ns = ""
/// name = "abbr"
/// value = {target = 1, search = "([A-Z])[a-z]+ ([A-Z])[a-z]+", replacement = "${1}${2}"}
/// ```
/// This example would add an annotation with the value "NY".
///
/// The `copy` and `target` fields in the value description can also refer
/// to more than one copy of the query by using arrays instead of a single
/// number. In this case, the node values are concatenated using a space as
/// seperator.
///

#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(deny_unknown_fields)]
pub struct MapAnnos {
    /// The path of the TOML file containing an array of mapping rules.
    rule_file: PathBuf,
}

impl Manipulator for MapAnnos {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        workflow_directory: &std::path::Path,
        step_id: StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let read_from_path = {
            let p = Path::new(&self.rule_file).to_path_buf();
            if p.is_relative() {
                workflow_directory.join(p)
            } else {
                p
            }
        };
        let config = read_config(read_from_path.as_path())?;

        graph.ensure_loaded_all()?;

        let progress = ProgressReporter::new(tx.clone(), step_id.clone(), config.rules.len())?;
        let mut updates = {
            let tok_helper = TokenHelper::new(graph)?;
            let all_part_of_gs: Vec<_> = graph
                .get_all_components(Some(AnnotationComponentType::PartOf), None)
                .into_iter()
                .filter_map(|c| graph.get_graphstorage(&c))
                .collect();
            let all_part_of_edge_container: Vec<_> = all_part_of_gs
                .iter()
                .map(|gs| gs.as_edgecontainer())
                .collect();
            let part_of_gs = UnionEdgeContainer::new(all_part_of_edge_container);
            let mut map_impl = MapperImpl {
                config,
                added_spans: 0,
                graph,
                part_of_gs,
                tok_helper,
                progress,
            };
            map_impl.run()?
        };
        let progress = ProgressReporter::new_unknown_total_work(tx, step_id)?;
        graph.apply_update(&mut updates, move |msg| {
            if let Err(e) = progress.info(&format!("`map` updates: {msg}")) {
                log::error!("{e}");
            }
        })?;

        Ok(())
    }
}

fn read_config(path: &Path) -> Result<Mapping, Box<dyn std::error::Error>> {
    let config_string = fs::read_to_string(path)?;
    let m: Mapping = toml::from_str(config_string.as_str())?;
    Ok(m)
}

#[derive(Deserialize, Debug)]
struct Mapping {
    rules: Vec<Rule>,
}

#[derive(Clone, Deserialize, Debug)]
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

#[derive(Clone, Deserialize, Debug)]
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

#[derive(Clone, Debug, Deserialize)]
struct Rule {
    query: String,
    target: TargetRef,
    ns: String,
    name: String,
    value: Value,
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

struct MapperImpl<'a> {
    config: Mapping,
    added_spans: usize,
    graph: &'a AnnotationGraph,
    part_of_gs: UnionEdgeContainer<'a>,
    tok_helper: TokenHelper<'a>,
    progress: ProgressReporter,
}

impl<'a> MapperImpl<'a> {
    fn run(&mut self) -> anyhow::Result<GraphUpdate> {
        let mut update = GraphUpdate::default();

        for rule in self.config.rules.clone() {
            let query = graphannis::aql::parse(&rule.query, false)
                .with_context(|| format!("could not parse query '{}'", &rule.query))?;
            let result_it =
                graphannis::aql::execute_query_on_graph(self.graph, &query, true, None)?;
            for match_group in result_it {
                let match_group = match_group?;
                match rule.target {
                    TargetRef::Node(target) => {
                        self.map_single_node(&rule, target, &match_group, &mut update)?;
                    }
                    TargetRef::Span(ref all_targets) => {
                        self.map_span(&rule, all_targets, &match_group, &mut update)?;
                    }
                }
            }

            self.progress.worked(1)?;
        }
        Ok(update)
    }

    fn map_single_node(
        &self,
        rule: &Rule,
        target: usize,
        match_group: &[Match],
        update: &mut GraphUpdate,
    ) -> anyhow::Result<()> {
        if let Some(m) = match_group.get(target - 1) {
            let match_node_name = self
                .graph
                .get_node_annos()
                .get_value_for_item(&m.node, &NODE_NAME_KEY)?
                .context("Missing node name for matched node")?;
            update.add_event(UpdateEvent::AddNodeLabel {
                node_name: match_node_name.to_string(),
                anno_ns: rule.ns.to_string(),
                anno_name: rule.name.to_string(),
                anno_value: rule.resolve_value(self.graph, match_group)?,
            })?;
        }
        Ok(())
    }

    fn map_span(
        &mut self,
        rule: &Rule,
        targets: &[usize],
        match_group: &[Match],
        update: &mut GraphUpdate,
    ) -> anyhow::Result<()> {
        if let Some(first_match) = targets
            .first()
            .copied()
            .and_then(|t| match_group.get(t - 1))
        {
            // Calculate all token that should be covered by the newly create span
            let mut covered_token = BTreeSet::new();
            for t in targets {
                if let Some(n) = match_group.get(t - 1) {
                    if self.tok_helper.is_token(n.node)? {
                        covered_token.insert(n.node);
                    } else {
                        covered_token.extend(self.tok_helper.covered_token(n.node)?);
                    }
                }
            }

            // Determine the new node name by extending the node name of the first target
            let first_node_name = self
                .graph
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
                anno_ns: rule.ns.to_string(),
                anno_name: rule.name.to_string(),
                anno_value: rule.resolve_value(self.graph, match_group)?,
            })?;

            // Add the new node to the common parent
            if let Some(parent_node) = self.part_of_gs.get_outgoing_edges(first_match.node).next() {
                let parent_node = parent_node?;
                let parent_node_name = self
                    .graph
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
                let token_node_name = self
                    .graph
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
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use graphannis::{
        aql,
        model::AnnotationComponentType,
        update::{GraphUpdate, UpdateEvent},
        AnnotationGraph,
    };
    use graphannis_core::{annostorage::ValueSearch, graph::ANNIS_NS};
    use tempfile::NamedTempFile;

    use crate::{manipulator::Manipulator, test_util, util::example_generator, StepID};

    use super::*;

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
            ns: "test_ns".to_string(),
            name: "test".to_string(),
            value: Value::Fixed("myvalue".to_string()),
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
        ns = "test_ns" 
        name = "test"
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
            ns: "test_ns".to_string(),
            name: "test".to_string(),
            value: Value::Replace {
                target: TargetRef::Node(1),
                replacements: vec![("cat".to_string(), "dog".to_string())],
            },
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
            ns: "test_ns".to_string(),
            name: "test".to_string(),
            value: Value::Replace {
                target: TargetRef::Node(1),
                replacements: vec![("cat.*".to_string(), "$0$0".to_string())],
            },
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
ns = "" 
name = "abbr"

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
ns = "mapper"
name = "form"
value = "comparison"
        "#;
        let tmp = NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), config).unwrap();
        let mapper = MapAnnos {
            rule_file: tmp.path().to_path_buf(),
        };
        let step_id = StepID {
            module_name: "test_map".to_string(),
            path: None,
        };
        mapper
            .manipulate_corpus(&mut g, tmp.path().parent().unwrap(), step_id, None)
            .unwrap();

        let query = aql::parse(
            "mapper:form=\"comparison\" & \"more\" . \"complicated\" & #1 _l_ #2 & #1 _r_ #3",
            false,
        )
        .unwrap();
        let result: Vec<_> = aql::execute_query_on_graph(&g, &query, true, None)
            .unwrap()
            .collect();
        assert_eq!(1, result.len());
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
ns = ""
name = "pos"
value = "PRON"            

[[rules]]
query = "tok=/am/"
target = 1
ns = ""
name = "pos"
value = "VERB"            

[[rules]]
query = "tok=/in/"
target = 1
ns = ""
name = "pos"
value = "ADP"            

[[rules]]
query = "tok=/New York/"
target = 1
ns = ""
name = "pos"
value = "PROPN"
        "#;
        let tmp = NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), config).unwrap();
        let mapper = MapAnnos {
            rule_file: tmp.path().to_path_buf(),
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
