use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::{progress::ProgressReporter, workflow::StatusSender, StepID};
use anyhow::Context;
use documented::{Documented, DocumentedFields};
use graphannis::{
    update::{GraphUpdate, UpdateEvent},
    AnnotationGraph,
};
use graphannis_core::graph::NODE_NAME_KEY;
use serde_derive::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use super::Manipulator;

/// Creates new annotations based on existing annotation values.
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(deny_unknown_fields)]
pub struct MapAnnos {
    /// The path of the TOML file containing an array of mapping rules.
    ///
    /// Each rule can contain a `query` field which describes the nodes the
    /// annotation are added to. The `target` field defines which node of the
    /// query the annotation should be added to. The annotation itself is
    /// defined by the `ns` (namespace), `name` and `value` fields. The `value`
    /// is currently a fixed value.
    ///
    /// ```toml
    /// [[rules]]
    /// query = "clean _o_ pos_lang=/(APPR)?ART/ _=_ lemma!=/[Dd](ie|as|er|ies)?/"
    /// target = 1
    /// ns = ""
    /// name = "indef"
    /// value = ""
    /// ```
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
        let mapping = read_config(read_from_path.as_path())?;
        self.run(graph, mapping, &tx, &step_id)?;
        Ok(())
    }
}

impl MapAnnos {
    fn run(
        &self,
        graph: &mut AnnotationGraph,
        mapping: Mapping,
        tx: &Option<StatusSender>,
        step_id: &StepID,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();

        let progress = ProgressReporter::new(tx.clone(), step_id.clone(), mapping.rules.len())?;
        for rule in mapping.rules {
            let query = graphannis::aql::parse(&rule.query, false)?;
            let result_it = graphannis::aql::execute_query_on_graph(graph, &query, true, None)?;
            for match_group in result_it {
                let match_group = match_group?;
                if let Some(m) = match_group.get(rule.target - 1) {
                    let match_node_name = graph
                        .get_node_annos()
                        .get_value_for_item(&m.node, &NODE_NAME_KEY)?
                        .context("Missing node name for matched node")?;
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: match_node_name.to_string(),
                        anno_ns: rule.ns.to_string(),
                        anno_name: rule.name.to_string(),
                        anno_value: rule.value.to_string(),
                    })?;
                }
            }

            progress.worked(1)?;
        }
        graph.apply_update(&mut update, |_| {})?;
        Ok(())
    }
}

fn read_config(path: &Path) -> Result<Mapping, Box<dyn std::error::Error>> {
    let config_string = fs::read_to_string(path)?;
    let m: Mapping = toml::from_str(config_string.as_str())?;
    Ok(m)
}

#[derive(Deserialize)]
struct Mapping {
    rules: Vec<Rule>,
}

#[derive(Deserialize)]
struct Rule {
    query: String,
    target: usize,
    ns: String,
    name: String,
    value: String,
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use graphannis::{
        model::AnnotationComponentType,
        update::{GraphUpdate, UpdateEvent},
        AnnotationGraph,
    };
    use graphannis_core::graph::ANNIS_NS;
    use tempfile::NamedTempFile;

    use crate::{manipulator::Manipulator, test_util, StepID};

    use super::MapAnnos;

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
