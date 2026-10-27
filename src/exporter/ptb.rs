use std::{collections::BTreeMap, fmt::Write, fs, path::Path};

use anyhow::{anyhow, bail};
use facet::Facet;
use graphannis::{
    AnnotationGraph,
    graph::{AnnoKey, NodeID},
    model::{AnnotationComponent, AnnotationComponentType},
};
use graphannis_core::{annostorage::ValueSearch, graph::ANNIS_NS, util::join_qname};
use itertools::Itertools;
use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};

use crate::{exporter::Exporter, importer::GLOBAL_DEFAULT_PART_OF, progress::ProgressReporter};

/// This module exports tree structures in parentheses format as known from Penn Treebank.
#[derive(Clone, Deserialize, Facet, PartialEq, Serialize)]
pub struct ExportPTB {
    /// This is used to set the syntax component. The default is `{ ctype = "Dominance", layer = "syntax", name = ""}`.
    #[serde(
        with = "crate::estarde::annotation_component",
        default = "default_component"
    )]
    component: AnnotationComponent,
    /// This component is used to split the corpus graph in separate files. Default is `{ ctype = "PartOf", layer = "annis", name = ""}`.
    #[serde(
        default = "default_file_component",
        with = "crate::estarde::annotation_component"
    )]
    file_component: AnnotationComponent,
    /// The given key identifies the nodes gathering all nodes of a document. Default is `annis::doc`.
    #[serde(default = "default_file_key")]
    file_key: AnnoKey,
    /// This annotation key is used to derive the category of nonterminal nodes. Default is `syntax::cat`.
    #[serde(default = "default_nonterminal_key", with = "crate::estarde::anno_key")]
    nonterminal_key: AnnoKey,
    /// This annotation key is used to derive the category of terminal nodes. Default is `syntax::pos`.
    #[serde(default = "default_terminal_key", with = "crate::estarde::anno_key")]
    terminal_key: AnnoKey,
    /// This annotation key contains the text value of the leaves of the tree. Default is `annis::tok`.
    #[serde(default = "default_text_key", with = "crate::estarde::anno_key")]
    text_key: AnnoKey,
    /// This component, usually an ordering, is used to identify the linear sequence of leave nodes. Default is `{ ctype = "Ordering", layer = "annis", name = ""}`.
    #[serde(
        default = "default_terminal_component",
        with = "crate::estarde::annotation_component"
    )]
    terminal_component: AnnotationComponent,
}

impl Default for ExportPTB {
    fn default() -> Self {
        Self {
            component: default_component(),
            file_component: default_file_component(),
            file_key: default_file_key(),
            nonterminal_key: default_nonterminal_key(),
            terminal_key: default_terminal_key(),
            text_key: default_text_key(),
            terminal_component: default_terminal_component(),
        }
    }
}

fn default_component() -> AnnotationComponent {
    AnnotationComponent::new(
        AnnotationComponentType::Dominance,
        "syntax".to_string(),
        "".to_string(),
    )
}

fn default_file_component() -> AnnotationComponent {
    GLOBAL_DEFAULT_PART_OF.clone()
}

fn default_file_key() -> AnnoKey {
    AnnoKey {
        name: "doc".to_string(),
        ns: ANNIS_NS.to_string(),
    }
}

fn default_nonterminal_key() -> AnnoKey {
    AnnoKey {
        name: "cat".to_string(),
        ns: "syntax".to_string(),
    }
}

fn default_terminal_key() -> AnnoKey {
    AnnoKey {
        name: "pos".to_string(),
        ns: "syntax".to_string(),
    }
}

fn default_terminal_component() -> AnnotationComponent {
    AnnotationComponent::new(
        AnnotationComponentType::Ordering,
        ANNIS_NS.to_string(),
        "".to_string(),
    )
}

fn default_text_key() -> AnnoKey {
    AnnoKey {
        name: "tok".to_string(),
        ns: ANNIS_NS.to_string(),
    }
}

impl Exporter for ExportPTB {
    fn export_corpus(
        &self,
        graph: &graphannis::AnnotationGraph,
        output_path: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let document_nodes = graph
            .get_node_annos()
            .exact_anno_search(
                Some(&self.file_key.ns),
                &self.file_key.name,
                ValueSearch::Any,
            )
            .flatten()
            .map(|m| m.node)
            .collect_vec();
        let progress = ProgressReporter::new(tx, step_id, document_nodes.len())?;
        let ordering_storage = graph
            .get_graphstorage(&self.terminal_component)
            .ok_or(anyhow!(
                "Terminal component does not exist or has no storage."
            ))?;
        let n_source_nodes = ordering_storage.source_nodes().size_hint().0;
        let n_root_nodes = ordering_storage.root_nodes().size_hint().0;
        let total_ordering = {
            let mut v = Vec::with_capacity(n_root_nodes + n_source_nodes);
            for rn in ordering_storage.root_nodes() {
                for reachable in ordering_storage.find_connected(rn?, 0, std::ops::Bound::Unbounded)
                {
                    v.push(reachable?);
                }
            }
            v
        };
        let sorting_guide = SortingGuide {
            guiding_sequence: &total_ordering,
        };
        document_nodes.into_iter().try_for_each(|n| {
            self.export_document(graph, n, output_path, &sorting_guide)?;
            progress.worked(1)
        })?;
        Ok(())
    }

    fn file_extension(&self) -> &str {
        "ptb"
    }
}

impl ExportPTB {
    fn export_document(
        &self,
        graph: &AnnotationGraph,
        doc_node_id: NodeID,
        target_path: &Path,
        total_ordering: &SortingGuide,
    ) -> Result<(), anyhow::Error> {
        let document_name = graph
            .get_node_annos()
            .get_value_for_item(&doc_node_id, &self.file_key)?
            .ok_or(anyhow!("Document has no name."))?;
        let export_path = target_path.join(format!("{document_name}.{}", self.file_extension()));
        let part_of_storage = graph
            .get_graphstorage(&self.file_component)
            .ok_or(anyhow!("File component has no storage"))?;
        let syntax_storage = graph.get_graphstorage(&self.component).ok_or(anyhow!(
            "Syntax component `{}` has no storage",
            self.component
        ))?;
        let syntax_roots: LinkedHashMap<NodeID, (usize, usize)> = {
            let mut roots = Vec::default();
            for reachable_node in
                part_of_storage.find_connected_inverse(doc_node_id, 1, std::ops::Bound::Unbounded)
            {
                let n = reachable_node?;
                if !syntax_storage.has_ingoing_edges(n)?
                    && (graph
                        .get_node_annos()
                        .has_value_for_item(&n, &self.nonterminal_key)?
                        || graph
                            .get_node_annos()
                            .has_value_for_item(&n, &self.terminal_key)?)
                {
                    roots.push(n);
                }
            }
            let mut root_to_involved_tokens_unsorted: BTreeMap<u64, (usize, usize)> = {
                let ordering_gs = graph
                    .get_graphstorage(&self.terminal_component)
                    .ok_or(anyhow!("Ordering component could not be loaded."))?;
                let mut roots_with_text_nodes = BTreeMap::default();
                for n in &roots {
                    let mut v = Vec::default();
                    for reachable in
                        syntax_storage.find_connected(*n, 0, std::ops::Bound::Unbounded)
                    {
                        let reachable = reachable?;
                        if ordering_gs.has_ingoing_edges(reachable)?
                            || ordering_gs.has_outgoing_edges(reachable)?
                        {
                            v.push(reachable);
                        }
                    }
                    v.sort_unstable_by(|a, b| total_ordering.cmp(*a, *b));
                    if v.is_empty() {
                        bail!("Data for {target_path:?} contains empty trees.");
                    }
                    let start_index = total_ordering
                        .guiding_sequence
                        .iter()
                        .position(|e| *e == v.first().map(|n| *n).unwrap_or_default())
                        .ok_or(anyhow!("Could not determine position of terminal node"))?;
                    let end_index = total_ordering
                        .guiding_sequence
                        .iter()
                        .position(|e| *e == v.last().map(|n| *n).unwrap_or_default())
                        .ok_or(anyhow!("Could not determine position of terminal node"))?;
                    roots_with_text_nodes.insert(*n, (start_index, end_index + 1));
                }
                roots_with_text_nodes
            };
            roots.sort_by(|a, b| {
                if let Some((start_a, _)) = root_to_involved_tokens_unsorted.get(a) {
                    if let Some((start_b, _)) = root_to_involved_tokens_unsorted.get(b) {
                        start_a.cmp(start_b)
                    } else {
                        // should be unreachable
                        std::cmp::Ordering::Equal
                    }
                } else {
                    // should be unreachable
                    std::cmp::Ordering::Equal
                }
            });
            let mut roots_with_tokens = LinkedHashMap::with_capacity(roots.len());
            for root in roots {
                let token_interval = root_to_involved_tokens_unsorted
                    .remove(&root)
                    .ok_or(anyhow!("No token interval for syntactic root"))?;
                roots_with_tokens.insert(root, token_interval);
            }
            roots_with_tokens
        };
        let mut out_file = fs::File::create(export_path)?;
        for (_, (start, end_excl)) in syntax_roots {
            // first gather all involved tokens as these allow to proceed in the correct order (for dominated non-terminals it is not guaranteed they are retrieved following the text order)
            let node_sequence = &total_ordering.guiding_sequence[start..end_excl];
            let tree = self.to_tree(graph, node_sequence)?;
            std::io::Write::write_all(&mut out_file, tree.as_bytes())?;
        }
        Ok(())
    }

    fn to_tree(
        &self,
        graph: &AnnotationGraph,
        node_sequence: &[NodeID],
    ) -> Result<String, anyhow::Error> {
        let syntax_gs = graph
            .get_graphstorage(&self.component)
            .ok_or(anyhow!("Syntax edge storage unavailable."))?;
        let mut terminal_strings = Vec::with_capacity(node_sequence.len());
        for text_node in node_sequence {
            let anno_value = graph
                .get_node_annos()
                .get_value_for_item(text_node, &self.terminal_key)?
                .ok_or(anyhow!(
                    "Text node has no terminal key annotation {}.",
                    join_qname(&self.terminal_key.ns, &self.terminal_key.name)
                ))?;
            let text_value = graph
                .get_node_annos()
                .get_value_for_item(text_node, &self.text_key)?
                .ok_or(anyhow!("Terminal node has no text annotation."))?;
            let terminal_str = format!(
                "({} {})",
                clean_anno_value(&anno_value),
                clean_text_value(&text_value)?
            );
            terminal_strings.push(terminal_str);
        }
        let mut terminal_iter = node_sequence.iter().zip(terminal_strings);
        let mut tree = String::from("("); // open outer
        let mut visited_nodes = vec![];
        while let Some((terminal_id, terminal_repr)) = terminal_iter.next() {
            for (i, node) in visited_nodes.iter().rev().enumerate() {
                let is_connected =
                    syntax_gs.is_connected(*node, *terminal_id, 1, std::ops::Bound::Unbounded)?;
                if is_connected {
                    visited_nodes = visited_nodes[..visited_nodes.len() - i].to_vec();
                    break;
                } else {
                    write!(&mut tree, ")")?;
                }
            }
            let mut node_iter = syntax_gs
                .find_connected_inverse(*terminal_id, 1, std::ops::Bound::Unbounded)
                .flatten();
            if let Some(gov) = node_iter.next() {
                self.write_recursive(graph, gov, &mut node_iter, &mut tree, &mut visited_nodes)?;
            }
            write!(&mut tree, " {terminal_repr}")?;
        }
        for _ in visited_nodes {
            write!(&mut tree, ")")?;
        }
        write!(&mut tree, ")\n")?; // close outer
        Ok(tree)
    }

    fn write_recursive(
        &self,
        graph: &AnnotationGraph,
        node: NodeID,
        nodes: &mut impl Iterator<Item = NodeID>,
        write_target: &mut String,
        visited: &mut Vec<NodeID>,
    ) -> Result<(), anyhow::Error> {
        if let Some(parent) = nodes.next() {
            self.write_recursive(graph, parent, nodes, write_target, visited)?;
        }
        if !visited.contains(&node) {
            visited.push(node);
            let anno_value = graph
                .get_node_annos()
                .get_value_for_item(&node, &self.nonterminal_key)?
                .ok_or(anyhow!(
                    "Nonterminal node has no nonterminal key annotation ({}).",
                    join_qname(&self.nonterminal_key.ns, &self.nonterminal_key.name)
                ))?;
            write!(write_target, " ({}", clean_anno_value(&anno_value))?;
        }
        Ok(())
    }
}

fn clean_text_value(value: &str) -> Result<String, anyhow::Error> {
    let mut clean_value = String::with_capacity(value.len() + 2);
    let is_multi_token_string = value.contains(" ");
    if is_multi_token_string && value.chars().nth(0) != Some('"') {
        write!(&mut clean_value, "\"")?;
    }
    write!(
        &mut clean_value,
        "{}",
        value.replace("(", "-LRB-").replace(")", "-RRB-")
    )?;
    if is_multi_token_string && value.chars().nth(value.len() - 1) != Some('"') {
        write!(&mut clean_value, "\"")?;
    }
    Ok(clean_value)
}

fn clean_anno_value(value: &str) -> String {
    format!("{}", value.replace("(", "-LRB-").replace(")", "-RRB-"))
}

struct SortingGuide<'a> {
    guiding_sequence: &'a Vec<NodeID>,
}

impl SortingGuide<'_> {
    fn cmp(&self, a: NodeID, b: NodeID) -> std::cmp::Ordering {
        if a < b {
            // for most situations, a smaller node id was created earlier in the ordering (it's rarely different)
            if let Some(pos_a) = self.guiding_sequence.iter().position(|e| *e == a) {
                if pos_a < self.guiding_sequence.len() / 2 {
                    // search on the left
                    if (&self.guiding_sequence[0..pos_a]).contains(&b) {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                } else {
                    // search on the right
                    if (&self.guiding_sequence[pos_a..]).contains(&b) {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    }
                }
            } else {
                // this branch should be unreachable (for consistent models)
                std::cmp::Ordering::Equal
            }
        } else {
            if let Some(pos_b) = self.guiding_sequence.iter().position(|e| *e == b) {
                if pos_b < self.guiding_sequence.len() / 2 {
                    // check to the left
                    if (&self.guiding_sequence[0..pos_b]).contains(&a) {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    }
                } else {
                    // check to the right
                    if (&self.guiding_sequence[pos_b..]).contains(&a) {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                }
            } else {
                // this branch should be unreachable (for consistent models)
                std::cmp::Ordering::Equal
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use graphannis::AnnotationGraph;
    use insta::assert_snapshot;

    use crate::{
        exporter::ptb::ExportPTB,
        importer::{DefaultImportConfiguration, Importer, ptb::ImportPTB},
        test_util::export_to_string,
        util::graphupdate::UpdateWriter,
    };

    #[test]
    fn export_ptb() {
        let import = ImportPTB::default();
        let u = import.import_corpus(
            Path::new("tests/data/import/ptb/forest"),
            crate::StepID {
                module_name: "import_ptb".to_string(),
                path: None,
            },
            UpdateWriter::from(import.default_configuration()),
            None,
        );
        assert!(u.is_ok(), "Err: {}", u.err().unwrap());
        let mut graph = AnnotationGraph::with_default_graphstorages(false).unwrap();
        assert!(graph.apply_update(&mut u.unwrap(), |_| {}).is_ok());
        let export = ExportPTB::default();
        let actual = export_to_string(&graph, export);
        assert!(actual.is_ok(), "Err: {}", actual.err().unwrap());
        assert_snapshot!(actual.unwrap());
    }
}
