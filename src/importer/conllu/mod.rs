use std::{
    collections::{btree_map::Entry, BTreeMap},
    fmt::Display,
    io::Read,
    path::{Path, PathBuf},
};

use anyhow::anyhow;
use documented::{Documented, DocumentedFields};
use encoding_rs_io::DecodeReaderBytes;
use graphannis::{
    graph::AnnoKey,
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::{
    graph::ANNIS_NS,
    util::{join_qname, split_qname},
};
use itertools::Itertools;
use linked_hash_set::LinkedHashSet;
use pest::{
    iterators::{Pair, Pairs},
    Parser,
};
use pest_derive::Parser;
use serde::Serialize;
use serde_derive::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use super::Importer;
use crate::{
    error::AnnattoError, progress::ProgressReporter,
    util::graphupdate::import_corpus_graph_from_files, workflow::StatusSender, StepID,
};

/// Import files in the [CONLL-U format](https://universaldependencies.org/format.html)
/// from the Universal Dependencies project.
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ImportCoNLLU {
    /// This key defines the annotation name and namespace for sentence comments, sometimes referred to as metadata in the CoNLL-X universe.
    /// Example:
    /// ```toml
    /// comment_anno = { ns = "comment_namespace", name = "comment_name"}
    ///
    /// ```
    ///
    /// The field defaults to `{ ns = "conll", name = "comment" }`.
    ///
    #[serde(default = "default_comment_key", with = "crate::estarde::anno_key")]
    comment_anno: AnnoKey,
    /// For importing multi-tokens, a mode can be set. By default, multi-tokens are skipped.
    #[serde(default, with = "crate::estarde::anno_key::as_option")]
    multi_tok: Option<AnnoKey>,
}

impl Default for ImportCoNLLU {
    fn default() -> Self {
        Self {
            comment_anno: default_comment_key(),
            multi_tok: Default::default(),
        }
    }
}

fn default_comment_key() -> AnnoKey {
    AnnoKey {
        name: "comment".into(),
        ns: "conll".into(),
    }
}

const FILE_EXTENSIONS: [&str; 2] = ["conll", "conllu"];

impl Importer for ImportCoNLLU {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        step_id: StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let paths_and_node_names =
            import_corpus_graph_from_files(&mut update, input_path, self.file_extensions())?;
        let progress =
            ProgressReporter::new(tx.clone(), step_id.clone(), paths_and_node_names.len())?;
        for (pathbuf, doc_node_name) in paths_and_node_names {
            self.import_document(&step_id, &mut update, pathbuf.as_path(), doc_node_name, &tx)?;
            progress.worked(1)?;
        }
        Ok(update)
    }

    fn file_extensions(&self) -> &[&str] {
        &FILE_EXTENSIONS
    }
}

impl Display for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Rule::lemma => "lemma",
            Rule::upos => "upos",
            Rule::xpos => "xpos",
            Rule::deprel => "deprel",
            Rule::enhanced_rel => "rel",
            _ => "",
        };
        write!(f, "{s}")
    }
}

type DepSpec = LinkedHashSet<(usize, Option<String>)>;

impl ImportCoNLLU {
    fn import_document(
        &self,
        step_id: &StepID,
        update: &mut GraphUpdate,
        document_path: &Path,
        document_node_name: String,
        tx: &Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let f = std::fs::File::open(document_path)?;
        let mut decoder = DecodeReaderBytes::new(f);
        let mut file_content = String::new();
        decoder.read_to_string(&mut file_content)?; // TODO this needs to be buffered. UD Files can be very large
        let conllu: Pairs<Rule> = CoNLLUParser::parse(Rule::conllu, &file_content)?;
        self.map_document(step_id, update, document_node_name, conllu, tx)?;
        Ok(())
    }

    fn map_document(
        &self,
        step_id: &StepID,
        update: &mut GraphUpdate,
        document_node_name: String,
        mut conllu: Pairs<Rule>,
        tx: &Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut token_names = Vec::new();
        if let Some(pair) = conllu.next() {
            if pair.as_rule() == Rule::conllu {
                for sentence in pair.into_inner() {
                    // iterate over sentences
                    if sentence.as_rule() == Rule::sentence {
                        token_names.extend(self.map_sentence(
                            step_id,
                            update,
                            document_node_name.as_str(),
                            sentence,
                            tx,
                        )?);
                    }
                }
            }
        } else {
            let msg = format!("Could not parse file as conllu: {document_node_name}");
            let err = AnnattoError::Import {
                reason: msg,
                importer: step_id.module_name.clone(),
                path: PathBuf::from(document_node_name),
            };
            return Err(err.into());
        }
        for (source, target) in token_names.iter().tuple_windows() {
            update.add_event(UpdateEvent::AddEdge {
                source_node: source.to_string(),
                target_node: target.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::Ordering.to_string(),
                component_name: "".to_string(),
            })?;
        }
        Ok(())
    }

    fn map_sentence(
        &self,
        step_id: &StepID,
        update: &mut GraphUpdate,
        document_node_name: &str,
        sentence: Pair<Rule>,
        tx: &Option<StatusSender>,
    ) -> anyhow::Result<Vec<String>> {
        let mut id_to_tok_name = BTreeMap::new();
        let mut dependencies = Vec::new();
        let mut s_annos: BTreeMap<String, Vec<String>> = BTreeMap::default();
        let mut multi_tok = None;
        let mut multi_tok_time_out: isize = 0;
        let (l, _) = sentence.line_col();
        for member in sentence.into_inner() {
            match member.as_rule() {
                Rule::token => {
                    let (tok_name, tok_id, mut deps) = self.map_token(
                        step_id,
                        update,
                        document_node_name,
                        member,
                        &multi_tok,
                        tx,
                    )?;

                    id_to_tok_name.insert(tok_id, tok_name.to_string());
                    if let Some(dependency) = deps.pop_front() {
                        dependencies.push((
                            tok_name.to_string(),
                            dependency.0,
                            dependency.1.clone(),
                            "",
                            "dep",
                        ));
                    }

                    multi_tok_time_out -= 1;
                    if multi_tok_time_out < 0 {
                        multi_tok_time_out = 0; // prevent underflow for very large corpora
                        multi_tok = None;
                    }

                    for (h, r) in deps {
                        dependencies.push((tok_name.to_string(), h, r, "enh", "dep"));
                    }
                }
                Rule::multi_token => {
                    let mut inner = member.into_inner();
                    let multi_id = inner
                        .next()
                        .ok_or(anyhow!("No valid id for multi token."))?
                        .as_str();
                    let form = inner
                        .next()
                        .ok_or(anyhow!("Multi token has no valid form."))?
                        .as_str();
                    let (from_token, to_token) = multi_id
                        .split_once("-")
                        .ok_or(anyhow!("No valid id range for multi token."))?;
                    let start_id = from_token.parse::<usize>()?;
                    let end_id = to_token.parse::<usize>()?;
                    multi_tok = Some((start_id, end_id, form.to_string()));
                    multi_tok_time_out = end_id as isize - start_id as isize;
                }
                Rule::s_anno => {
                    let mut name = None;
                    let mut value = None;
                    for name_or_s_value in member.into_inner() {
                        match name_or_s_value.as_rule() {
                            Rule::name => {
                                name = Some(name_or_s_value.as_str().trim().to_string());
                            }
                            Rule::s_anno_value => {
                                value = Some(name_or_s_value.as_str().trim().to_string())
                            }
                            _ => {}
                        }
                    }
                    if let (Some(fk), Some(fv)) = (name, value) {
                        match s_annos.entry(fk) {
                            Entry::Vacant(e) => {
                                e.insert(vec![fv]);
                            }
                            Entry::Occupied(mut e) => {
                                e.get_mut().push(fv);
                            }
                        }
                    }
                }
                Rule::s_comment => {
                    let comment = member.into_inner().as_str();
                    let key = join_qname(&self.comment_anno.ns, &self.comment_anno.name);
                    match s_annos.entry(key) {
                        Entry::Vacant(vacant_entry) => {
                            vacant_entry.insert(vec![comment.to_string()]);
                        }
                        Entry::Occupied(mut occupied_entry) => {
                            occupied_entry.get_mut().push(comment.to_string());
                        }
                    }
                }
                _ => {}
            }
        }
        if !id_to_tok_name.is_empty() {
            let node_name = format!("{document_node_name}#s{l}");
            update.add_event(UpdateEvent::AddNode {
                node_name: node_name.to_string(),
                node_type: "node".to_string(),
            })?;
            update.add_event(UpdateEvent::AddEdge {
                source_node: node_name.to_string(),
                target_node: document_node_name.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string(),
            })?;
            for (anno_name, anno_values) in s_annos {
                let (ns, name) = split_qname(&anno_name);
                let anno_value = anno_values.join("\n");
                update.add_event(UpdateEvent::AddNodeLabel {
                    node_name: node_name.to_string(),
                    anno_ns: ns.unwrap_or_default().to_string(),
                    anno_name: name.to_string(),
                    anno_value,
                })?;
            }
            for token_name in id_to_tok_name.values() {
                update.add_event(UpdateEvent::AddEdge {
                    source_node: node_name.to_string(),
                    target_node: token_name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Coverage.to_string(),
                    component_name: "".to_string(),
                })?;
            }
            for (target_node_name, head_id, deprel, clayer, cname) in dependencies {
                if head_id > 0 {
                    if let Some(source_node_name) = id_to_tok_name.get(&head_id) {
                        update.add_event(UpdateEvent::AddEdge {
                            source_node: source_node_name.to_string(),
                            target_node: target_node_name.to_string(),
                            layer: clayer.to_string(),
                            component_type: AnnotationComponentType::Pointing.to_string(),
                            component_name: cname.to_string(),
                        })?;
                        if let Some(deprel_value) = deprel {
                            update.add_event(UpdateEvent::AddEdgeLabel {
                                source_node: source_node_name.to_string(),
                                target_node: target_node_name.to_string(),
                                layer: clayer.to_string(),
                                component_type: AnnotationComponentType::Pointing.to_string(),
                                component_name: cname.to_string(),
                                anno_ns: "".to_string(),
                                anno_name: "deprel".to_string(),
                                anno_value: deprel_value.to_string(),
                            })?;
                        }
                    } else {
                        let msg =
                            format!("Failed to build dependency tree: Unknown head id `{head_id}` (line {l})");
                        let err = AnnattoError::Import {
                            reason: msg,
                            importer: step_id.module_name.clone(),
                            path: Path::new(document_node_name).to_path_buf(),
                        };
                        return Err(err.into());
                    }
                }
            }
        }
        Ok(id_to_tok_name.into_iter().map(|e| e.1).collect_vec())
    }

    fn map_token(
        &self,
        step_id: &StepID,
        update: &mut GraphUpdate,
        document_node_name: &str,
        token: Pair<Rule>,
        multi_token: &Option<(usize, usize, String)>,
        _tx: &Option<StatusSender>,
    ) -> anyhow::Result<(String, usize, DepSpec)> {
        let (l, _) = token.line_col();
        let line = token.as_str().to_string();
        let node_name = format!("{document_node_name}#t{l}");
        update.add_event(UpdateEvent::AddNode {
            node_name: node_name.to_string(),
            node_type: "node".to_string(),
        })?;
        update.add_event(UpdateEvent::AddEdge {
            source_node: node_name.to_string(),
            target_node: document_node_name.to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        update.add_event(UpdateEvent::AddNodeLabel {
            node_name: node_name.to_string(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "layer".to_string(),
            anno_value: "default_layer".to_string(),
        })?;
        let mut token_id = None;
        let mut dependencies = DepSpec::default();
        for member in token.into_inner() {
            let rule = member.as_rule();
            match rule {
                Rule::id => {
                    token_id = Some(member.as_str().parse::<usize>()?);
                }
                Rule::form => {
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: node_name.to_string(),
                        anno_ns: ANNIS_NS.to_string(),
                        anno_name: "tok".to_string(),
                        anno_value: member.as_str().to_string(),
                    })?;
                    if let Some(anno) = &self.multi_tok {
                        let (span_name, text_value) = if let Some((start, end, value)) = multi_token
                        {
                            (
                                format!("{document_node_name}#span{start}-{end}"),
                                value.to_string(),
                            )
                        } else {
                            (
                                format!("{document_node_name}#span{l}"),
                                member.as_str().to_string(),
                            )
                        };
                        update.add_event(UpdateEvent::AddNode {
                            node_name: span_name.to_string(),
                            node_type: "node".to_string(),
                        })?;
                        update.add_event(UpdateEvent::AddNodeLabel {
                            node_name: span_name.to_string(),
                            anno_ns: anno.ns.to_string(),
                            anno_name: anno.name.to_string(),
                            anno_value: text_value.to_string(),
                        })?;
                        update.add_event(UpdateEvent::AddEdge {
                            source_node: span_name.to_string(),
                            target_node: document_node_name.to_string(),
                            layer: ANNIS_NS.to_string(),
                            component_type: AnnotationComponentType::PartOf.to_string(),
                            component_name: "".to_string(),
                        })?;
                        update.add_event(UpdateEvent::AddEdge {
                            source_node: span_name.to_string(),
                            target_node: node_name.to_string(),
                            layer: ANNIS_NS.to_string(),
                            component_type: AnnotationComponentType::Coverage.to_string(),
                            component_name: "".to_string(),
                        })?;
                    }
                }
                Rule::lemma | Rule::upos | Rule::xpos => {
                    let anno_name = rule.to_string();
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: node_name.to_string(),
                        anno_ns: "".to_string(),
                        anno_name: anno_name.to_string(),
                        anno_value: member.as_str().to_string(),
                    })?;
                }
                Rule::features | Rule::misc => {
                    for feature in member.into_inner() {
                        if feature.as_rule() == Rule::feature {
                            let mut anno_name = None;
                            let mut anno_value = None;
                            for name_or_value in feature.into_inner() {
                                let feature_rule = name_or_value.as_rule();
                                if feature_rule == Rule::name {
                                    anno_name = Some(name_or_value.as_str().to_string());
                                } else if feature_rule == Rule::value {
                                    anno_value = Some(name_or_value.as_str().to_string());
                                }
                                if let (Some(n), Some(v)) = (&anno_name, &anno_value) {
                                    update.add_event(UpdateEvent::AddNodeLabel {
                                        node_name: node_name.to_string(),
                                        anno_ns: "".to_string(),
                                        anno_name: n.trim().to_string(),
                                        anno_value: v.trim().to_string(),
                                    })?;
                                }
                            }
                        }
                    }
                }
                Rule::head => {
                    dependencies.insert((member.as_str().trim().parse::<usize>()?, None));
                }
                Rule::deprel => {
                    if let Some((base_head, None)) = dependencies.pop_back() {
                        dependencies.insert((base_head, Some(member.as_str().trim().to_string())));
                    }
                }
                Rule::enhanced_deps => {
                    for enh_dep in member.into_inner() {
                        let mut inner = enh_dep.into_inner();
                        if let Some(enh_id) = inner.next() {
                            let head = enh_id.as_str().trim().parse::<usize>()?;
                            if let Some(enh_rel) = inner.next() {
                                let rel = enh_rel.as_str().to_string();
                                let value = (head, Some(rel));
                                // this is to avoid the basic dependency to be anywhere else than in the first position, because this position needs to be treated differently
                                // to avoid cycles in the graph
                                if !dependencies.contains(&value) {
                                    dependencies.insert(value);
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        if let Some(id) = token_id {
            Ok((node_name, id, dependencies))
        } else {
            // by grammar spec this branch should never be possible
            let reason = format!("Token `{line}` ({l}) has no id which is invalid.");

            Err(AnnattoError::Import {
                reason,
                importer: step_id.module_name.clone(),
                path: document_node_name.into(),
            }
            .into())
        }
    }
}

/// This implements the Pest parser for the given grammar.
#[derive(Parser)]
#[grammar = "importer/conllu/conllu.pest"]
struct CoNLLUParser;

#[cfg(test)]
mod tests;
