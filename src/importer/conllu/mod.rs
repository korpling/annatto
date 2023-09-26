use std::{
    collections::BTreeMap,
    io::Read,
    path::{Path, PathBuf},
};

use encoding_rs_io::DecodeReaderBytes;
use graphannis::{
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::ANNIS_NS;
use itertools::Itertools;
use pest::{
    iterators::{Pair, Pairs},
    Parser,
};
use pest_derive::Parser;
use serde_derive::Deserialize;

use crate::{
    error::AnnattoError,
    util::graphupdate::import_corpus_graph_from_files,
    workflow::{StatusMessage, StatusSender},
    Module, StepID,
};

use super::Importer;

pub const MODULE_NAME: &str = "import_conllu";

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct ImportCoNLLU {}

impl Module for ImportCoNLLU {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}

impl Importer for ImportCoNLLU {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        _step_id: StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        // TODO use ProgressReporter
        let mut update = GraphUpdate::default();
        let paths_and_node_names =
            import_corpus_graph_from_files(&mut update, input_path, &["conll", "conllu"])?;
        for (pathbuf, doc_node_name) in paths_and_node_names {
            if let Err(e) = self.import_document(&mut update, pathbuf.as_path(), doc_node_name, &tx)
            {
                if let Some(ref sender) = tx {
                    let reason = e.to_string();
                    sender.send(StatusMessage::Failed(AnnattoError::Import {
                        reason,
                        importer: self.module_name().to_string(),
                        path: input_path.to_path_buf(),
                    }))?;
                }
            }
        }
        Ok(update)
    }
}

impl ToString for Rule {
    fn to_string(&self) -> String {
        match self {
            Rule::lemma => "lemma".to_string(),
            Rule::upos => "upos".to_string(),
            Rule::xpos => "xpos".to_string(),
            Rule::deprel => "deprel".to_string(),
            Rule::enhanced_rel => "rel".to_string(),
            _ => "".to_string(),
        }
    }
}

type DepSpec = (usize, Option<String>);

impl ImportCoNLLU {
    fn import_document(
        &self,
        update: &mut GraphUpdate,
        document_path: &Path,
        document_node_name: String,
        tx: &Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let f = std::fs::File::open(document_path)?;
        let mut decoder = DecodeReaderBytes::new(f);
        let mut file_content = String::new();
        decoder.read_to_string(&mut file_content)?; // TODO this needs to be buffered. UD Files can be very large
        let conllu: Pairs<Rule> = CoNLLUParser::parse(Rule::conll, &file_content)?;
        self.map_document(update, document_node_name, conllu, tx)?;
        Ok(())
    }

    fn map_document(
        &self,
        update: &mut GraphUpdate,
        document_node_name: String,
        mut conllu: Pairs<Rule>,
        tx: &Option<StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut token_names = Vec::new();
        if let Some(pair) = conllu.next() {
            if pair.as_rule() == Rule::conll {
                for sentence in pair.into_inner() {
                    // iterate over sentences
                    if sentence.as_rule() == Rule::sentence {
                        token_names.extend(self.map_sentence(
                            update,
                            document_node_name.as_str(),
                            sentence,
                            tx,
                        )?);
                    }
                }
            }
        } else if let Some(sender) = tx {
            let msg = format!("Could not parse file as conllu: {document_node_name}");
            let err = AnnattoError::Import {
                reason: msg,
                importer: self.module_name().to_string(),
                path: PathBuf::from(document_node_name),
            };
            sender.send(crate::workflow::StatusMessage::Failed(err))?;
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
        update: &mut GraphUpdate,
        document_node_name: &str,
        sentence: Pair<Rule>,
        tx: &Option<StatusSender>,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut id_to_tok_name = BTreeMap::new();
        let mut dependencies = Vec::new();
        let mut s_annos = Vec::new();
        let (l, c) = sentence.line_col();
        for member in sentence.into_inner() {
            match member.as_rule() {
                Rule::token => {
                    let (tok_name, tok_id, dep) =
                        self.map_token(update, document_node_name, member, tx)?;
                    id_to_tok_name.insert(tok_id, tok_name.to_string());
                    if let Some(dependency) = dep {
                        dependencies.push((tok_name, dependency.0, dependency.1));
                    }
                }
                Rule::multi_token | Rule::invalid_multi_token => {
                    // todo future work if needed
                }
                Rule::empty_node | Rule::invalid_empty_node => {
                    // todo future work if needed
                }
                Rule::s_anno => {
                    let mut name = None;
                    let mut value = None;
                    for name_or_s_value in member.into_inner() {
                        match name_or_s_value.as_rule() {
                            Rule::name => {
                                name = Some(name_or_s_value.as_str().trim().to_string());
                            }
                            Rule::s_value => {
                                value = Some(name_or_s_value.as_str().trim().to_string())
                            }
                            _ => {}
                        }
                        if let Some(ref n) = name {
                            if let Some(ref v) = value {
                                s_annos.push((n.to_string(), v.to_string()));
                                break;
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        if !id_to_tok_name.is_empty() {
            let node_name = format!("{document_node_name}#s{l}_{c}");
            update.add_event(UpdateEvent::AddNode {
                node_name: node_name.to_string(),
                node_type: "node".to_string(),
            })?;
            for (anno_name, anno_value) in s_annos {
                update.add_event(UpdateEvent::AddNodeLabel {
                    node_name: node_name.to_string(),
                    anno_ns: "".to_string(),
                    anno_name: anno_name.to_string(),
                    anno_value: anno_value.to_string(),
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
            for (target_node_name, head_id, deprel) in dependencies {
                if head_id > 0 {
                    if let Some(source_node_name) = id_to_tok_name.get(&head_id) {
                        update.add_event(UpdateEvent::AddEdge {
                            source_node: source_node_name.to_string(),
                            target_node: target_node_name.to_string(),
                            layer: "".to_string(),
                            component_type: AnnotationComponentType::Pointing.to_string(),
                            component_name: "dep".to_string(),
                        })?;
                        if let Some(deprel_value) = deprel {
                            update.add_event(UpdateEvent::AddEdgeLabel {
                                source_node: source_node_name.to_string(),
                                target_node: target_node_name.to_string(),
                                layer: "".to_string(),
                                component_type: AnnotationComponentType::Pointing.to_string(),
                                component_name: "dep".to_string(),
                                anno_ns: "".to_string(),
                                anno_name: "deprel".to_string(),
                                anno_value: deprel_value.to_string(),
                            })?;
                        }
                    } else if let Some(sender) = tx {
                        let msg =
                            format!("Failed to build dependency tree: Unknown head id `{head_id}` ({l}, {c})");
                        let err = AnnattoError::Import {
                            reason: msg,
                            importer: self.module_name().to_string(),
                            path: Path::new(document_node_name).to_path_buf(),
                        };
                        sender.send(StatusMessage::Failed(err))?;
                    }
                }
            }
        }
        Ok(id_to_tok_name.into_iter().map(|e| e.1).collect_vec())
    }

    fn map_token(
        &self,
        update: &mut GraphUpdate,
        document_node_name: &str,
        token: Pair<Rule>,
        _tx: &Option<StatusSender>,
    ) -> Result<(String, usize, Option<DepSpec>), Box<dyn std::error::Error>> {
        let (l, c) = token.line_col();
        let line = token.as_str().to_string();
        let node_name = format!("{document_node_name}#t{l}_{c}");
        update.add_event(UpdateEvent::AddNode {
            node_name: node_name.to_string(),
            node_type: "node".to_string(),
        })?;
        update.add_event(UpdateEvent::AddNodeLabel {
            node_name: node_name.to_string(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "layer".to_string(),
            anno_value: "default_layer".to_string(),
        })?;
        let mut token_id = None;
        let mut head_id = None;
        let mut deprel = None;
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
                                if anno_name.is_some() && anno_value.is_some() {
                                    update.add_event(UpdateEvent::AddNodeLabel {
                                        node_name: node_name.to_string(),
                                        anno_ns: "".to_string(),
                                        anno_name: anno_name.unwrap().trim().to_string(),
                                        anno_value: anno_value.unwrap().trim().to_string(),
                                    })?;
                                    break;
                                }
                            }
                        } else if feature.as_rule() == Rule::no_value {
                            break;
                        }
                    }
                }
                Rule::head => {
                    for id_or_else in member.into_inner() {
                        if id_or_else.as_rule() == Rule::id {
                            head_id = Some(id_or_else.as_str().trim().parse::<usize>()?);
                            break;
                        }
                    }
                }
                Rule::deprel => {
                    deprel = Some(member.as_str().trim().to_string());
                }
                Rule::enhanced_deps => {}
                _ => {}
            }
        }
        let dependency = head_id.map(|v| (v, deprel));
        if let Some(id) = token_id {
            Ok((node_name, id, dependency))
        } else {
            // by grammar spec this branch should never be possible
            let reason = format!("Token `{line}` ({l}, {c}) has no id which is invalid.");
            Err(Box::new(AnnattoError::Import {
                reason,
                importer: self.module_name().to_string(),
                path: document_node_name.into(),
            }))
        }
    }
}

#[derive(Parser)]
#[grammar = "importer/conllu/conllu.pest"]
struct CoNLLUParser;

#[cfg(test)]
mod tests;
