use std::{collections::BTreeSet, fs, path::Path};

use anyhow::anyhow;
use facet::Facet;
use graphannis::{AnnotationGraph, update::GraphUpdate};
use pest::{Parser, iterators::Pairs};
use pest_derive::Parser;
use serde::{Deserialize, Serialize};

use crate::{error::AnnattoError, importer::Importer};

/// Import annotations provided in the fieldlinguist's toolbox text format.
#[derive(Facet, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FLToolbox {
    /// This attribute sets the annotation layer, that other annotations will point to.
    /// This needs to be set to avoid an invalid model.
    target: String,
    /// The annotation names named here are considered single-valued per line. Space values
    /// are not considered delimiters, but part of the annotation value. Such annotations
    /// rely on the existence of the target nodes, i. e. annotation lines without any other
    /// non-spanning annotation in the block will be dropped.
    #[serde(default)]
    span: BTreeSet<String>,
    /// Null values are represented as `-` in toolbox. If you want those to remain explicit
    /// annotations, set `explicit_null = true`.
    #[serde(default)]
    explicit_null: bool,
}

impl Importer for FLToolbox {
    fn import_corpus(
        &self,
        input_path: &Path,
        step_id: crate::StepID,
        config: super::GenericImportConfiguration,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let named_paths = config.derive_corpus_graph(input_path, &mut update)?;

        Ok(update)
    }

    fn default_file_extensions(&self) -> &[&str] {
        &FLToolbox::DEFAULT_FILE_EXTENSIONS
    }
}

impl FLToolbox {
    pub(crate) const DEFAULT_FILE_EXTENSIONS: [&str; 1] = ["txt"];

    fn import_document(
        &self,
        path: &Path,
        doc_node_name: &str,
        update: &mut GraphUpdate,
    ) -> crate::error::Result<()> {
        let content = fs::read_to_string(path)?;
        let parsed_data = FLTBParser::parse(Rule::data, &content).map_err(|e| {
            AnnattoError::PestParsingError {
                file_path: path.to_path_buf(),
                error: Box::new(e),
            }
        })?;

        Ok(())
    }

    fn map_data(
        &self,
        data: Pairs<Rule>,
        graph: &AnnotationGraph,
        update: &mut GraphUpdate,
    ) -> crate::error::Result<()> {
        data.into_iter()
            .try_for_each(|b| self.map_block(b.into_inner(), graph, update))
    }

    fn map_block(
        &self,
        block: Pairs<Rule>,
        graph: &AnnotationGraph,
        update: &mut GraphUpdate,
    ) -> crate::error::Result<()> {
        for line in block.into_iter() {}
        Ok(())
    }

    fn map_line(
        &self,
        mut line: Pairs<Rule>,
        graph: &AnnotationGraph,
        update: &mut GraphUpdate,
    ) -> crate::error::Result<ByteGridLine> {
        let marker = line
            .next()
            .ok_or(anyhow!("Line has no marker: `{}`", line.as_str()))?
            .to_string();
        let line_data = line
            .next()
            .ok_or(anyhow!("Line has no data: `{}`", line.as_str()))?
            .as_str();

        Ok(ByteGridLine {
            marker,
            slots: BTreeSet::default(),
            bytes: vec![],
        })
    }
}

struct ByteGridLine {
    marker: String,
    slots: BTreeSet<u8>, // this is a set of indices, NOT bytes (it's just lines are never longer than 256)
    bytes: Vec<Vec<u8>>,
}

type ByteGrid = Vec<ByteGridLine>;

#[derive(Parser)]
#[grammar = "importer/fltb/minimal_grammar.pest"]
struct FLTBParser;
