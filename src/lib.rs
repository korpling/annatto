#[cfg(feature = "embed-documentation")]
pub mod documentation_server;
pub mod error;
pub mod exporter;
pub mod importer;
pub mod manipulator;
pub mod models;
pub mod progress;
pub mod util;
pub mod workflow;

use std::{
    fmt::Display,
    path::{Path, PathBuf},
};

use error::Result;
use exporter::{graphml::GraphMLExporter, Exporter};
use importer::{
    conllu::ImportCoNLLU, corpus_annotations::AnnotateCorpus, exmaralda::ImportEXMARaLDA,
    graphml::GraphMLImporter, ptb::PtbImporter, spreadsheet::ImportSpreadsheet,
    textgrid::TextgridImporter, CreateEmptyCorpus, Importer,
};
use manipulator::{
    check::Check, map_annos::MapAnnos, merge::Merge, no_op::NoOp, re::Replace, Manipulator,
};
use serde_derive::Deserialize;

#[derive(Deserialize)]
#[serde(tag = "format", rename_all = "lowercase")]
pub enum WriteAs {
    GraphML(GraphMLExporter),
}

impl Default for WriteAs {
    fn default() -> Self {
        WriteAs::GraphML(GraphMLExporter::default())
    }
}

impl ToString for WriteAs {
    fn to_string(&self) -> String {
        self.writer().module_name().to_string()
    }
}

impl WriteAs {
    fn writer(&self) -> &dyn Exporter {
        match self {
            WriteAs::GraphML(m) => m,
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "format", rename_all = "lowercase")]
pub enum ReadFrom {
    CoNLLU(ImportCoNLLU),
    EXMARaLDA(ImportEXMARaLDA),
    GraphML(GraphMLImporter),
    Meta(AnnotateCorpus),
    None(CreateEmptyCorpus),
    PTB(PtbImporter),
    TextGrid(TextgridImporter),
    Xlsx(ImportSpreadsheet),
}

impl Default for ReadFrom {
    fn default() -> Self {
        ReadFrom::None(CreateEmptyCorpus::default())
    }
}

impl ToString for ReadFrom {
    fn to_string(&self) -> String {
        self.reader().module_name().to_string()
    }
}

impl ReadFrom {
    fn reader(&self) -> &dyn Importer {
        match self {
            ReadFrom::CoNLLU(m) => m,
            ReadFrom::EXMARaLDA(m) => m,
            ReadFrom::PTB(m) => m,
            ReadFrom::TextGrid(m) => m,
            ReadFrom::None(m) => m,
            ReadFrom::Meta(m) => m,
            ReadFrom::Xlsx(m) => m,
            ReadFrom::GraphML(m) => m,
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "lowercase")]
pub enum GraphOp {
    Check(Check),
    Map(MapAnnos),
    Merge(Merge),
    Re(Replace),
    None(NoOp),
}

impl Default for GraphOp {
    fn default() -> Self {
        GraphOp::None(NoOp::default())
    }
}

impl ToString for GraphOp {
    fn to_string(&self) -> String {
        self.processor().module_name().to_string()
    }
}

impl GraphOp {
    fn processor(&self) -> &dyn Manipulator {
        match self {
            GraphOp::Check(m) => m,
            GraphOp::Map(m) => m,
            GraphOp::Merge(m) => m,
            GraphOp::Re(m) => m,
            GraphOp::None(m) => m,
        }
    }
}

/// Unique ID of a single step in the conversion pipeline.
#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct StepID {
    /// The name of the module used in this step.
    pub module_name: String,
    /// The path (input or output) used in this step.
    pub path: Option<PathBuf>,
}

impl Display for StepID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(path) = &self.path {
            write!(f, "{} [{}]", self.module_name, path.to_string_lossy())
        } else {
            write!(f, "{}", self.module_name)
        }
    }
}

/// Represents a single step in a conversion pipeline.
pub trait Step {
    fn get_step_id(&self) -> StepID;
}

#[derive(Deserialize)]
struct ImporterStep {
    #[serde(flatten)]
    config: ReadFrom,
    path: PathBuf,
}

impl Step for ImporterStep {
    fn get_step_id(&self) -> StepID {
        StepID {
            module_name: self.config.to_string(),
            path: Some(self.path.clone()),
        }
    }
}

#[derive(Deserialize)]
struct ExporterStep {
    #[serde(flatten)]
    config: WriteAs,
    path: PathBuf,
}

impl Step for ExporterStep {
    fn get_step_id(&self) -> StepID {
        StepID {
            module_name: self.config.to_string(),
            path: Some(self.path.clone()),
        }
    }
}

#[derive(Deserialize)]
struct ManipulatorStep {
    #[serde(flatten)]
    config: GraphOp,
    workflow_directory: Option<PathBuf>,
}

impl Step for ManipulatorStep {
    fn get_step_id(&self) -> StepID {
        StepID {
            module_name: self.config.to_string(),
            path: None,
        }
    }
}

/// A module that can be used in the conversion pipeline.
pub trait Module: Sync {
    /// Get the name of the module as string.
    fn module_name(&self) -> &str;

    /// Return the ID of the module when used with the given specific path.
    fn step_id(&self, path: Option<&Path>) -> StepID {
        StepID {
            module_name: self.module_name().to_string(),
            path: path.map(|p| p.to_path_buf()),
        }
    }
}
