//! # Introduction to using annatto
//!
//! ## Command line
//!
//! The main usage of annatto is through the command line interface. Run
//! ```bash
//! annatto --help
//! ```
//!
//! to get more help on the sub-commands.
//! The most important command is `annatto run <workflow-file>`, which runs all the modules as defined in the given [workflow] file.
//!
//! ## Modules
//!
//! Annatto comes with a number of modules, which have different types:
//!
//! [**Importer**](importer) modules allow importing files from different formats.
//! More than one importer can be used in a workflow, but then the corpus data needs
//! to be merged using one of the merger manipulators.
//! When running a workflow, the importers are executed first and in parallel.
//!   
//!
//!
//! [**Graph operation**](manipulator) modules change the imported corpus data.
//! They are executed one after another (non-parallel) and in the order they have been defined in the workflow.
//!
//! [**Exporter**](exporter) modules export the data into different formats.
//! More than one exporter can be used in a workflow.
//! When running a workflow, the exporters are executed last and in parallel.
//!

pub mod error;
pub mod exporter;
pub mod importer;
pub mod manipulator;
pub mod models;
pub mod progress;
pub mod runtime;
pub mod util;
pub mod workflow;

use std::{
    fmt::Display,
    path::{Path, PathBuf},
};

use error::Result;
use exporter::{
    exmaralda::ExportExmaralda, graphml::GraphMLExporter, xlsx::XlsxExporter, Exporter,
};
use importer::{
    conllu::ImportCoNLLU, exmaralda::ImportEXMARaLDA, file_nodes::CreateFileNodes,
    graphml::GraphMLImporter, meta::AnnotateCorpus, none::CreateEmptyCorpus, opus::ImportOpusLinks,
    ptb::PtbImporter, textgrid::TextgridImporter, treetagger::TreeTaggerImporter,
    xlsx::ImportSpreadsheet, xml::ImportXML, Importer,
};
use manipulator::{
    check::Check, collapse::Collapse, enumerate::EnumerateMatches, link::LinkNodes, map::MapAnnos,
    merge::Merge, no_op::NoOp, re::Revise, Manipulator,
};
use serde_derive::Deserialize;

#[derive(Deserialize)]
#[serde(tag = "format", rename_all = "lowercase", content = "config")]
pub enum WriteAs {
    GraphML(#[serde(default)] GraphMLExporter), // the purpose of serde(default) here is, that an empty `[export.config]` table can be omited
    EXMARaLDA(#[serde(default)] ExportExmaralda),
    Xlsx(#[serde(default)] XlsxExporter),
}

impl Default for WriteAs {
    // the purpose of this default is to allow to omit `format` in an `[[export]]` table
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
            WriteAs::EXMARaLDA(m) => m,
            WriteAs::Xlsx(m) => m,
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "format", rename_all = "lowercase", content = "config")]
pub enum ReadFrom {
    CoNLLU(#[serde(default)] ImportCoNLLU),
    EXMARaLDA(#[serde(default)] ImportEXMARaLDA),
    GraphML(#[serde(default)] GraphMLImporter),
    Meta(#[serde(default)] AnnotateCorpus),
    None(#[serde(default)] CreateEmptyCorpus),
    Opus(#[serde(default)] ImportOpusLinks),
    Path(#[serde(default)] CreateFileNodes),
    PTB(#[serde(default)] PtbImporter),
    TextGrid(#[serde(default)] TextgridImporter),
    TreeTagger(#[serde(default)] TreeTaggerImporter),
    Xlsx(#[serde(default)] ImportSpreadsheet),
    Xml(ImportXML),
}

impl Default for ReadFrom {
    // the purpose of this default is to allow to omit `format` in an `[[import]]` table
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
            ReadFrom::TreeTagger(m) => m,
            ReadFrom::None(m) => m,
            ReadFrom::Meta(m) => m,
            ReadFrom::Xlsx(m) => m,
            ReadFrom::GraphML(m) => m,
            ReadFrom::Path(m) => m,
            ReadFrom::Xml(m) => m,
            ReadFrom::Opus(m) => m,
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "lowercase", content = "config")]
pub enum GraphOp {
    Check(Check),       // no default, has a (required) path attribute
    Collapse(Collapse), // no default, there is no such thing as a default component
    Enumerate(#[serde(default)] EnumerateMatches),
    Link(LinkNodes),                  // no default, has required attributes
    Map(MapAnnos),                    // no default, has a (required) path attribute
    Merge(Merge),                     // no default, has required attributes
    Revise(#[serde(default)] Revise), // does nothing on default
    None(#[serde(default)] NoOp),     // has no attributes
}

impl Default for GraphOp {
    // the purpose of this default is to allow to omit `format` in an `[[graph_op]]` table
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
            GraphOp::Collapse(m) => m,
            GraphOp::Link(m) => m,
            GraphOp::Map(m) => m,
            GraphOp::Merge(m) => m,
            GraphOp::Revise(m) => m,
            GraphOp::None(m) => m,
            GraphOp::Enumerate(m) => m,
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
            write!(f, "{} ({})", self.module_name, path.to_string_lossy())
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
pub struct ImporterStep {
    #[serde(flatten)]
    module: ReadFrom,
    path: PathBuf,
}

impl Step for ImporterStep {
    fn get_step_id(&self) -> StepID {
        StepID {
            module_name: self.module.to_string(),
            path: Some(self.path.clone()),
        }
    }
}

#[derive(Deserialize)]
pub struct ExporterStep {
    #[serde(flatten)]
    module: WriteAs,
    path: PathBuf,
}

impl Step for ExporterStep {
    fn get_step_id(&self) -> StepID {
        StepID {
            module_name: self.module.to_string(),
            path: Some(self.path.clone()),
        }
    }
}

#[derive(Deserialize)]
pub struct ManipulatorStep {
    #[serde(flatten)]
    module: GraphOp,
    workflow_directory: Option<PathBuf>,
}

impl Step for ManipulatorStep {
    fn get_step_id(&self) -> StepID {
        StepID {
            module_name: self.module.to_string(),
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
