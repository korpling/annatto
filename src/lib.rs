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
    conllu::ImportCoNLLU, exmaralda::ImportEXMARaLDA, ptb::PtbImporter, textgrid::TextgridImporter,
    CreateEmptyCorpus, Importer,
};
use manipulator::{check::Check, map_annos::MapAnnos, merge::Merge, re::Replace, Manipulator};
use serde_derive::Deserialize;

#[derive(Deserialize)]
pub enum ExportDefinition {
    ExportGraphML(GraphMLExporter),
}

impl ToString for ExportDefinition {
    fn to_string(&self) -> String {
        match self {
            ExportDefinition::ExportGraphML(m) => m.module_name().to_string(),
        }
    }
}

impl ExportDefinition {
    // todo would it be (more) useful to actually implement deref?
    fn exporter(&self) -> &dyn Exporter {
        match self {
            ExportDefinition::ExportGraphML(m) => m,
        }
    }
}

#[derive(Deserialize)]
pub enum ImportDefinition {
    ImportCoNLLU(ImportCoNLLU),
    ImportEXMARaLDA(ImportEXMARaLDA),
    ImportPTB(PtbImporter),
    ImportTextGrid(TextgridImporter),
    InitEmptyCorpus(CreateEmptyCorpus),
}

impl ToString for ImportDefinition {
    fn to_string(&self) -> String {
        match self {
            ImportDefinition::ImportCoNLLU(m) => m.module_name().to_string(),
            ImportDefinition::ImportEXMARaLDA(m) => m.module_name().to_string(),
            ImportDefinition::ImportPTB(m) => m.module_name().to_string(),
            ImportDefinition::ImportTextGrid(m) => m.module_name().to_string(),
            ImportDefinition::InitEmptyCorpus(m) => m.module_name().to_string(),
        }
    }
}

impl ImportDefinition {
    fn importer(&self) -> &dyn Importer {
        match self {
            ImportDefinition::ImportCoNLLU(m) => m,
            ImportDefinition::ImportEXMARaLDA(m) => m,
            ImportDefinition::ImportPTB(m) => m,
            ImportDefinition::ImportTextGrid(m) => m,
            ImportDefinition::InitEmptyCorpus(m) => m,
        }
    }
}

#[derive(Deserialize)]
pub enum ProcessingDefinition {
    Check(Check),
    Map(MapAnnos),
    Merge(Merge),
    Re(Replace),
}

impl ToString for ProcessingDefinition {
    fn to_string(&self) -> String {
        match self {
            ProcessingDefinition::Check(m) => m.module_name().to_string(),
            ProcessingDefinition::Map(m) => m.module_name().to_string(),
            ProcessingDefinition::Merge(m) => m.module_name().to_string(),
            ProcessingDefinition::Re(m) => m.module_name().to_string(),
        }
    }
}

impl ProcessingDefinition {
    fn manipulator(&self) -> &dyn Manipulator {
        match self {
            ProcessingDefinition::Check(m) => m,
            ProcessingDefinition::Map(m) => m,
            ProcessingDefinition::Merge(m) => m,
            ProcessingDefinition::Re(m) => m,
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
    module: ImportDefinition,
    corpus_path: PathBuf,
}

impl Step for ImporterStep {
    fn get_step_id(&self) -> StepID {
        StepID {
            module_name: self.module.to_string(),
            path: Some(self.corpus_path.clone()),
        }
    }
}

#[derive(Deserialize)]
struct ExporterStep {
    module: ExportDefinition,
    corpus_path: PathBuf,
}

impl Step for ExporterStep {
    fn get_step_id(&self) -> StepID {
        StepID {
            module_name: self.module.to_string(),
            path: Some(self.corpus_path.clone()),
        }
    }
}

#[derive(Deserialize)]
struct ManipulatorStep {
    module: ProcessingDefinition,
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
