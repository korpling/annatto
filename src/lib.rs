pub mod error;
pub mod exporter;
pub mod importer;
pub mod manipulator;
pub mod models;
pub mod progress;
pub mod util;
pub mod workflow;

use std::{
    collections::BTreeMap,
    fmt::Display,
    path::{Path, PathBuf},
};

use error::{AnnattoError, Result};
use exporter::Exporter;
use importer::Importer;
use manipulator::Manipulator;

/// Retrieve a new instance of an importer using its module name
pub fn importer_by_name(name: &str) -> Result<Box<dyn Importer>> {
    match name {
        importer::graphml::MODULE_NAME => Ok(Box::<importer::graphml::GraphMLImporter>::default()),
        importer::CREATE_EMPTY_CORPUS_MODULE_NAME => {
            Ok(Box::<importer::CreateEmptyCorpus>::default())
        }
        importer::textgrid::MODULE_NAME => {
            Ok(Box::<importer::textgrid::TextgridImporter>::default())
        }
        importer::ptb::MODULE_NAME => Ok(Box::<importer::ptb::PtbImporter>::default()),
        importer::corpus_annotations::MODULE_NAME => {
            Ok(Box::<importer::corpus_annotations::AnnotateCorpus>::default())
        }
        importer::spreadsheet::MODULE_NAME => {
            Ok(Box::<importer::spreadsheet::ImportSpreadsheet>::default())
        }
        _ => Err(AnnattoError::NoSuchModule(name.to_string())),
    }
}

/// Retrieve a new instance of a manipulator using its module name
pub fn manipulator_by_name(name: &str) -> Result<Box<dyn Manipulator>> {
    match name {
        manipulator::merge::MODULE_NAME => Ok(Box::<manipulator::merge::Merge>::default()),
        manipulator::re::MODULE_NAME => Ok(Box::<manipulator::re::Replace>::default()),
        manipulator::check::MODULE_NAME => Ok(Box::<manipulator::check::Check>::default()),
        _ => Err(AnnattoError::NoSuchModule(name.to_string())),
    }
}

/// Retrieve a new instance of an exporter using its module name
pub fn exporter_by_name(name: &str) -> Result<Box<dyn Exporter>> {
    match name {
        exporter::graphml::MODULE_NAME => Ok(Box::<exporter::graphml::GraphMLExporter>::default()),
        _ => Err(AnnattoError::NoSuchModule(name.to_string())),
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

struct ImporterStep {
    module: Box<dyn Importer>,
    corpus_path: PathBuf,
    leak_path: Option<PathBuf>,
    properties: BTreeMap<String, String>,
}

impl Step for ImporterStep {
    fn get_step_id(&self) -> StepID {
        StepID {
            module_name: self.module.module_name().to_string(),
            path: Some(self.corpus_path.clone()),
        }
    }
}

struct ExporterStep {
    module: Box<dyn Exporter>,
    corpus_path: PathBuf,
    properties: BTreeMap<String, String>,
}

impl Step for ExporterStep {
    fn get_step_id(&self) -> StepID {
        StepID {
            module_name: self.module.module_name().to_string(),
            path: Some(self.corpus_path.clone()),
        }
    }
}

struct ManipulatorStep {
    module: Box<dyn Manipulator>,
    properties: BTreeMap<String, String>,
    workflow_directory: Option<PathBuf>,
}

impl Step for ManipulatorStep {
    fn get_step_id(&self) -> StepID {
        StepID {
            module_name: self.module.module_name().to_string(),
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
