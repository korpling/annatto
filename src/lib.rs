pub mod error;
pub mod exporter;
pub mod importer;
pub mod legacy;
pub mod manipulator;
pub mod progress;
pub mod workflow;

use std::{
    collections::BTreeMap,
    fmt::Display,
    path::{Path, PathBuf},
};

use error::{PepperError, Result};
use exporter::Exporter;
use importer::Importer;
use manipulator::Manipulator;

/// Retrieve a new instance of an importer using its module name
pub fn importer_by_name(name: &str) -> Result<Box<dyn Importer>> {
    match name {
        "EXMARaLDAImporter" => Ok(Box::new(legacy::importer::JavaImporter::new(
            "org.corpus_tools.peppermodules.exmaralda.EXMARaLDAImporter",
            "org.corpus_tools.peppermodules.exmaralda.EXMARaLDAImporterProperties",
            "EXMARaLDAImporter",
            Some(".*\\.(exb|xml|xmi|exmaralda)$"),
        )?)),
        "GraphMLImporter" => Ok(Box::new(importer::graphml::GraphMLImporter::new())),
        "DoNothingImporter" => Ok(Box::new(importer::DoNothingImporter::new())),
        _ => Err(PepperError::NoSuchModule(name.to_string())),
    }
}

/// Retrieve a new instance of a manipulator using its module name
pub fn manipulator_by_name(name: &str) -> Result<Box<dyn Manipulator>> {
    match name {
        "DoNothingManipulator" => Ok(Box::new(manipulator::DoNothingManipulator::new())),
        _ => Err(PepperError::NoSuchModule(name.to_string())),
    }
}

/// Retrieve a new instance of an exporter using its module name
pub fn exporter_by_name(name: &str) -> Result<Box<dyn Exporter>> {
    match name {
        "GraphMLExporter" => Ok(Box::new(exporter::graphml::GraphMLExporter::new())),
        "DoNothingExporter" => Ok(Box::new(exporter::DoNothingExporter::new())),
        _ => Err(PepperError::NoSuchModule(name.to_string())),
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
