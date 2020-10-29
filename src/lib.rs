use std::{collections::BTreeMap, convert::TryFrom, convert::TryInto, path::PathBuf};

use error::PepperError;
use exporter::Exporter;
use importer::Importer;
use manipulator::Manipulator;

pub mod donothing;
pub mod error;
pub mod exporter;
pub mod importer;
pub mod manipulator;
pub mod workflow;

#[derive(Debug)]
pub enum StepDesc {
    Importer {
        module_name: String,
        corpus_path: PathBuf,
        properties: BTreeMap<String, String>,
    },
    Exporter {
        module_name: String,
        corpus_path: PathBuf,
        properties: BTreeMap<String, String>,
    },
    Manipulator {
        module_name: String,
        properties: BTreeMap<String, String>,
    },
}

struct ImporterStep {
    module: Box<dyn Importer>,
    corpus_path: PathBuf,
    properties: BTreeMap<String, String>,
}

struct ExporterStep {
    module: Box<dyn Exporter>,
    corpus_path: PathBuf,
    properties: BTreeMap<String, String>,
}

struct ManipulatorStep {
    module: Box<dyn Manipulator>,
    properties: BTreeMap<String, String>,
}

use crate::donothing::*;

fn importer_by_name(name: String) -> Box<dyn Importer> {
    Box::new(DoNothingImporter::new()) // dummy impl
}

fn manipulator_by_name(name: String) -> Box<dyn Manipulator> {
    Box::new(DoNothingManipulator::new()) // dummy impl
}

fn exporter_by_name(name: String) -> Box<dyn Exporter> {
    Box::new(DoNothingExporter::new()) // dummy impl
}

impl TryFrom<&StepDesc> for ImporterStep {
    type Error = PepperError;

    fn try_from(value: &StepDesc) -> Result<Self, Self::Error> {
        match value {
            StepDesc::Importer {
                module_name,
                corpus_path,
                properties,
            } => {
                let module = ImporterStep {
                    corpus_path: corpus_path.clone(),
                    properties: properties.clone(),
                    module: importer_by_name(module_name.to_string()),
                };
                Ok(module)
            }
            _ => Err(PepperError::WrongStepType {
                expected: "Importer".to_string(),
            }),
        }
    }
}

impl TryFrom<&StepDesc> for ExporterStep {
    type Error = PepperError;

    fn try_from(value: &StepDesc) -> Result<Self, Self::Error> {
        match value {
            StepDesc::Exporter {
                module_name,
                corpus_path,
                properties,
            } => {
                let module = ExporterStep {
                    corpus_path: corpus_path.clone(),
                    properties: properties.clone(),
                    module: exporter_by_name(module_name.to_string()),
                };
                Ok(module)
            }
            _ => Err(PepperError::WrongStepType {
                expected: "Exporter".to_string(),
            }),
        }
    }
}

impl TryFrom<&StepDesc> for ManipulatorStep {
    type Error = PepperError;

    fn try_from(value: &StepDesc) -> Result<Self, Self::Error> {
        match value {
            StepDesc::Manipulator {
                module_name,
                properties,
            } => {
                let module = ManipulatorStep {
                    properties: properties.clone(),
                    module: manipulator_by_name(module_name.to_string()),
                };
                Ok(module)
            }
            _ => Err(PepperError::WrongStepType {
                expected: "Manipulator".to_string(),
            }),
        }
    }
}

pub trait Module: Sync {
    fn module_name(&self) -> String;
}
