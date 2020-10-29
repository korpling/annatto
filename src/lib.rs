use std::{collections::BTreeMap, path::PathBuf};

use exporter::Exporter;
use importer::Importer;
use manipulator::Manipulator;

pub mod donothing;
pub mod error;
pub mod exporter;
pub mod importer;
pub mod manipulator;
pub mod workflow;

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
pub struct StepID {
    pub module_name: String,
    pub path: Option<PathBuf>,
}

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
            module_name: self.module.module_name(),
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
            module_name: self.module.module_name(),
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
            module_name: self.module.module_name(),
            path: None,
        }
    }
}

pub trait Module: Sync {
    fn module_name(&self) -> String;
}
