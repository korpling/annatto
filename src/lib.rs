use std::{collections::BTreeMap, fmt::Display, path::{Path, PathBuf}};

use exporter::Exporter;
use importer::Importer;
use manipulator::Manipulator;

pub mod error;
pub mod exporter;
pub mod importer;
pub mod manipulator;
pub mod modules;
pub mod workflow;

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct StepID {
    pub module_name: String,
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

pub trait Module: Sync {
    fn module_name(&self) -> &str;

    fn step_id(&self, path: Option<&Path>) -> StepID {
        StepID {
            module_name: self.module_name().to_string(),
            path: path.map(|p| p.to_path_buf()),
        }
    }
}
