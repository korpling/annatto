use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
};

use graphannis::AnnotationGraph;

use crate::{
    error::PepperError, error::Result, exporter::Exporter, importer::Importer,
    manipulator::Manipulator,
};

struct ImporterDesc {
    module: Box<dyn Importer>,
    corpus_path: PathBuf,
    properties: HashMap<String, String>,
}

struct ExporterDesc {
    module: Box<dyn Exporter>,
    corpus_path: PathBuf,
    properties: HashMap<String, String>,
}

struct ManipulatorDesc {
    module: Box<dyn Manipulator>,
    properties: HashMap<String, String>,
}

pub struct Workflow {
    importer: Vec<ImporterDesc>,
    manipulator: Vec<ManipulatorDesc>,
    exporter: Vec<Box<ExporterDesc>>,
}

impl From<File> for Workflow {
    fn from(_: File) -> Self {
        todo!("Implement parsing a file into workflow description")
    }
}

pub fn execute_from_file(workflow_file: &Path) -> Result<()> {
    let f = File::open(workflow_file).map_err(|reason| PepperError::OpenWorkflowFile {
        reason,
        file: workflow_file.to_path_buf(),
    })?;

    execute(f.into())
}

pub fn execute(workflow: Workflow) -> Result<()> {
    // Create a new empty annotation graph
    let mut g = AnnotationGraph::new(true).map_err(|e| PepperError::CreateGraph(e.into()))?;

    // Execute all importers
    for importer in workflow.importer.iter() {
        importer
            .module
            .import_corpus(&importer.corpus_path, &importer.properties)
            .map_err(|reason| PepperError::Import {
                reason,
                importer: importer.module.module_name(),
                path: importer.corpus_path.clone(),
            })?;
    }

    // Execute all manipulators
    for desc in workflow.manipulator.iter() {
        desc.module
            .manipulate_corpus(&mut g, &desc.properties)
            .map_err(|reason| PepperError::Manipulator {
                reason,
                manipulator: desc.module.module_name(),
            })?;
    }

    // Execute all exporters
    for desc in workflow.exporter.iter() {
        desc.module
            .export_corpus(&g, &desc.properties, &desc.corpus_path)
            .map_err(|reason| PepperError::Export {
                reason,
                exporter: desc.module.module_name(),
                path: desc.corpus_path.clone(),
            })?;
    }

    Ok(())
}
