use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
    sync::mpsc::Sender,
};

use graphannis::{update::GraphUpdate, AnnotationGraph};

use crate::{
    error::PepperError, error::Result, exporter::Exporter, importer::Importer,
    manipulator::Manipulator,
};
use rayon::prelude::*;

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

/// Status updates are send as single messages when the workflow is executed.
#[derive(Debug)]
pub enum StatusMessage {
    /// An informing message
    Info(String),
    /// A warning message
    Warning(String),
    /// Progress report, 100 percent is 1.0
    Progress(f32),
}

pub struct Workflow {
    importer: Vec<ImporterDesc>,
    manipulator: Vec<ManipulatorDesc>,
    exporter: Vec<ExporterDesc>,
}

impl From<File> for Workflow {
    fn from(_: File) -> Self {
        todo!("Implement parsing a file into workflow description")
    }
}

pub fn execute_from_file(workflow_file: &Path, tx: Option<Sender<StatusMessage>>) -> Result<()> {
    let f = File::open(workflow_file).map_err(|reason| PepperError::OpenWorkflowFile {
        reason,
        file: workflow_file.to_path_buf(),
    })?;

    let workflow: Workflow = f.into();
    workflow.execute(tx)
}

impl Workflow {
    pub fn execute(&self, tx: Option<Sender<StatusMessage>>) -> Result<()> {
        // Create a new empty annotation graph
        let mut g =
            AnnotationGraph::new(true).map_err(|e| PepperError::CreateGraph(e.to_string()))?;

        // Execute all importers and store their graph updates in parallel
        let updates: Result<Vec<GraphUpdate>> = self
            .importer
            .par_iter()
            .map(|desc| self.execute_single_importer(desc))
            .collect();
        // Apply each graph update
        for mut u in updates? {
            g.apply_update(&mut u, |_msg| {})
                .map_err(|reason| PepperError::UpdateGraph(reason.to_string()))?;
        }
        self.send_progress(0.3, &tx)?;

        // Execute all manipulators in sequence
        for desc in self.manipulator.iter() {
            desc.module
                .manipulate_corpus(&mut g, &desc.properties)
                .map_err(|reason| PepperError::Manipulator {
                    reason: reason.to_string(),
                    manipulator: desc.module.module_name(),
                })?;
        }
        self.send_progress(0.6, &tx)?;

        // Execute all exporters in parallel
        let export_result: Result<Vec<_>> = self
            .exporter
            .par_iter()
            .map(|desc| self.execute_single_exporter(&g, desc))
            .collect();
        self.send_progress(1.0, &tx)?;
        // Check for errors during export
        export_result?;
        Ok(())
    }

    fn send_progress(&self, progress: f32, tx: &Option<Sender<StatusMessage>>) -> Result<()> {
        // TODO: calculate progress based on the sum of module-specific progress updates
        if let Some(tx) = tx.as_ref() {
            tx.send(StatusMessage::Progress(progress))?;
        }
        Ok(())
    }

    fn execute_single_importer(&self, desc: &ImporterDesc) -> Result<GraphUpdate> {
        desc.module
            .import_corpus(&desc.corpus_path, &desc.properties)
            .map_err(|reason| PepperError::Import {
                reason: reason.to_string(),
                importer: desc.module.module_name(),
                path: desc.corpus_path.to_path_buf(),
            })
    }

    fn execute_single_exporter(&self, g: &AnnotationGraph, desc: &ExporterDesc) -> Result<()> {
        desc.module
            .export_corpus(&g, &desc.properties, &desc.corpus_path)
            .map_err(|reason| PepperError::Export {
                reason: reason.to_string(),
                exporter: desc.module.module_name(),
                path: desc.corpus_path.clone(),
            })?;
        Ok(())
    }
}
