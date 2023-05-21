use std::{
    fs,
    path::{Path, PathBuf},
    sync::mpsc::Sender,
};

use graphannis::{update::GraphUpdate, AnnotationGraph};
use serde_derive::Deserialize;

use crate::{
    error::AnnattoError, error::Result, ExporterStep, ImporterStep, ManipulatorStep, Step, StepID,
};
use rayon::prelude::*;

/// Status updates are send as single messages when the workflow is executed.
#[derive(Debug)]
pub enum StatusMessage {
    /// Sent at the beginning when the workflow is parsed and before the pipeline steps are executed.
    StepsCreated(Vec<StepID>),
    /// An informing message.
    Info(String),
    /// A warning message.
    Warning(String),
    /// Progress report for a single conversion step.
    Progress {
        /// Determines which step the progress is reported for.
        id: StepID,
        /// Estimated total needed steps to complete conversion
        total_work: usize,
        /// Number of steps finished. Should never be larger than `total_work`.
        finished_work: usize,
    },
    /// Indicates a step has finished.
    StepDone { id: StepID },
    /// Send when some error occurred in the pipeline. Any error will stop the conversion.
    Failed(AnnattoError),
}

/// A workflow describes the steps in the conversion pipeline process. It can be represented as XML file.
///
/// First , all importers are executed in parallel. Then their output are appended to create a single annotation graph.
/// The manipulators are executed in their defined sequence and can change the annotation graph.
/// Last, all exporters are called with the now read-only annotation graph in parallel.
#[derive(Deserialize)]
pub struct Workflow {
    importer: Vec<ImporterStep>,
    manipulator: Vec<ManipulatorStep>,
    exporter: Vec<ExporterStep>,
}

use std::convert::TryFrom;
use toml;

impl TryFrom<PathBuf> for Workflow {
    type Error = AnnattoError;
    fn try_from(workflow_file: PathBuf) -> Result<Workflow> {
        let toml_content = fs::read_to_string(workflow_file.as_path())?;
        let workflow: Workflow = toml::from_str(toml_content.as_str())?;
        Ok(workflow)
    }
}

/// Executes a workflow from a TOML file.
///
/// * `workflow_file` - The TOML workflow file.
/// * `tx` - If supported by the caller, this is a sender object that allows to send [status updates](enum.StatusMessage.html) (like information messages, warnings and module progress) to the calling entity.
pub fn execute_from_file(workflow_file: &Path, tx: Option<Sender<StatusMessage>>) -> Result<()> {
    let wf = Workflow::try_from(workflow_file.to_path_buf())?;
    wf.execute(tx)?;
    Ok(())
}

pub type StatusSender = Sender<StatusMessage>;

impl Workflow {
    pub fn execute(&self, tx: Option<StatusSender>) -> Result<()> {
        // Create a vector of all conversion steps and report these as current status
        if let Some(tx) = &tx {
            let mut steps: Vec<StepID> = Vec::default();
            steps.extend(self.importer.iter().map(|importer| importer.get_step_id()));
            // TODO: also add a step for importer that tracks applying the graph update
            steps.extend(
                self.manipulator
                    .iter()
                    .map(|manipulator| manipulator.get_step_id()),
            );
            steps.extend(self.exporter.iter().map(|exporter| exporter.get_step_id()));
            tx.send(StatusMessage::StepsCreated(steps))?;
        }

        // Create a new empty annotation graph
        let mut g = AnnotationGraph::with_default_graphstorages(true)
            .map_err(|e| AnnattoError::CreateGraph(e.to_string()))?;

        // Execute all importers and store their graph updates in parallel
        let updates: Result<Vec<GraphUpdate>> = self
            .importer
            .par_iter()
            .map_with(tx.clone(), |tx, step| {
                self.execute_single_importer(step, tx.clone())
            })
            .collect();
        if let Some(sender) = &tx {
            sender.send(StatusMessage::Info(String::from(
                "Applying importer updates ...",
            )))?;
        }
        // collect all updates in a single update to only have a single call to `apply_update`
        let mut super_update = GraphUpdate::new();
        for u in updates? {
            for uer in u.iter()? {
                let ue = uer?;
                let event = ue.1;
                super_update.add_event(event)?;
            }
        }
        // Apply super update
        g.apply_update(&mut super_update, |_msg| {})
            .map_err(|reason| AnnattoError::UpdateGraph(reason.to_string()))?;

        // Execute all manipulators in sequence
        for desc in self.manipulator.iter() {
            let workflow_directory = desc.workflow_directory.as_ref();
            desc.module
                .manipulator()
                .manipulate_corpus(&mut g, workflow_directory.map(|d| d.as_path()), tx.clone())
                .map_err(|reason| AnnattoError::Manipulator {
                    reason: reason.to_string(),
                    manipulator: desc.module.to_string(),
                })?;
            if let Some(ref tx) = tx {
                tx.send(crate::workflow::StatusMessage::StepDone {
                    id: desc.module.manipulator().step_id(None),
                })?;
            }
        }

        // Execute all exporters in parallel
        let export_result: Result<Vec<_>> = self
            .exporter
            .par_iter()
            .map_with(tx, |tx, step| {
                self.execute_single_exporter(&g, step, tx.clone())
            })
            .collect();
        // Check for errors during export
        export_result?;
        Ok(())
    }

    fn execute_single_importer(
        &self,
        step: &ImporterStep,
        tx: Option<StatusSender>,
    ) -> Result<GraphUpdate> {
        let updates = step
            .module
            .importer()
            .import_corpus(&step.corpus_path, tx.clone())
            .map_err(|reason| AnnattoError::Import {
                reason: reason.to_string(),
                importer: step.module.to_string(),
                path: step.corpus_path.to_path_buf(),
            })?;
        if let Some(ref tx) = tx {
            tx.send(crate::workflow::StatusMessage::StepDone {
                id: step.module.importer().step_id(Some(&step.corpus_path)),
            })?;
        }
        Ok(updates)
    }

    fn execute_single_exporter(
        &self,
        g: &AnnotationGraph,
        step: &ExporterStep,
        tx: Option<StatusSender>,
    ) -> Result<()> {
        step.module
            .exporter()
            .export_corpus(g, &step.corpus_path, tx.clone())
            .map_err(|reason| AnnattoError::Export {
                reason: reason.to_string(),
                exporter: step.module.to_string(),
                path: step.corpus_path.clone(),
            })?;
        if let Some(ref tx) = tx {
            tx.send(crate::workflow::StatusMessage::StepDone {
                id: step.module.exporter().step_id(Some(&step.corpus_path)),
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_export_step() {
        // This should not fail
        execute_from_file(Path::new("tests/data/import/empty/empty.ato"), None).unwrap();
    }
}
