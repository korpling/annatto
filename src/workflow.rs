//! ## Creating a workflow file
//!
//! Annatto workflow files list which importers, graph operations and exporters to execute.
//! We use an [TOML file](https://toml.io/) with the ending `.toml` to configure the workflow.
//! TOML files can be as simple as key-value pairs, like `config-key = "config-value"`.
//! But they allow representing more complex structures, such as lists.
//! The [TOML website](https://toml.io/) has a great "Quick Tour" section which explains the basics concepts of TOML with examples.
//!
//! ### Import
//!
//! An import step starts with the header `[[import]]`[^toml-array], and a
//! configuration value for the key `path` where to read the corpus from and the key `format` which declares in which format the corpus is encoded.
//! The file path is relative to the workflow file.
//! Importers also have an additional configuration header, that follows the `[[import]]` section and is marked with the `[import.config]` header.
//!
//!
//! ```toml
//! [[import]]
//! path = "textgrid/exampleCorpus/"
//! format = "textgrid"
//!
//! [import.config]
//! tier_groups = { tok = [ "pos", "lemma", "Inf-Struct" ] }
//! skip_timeline_generation = true
//! skip_audio = true
//! skip_time_annotations = true
//! audio_extension = "wav"
//! ```
//!
//! You can have more than one importer, and you can simply list all the different importers at the beginning of the workflow file.
//! An importer always needs to have a configuration header, even if it does not set any specific configuration option.
//!
//! ```toml
//! [[import]]
//! path = "a/mycorpus/"
//! format = "format-a"
//!
//! [import.config]
//!
//! [[import]]
//! path = "b/mycorpus/"
//! format = "format-b"
//!
//! [import.config]
//!
//! [[import]]
//! path = "c/mycorpus/"
//! format = "format-c"
//!
//! [import.config]
//!
//! # ...
//! ```
//!
//! ### Graph operations
//!
//! Graph operations use the header `[[graph_op]]` and the key `action` to describe which action to execute.
//! Since there are no files to import/export, they don't have a `path` configuration.
//!
//! ```toml
//! [[graph_op]]
//! action = "check"
//!
//! [graph_op.config]
//! # Empty list of tests
//! tests = []
//! ```
//!
//! ### Export
//!
//! Exporters work similar to importers, but use the keyword `[[export]]` instead.
//!
//! ```toml
//! [[export]]
//! path = "output/exampleCorpus"
//! format = "graphml"
//!
//! [export.config]
//! add_vis = "# no vis"
//! guess_vis = true
//! ```
//!
//! ### Full example
//!
//! You cannot mix import, graph operations and export headers. You have to first list all the import steps, then the graph operations and then the export steps.
//!
//! ```toml
//! [[import]]
//! path = "conll/ExampleCorpus"
//! format = "conllu"
//! config = {}
//!
//! [[graph_op]]
//! action = "check"
//!
//! [graph_op.config]
//! report = "list"
//!
//! [[graph_op.config.tests]]
//! query = "tok"
//! expected = [ 1, inf ]
//! description = "There is at least one token."
//!
//! [[graph_op.config.tests]]
//! query = "node ->dep node"
//! expected = [ 1, inf ]
//! description = "There is at least one dependency relation."
//!
//! [[export]]
//! path = "grapml/"
//! format = "graphml"
//!
//! [export.config]
//! add_vis = "# no vis"
//! guess_vis = true
//!
//! ```
//!
//! [^toml-array]: TOML can represent lists of the things as [Arrays of Tables](https://toml.io/en/v1.0.0#array-of-tables).
//!
use std::{
    fs,
    path::{Path, PathBuf},
    sync::mpsc::Sender,
};

use graphannis::{update::GraphUpdate, AnnotationGraph};

use regex::Regex;
use serde_derive::Deserialize;

use crate::{
    error::AnnattoError, error::Result, progress::ProgressReporter, runtime, ExporterStep,
    ImporterStep, ManipulatorStep, Step, StepID,
};
use log::error;
use normpath::PathExt;
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
        total_work: Option<usize>,
        /// Number of steps finished. Should never be larger than `total_work`.
        finished_work: usize,
    },
    /// Indicates a step has finished.
    StepDone { id: StepID },
}

/// A workflow describes the steps in the conversion pipeline process. It can be represented as TOML file.
///
/// First , all importers are executed in parallel. Then their output are appended to create a single annotation graph.
/// The manipulators are executed in their defined sequence and can change the annotation graph.
/// Last, all exporters are called with the now read-only annotation graph in parallel.
#[derive(Deserialize)]
pub struct Workflow {
    import: Vec<ImporterStep>,
    graph_op: Option<Vec<ManipulatorStep>>,
    export: Option<Vec<ExporterStep>>,
}

use std::convert::TryFrom;
use toml;

fn contained_variables(workflow: &'_ str) -> Result<Vec<(i32, &'_ str)>> {
    let pattern = Regex::new("[$][^\\s\\-/\"'.;,?!]+")?;
    let mut variables = Vec::new();
    for m in pattern.find_iter(workflow) {
        variables.push((m.start() as i32, m.as_str()))
    }
    Ok(variables)
}

fn parse_variables(workflow: String, buf: &mut String) -> Result<()> {
    let vars = contained_variables(&workflow)?;
    let content = workflow.as_bytes();
    let mut p = 0;
    for (start_index, var) in vars {
        for ci in p..start_index {
            buf.push(content[ci as usize] as char);
        }
        p = start_index + var.len() as i32;
        if let Ok(value) = std::env::var(&var[1..]) {
            for c in value.chars() {
                buf.push(c);
            }
        } else {
            for ci in start_index..start_index + var.len() as i32 {
                buf.push(content[ci as usize] as char);
            }
        }
    }
    for remain_i in p..workflow.len() as i32 {
        buf.push(content[remain_i as usize] as char);
    }
    Ok(())
}

fn read_workflow(path: PathBuf, read_env: bool) -> Result<String> {
    let toml_content = fs::read_to_string(path.as_path())?;
    if read_env {
        let mut buf = String::new();
        parse_variables(toml_content, &mut buf)?;
        Ok(buf)
    } else {
        Ok(toml_content)
    }
}

impl TryFrom<(PathBuf, bool)> for Workflow {
    type Error = AnnattoError;
    fn try_from(workflow_config: (PathBuf, bool)) -> Result<Workflow> {
        let (workflow_file, read_env) = workflow_config;
        let final_content = read_workflow(workflow_file, read_env)?;
        let workflow: Workflow = toml::from_str(final_content.as_str())?;
        Ok(workflow)
    }
}

/// Executes a workflow from a TOML file.
///
/// * `workflow_file` - The TOML workflow file.
/// * `tx` - If supported by the caller, this is a sender object that allows to send [status updates](enum.StatusMessage.html) (like information messages, warnings and module progress) to the calling entity.
pub fn execute_from_file(
    workflow_file: &Path,
    read_env: bool,
    tx: Option<Sender<StatusMessage>>,
) -> Result<()> {
    let wf = Workflow::try_from((workflow_file.to_path_buf(), read_env))?;
    let parent_dir = if let Some(directory) = workflow_file.parent() {
        directory
    } else {
        Path::new("")
    };
    wf.execute(tx, parent_dir)?;
    Ok(())
}

pub type StatusSender = Sender<StatusMessage>;

impl Workflow {
    pub fn execute(
        &self,
        tx: Option<StatusSender>,
        default_workflow_directory: &Path,
    ) -> Result<()> {
        // Create a vector of all conversion steps and report these as current status
        let apply_update_step_id = StepID {
            module_name: "create_annotation_graph".to_string(),
            path: None,
        };
        if let Some(tx) = &tx {
            let mut steps: Vec<StepID> = Vec::default();
            steps.extend(self.import.iter().map(|importer| importer.get_step_id()));
            steps.push(apply_update_step_id.clone());
            if let Some(ref manipulators) = self.graph_op {
                steps.extend(
                    manipulators
                        .iter()
                        .map(|manipulator| manipulator.get_step_id()),
                );
            }
            if let Some(ref exporters) = self.export {
                steps.extend(exporters.iter().map(|exporter| exporter.get_step_id()));
            }
            tx.send(StatusMessage::StepsCreated(steps))?;
        }

        // Execute all importers and store their graph updates in parallel
        let updates: Result<Vec<GraphUpdate>> = self
            .import
            .par_iter()
            .map_with(tx.clone(), |tx, step| {
                self.execute_single_importer(step, default_workflow_directory, tx.clone())
            })
            .collect();
        // Create a new empty annotation graph and apply updates
        let apply_update_reporter =
            ProgressReporter::new_unknown_total_work(tx.clone(), apply_update_step_id.clone())?;
        apply_update_reporter
            .info("Creating annotation graph by applying the updates from the import steps")?;
        let mut g = runtime::initialize_graph(&tx)?;

        // collect all updates in a single update to only have a single atomic
        // call to `apply_update`
        let mut updates = updates?;
        let mut combined_updates = if updates.len() == 1 {
            updates.remove(0)
        } else {
            let mut super_update = GraphUpdate::new();
            for u in updates {
                for uer in u.iter()? {
                    let ue = uer?;
                    let event = ue.1;
                    super_update.add_event(event)?;
                }
            }
            super_update
        };

        // Apply super update
        g.apply_update(&mut combined_updates, |msg| {
            if let Err(e) = apply_update_reporter.info(msg) {
                error!("{e}");
            }
        })
        .map_err(|reason| AnnattoError::UpdateGraph(reason.to_string()))?;
        if let Some(ref tx) = tx {
            tx.send(crate::workflow::StatusMessage::StepDone {
                id: apply_update_step_id,
            })?;
        }

        // Execute all manipulators in sequence
        if let Some(ref manipulators) = self.graph_op {
            for desc in manipulators.iter() {
                let workflow_directory = &desc.workflow_directory;
                desc.module
                    .processor()
                    .manipulate_corpus(
                        &mut g,
                        workflow_directory
                            .as_ref()
                            .map_or(default_workflow_directory, PathBuf::as_path),
                        tx.clone(),
                    )
                    .map_err(|reason| AnnattoError::Manipulator {
                        reason: reason.to_string(),
                        manipulator: desc.module.to_string(),
                    })?;
                if let Some(ref tx) = tx {
                    tx.send(crate::workflow::StatusMessage::StepDone {
                        id: desc.module.processor().step_id(None),
                    })?;
                }
            }
        }
        // Execute all exporters in parallel
        if let Some(ref exporters) = self.export {
            let export_result: Result<Vec<_>> = exporters
                .par_iter()
                .map_with(tx, |tx, step| {
                    self.execute_single_exporter(&g, step, default_workflow_directory, tx.clone())
                })
                .collect();
            // Check for errors during export
            export_result?;
        }
        Ok(())
    }

    pub fn import_steps(&self) -> &Vec<ImporterStep> {
        &self.import
    }

    pub fn export_steps(&self) -> Option<&Vec<ExporterStep>> {
        self.export.as_ref()
    }

    pub fn graph_op_steps(&self) -> Option<&Vec<ManipulatorStep>> {
        self.graph_op.as_ref()
    }

    fn execute_single_importer(
        &self,
        step: &ImporterStep,
        default_workflow_directory: &Path,
        tx: Option<StatusSender>,
    ) -> Result<GraphUpdate> {
        let import_path = if step.path.is_relative() {
            default_workflow_directory.join(&step.path)
        } else {
            step.path.clone()
        };
        let resolved_import_path: PathBuf = if import_path.exists() {
            import_path.normalize()?.into()
        } else {
            import_path
        };

        let updates = step
            .module
            .reader()
            .import_corpus(
                resolved_import_path.as_path(),
                step.get_step_id(),
                tx.clone(),
            )
            .map_err(|reason| AnnattoError::Import {
                reason: reason.to_string(),
                importer: step.module.to_string(),
                path: step.path.to_path_buf(),
            })?;
        if let Some(ref tx) = tx {
            tx.send(crate::workflow::StatusMessage::StepDone {
                id: step.get_step_id(),
            })?;
        }
        Ok(updates)
    }

    fn execute_single_exporter(
        &self,
        g: &AnnotationGraph,
        step: &ExporterStep,
        default_workflow_directory: &Path,
        tx: Option<StatusSender>,
    ) -> Result<()> {
        let resolved_output_path = if step.path.is_relative() {
            default_workflow_directory.join(&step.path).normalize()?
        } else {
            step.path.normalize()?
        };

        step.module
            .writer()
            .export_corpus(
                g,
                resolved_output_path.as_path(),
                step.get_step_id(),
                tx.clone(),
            )
            .map_err(|reason| AnnattoError::Export {
                reason: reason.to_string(),
                exporter: step.module.to_string(),
                path: step.path.clone(),
            })?;
        if let Some(ref tx) = tx {
            tx.send(crate::workflow::StatusMessage::StepDone {
                id: step.get_step_id(),
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
        execute_from_file(
            Path::new("./tests/data/import/empty/empty.toml"),
            false,
            None,
        )
        .unwrap();
    }

    #[test]
    fn with_env() {
        let k1 = "TEST_VAR_FORMAT_NAME";
        let k2 = "TEST_VAR_GRAPH_OP_NAME";
        std::env::set_var(k1, "none");
        std::env::set_var(k2, "check");
        let read_result = read_workflow(
            Path::new("./tests/data/import/empty/empty_with_vars.toml").to_path_buf(),
            true,
        );
        assert!(
            read_result.is_ok(),
            "Failed to read variable workflow with error {:?}",
            read_result.err()
        );
        if let Ok(workflow_with_vars) = read_result {
            let workflow_no_vars = read_workflow(
                Path::new("./tests/data/import/empty/empty.toml").to_path_buf(),
                false,
            )
            .unwrap();
            assert_eq!(workflow_with_vars, workflow_no_vars);
        }
    }

    #[test]
    fn multiple_importers() {
        // The workflow contains a check for the number of corpora
        execute_from_file(
            Path::new("./tests/workflows/multiple_importer.toml"),
            false,
            None,
        )
        .unwrap();
    }

    #[test]
    /// Test that exporting to an non-existing directory does not fail.
    fn nonexisting_export_dir() {
        let tmp_out = tempfile::tempdir().unwrap();
        std::env::set_var("TEST_OUTPUT", tmp_out.path().to_string_lossy().as_ref());

        execute_from_file(
            Path::new("./tests/workflows/multiple_importer.toml"),
            false,
            None,
        )
        .unwrap();
    }
}
