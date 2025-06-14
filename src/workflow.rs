use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::mpsc::Sender,
};

use anyhow::anyhow;
use graphannis::{update::GraphUpdate, AnnotationGraph};

use regex::Regex;
use serde::Serialize;
use serde_derive::Deserialize;

use crate::{
    core::update_graph,
    error::{AnnattoError, Result},
    progress::ProgressReporter,
    ExporterStep, ImporterStep, ManipulatorStep, StepID,
};
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
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Workflow {
    import: Vec<ImporterStep>,
    graph_op: Option<Vec<ManipulatorStep>>,
    export: Option<Vec<ExporterStep>>,
    #[serde(default)]
    footer: Metadata,
}

#[derive(Deserialize, Serialize)]
struct Metadata {
    #[serde(default = "metadata_default_version")]
    annatto_version: String,
    #[serde(default)]
    success: bool,
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            annatto_version: metadata_default_version(),
            success: Default::default(),
        }
    }
}

fn metadata_default_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

use std::convert::TryFrom;
use toml;

fn contained_variables(workflow: &'_ str) -> Result<Vec<(i32, &'_ str)>> {
    let pattern = Regex::new(r#"[$][A-Za-z_0-9]+"#)?;
    let mut variables = Vec::new();
    for m in pattern.find_iter(workflow) {
        variables.push((m.start() as i32, m.as_str()))
    }
    Ok(variables)
}

fn parse_variables(
    workflow: String,
    buf: &mut String,
    visited: &mut BTreeSet<String>,
) -> Result<()> {
    let vars = contained_variables(&workflow)?;
    let content = workflow.as_bytes();
    let mut p = 0;
    for (start_index, var) in vars {
        for ci in p..start_index {
            buf.push(content[ci as usize] as char);
        }
        p = start_index + var.len() as i32;
        let var_name = &var[1..];
        if visited.contains(var_name) {
            return Err(AnnattoError::Anyhow(anyhow!(
                "Workflow contains a cycle of variables, observed {var_name} for the second time."
            )));
        }
        visited.insert(var_name.to_string());
        if let Ok(value) = std::env::var(var_name) {
            // value might contain variables again, parse
            let mut value_buf = String::new();
            parse_variables(value, &mut value_buf, visited)?;
            visited.remove(var_name);
            for c in value_buf.chars() {
                buf.push(c);
            }
        } else {
            // variable value could not be resolved, do not step into, just copy
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
        parse_variables(toml_content, &mut buf, &mut Default::default())?;
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
/// * `read_env` - Set whether to resolve environment variables in the workflow file.
/// * `in_memory` - If true, use a main memory implementation to store the temporary graphs.
/// * `tx` - If supported by the caller, this is a sender object that allows to send [status updates](enum.StatusMessage.html) (like information messages, warnings and module progress) to the calling entity.
pub fn execute_from_file(
    workflow_file: &Path,
    read_env: bool,
    in_memory: bool,
    tx: Option<Sender<StatusMessage>>,
    save_workflow: Option<PathBuf>,
) -> Result<()> {
    let mut wf = Workflow::try_from((workflow_file.to_path_buf(), read_env))?;
    let parent_dir = if let Some(directory) = workflow_file.parent() {
        directory
    } else {
        Path::new("")
    };
    let result = wf.execute(tx, parent_dir, in_memory);
    if let Some(save_path) = save_workflow {
        wf.footer.success = result.is_ok();
        wf.save(save_path)?;
    }
    result
}

pub type StatusSender = Sender<StatusMessage>;

impl Workflow {
    pub fn execute(
        &self,
        tx: Option<StatusSender>,
        default_workflow_directory: &Path,
        in_memory: bool,
    ) -> Result<()> {
        // Create a vector of all conversion steps and report these as current status
        let apply_update_step_id = StepID {
            module_name: "create_annotation_graph".to_string(),
            path: None,
        };

        if let Some(tx) = &tx {
            let mut steps: Vec<StepID> = Vec::default();
            steps.extend(self.import.iter().map(StepID::from_importer_step));
            steps.push(apply_update_step_id.clone());

            let mut graph_op_position = 1;
            if let Some(ref manipulators) = self.graph_op {
                for m in manipulators {
                    steps.push(StepID::from_graphop_step(m, graph_op_position));
                    graph_op_position += 1;
                }
            }
            if let Some(ref exporters) = self.export {
                steps.extend(exporters.iter().map(StepID::from_exporter_step));
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
        if in_memory {
            apply_update_reporter.info(
                "Creating in-memory annotation graph by applying the updates from the import steps",
            )?;
        } else {
            apply_update_reporter.info(
                "Creating on-disk annotation graph by applying the updates from the import steps",
            )?;
        }
        let mut g = AnnotationGraph::with_default_graphstorages(!in_memory)
            .map_err(|e| AnnattoError::CreateGraph(e.to_string()))?;

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
        update_graph(
            &mut g,
            &mut combined_updates,
            Some(apply_update_step_id),
            tx.clone(),
        )?;

        // Execute all manipulators in sequence
        if let Some(ref manipulators) = self.graph_op {
            let mut graph_op_position = 1;
            for desc in manipulators.iter() {
                let step_id = StepID::from_graphop_step(desc, graph_op_position);
                let workflow_directory = &desc.workflow_directory;
                desc.execute(
                    &mut g,
                    workflow_directory
                        .as_ref()
                        .map_or(default_workflow_directory, PathBuf::as_path),
                    graph_op_position,
                    tx.clone(),
                )
                .map_err(|reason| AnnattoError::Manipulator {
                    reason: reason.to_string(),
                    manipulator: step_id.to_string(),
                })?;
                graph_op_position += 1;

                if let Some(ref tx) = tx {
                    tx.send(crate::workflow::StatusMessage::StepDone { id: step_id })?;
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
        let step_id = StepID::from_importer_step(step);

        // Do not use the import path directly, but resolve it against the
        // workflow directory if the path is relative.
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
            .import_corpus(resolved_import_path.as_path(), step_id.clone(), tx.clone())
            .map_err(|reason| AnnattoError::Import {
                reason: reason.to_string(),
                importer: step_id.module_name.to_string(),
                path: step.path.to_path_buf(),
            })?;
        if let Some(ref tx) = tx {
            tx.send(crate::workflow::StatusMessage::StepDone { id: step_id })?;
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
        let step_id = StepID::from_exporter_step(step);

        // Do not use the output path directly, but resolve it against the
        // workflow directory if the path is relative.
        let mut resolved_output_path = if step.path.is_relative() {
            default_workflow_directory.join(&step.path)
        } else {
            step.path.clone()
        };
        if resolved_output_path.exists() {
            resolved_output_path = resolved_output_path.normalize()?.into();
        }

        step.module
            .writer()
            .export_corpus(
                g,
                resolved_output_path.as_path(),
                step_id.clone(),
                tx.clone(),
            )
            .map_err(|reason| AnnattoError::Export {
                reason: reason.to_string(),
                exporter: step_id.module_name.to_string(),
                path: step.path.clone(),
            })?;
        if let Some(ref tx) = tx {
            tx.send(crate::workflow::StatusMessage::StepDone { id: step_id })?;
        }
        Ok(())
    }

    fn save(&self, path: PathBuf) -> Result<()> {
        let wf_string = toml::to_string(&self).map_err(|_| {
            AnnattoError::Anyhow(anyhow!(
                "Could not serialize workflow after run. The workflow run was {}successful",
                if self.footer.success { "NOT " } else { "" }
            ))
        })?;
        fs::write(path, wf_string).map_err(AnnattoError::IO)
    }
}

#[cfg(test)]
mod tests {

    use std::env;

    use insta::assert_snapshot;

    use super::*;

    #[test]
    fn no_export_step() {
        // This should not fail
        execute_from_file(
            Path::new("./tests/data/import/empty/empty.toml"),
            false,
            false,
            None,
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
    fn with_env_recursive() {
        let k1 = "TEST_VAR_FORMAT_NAME_REC";
        let k2 = "TEST_VAR_GRAPH_OP_NAME_REC";
        let k3 = "TEST_VAR_WAIT_FOR_IT_REC";
        std::env::set_var(k1, "none");
        std::env::set_var(k2, "$TEST_VAR_WAIT_FOR_IT_REC");
        std::env::set_var(k3, "check");
        let read_result = read_workflow(
            Path::new("./tests/data/import/empty/empty_with_vars_rec.toml").to_path_buf(),
            true,
        );
        assert!(
            read_result.is_ok(),
            "Failed to read variable workflow with error {:?}",
            read_result.err()
        );
        assert_snapshot!(read_result.unwrap());
    }

    #[test]
    fn with_env_repetition() {
        let k1 = "TEST_VAR_FORMAT_NAME_REP";
        let k2 = "TEST_VAR_GRAPH_OP_NAME_REP";
        let k3 = "TEST_VAR_WAIT_FOR_IT_REP";
        std::env::set_var(k1, "none");
        std::env::set_var(k2, "$TEST_VAR_WAIT_FOR_IT_REP");
        std::env::set_var(k3, "check");
        let read_result = read_workflow(
            Path::new("./tests/data/import/empty/empty_with_vars_rep.toml").to_path_buf(),
            true,
        );
        assert!(
            read_result.is_ok(),
            "Failed to read variable workflow with error {:?}",
            read_result.err()
        );
        assert_snapshot!(read_result.unwrap());
    }

    #[test]
    fn with_env_var_cycle() {
        let k1 = "TEST_VAR_FORMAT_NAME_CYC";
        let k2 = "TEST_VAR_GRAPH_OP_NAME_CYC";
        let k3 = "TEST_VAR_WAIT_FOR_IT_CYC";
        std::env::set_var(k1, "$TEST_VAR_GRAPH_OP_NAME_CYC");
        std::env::set_var(k2, "$TEST_VAR_WAIT_FOR_IT_CYC");
        std::env::set_var(k3, "$TEST_VAR_FORMAT_NAME_CYC");
        let read_result = read_workflow(
            Path::new("./tests/data/import/empty/empty_with_vars_cyc.toml").to_path_buf(),
            true,
        );
        assert!(read_result.is_err());
    }

    #[test]
    fn invalid_variable_name() {
        let k = "ß";
        std::env::set_var(k, "any_value");
        let r = contained_variables("this text contains an invalid variable with name $ß");
        assert!(r.is_ok());
        assert_eq!(0, r.unwrap().len());
    }

    #[test]
    fn multiple_importers() {
        // The workflow contains a check for the number of corpora
        execute_from_file(
            Path::new("./tests/workflows/multiple_importer.toml"),
            false,
            false,
            None,
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
            Path::new("./tests/workflows/nonexisting_dir.toml"),
            true,
            false,
            None,
            None,
        )
        .unwrap();
    }

    #[test]
    fn serialize_workflow() {
        let ts = fs::read_to_string("tests/data/workflow/complex.toml");
        assert!(ts.is_ok());
        env::set_var(
            "NOT_SO_RANDOM_VARIABLE",
            "export/to/this/path/if/you/can/if/not/no/worries",
        );
        let mut clean_str = String::new();
        assert!(parse_variables(ts.unwrap(), &mut clean_str, &mut BTreeSet::default()).is_ok());
        let wf: std::result::Result<Workflow, _> = toml::from_str(&clean_str);
        assert!(wf.is_ok(), "Could not deserialize workflow: {:?}", wf.err());
        let workflow = wf.unwrap();
        let of = tempfile::NamedTempFile::new();
        assert!(of.is_ok());
        let outfile = of.unwrap();
        assert!(workflow.save(outfile.path().to_path_buf()).is_ok());
        let ww = fs::read_to_string(outfile);
        assert!(
            ww.is_ok(),
            "Could not read written workflow file: {:?}",
            ww.err()
        );
        let written_workflow = ww.unwrap();
        assert_snapshot!(Regex::new(r#"[0-9]+\.[0-9]+\.[0-9]+"#)
            .unwrap()
            .replace(&written_workflow, "<VERSION>"));
        let deserialize: std::result::Result<Workflow, _> = toml::from_str(&written_workflow);
        assert!(
            deserialize.is_ok(),
            "Could not deserialize workflow that was written by annatto: {:?}",
            deserialize.err()
        );
    }
}
