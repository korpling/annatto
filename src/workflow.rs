use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    path::{Path, PathBuf},
    sync::mpsc::Sender,
};

use graphannis::{update::GraphUpdate, AnnotationGraph};

use crate::{
    error::AnnattoError, error::Result, exporter_by_name, importer_by_name, manipulator_by_name,
    util::write_to_file, ExporterStep, ImporterStep, ManipulatorStep, Step, StepID,
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
pub struct Workflow {
    importer: Vec<ImporterStep>,
    manipulator: Vec<ManipulatorStep>,
    exporter: Vec<ExporterStep>,
}

use std::convert::TryFrom;
use xml::attribute::OwnedAttribute;
use xml::reader::{EventReader, ParserConfig, XmlEvent};

/* elements */
const ELEM_IMPORTER: &str = "importer";
const ELEM_MANIPULATOR: &str = "manipulator";
const ELEM_EXPORTER: &str = "exporter";
const ELEM_PROPERTY: &str = "property";

/* attributes */
const ATT_NAME: &str = "name";
const ATT_PATH: &str = "path";
const ATT_KEY: &str = "key";
const ATT_LEAK_PATH: &str = "leak_results_to";

fn into_hash_map(attributes: &[OwnedAttribute]) -> HashMap<String, String> {
    let mut attr_map = HashMap::new();
    for attribute in attributes {
        attr_map.insert(attribute.name.local_name.clone(), attribute.value.clone());
    }
    attr_map
}

impl TryFrom<PathBuf> for Workflow {
    type Error = AnnattoError;
    fn try_from(workflow_file: PathBuf) -> Result<Workflow> {
        let workflow_file = std::fs::canonicalize(workflow_file)?;
        let f = File::open(&workflow_file).map_err(|reason| AnnattoError::OpenWorkflowFile {
            reason,
            file: workflow_file.clone(),
        })?;
        let workflow_dir = workflow_file.parent();

        let mut parser_cfg = ParserConfig::new();
        parser_cfg.trim_whitespace = true;
        let mut reader = EventReader::new_with_config(f, parser_cfg);
        let mut importers: Vec<ImporterStep> = Vec::new();
        let mut manipulators: Vec<ManipulatorStep> = Vec::new();
        let mut exporters: Vec<ExporterStep> = Vec::new();
        let mut properties: BTreeMap<String, String> = BTreeMap::new();
        let mut key: Option<String> = None;
        let mut value: Option<String> = None;
        let mut mod_name: Option<String> = None;
        let mut path: Option<PathBuf> = None;
        let mut leak_path: Option<PathBuf> = None;

        loop {
            match reader.next() {
                Ok(event) => match event {
                    XmlEvent::EndDocument => break,
                    XmlEvent::StartElement {
                        name, attributes, ..
                    } => {
                        let mut attr = into_hash_map(&attributes);
                        match name.local_name.as_str() {
                            ELEM_IMPORTER => {
                                mod_name = attr.remove(ATT_NAME);
                                path = attr.remove(ATT_PATH).map(PathBuf::from);
                                // leaking currently only supported for importers, a serialization of graph objects
                                // as they result from manipulators could be achieved by graphml exports;
                                // leaking exporter results is redundant
                                leak_path = attr.remove(ATT_LEAK_PATH).map(PathBuf::from);
                            }
                            ELEM_MANIPULATOR => mod_name = attr.remove(ATT_NAME),
                            ELEM_EXPORTER => {
                                mod_name = attr.remove(ATT_NAME);
                                path = attr.remove(ATT_PATH).map(PathBuf::from);
                            }
                            ELEM_PROPERTY => key = attr.remove(ATT_KEY),
                            _ => continue,
                        };
                    }
                    XmlEvent::Characters(characters) => value = Some(characters),
                    XmlEvent::EndElement { name } => match name.local_name.as_str() {
                        ELEM_IMPORTER => {
                            if let Some(module_name) = mod_name {
                                if let Some(mut corpus_path) = path {
                                    let mut leak_results_to: Option<PathBuf> = None;
                                    if let Some(workflow_dir) = workflow_dir {
                                        if corpus_path.is_relative() {
                                            // Resolve the input path against the workflow file
                                            corpus_path = workflow_dir.join(corpus_path);
                                        }
                                        leak_results_to = match &leak_path {
                                            None => None,
                                            Some(path_buf) => {
                                                if path_buf.is_relative() {
                                                    Some(workflow_dir.join(path_buf))
                                                } else {
                                                    Some(PathBuf::from(path_buf))
                                                }
                                            }
                                        };
                                    }
                                    let step = ImporterStep {
                                        module: importer_by_name(&module_name)?,
                                        corpus_path,
                                        leak_path: leak_results_to,
                                        properties,
                                    };
                                    importers.push(step);
                                } else {
                                    return Err(AnnattoError::ReadWorkflowFile(format!(
                                        "Corpus path not specified for importer: {}",
                                        module_name
                                    )));
                                }
                            } else {
                                return Err(AnnattoError::ReadWorkflowFile(String::from(
                                    "Name of importer not specified.",
                                )));
                            }

                            // Reset the collected properties and other attributes
                            properties = BTreeMap::new();
                            mod_name = None;
                            path = None;
                            leak_path = None;
                        }
                        ELEM_MANIPULATOR => {
                            if let Some(module_name) = mod_name {
                                let step = ManipulatorStep {
                                    module: manipulator_by_name(&module_name)?,
                                    properties,
                                };
                                manipulators.push(step);
                            } else {
                                return Err(AnnattoError::ReadWorkflowFile(String::from(
                                    "Name of manipulator not specified.",
                                )));
                            }

                            // Reset the collected properties and other attributes
                            properties = BTreeMap::new();
                            mod_name = None;
                        }
                        ELEM_EXPORTER => {
                            if let Some(module_name) = mod_name {
                                if let Some(mut corpus_path) = path {
                                    if let Some(workflow_dir) = workflow_dir {
                                        if corpus_path.is_relative() {
                                            // Resolve the output path against the workflow file
                                            corpus_path = workflow_dir.join(corpus_path);
                                        }
                                    }
                                    let desc = ExporterStep {
                                        module: exporter_by_name(&module_name)?,
                                        corpus_path,
                                        properties,
                                    };
                                    exporters.push(desc);
                                } else {
                                    return Err(AnnattoError::ReadWorkflowFile(format!(
                                        "Corpus path not specified for exporter: {}",
                                        module_name
                                    )));
                                }
                            } else {
                                return Err(AnnattoError::ReadWorkflowFile(String::from(
                                    "Name of exporter not specified.",
                                )));
                            }

                            // Reset the collected properties and other attributes
                            properties = BTreeMap::new();
                            mod_name = None;
                            path = None;
                            leak_path = None;
                        }
                        ELEM_PROPERTY => {
                            if key.is_none() {
                                return Err(AnnattoError::ReadWorkflowFile(String::from(
                                    "Property's key not specified.",
                                )));
                            }
                            if value.is_none() {
                                return Err(AnnattoError::ReadWorkflowFile(format!(
                                    "Value for property `{}` not specified.",
                                    key.as_ref().unwrap()
                                )));
                            }
                            properties.insert(key.unwrap(), value.unwrap());
                            key = None;
                            value = None;
                        }
                        _ => continue,
                    },
                    _ => continue,
                },
                Err(e) => {
                    return Err(AnnattoError::ReadWorkflowFile(format!(
                        "Parsing error\n{:?}",
                        e
                    )))
                }
            };
        }
        Ok(Workflow {
            importer: importers,
            manipulator: manipulators,
            exporter: exporters,
        })
    }
}

/// Executes a workflow from an XML file.
///
/// Such a file has the root element `annatto-job` and contains entries for importers, exporters and manipulators.
/// Each of this modules have an attribute with their input/output path (except manipulators) and the module name.
/// They can also contain `property` child elements which key-value string properties.
///
/// ```xml
/// <?xml version='1.0' encoding='UTF-8'?>
/// <annatto-job>
///     <importer name="WebannoTSVImporter" path="./tsv/SomeCorpus/">
///     </importer>
///     <importer name="TextImporter" path="./meta/SomeCorpus/">
///             <property key="readMeta">meta</property>
///     </importer>
///     <manipulator name="Merger">
///     <property key="firstAsBase">true</property>
///     </manipulator>
///     <exporter name="ANNISExporter" path="./annis/">
///     </exporter>
/// </annatto-job>
/// ```
///
/// # Arguments
///
/// * `workflow_file` - The XML workflow file.
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
        let mut g =
            AnnotationGraph::new(true).map_err(|e| AnnattoError::CreateGraph(e.to_string()))?;

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
            desc.module
                .manipulate_corpus(&mut g, &desc.properties, tx.clone())
                .map_err(|reason| AnnattoError::Manipulator {
                    reason: reason.to_string(),
                    manipulator: desc.module.module_name().to_string(),
                })?;
            if let Some(ref tx) = tx {
                tx.send(crate::workflow::StatusMessage::StepDone {
                    id: desc.module.step_id(None),
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
            .import_corpus(&step.corpus_path, &step.properties, tx.clone())
            .map_err(|reason| AnnattoError::Import {
                reason: reason.to_string(),
                importer: step.module.module_name().to_string(),
                path: step.corpus_path.to_path_buf(),
            })?;
        if let Some(ref tx) = tx {
            tx.send(crate::workflow::StatusMessage::StepDone {
                id: step.module.step_id(Some(&step.corpus_path)),
            })?;
        }
        if let Some(path_buf) = &step.leak_path {
            write_to_file(&updates, path_buf.as_path())?;
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
            .export_corpus(g, &step.properties, &step.corpus_path, tx.clone())
            .map_err(|reason| AnnattoError::Export {
                reason: reason.to_string(),
                exporter: step.module.module_name().to_string(),
                path: step.corpus_path.clone(),
            })?;
        if let Some(ref tx) = tx {
            tx.send(crate::workflow::StatusMessage::StepDone {
                id: step.module.step_id(Some(&step.corpus_path)),
            })?;
        }
        Ok(())
    }
}
