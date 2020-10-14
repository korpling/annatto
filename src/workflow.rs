use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
};

use graphannis::{update::GraphUpdate, AnnotationGraph};

use crate::{
    error::PepperError, error::Result, exporter::Exporter, importer::Importer,
    manipulator::Manipulator
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

pub struct Workflow {
    importer: Vec<ImporterDesc>,
    manipulator: Vec<ManipulatorDesc>,
    exporter: Vec<ExporterDesc>,
}

use std::convert::TryFrom;
use xml::reader::{EventReader, ParserConfig, XmlEvent};
use xml::attribute::OwnedAttribute;

/* elements */
const ELEM_IMPORTER: &str = "importer";
const ELEM_MANIPULATOR: &str = "manipulator";
const ELEM_EXPORTER: &str = "exporter";
const ELEM_PROPERTY: &str = "property";

/* attributes */
const ATT_NAME: &str = "name";
const ATT_PATH: &str = "path";
const ATT_KEY: &str = "key";

fn into_hash_map(attributes: &Vec<OwnedAttribute>) -> HashMap<String, String> {
    let mut attr_map = HashMap::new();
    for attribute in attributes {
        attr_map.insert(attribute.name.local_name.clone(), attribute.value.clone());
    }
    attr_map
}

use crate::donothing::*;

fn importer_by_name(name: String) -> Box<dyn Importer> {
    Box::new(DoNothingImporter::new())  // dummy impl
}

fn manipulator_by_name(name: String) -> Box<dyn Manipulator> {
    Box::new(DoNothingManipulator::new())  // dummy impl
}

fn exporter_by_name(name: String) -> Box<dyn Exporter> {
    Box::new(DoNothingExporter::new())  // dummy impl
}

impl TryFrom<File> for Workflow {
    type Error = PepperError;
    fn try_from(f: File) -> Result<Workflow> {
        let mut parser_cfg = ParserConfig::new();
        parser_cfg.trim_whitespace = true;
        let mut reader = EventReader::new_with_config(f, parser_cfg);
        let mut importers: Vec<ImporterDesc> = Vec::new();
        let mut manipulators: Vec<ManipulatorDesc> = Vec::new();
        let mut exporters: Vec<ExporterDesc> = Vec::new();
        let mut properties: HashMap<String, String> = HashMap::new();
        let mut key: Option<String> = None;
        let mut value: Option<String> = None;
        let mut mod_name: Option<String> = None;
        let mut path: Option<PathBuf> = None;
        loop {
            match reader.next() {
                Ok(event) => {
                    match event {
                        XmlEvent::EndDocument => break,
                        XmlEvent::StartElement {name, attributes, ..} => {
                            let mut attr = into_hash_map(&attributes);
                            match name.local_name.as_str() {
                                ELEM_IMPORTER => {
                                    mod_name = attr.remove(ATT_NAME);
                                    path = match attr.remove(ATT_PATH) {
                                        Some(s) => Some(PathBuf::from(s)),
                                        None => None 
                                    };
                                },
                                ELEM_MANIPULATOR => mod_name = attr.remove(ATT_NAME),
                                ELEM_EXPORTER => {
                                    mod_name = attr.remove(ATT_NAME);                                    
                                    path = match attr.remove(ATT_PATH) {
                                        Some(s) => Some(PathBuf::from(s)),
                                        None => None 
                                    };
                                },
                                ELEM_PROPERTY => key = attr.remove(ATT_KEY),
                                _ => continue
                            };
                        },
                        XmlEvent::Characters(characters) => value = Some(characters),
                        XmlEvent::EndElement {name} => {
                            match name.local_name.as_str() {
                                ELEM_IMPORTER => {
                                    if mod_name.is_none() {
                                        return Err(PepperError::ReadWorkflowFile(String::from("Name of importer not specified.")));
                                    }
                                    if path.is_none() {
                                        return Err(PepperError::ReadWorkflowFile(format!("Corpus path not specified for importer: {}", mod_name.unwrap())));
                                    }
                                    let importer = ImporterDesc { module: importer_by_name(mod_name.unwrap()),
                                                                  corpus_path: path.unwrap(),
                                                                  properties: properties };
                                    importers.push(importer);
                                    properties = HashMap::new();
                                    mod_name = None;
                                    path = None;
                                },
                                ELEM_MANIPULATOR => {   
                                    if mod_name.is_none() {
                                        return Err(PepperError::ReadWorkflowFile(String::from("Name of manipulator not specified.")));
                                    }                                
                                    let manipulator = ManipulatorDesc { module: manipulator_by_name(mod_name.unwrap()),
                                                                        properties: properties };
                                    manipulators.push(manipulator);
                                    properties = HashMap::new();
                                    mod_name = None;
                                },
                                ELEM_EXPORTER => {
                                    if mod_name.is_none() {
                                        return Err(PepperError::ReadWorkflowFile(String::from("Name of exporter not specified.")))
                                    }
                                    if path.is_none() {
                                        return Err(PepperError::ReadWorkflowFile(format!("Corpus path not specified for exporter: {}", mod_name.unwrap())))
                                    }                                    
                                    let exporter = ExporterDesc { module: exporter_by_name(mod_name.unwrap()),
                                                                  corpus_path: path.unwrap(),
                                                                  properties: properties};
                                    exporters.push(exporter);
                                    properties = HashMap::new();
                                    mod_name = None;
                                    path = None;
                                },
                                ELEM_PROPERTY => {
                                    if key.is_none() {
                                        return Err(PepperError::ReadWorkflowFile(String::from("Property's key not specified.")))
                                    }                                    
                                    if value.is_none() {
                                        return Err(PepperError::ReadWorkflowFile(format!("Value for property `{}` not specified.", (&key).as_ref().unwrap())))
                                    }
                                    properties.insert(key.unwrap(), value.unwrap());
                                    key = None;
                                    value = None;
                                },
                                _ => continue
                            }
                        }
                        _ => continue
                    }
                },
                Err(_) => {
                    return Err(PepperError::ReadWorkflowFile(String::from("Parsing error.")))
                }
            };
        }
        Ok(Workflow {importer: importers, manipulator: manipulators, exporter: exporters})
    }
}

pub fn execute_from_file(workflow_file: &Path) -> Result<()> {
    let f = File::open(workflow_file).map_err(|reason| PepperError::OpenWorkflowFile {
        reason,
        file: workflow_file.to_path_buf(),
    })?;

    execute(Workflow::try_from(f).unwrap())
}

pub fn execute(workflow: Workflow) -> Result<()> {
    // Create a new empty annotation graph
    let mut g = AnnotationGraph::new(true).map_err(|e| PepperError::CreateGraph(e.to_string()))?;

    // Execute all importers and store their graph updates in parallel
    let updates: Result<Vec<GraphUpdate>> = workflow
        .importer
        .into_par_iter()
        .map(execute_single_importer)
        .collect();
    // Apply each graph update
    for mut u in updates? {
        g.apply_update(&mut u, |_msg| {})
            .map_err(|reason| PepperError::UpdateGraph(reason.to_string()))?;
    }

    // Execute all manipulators in sequence
    for desc in workflow.manipulator.into_iter() {
        desc.module
            .manipulate_corpus(&mut g, &desc.properties)
            .map_err(|reason| PepperError::Manipulator {
                reason: reason.to_string(),
                manipulator: desc.module.module_name(),
            })?;
    }

    // Execute all exporters in parallel
    let export_result: Result<Vec<_>> = workflow
        .exporter
        .into_par_iter()
        .map(|desc| execute_single_exporter(&g, desc))
        .collect();
    // Check for errors during export
    export_result?;
    Ok(())
}

fn execute_single_importer(desc: &ImporterDesc) -> Result<GraphUpdate> {
    desc.module
        .import_corpus(&desc.corpus_path, &desc.properties)
        .map_err(|reason| PepperError::Import {
            reason: reason.to_string(),
            importer: desc.module.module_name(),
            path: desc.corpus_path.to_path_buf(),
        })
}

fn execute_single_exporter(g: &AnnotationGraph, desc: &ExporterDesc) -> Result<()> {
    desc.module
        .export_corpus(&g, &desc.properties, &desc.corpus_path)
        .map_err(|reason| PepperError::Export {
            reason: reason.to_string(),
            exporter: desc.module.module_name(),
            path: desc.corpus_path.clone(),
        })?;
    Ok(())
}
