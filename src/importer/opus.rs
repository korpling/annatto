use std::{collections::BTreeMap, fs::File, path::Path};

use facet::Facet;
use graphannis::model::AnnotationComponentType;
use serde::Serialize;
use serde_derive::Deserialize;
use xml::{EventReader, ParserConfig};

use crate::{
    error::{AnnattoError, Result},
    importer::{DefaultImportConfiguration, GenericImportConfiguration},
    progress::ProgressReporter,
    util::graphupdate::ConfiguredUpdate,
};

use super::Importer;

/// Add alignment edges for parallel corpora from the XML format used by the
/// [OPUS](https://opus.nlpl.eu/) corpora.
#[derive(Facet, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ImportOpusLinks {
    /// The component name of the edges for the default direction.
    #[serde(default = "default_align_name")]
    default_name: String,
    /// The component type of the edges for the default direction.
    #[serde(default = "default_component_type")]
    default_type: AnnotationComponentType,
    /// The component name of the edges for the reverse direction.
    #[serde(default)]
    reverse_name: Option<String>,
    /// The component type of the edges for the reverse direction.
    #[serde(default = "default_component_type")]
    reverse_type: AnnotationComponentType,
}

fn default_align_name() -> String {
    "align".to_string()
}

fn default_component_type() -> AnnotationComponentType {
    AnnotationComponentType::Pointing
}

impl Default for ImportOpusLinks {
    fn default() -> Self {
        Self {
            default_name: default_align_name(),
            default_type: default_component_type(),
            reverse_name: Default::default(),
            reverse_type: default_component_type(),
        }
    }
}

const FILE_EXTENSIONS: [&str; 1] = ["xml"];

impl Importer for ImportOpusLinks {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        step_id: crate::StepID,
        config: GenericImportConfiguration,
        tx: Option<crate::workflow::StatusSender>,
    ) -> std::result::Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = ConfiguredUpdate::from(config);
        let all_files = update.build_corpus_graph(input_path)?;
        let progress = ProgressReporter::new(tx, step_id.clone(), all_files.len())?;
        all_files.into_iter().try_for_each(|(p, d)| {
            if let Err(e) =
                self.import_document(&step_id, p.as_path(), Path::new(d.as_str()), &mut update)
            {
                Err(e)
            } else {
                progress.worked(1)
            }
        })?;
        Ok(update.into())
    }
}

impl DefaultImportConfiguration for ImportOpusLinks {
    fn default_file_extensions(&self) -> &[&str] {
        &FILE_EXTENSIONS
    }

    fn preset_default_namespace(&self) -> Option<&str> {
        // this module creates no node labels,
        // thus a default namespace is irrelevant
        None
    }
}

impl ImportOpusLinks {
    fn link(
        &self,
        update: &mut ConfiguredUpdate,
        source_doc_node_id: &str,
        target_doc_node_id: &str,
        single_source: &str,
        single_target: &str,
    ) -> Result<()> {
        let source_id = format!("{source_doc_node_id}#{single_source}");
        let target_id = format!("{target_doc_node_id}#{single_target}");
        update.node(source_id.to_string(), "node".to_string())?;
        update.part_of(source_id.to_string(), source_doc_node_id.to_string())?;
        update.node(target_id.to_string(), "node".to_string())?;
        update.part_of(target_id.to_string(), target_doc_node_id.to_string())?;
        update.edge_by_parts(
            source_id.to_string(),
            target_id.to_string(),
            "".to_string(),
            self.default_type.clone(),
            self.default_name.to_string(),
        )?;
        if let Some(name) = &self.reverse_name {
            update.edge_by_parts(
                target_id.to_string(),
                source_id.to_string(),
                "".to_string(),
                self.reverse_type.clone(),
                name.to_string(),
            )?;
        }
        Ok(())
    }

    fn import_document(
        &self,
        step_id: &crate::StepID,
        path: &Path,
        corpus_node_path: &Path,
        update: &mut ConfiguredUpdate,
    ) -> Result<()> {
        // buffers
        let mut source_doc_node_id = String::new();
        let mut target_doc_node_id = String::new();
        // parsing
        let f = File::open(path)?;
        let mut parser_cfg = ParserConfig::new();
        parser_cfg.trim_whitespace = true;
        let mut reader = EventReader::new_with_config(f, parser_cfg);
        loop {
            let xml_event = reader.next().map_err(|_| AnnattoError::Import {
                reason: "Error parsing xml.".to_string(),
                importer: step_id.module_name.clone(),
                path: path.to_path_buf(),
            })?;
            match xml_event {
                xml::reader::XmlEvent::EndDocument => break,
                xml::reader::XmlEvent::StartElement {
                    name, attributes, ..
                } => {
                    let attribute_map: BTreeMap<String, String> = attributes
                        .into_iter()
                        .map(|a| (a.name.to_string(), a.value.to_string()))
                        .collect();
                    match &name.to_string()[..] {
                        "linkGrp" => {
                            if let Some(parent_name) = corpus_node_path.parent() {
                                let err = Err(AnnattoError::Import {
                                    reason: "Source or target document undefined.".to_string(),
                                    importer: step_id.module_name.clone(),
                                    path: path.to_path_buf(),
                                });
                                if let Some(source_doc_path) = attribute_map.get("fromDoc") {
                                    let end_index = if source_doc_path.ends_with(".xml.gz") {
                                        source_doc_path.len() - 7
                                    } else {
                                        *source_doc_path
                                            .rfind('.')
                                            .get_or_insert(source_doc_path.len())
                                    };
                                    source_doc_node_id = parent_name
                                        .join(&(source_doc_path.to_string())[..end_index])
                                        .to_string_lossy()
                                        .to_string();
                                } else {
                                    return err;
                                }
                                if let Some(target_doc_path) = attribute_map.get("toDoc") {
                                    let end_index = if target_doc_path.ends_with(".xml.gz") {
                                        target_doc_path.len() - 7
                                    } else {
                                        *target_doc_path
                                            .rfind('.')
                                            .get_or_insert(target_doc_path.len())
                                    };
                                    target_doc_node_id = parent_name
                                        .join(&(target_doc_path.to_string())[..end_index])
                                        .to_string_lossy()
                                        .to_string();
                                } else {
                                    return err;
                                }
                            }
                        }
                        "link" => {
                            if let Some(attr_val) = attribute_map.get("xtargets")
                                && let Some((source, target)) = attr_val.split_once(';')
                            {
                                for single_source in source.split(' ') {
                                    for single_target in target.split(' ') {
                                        self.link(
                                            update,
                                            source_doc_node_id.as_str(),
                                            target_doc_node_id.as_str(),
                                            single_source,
                                            single_target,
                                        )?;
                                    }
                                }
                            }
                        }
                        _ => {}
                    };
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
