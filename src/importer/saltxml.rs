use std::fs::File;

use documented::{Documented, DocumentedFields};
use graphannis::update::GraphUpdate;
use serde::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use crate::progress::ProgressReporter;

use super::Importer;

/// Imports the SaltXML format used by Pepper (<https://corpus-tools.org/pepper/>).
/// SaltXML is an XMI serialization of the [Salt model](https://raw.githubusercontent.com/korpling/salt/master/gh-site/doc/salt_modelGuide.pdf).
#[derive(Default, Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(default, deny_unknown_fields)]
pub struct ImportSaltXml {}

impl Importer for ImportSaltXml {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut updates = GraphUpdate::new();
        // Start  with an undetermined progress reporter
        let reporter = ProgressReporter::new_unknown_total_work(tx.clone(), step_id.clone())?;
        let mut mapper = mapper::SaltXmlMapper::new(reporter);

        // Read the corpus structure from the Salt project and get the number of documents to create
        mapper.reporter.info("Reading SaltXML project structure")?;
        let project_file = std::fs::read_to_string(input_path.join("saltProject.salt"))?;
        let documents = mapper.map_corpus_structure(&project_file, &mut updates)?;

        // Create a new progress reporter that can now estimate the work based on the number of documents
        mapper.reporter = ProgressReporter::new(tx, step_id, documents.len())?;
        for (document_node_name, document_path) in documents.iter() {
            mapper.reporter.info("Reading document {document_path}")?;
            let mut document_file = File::open(document_path)?;
            mapper.read_document(&mut document_file, document_node_name, &mut updates)?;
            mapper.reporter.worked(1)?;
        }

        Ok(updates)
    }

    fn file_extensions(&self) -> &[&str] {
        &[]
    }
}

mod mapper;

#[cfg(test)]
mod tests;
