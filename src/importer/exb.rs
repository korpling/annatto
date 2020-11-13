use std::convert::TryFrom;

use graphannis::update::GraphUpdate;
use j4rs::{InvocationArg, Jvm};
use rayon::prelude::*;

use crate::{error::PepperError, importer::Importer, progress::ProgressReporter, Module};

pub struct EXMARaLDAImporter {}

impl EXMARaLDAImporter {
    pub fn new() -> EXMARaLDAImporter {
        EXMARaLDAImporter {}
    }

    fn create_jvm(&self) -> Result<Jvm, PepperError> {
        let jvm = j4rs::JvmBuilder::new().build()?;
        Ok(jvm)
    }
}

impl Module for EXMARaLDAImporter {
    fn module_name(&self) -> &str {
        "EXMARaLDAImporter"
    }
}

impl Importer for EXMARaLDAImporter {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        _properties: &std::collections::BTreeMap<String, String>,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let reporter = ProgressReporter::new(tx, self as &dyn Module, Some(input_path));
        reporter.set_progress(0.0)?;
        let mut updates = GraphUpdate::new();

        // Create the corpus structure and all Java document objects
        let documents = crate::legacy::import_corpus_structure(
            input_path,
            Some(".*\\.(exb|xml|xmi|exmaralda)$"),
            &mut updates,
        )?;

        //  Process all documents in parallel and merge graph updates afterwards
        let doc_updates: Result<Vec<_>, PepperError> = documents
            .into_par_iter()
            .map(move |(file_path, _document_name)| {
                let jvm = self.create_jvm()?;
                // Create an instance of the Salt to Exmaralda mapper
                let mapper = jvm.create_instance(
                    "org.corpus_tools.peppermodules.exmaralda.EXMARaLDA2SaltMapper",
                    &vec![],
                )?;

                // Make sure the mapper knows where to find the file
                let uri_as_string =
                    InvocationArg::try_from(file_path.as_os_str().to_string_lossy().as_ref())?;
                let resource_uri = jvm.invoke_static(
                    "org.eclipse.emf.common.util.URI",
                    "createFileURI",
                    &vec![uri_as_string],
                )?;
                jvm.invoke(
                    &mapper,
                    "setResourceURI",
                    &vec![InvocationArg::from(resource_uri)],
                )?;

                // Invoke the internal mapper
                jvm.invoke(&mapper, "mapSDocument", &vec![])?;

                // Retrieve the reference to the created graph

                let u = GraphUpdate::new();
                // TODO: map Salt to GraphML
                Ok(u)
            })
            .collect();
        let doc_updates = doc_updates?;

        // TODO: merge graph updates into own

        todo!()
    }
}

#[cfg(test)]
mod tests {

    use std::{collections::BTreeMap, path::PathBuf};

    use super::*;

    #[test]
    fn load_jvm() {
        let importer = EXMARaLDAImporter::new();
        let properties: BTreeMap<String, String> = BTreeMap::new();
        importer
            .import_corpus(
                &PathBuf::from("test-corpora/exb/rootCorpus"),
                &properties,
                None,
            )
            .unwrap();
    }
}
