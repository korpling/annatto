use std::{convert::TryFrom, path::PathBuf};

use graphannis::update::GraphUpdate;
use j4rs::{Instance, InvocationArg, JavaOpt, Jvm};
use rayon::prelude::*;

use crate::{error::PepperError, importer::Importer, progress::ProgressReporter, Module};

pub struct EXMARaLDAImporter {}

impl EXMARaLDAImporter {
    pub fn new() -> EXMARaLDAImporter {
        EXMARaLDAImporter {}
    }

    fn create_jvm(&self, debug: bool) -> Result<Jvm, PepperError> {
        let jvm = if debug {
            j4rs::JvmBuilder::new()
                .java_opt(JavaOpt::new("-Xdebug"))
                .java_opt(JavaOpt::new(
                    "-Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5000",
                ))
                .build()?
        } else {
            j4rs::JvmBuilder::new().build()?
        };
        Ok(jvm)
    }
}

impl Module for EXMARaLDAImporter {
    fn module_name(&self) -> &str {
        "EXMARaLDAImporter"
    }
}

fn get_identifier(sdocument: &Instance, jvm: &Jvm) -> Result<Instance, PepperError> {
    let id = jvm.invoke(
        &jvm.cast(sdocument, "org.corpus_tools.salt.graph.IdentifiableElement")?,
        "getIdentifier",
        &vec![],
    )?;
    Ok(id)
}

fn prepare_mapper(mapper: &Instance, document: Instance, jvm: &Jvm) -> Result<(), PepperError> {
    // Create and set an empty property map
    let props = jvm.create_instance(
        "org.corpus_tools.peppermodules.exmaralda.EXMARaLDAImporterProperties",
        &vec![],
    )?;
    jvm.invoke(mapper, "setProperties", &vec![InvocationArg::from(props)])?;

    // Explicitly set the document object
    jvm.invoke(&mapper, "setDocument", &vec![InvocationArg::from(document)])?;

    Ok(())
}

fn map_document(
    file_path: PathBuf,
    document_name: &str,
    jvm: &Jvm,
) -> Result<GraphUpdate, PepperError> {
    // Create an instance of the Exmaralda importer
    let importer = jvm.create_instance(
        "org.corpus_tools.peppermodules.exmaralda.EXMARaLDAImporter",
        &vec![],
    )?;

    // Create a new document object that will be mapped
    let sdocument = jvm.invoke_static(
        "org.corpus_tools.salt.SaltFactory",
        "createSDocument",
        &vec![],
    )?;

    jvm.invoke(
        &jvm.cast(&sdocument, "org.corpus_tools.salt.core.SNamedElement")?,
        "setName",
        &vec![InvocationArg::try_from(document_name)?],
    )?;
    jvm.invoke(
        &jvm.cast(
            &sdocument,
            "org.corpus_tools.salt.graph.IdentifiableElement",
        )?,
        "setId",
        &vec![InvocationArg::try_from(&format!(
            "salt:/{}",
            document_name
        ))?],
    )?;

    let sdocument_identifier = get_identifier(&sdocument, jvm)?;

    // Get the identifier and link it with the URI
    let resource_table = jvm.invoke(&importer, "getIdentifier2ResourceTable", &vec![])?;
    let uri_as_string = InvocationArg::try_from(file_path.as_os_str().to_string_lossy().as_ref())?;
    let resource_uri = jvm.invoke_static(
        "org.eclipse.emf.common.util.URI",
        "createFileURI",
        &vec![uri_as_string],
    )?;

    jvm.invoke(
        &resource_table,
        "put",
        &vec![
            InvocationArg::from(sdocument_identifier),
            InvocationArg::from(resource_uri),
        ],
    )?;

    let sdocument_identifier = get_identifier(&sdocument, jvm)?;

    // Get an instance of the Salt to Exmaralda mapper from the importer
    let mapper = jvm.invoke(
        &importer,
        "createPepperMapper",
        &vec![InvocationArg::from(sdocument_identifier)],
    )?;

    prepare_mapper(&mapper, sdocument, jvm)?;

    // Invoke the internal mapper
    jvm.invoke(&mapper, "mapSDocument", &vec![])?;

    // Retrieve the reference to the created graph

    let u = GraphUpdate::new();
    // TODO: map Salt to GraphML
    Ok(u)
}

impl Importer for EXMARaLDAImporter {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        _properties: &std::collections::BTreeMap<String, String>,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut updates = GraphUpdate::new();

        // Create the corpus structure and all Java document objects
        let documents = crate::legacy::import_corpus_structure(
            input_path,
            Some(".*\\.(exb|xml|xmi|exmaralda)$"),
            &mut updates,
        )?;

        let num_of_documents = documents.len();
        let reporter = ProgressReporter::new(tx, self as &dyn Module, Some(input_path), num_of_documents + 1)?;

        //  Process all documents in parallel and merge graph updates afterwards
        let doc_updates: Result<Vec<_>, PepperError> = documents
            .into_par_iter()
            .map(move |(file_path, document_name)| {
                let jvm = self.create_jvm(false)?;
                map_document(file_path, &document_name, &jvm)
            })
            .collect();
        let doc_updates = doc_updates?;
        reporter.worked(num_of_documents)?;

        // merge graph updates for all documents into a single one
        let mut merged_graph_updates = GraphUpdate::default();
        for u in doc_updates.into_iter() {
            for (_, event) in u.iter()? {
                merged_graph_updates.add_event(event)?;
            }
        }
        reporter.worked(1)?;

        Ok(merged_graph_updates)
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
