use graphannis::update::GraphUpdate;
use j4rs::{Instance, InvocationArg, Jvm};
use rayon::prelude::*;
use std::{convert::TryFrom, path::PathBuf};

use crate::{
    error::{PepperError, Result},
    importer::Importer,
    progress::ProgressReporter,
    Module,
};

use super::{salt::get_identifier, PepperPluginClasspath};

pub struct JavaImporter {
    java_importer_qname: String,
    java_properties_class: String,
    module_name: String,
    file_pattern: Option<String>,
    classpath: PepperPluginClasspath,
}

impl JavaImporter {
    pub fn new(
        java_importer_qname: &str,
        java_properties_class: &str,
        module_name: &str,
        file_pattern: Option<&str>,
    ) -> Result<JavaImporter> {
        let classpath = PepperPluginClasspath::new()?;

        let importer = JavaImporter {
            java_importer_qname: java_importer_qname.to_string(),
            java_properties_class: java_properties_class.to_string(),
            module_name: module_name.to_string(),
            file_pattern: file_pattern.map(|s| s.to_string()),
            classpath,
        };
        Ok(importer)
    }

    fn prepare_mapper(&self, mapper: &Instance, document: Instance, jvm: &Jvm) -> Result<()> {
        // Create and set an empty property map
        let props = jvm.create_instance(&self.java_properties_class, &[])?;
        // TODO: set the property values from the importer in Java
        jvm.invoke(mapper, "setProperties", &[InvocationArg::from(props)])?;

        // Explicitly set the document object
        jvm.invoke(&mapper, "setDocument", &[InvocationArg::from(document)])?;
        Ok(())
    }

    fn map_document(
        &self,
        file_path: PathBuf,
        document_id: &str,
        jvm: &Jvm,
    ) -> Result<GraphUpdate> {
        // Create an instance of the Java importer
        let importer = jvm.create_instance(&self.java_importer_qname, &[])?;

        // Create a new document object that will be mapped
        let sdocument =
            jvm.invoke_static("org.corpus_tools.salt.SaltFactory", "createSDocument", &[])?;

        jvm.invoke(
            &jvm.cast(&sdocument, "org.corpus_tools.salt.core.SNamedElement")?,
            "setName",
            &[InvocationArg::try_from(document_id)?],
        )?;
        jvm.invoke(
            &jvm.cast(
                &sdocument,
                "org.corpus_tools.salt.graph.IdentifiableElement",
            )?,
            "setId",
            &[InvocationArg::try_from(&format!("salt:/{}", document_id))?],
        )?;

        let sdocument_identifier = get_identifier(&sdocument, jvm)?;

        // Get the identifier and link it with the URI
        let resource_table = jvm.invoke(&importer, "getIdentifier2ResourceTable", &[])?;
        let uri_as_string =
            InvocationArg::try_from(file_path.as_os_str().to_string_lossy().as_ref())?;
        let resource_uri = jvm.invoke_static(
            "org.eclipse.emf.common.util.URI",
            "createFileURI",
            &[uri_as_string],
        )?;

        jvm.invoke(
            &resource_table,
            "put",
            &[
                InvocationArg::from(sdocument_identifier),
                InvocationArg::from(resource_uri),
            ],
        )?;

        let sdocument_identifier = get_identifier(&sdocument, jvm)?;

        // Get an instance of the Salt to Exmaralda mapper from the importer
        let mapper = jvm.invoke(
            &importer,
            "createPepperMapper",
            &[InvocationArg::from(sdocument_identifier)],
        )?;

        self.prepare_mapper(&mapper, sdocument, jvm)?;

        // Invoke the internal mapper
        let document_status = jvm.invoke(&mapper, "mapSDocument", &[])?;

        // Check if conversion was successful
        let document_status = jvm.invoke(&document_status, "getName", &[])?;
        let document_status: String = jvm.to_rust(document_status)?;
        if document_status != "COMPLETED" {
            return Err(PepperError::Import {
                reason: format!("Legacy importer module returned status {}", document_status),
                importer: self.module_name.to_string(),
                path: file_path,
            });
        }

        // Retrieve the reference to the created graph and map Salt to graph updates
        let document = jvm.invoke(&mapper, "getDocument", &[])?;
        let graph = jvm.invoke(&document, "getDocumentGraph", &[])?;
        let u = super::salt::map_from::map_document_graph(graph, document_id, jvm)?;
        Ok(u)
    }
}

impl Module for JavaImporter {
    fn module_name(&self) -> &str {
        &self.module_name
    }
}

impl Importer for JavaImporter {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        _properties: &std::collections::BTreeMap<String, String>,
        tx: Option<crate::workflow::StatusSender>,
    ) -> std::result::Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut updates = GraphUpdate::new();

        // Create the corpus structure and all Java document objects
        let documents = crate::legacy::import_corpus_structure(
            input_path,
            self.file_pattern.as_deref(),
            &mut updates,
        )?;

        let num_of_documents = documents.len();
        let reporter = ProgressReporter::new(
            tx,
            self as &dyn Module,
            Some(input_path),
            num_of_documents + 1,
        )?;

        //  Process all documents in parallel and merge graph updates afterwards
        let doc_updates: Result<Vec<_>> = documents
            .into_par_iter()
            .map(|(file_path, document_name)| {
                let jvm = self.classpath.create_jvm(false)?;
                let updates_for_document = self.map_document(file_path, &document_name, &jvm)?;
                reporter.worked(1)?;
                Ok(updates_for_document)
            })
            .collect();
        let doc_updates = doc_updates?;

        // merge graph updates for all documents into a single one
        for u in doc_updates.into_iter() {
            for (_, event) in u.iter()? {
                updates.add_event(event)?;
            }
        }
        reporter.worked(1)?;

        Ok(updates)
    }
}
