use graphannis::update::GraphUpdate;
use jni::{objects::JObject, objects::JValue, AttachGuard, InitArgsBuilder, JNIVersion, JavaVM};
use rayon::prelude::*;
use std::{convert::TryFrom, path::PathBuf};

use crate::{error::PepperError, importer::Importer, progress::ProgressReporter, Module};

use super::PepperPluginClasspath;

pub struct JavaImporter {
    java_importer_qname: String,
    java_properties_class: String,
    module_name: String,
    file_pattern: Option<String>,
    jvm: JavaVM,
    _classpath: PepperPluginClasspath,
}

impl JavaImporter {
    pub fn new(
        java_importer_qname: &str,
        java_properties_class: &str,
        module_name: &str,
        file_pattern: Option<&str>,
    ) -> Result<JavaImporter, PepperError> {
        let classpath = PepperPluginClasspath::new()?;
        let jvm_args = InitArgsBuilder::new()
            .version(JNIVersion::V8)
            .option("-Xcheck:jni")
            .option(&classpath.get_classpath_argument())
            .build()?;
        let jvm = JavaVM::new(jvm_args)?;

        let importer = JavaImporter {
            java_importer_qname: java_importer_qname.to_string(),
            java_properties_class: java_properties_class.to_string(),
            module_name: module_name.to_string(),
            file_pattern: file_pattern.map(|s| s.to_string()),
            jvm,
            _classpath: classpath,
        };
        Ok(importer)
    }

    fn prepare_mapper(
        &self,
        mapper: &JObject,
        document: JObject,
        env: &AttachGuard,
    ) -> Result<(), PepperError> {
        // Create and set an empty property map
        let props = env.new_object(env.find_class(&self.java_properties_class)?, "()V", &vec![])?;

        env.call_method(
            mapper.clone(),
            "setProperties",
            "(Lorg/corpus_tools/pepper/modules/PepperModuleProperties;)V",
            &vec![JValue::Object(props)],
        )?;
        // TODO: set the property values from the importer in Java

        env.call_method(
            mapper.clone(),
            "setDocument",
            "(Lorg/corpus_tools/salt/common/SDocument;)V",
            &vec![JValue::Object(document)],
        )?;

        Ok(())
    }

    fn map_document(
        &self,
        file_path: PathBuf,
        document_name: &str,
    ) -> Result<GraphUpdate, PepperError> {
        let env = self.jvm.attach_current_thread()?;

        // Create an instance of the Java importer
        let importer =
            env.new_object(env.find_class(&self.java_importer_qname)?, "()V", &vec![])?;

        let salt_factory = env.find_class("org/corpus_tools/salt/SaltFactory")?;
        // Create a new document object that will be mapped
        let sdocument = env.call_static_method(
            salt_factory,
            "createSDocument",
            "()Lorg/corpus_tools/salt/common/SDocument;",
            &vec![],
        )?;
        env.call_method(
            sdocument.l()?,
            "setName",
            "(Ljava/lang/String;)V",
            &vec![JValue::try_from(env.new_string(document_name)?)?],
        )?;
        env.call_method(
            sdocument.l()?,
            "setId",
            "(Ljava/lang/String;)V",
            &vec![JValue::try_from(
                env.new_string(&format!("salt:/{}", document_name))?,
            )?],
        )?;

        let sdocument_identifier = env.call_method(
            sdocument.l()?,
            "getIdentifier",
            "()Lorg/corpus_tools/salt/graph/Identifier;",
            &vec![],
        )?;

        // Get the identifier and link it with the URI
        let resource_table = env.call_method(
            importer.clone(),
            "getIdentifier2ResourceTable",
            "()Ljava/util/Map;",
            &vec![],
        )?;
        let uri_as_string = env.new_string(file_path.as_os_str().to_string_lossy())?;
        let resource_uri = env.call_static_method(
            "org/eclipse/emf/common/util/URI",
            "createFileURI",
            "(Ljava/lang/String;)Lorg/eclipse/emf/common/util/URI;",
            &vec![JValue::try_from(uri_as_string)?],
        )?;
        env.call_method(
            resource_table.l()?,
            "put",
            "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
            &vec![
                JValue::try_from(sdocument_identifier)?,
                JValue::try_from(resource_uri)?,
            ],
        )?;

        // Get an instance of the Pepper mapper from the importer
        let mapper = env.call_method(
            importer.clone(),
            "createPepperMapper",
            "(Lorg/corpus_tools/salt/graph/Identifier;)Lorg/corpus_tools/pepper/modules/PepperMapper;",
            &vec![JValue::try_from(sdocument_identifier)?],
        )?;

        self.prepare_mapper(&mapper.l()?, sdocument.l()?, &env)?;

        // Invoke the internal mapper
        env.call_method(
            mapper.l()?,
            "mapSDocument",
            "()Lorg/corpus_tools/pepper/common/DOCUMENT_STATUS;",
            &vec![],
        )?;

        // TODO: Retrieve the reference to the created graph

        let u = GraphUpdate::new();
        // TODO: map Salt to graph updates
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
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
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
        let doc_updates: Result<Vec<_>, PepperError> = documents
            .into_par_iter()
            .map(|(file_path, document_name)| {
                let updates_for_document = self.map_document(file_path, &document_name)?;
                reporter.worked(1)?;
                Ok(updates_for_document)
            })
            .collect();
        let doc_updates = doc_updates?;

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
    fn import_exb_corpus() {
        let importer = JavaImporter::new(
            "org/corpus_tools/peppermodules/exmaralda/EXMARaLDAImporter",
            "org/corpus_tools/peppermodules/exmaralda/EXMARaLDAImporterProperties",
            "EXMARaLDAImporter",
            Some(".*\\.(exb|xml|xmi|exmaralda)$"),
        )
        .unwrap();
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
