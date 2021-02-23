use j4rs::{Instance, InvocationArg, Jvm};

use crate::{error::Result, exporter::Exporter, Module};

use super::PepperPluginClasspath;

pub struct JavaExporter {
    java_exporter_qname: String,
    java_properties_class: String,
    module_name: String,
    classpath: PepperPluginClasspath,
}

impl JavaExporter {
    pub fn new(
        java_exporter_qname: &str,
        java_properties_class: &str,
        module_name: &str,
    ) -> Result<JavaExporter> {
        let classpath = PepperPluginClasspath::new()?;

        let exporter = JavaExporter {
            java_exporter_qname: java_exporter_qname.to_string(),
            java_properties_class: java_properties_class.to_string(),
            module_name: module_name.to_string(),
            classpath,
        };
        Ok(exporter)
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
}

impl Module for JavaExporter {
    fn module_name(&self) -> &str {
        &self.module_name
    }
}

impl Exporter for JavaExporter {
    fn export_corpus(
        &self,
        graph: &graphannis::AnnotationGraph,
        properties: &std::collections::BTreeMap<String, String>,
        output_path: &std::path::Path,
        tx: Option<crate::workflow::StatusSender>,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        todo!()
    }
}
