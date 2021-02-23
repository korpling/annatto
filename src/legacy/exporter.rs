use graphannis::model::AnnotationComponentType;
use graphannis_core::{
    annostorage::ValueSearch,
    graph::{ANNIS_NS, NODE_NAME_KEY, NODE_TYPE},
};
use j4rs::{Instance, InvocationArg, Jvm};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::{
    error::Result, exporter::Exporter, progress::ProgressReporter, workflow::StatusSender, Module,
};

use super::PepperPluginClasspath;

pub struct JavaExporter {
    java_exporter_qname: String,
    java_properties_class: String,
    module_name: String,
    classpath: PepperPluginClasspath,
    map_parallel: bool,
}

impl JavaExporter {
    pub fn new(
        java_exporter_qname: &str,
        java_properties_class: &str,
        module_name: &str,
        map_parallel: bool,
    ) -> Result<JavaExporter> {
        let classpath = PepperPluginClasspath::new()?;

        let exporter = JavaExporter {
            java_exporter_qname: java_exporter_qname.to_string(),
            java_properties_class: java_properties_class.to_string(),
            module_name: module_name.to_string(),
            map_parallel,
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

    fn map_document(
        &self,
        graph: &graphannis::AnnotationGraph,
        document_id: &str,
        properties: &std::collections::BTreeMap<String, String>,
        jvm: &Jvm,
        output_path: &std::path::Path,
    ) -> Result<()> {
        todo!()
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
        tx: Option<StatusSender>,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        if let Some(part_of) = graph
            .get_all_components(Some(AnnotationComponentType::PartOf), None)
            .first()
        {
            if let Some(part_of) = graph.get_graphstorage_as_ref(part_of) {
                let node_annos = graph.get_node_annos();

                // Collect all documents for the corpus graph by querying the graph
                let documents: Vec<_> = node_annos
                    .exact_anno_search(Some(ANNIS_NS), NODE_TYPE, ValueSearch::Some("corpus"))
                    .filter(|m| !part_of.has_outgoing_edges(m.node))
                    .filter_map(|m| node_annos.get_value_for_item(&m.node, &NODE_NAME_KEY))
                    .collect();
                let num_of_documents = documents.len();
                let reporter = ProgressReporter::new(
                    tx,
                    self as &dyn Module,
                    Some(output_path),
                    num_of_documents,
                )?;

                if self.map_parallel {
                    let mapping_results: Result<Vec<_>> = documents
                        .into_par_iter()
                        .map(|document_id| {
                            let jvm = self.classpath.create_jvm(false)?;
                            self.map_document(graph, &document_id, properties, &jvm, output_path)?;
                            reporter.worked(1)?;
                            Ok(())
                        })
                        .collect();
                    // Return an error if any single mapping resulted in an error
                    mapping_results?;
                } else {
                    let jvm = self.classpath.create_jvm(false)?;
                    for document_id in documents {
                        self.map_document(graph, &document_id, properties, &jvm, output_path)?;
                        reporter.worked(1)?;
                    }
                }
            }
        }

        Ok(())
    }
}
