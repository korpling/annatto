use graphannis::model::AnnotationComponentType;
use graphannis_core::{
    annostorage::ValueSearch,
    graph::{ANNIS_NS, NODE_NAME_KEY, NODE_TYPE},
};
use j4rs::{Instance, InvocationArg, Jvm};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::{
    error::{PepperError, Result},
    exporter::Exporter,
    legacy::{
        prepare_mapper,
        salt::{get_identifier, map_to::map_document_graph},
    },
    progress::ProgressReporter,
    workflow::StatusSender,
    Module,
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

    fn map_document(
        &self,
        graph: &graphannis::AnnotationGraph,
        document_id: &str,
        properties: &std::collections::BTreeMap<String, String>,
        jvm: &Jvm,
        output_path: &std::path::Path,
    ) -> Result<()> {
        // Create an instance of the Java exporter
        let exporter = jvm.create_instance(&self.java_exporter_qname, &[])?;

        // Create an SDocument the exporter will use
        let sdocument = map_document_graph(graph, document_id, jvm)?;
        let sdocument_identifier = get_identifier(&sdocument, jvm)?;
        // Get an instance of the mapper from the exporter
        let mapper = jvm.invoke(
            &exporter,
            "createPepperMapper",
            &[InvocationArg::from(sdocument_identifier)],
        )?;

        prepare_mapper(
            &mapper,
            sdocument,
            &self.java_properties_class,
            properties,
            jvm,
        )?;

        // Invoke the internal mapper
        let document_status = jvm.invoke(&mapper, "mapSDocument", &[])?;

        // Check if conversion was successful
        let document_status = jvm.invoke(&document_status, "getName", &[])?;
        let document_status: String = jvm.to_rust(document_status)?;
        if document_status != "COMPLETED" {
            return Err(PepperError::Export {
                reason: format!("Legacy exporter module returned status {}", document_status),
                exporter: self.module_name.to_string(),
                path: output_path.to_path_buf(),
            });
        }

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
