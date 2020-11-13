use crate::{exporter::Exporter, progress::ProgressReporter, Module};

pub struct EXMARaLDAExporter {}

impl EXMARaLDAExporter {
    pub fn new() -> EXMARaLDAExporter {
        EXMARaLDAExporter {}
    }
}

impl Module for EXMARaLDAExporter {
    fn module_name(&self) -> &str {
        "EXMARaLDAExporter"
    }
}

impl Exporter for EXMARaLDAExporter {
    fn export_corpus(
        &self,
        _graph: &graphannis::AnnotationGraph,
        _properties: &std::collections::BTreeMap<String, String>,
        output_path: &std::path::Path,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let reporter = ProgressReporter::new(tx, self as &dyn Module, Some(output_path), 1)?;
        let jvm = j4rs::JvmBuilder::new().build()?;

        // Create an instance of the Salt to Exmaralda mapper
        let mapper = jvm.create_instance(
            "org.corpus_tools.peppermodules.exmaralda.Salt2EXMARaLDAMapper",
            &vec![],
        )?;

        // TODO: call mapSDocument for each document in parallel
        //jvm.invoke(&mapper, "setDocument", &vec![document]);

        todo!()
    }
}
