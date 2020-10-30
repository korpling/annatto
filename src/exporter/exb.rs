use crate::{Module, exporter::Exporter, progress::ProgressReporter};

pub struct ExmaraldaExporter {}

impl ExmaraldaExporter {
    pub fn new() -> ExmaraldaExporter {
        ExmaraldaExporter {}
    }
}

impl Module for ExmaraldaExporter {
    fn module_name(&self) -> &str {
        "ExmaraldaExporter"
    }
}

impl Exporter for ExmaraldaExporter {
    fn export_corpus(
        &self,
        _graph: &graphannis::AnnotationGraph,
        _properties: &std::collections::BTreeMap<String, String>,
        output_path: &std::path::Path,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let reporter = ProgressReporter::new(tx, self as &dyn Module, Some(output_path));
        reporter.set_progress(0.0)?;
        let jvm = j4rs::JvmBuilder::new().build()?;

        todo!()
    }
}
