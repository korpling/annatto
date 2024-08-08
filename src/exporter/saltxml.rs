use documented::{Documented, DocumentedFields};
use serde::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use super::Exporter;

/// Exports to the SaltXML format used by Pepper (<https://corpus-tools.org/pepper/>).
/// SaltXML is an XMI serialization of the [Salt model](https://raw.githubusercontent.com/korpling/salt/master/gh-site/doc/salt_modelGuide.pdf).
/// ```
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(deny_unknown_fields)]
pub struct ExportSaltXml {}

impl Exporter for ExportSaltXml {
    fn export_corpus(
        &self,
        _graph: &graphannis::AnnotationGraph,
        _output_path: &std::path::Path,
        _step_id: crate::StepID,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }

    fn file_extension(&self) -> &str {
        todo!()
    }
}
