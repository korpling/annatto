use documented::{Documented, DocumentedFields};
use serde::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use super::Exporter;

/// Exports Excel Spreadsheets where each line is a token, the other columns are
/// spans and merged cells can be used for spans that cover more than one token.
#[derive(Default, Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(default, deny_unknown_fields)]
pub struct SaltXmlExporter {}

impl Exporter for SaltXmlExporter {
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
