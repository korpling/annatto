mod corpus_structure;
mod document;
#[cfg(test)]
mod tests;

use corpus_structure::SaltCorpusStructureMapper;
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
        graph: &graphannis::AnnotationGraph,
        output_path: &std::path::Path,
        _step_id: crate::StepID,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let project_file = output_path.join("saltProject.salt");
        let mapper = SaltCorpusStructureMapper::new();
        mapper.map_corpus_structure(graph, &project_file)?;

        Ok(())
    }

    fn file_extension(&self) -> &str {
        ".salt"
    }
}
