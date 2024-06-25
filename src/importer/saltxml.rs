use documented::{Documented, DocumentedFields};
use serde::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use super::Importer;

/// Imports the SaltXML format used by Pepper (<https://corpus-tools.org/pepper/>).
/// SaltXML is an XMI serialization of the [Salt model](https://raw.githubusercontent.com/korpling/salt/master/gh-site/doc/salt_modelGuide.pdf).
#[derive(Default, Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(default, deny_unknown_fields)]
pub struct ImportSaltXml {}

impl Importer for ImportSaltXml {
    fn import_corpus(
        &self,
        _input_path: &std::path::Path,
        _step_id: crate::StepID,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        todo!()
    }

    fn file_extensions(&self) -> &[&str] {
        todo!()
    }
}
