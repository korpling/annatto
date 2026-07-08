use std::path::Path;

use anyhow::Result;
use graphannis::update::GraphUpdate;

use crate::importer::paulaxml::{PaulaDirectory, PaulaDocument};

pub(super) struct DocumentMapper {}

impl DocumentMapper {
    pub(super) fn read_document(
        input_directory: &Path,
        _doc_node_name: &str,
        updates: &mut GraphUpdate,
    ) -> Result<()> {
        let paula_dir = PaulaDirectory::open_directory(input_directory)?;
        let paula_doc = PaulaDocument::from_directory(&paula_dir)?;

        let mapper = DocumentMapper {};
        mapper.map_tokens(&paula_doc, updates)?;
        // TODO: map structs
        // TODO: map pointers
        // TODO: map document metadata
        Ok(())
    }

    fn map_tokens(&self, _paula_doc: &PaulaDocument, _updates: &mut GraphUpdate) -> Result<()> {
        todo!()
    }
}
