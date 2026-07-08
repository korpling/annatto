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

    fn map_tokens(&self, paula_doc: &PaulaDocument, _updates: &mut GraphUpdate) -> Result<()> {
        // We need both the texts and the markables for each text to construct the graphANNIS token.
        let texts = paula_doc.by_header_type("text");

        for doc in paula_doc.document_by_id.values() {
            if let Some(tok_list) = doc
                .root_element()
                .children()
                .filter(|n| {
                    n.is_element()
                        && n.has_tag_name("markList")
                        && n.attribute("type")
                            .is_some_and(|a| a.to_lowercase() == "tok")
                })
                .next()
                && let Some(xml_uri) = tok_list.lookup_namespace_uri(Some("xml"))
                && let Some(base) = tok_list.attribute((xml_uri, "base"))
            {
                let markables: Vec<_> = tok_list
                    .children()
                    .filter(|n| n.tag_name().name() == "mark")
                    .collect();
                todo!()
            }
        }

        todo!()
    }
}
