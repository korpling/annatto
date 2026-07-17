use std::path::Path;

use anyhow::Result;
use graphannis::update::GraphUpdate;
use itertools::Itertools;
use roxmltree::NS_XML_URI;

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

        for xml_doc in paula_doc.xml_document_by_id.values() {
            if let Some(tok_list) = xml_doc
                .root_element()
                .children()
                .filter(|n| {
                    n.is_element()
                        && n.has_tag_name("markList")
                        && n.attribute("type")
                            .is_some_and(|a| a.to_lowercase() == "tok")
                })
                .next()
                && let Some(base_uri) = tok_list.attribute((NS_XML_URI, "base"))
            {
                dbg!(base_uri);

                let markables: Vec<_> = tok_list
                    .children()
                    .filter(|n| n.tag_name().name() == "mark")
                    .collect();

                dbg!(markables);
                todo!()
            }
        }

        todo!()
    }
}

#[cfg(test)]
mod tests {

    use graphannis_core::errors::GraphAnnisCoreError;
    use insta::assert_debug_snapshot;

    use super::*;

    #[test]
    fn paula_xml_map_token() {
        let dir =
            PaulaDirectory::open_directory("tests/data/import/paulaxml/rootCorpus/subCorpus1/doc1")
                .unwrap();
        let doc1 = PaulaDocument::from_directory(&dir).unwrap();
        let mut updates = GraphUpdate::new();

        let mapper = DocumentMapper {};
        mapper.map_tokens(&doc1, &mut updates).unwrap();

        let events: Result<Vec<_>, GraphAnnisCoreError> = updates.iter().unwrap().collect();
        let events = events.unwrap();
        assert_debug_snapshot!(events);
    }
}
