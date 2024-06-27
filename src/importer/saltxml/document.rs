use anyhow::anyhow;
use graphannis::update::GraphUpdate;

pub(super) struct DocumentMapper {}

impl DocumentMapper {
    pub(super) fn new() -> DocumentMapper {
        DocumentMapper {}
    }

    pub(super) fn read_document(
        &self,
        input: &str,
        _document_node_name: &str,
        _updates: &mut GraphUpdate,
    ) -> anyhow::Result<()> {
        let doc = roxmltree::Document::parse(input)?;

        let root = doc.root_element();
        if root.tag_name().name() != "SDocumentGraph" {
            return Err(anyhow!(
                "SaltXML document file must start with <SDocumentGraph> tag"
            ));
        }

        Ok(())
    }
}
