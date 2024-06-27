use anyhow::{anyhow, Result};
use graphannis::update::GraphUpdate;
use itertools::Itertools;
use roxmltree::Node;

use super::{get_element_id, SaltType};

pub(super) struct DocumentMapper {
    base_texts: Vec<String>,
}

impl DocumentMapper {
    pub(super) fn read_document(
        input: &str,
        _document_node_name: &str,
        _updates: &mut GraphUpdate,
    ) -> Result<()> {
        let doc = roxmltree::Document::parse(input)?;
        let root = doc.root_element();
        if root.tag_name().name() != "SDocumentGraph" {
            return Err(anyhow!(
                "SaltXML document file must start with <SDocumentGraph> tag"
            ));
        }

        let layers = root
            .children()
            .filter(|n| SaltType::from(*n) == SaltType::Layer)
            .collect_vec();

        let mut mapper = DocumentMapper {
            base_texts: Vec::new(),
        };
        mapper.map_textual_ds(&root)?;
        mapper.map_token(&root, &layers)?;

        Ok(())
    }

    fn map_textual_ds(&mut self, root: &Node) -> Result<()> {
        for t in root
            .children()
            .filter(|n| SaltType::from(*n) == SaltType::TextualDs)
        {
            let element_id = get_element_id(&t)
                .ok_or_else(|| anyhow!("Missing element ID for textual data source"))?;
        }
        Ok(())
    }

    fn map_token(&self, root: &Node, layers: &[Node]) -> Result<()> {
        root.children()
            .filter(|n| SaltType::from(*n) == SaltType::Token);
        Ok(())
    }
}
