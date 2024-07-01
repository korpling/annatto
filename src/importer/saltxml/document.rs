use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use graphannis::update::{GraphUpdate, UpdateEvent};
use itertools::Itertools;
use roxmltree::Node;

use super::{get_element_id, get_feature_by_qname, SaltObject, SaltType};

pub(super) struct DocumentMapper {
    base_texts: BTreeMap<String, String>,
}

impl DocumentMapper {
    pub(super) fn read_document(
        input: &str,
        _document_node_name: &str,
        updates: &mut GraphUpdate,
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
            base_texts: BTreeMap::new(),
        };
        mapper.map_textual_ds(&root, updates)?;
        mapper.map_token(&root, &layers)?;

        Ok(())
    }

    fn map_textual_ds(&mut self, root: &Node, updates: &mut GraphUpdate) -> Result<()> {
        for text_node in root
            .children()
            .filter(|n| SaltType::from(*n) == SaltType::TextualDs)
        {
            let element_id = get_element_id(&text_node)
                .ok_or_else(|| anyhow!("Missing element ID for textual data source"))?;

            if let Some(SaltObject::Text(anno_value)) =
                get_feature_by_qname(&text_node, "saltCommon", "SDATA")
            {
                self.base_texts.insert(element_id.clone(), anno_value);
                updates.add_event(UpdateEvent::AddNode {
                    node_name: element_id.clone(),
                    node_type: "datasource".to_string(),
                })?;
            }
        }
        Ok(())
    }

    fn map_token(&self, _root: &Node, _layers: &[Node]) -> Result<()> {
        // root.children()
        //     .filter(|n| SaltType::from(*n) == SaltType::Token);
        Ok(())
    }
}
