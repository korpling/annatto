use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use graphannis::update::{GraphUpdate, UpdateEvent};
use roxmltree::Document;

use super::{get_element_id, get_feature_by_qname, SaltObject, SaltType};

pub(super) struct DocumentMapper<'input> {
    document: Document<'input>,
    base_texts: BTreeMap<String, String>,
}

impl<'input> DocumentMapper<'input> {
    pub(super) fn read_document(
        input: &'input str,
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

        //        let layers = root
        //            .children()
        //            .filter(|n| SaltType::from(*n) == SaltType::Layer)
        //            .collect_vec();

        let mut mapper = DocumentMapper {
            base_texts: BTreeMap::new(),
            document: doc,
        };
        mapper.map_textual_ds(updates)?;
        mapper.map_token(updates)?;

        Ok(())
    }

    fn map_textual_ds(&mut self, updates: &mut GraphUpdate) -> Result<()> {
        for text_node in self
            .document
            .root_element()
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

    fn map_token(&self, updates: &mut GraphUpdate) -> Result<()> {
        // Get the list of token in the same order as in the SaltXML file
        let tokens: Result<Vec<_>> = self
            .document
            .root_element()
            .children()
            .filter(|n| n.tag_name().name() == "nodes" && SaltType::from(*n) == SaltType::Token)
            .map(|t| {
                let id = get_element_id(&t)
                    .ok_or_else(|| anyhow!("Missing element ID for token source"))?;
                Ok((t, id))
            })
            .collect();
        let tokens = tokens?;

        for (_, t_id) in tokens.iter() {
            updates.add_event(UpdateEvent::AddNode {
                node_name: t_id.clone(),
                node_type: "node".to_string(),
            })?;
        }
        // Connect the token to the texts by the textual relations
        for _text_rel in self
            .document
            .root_element()
            .children()
            .filter(|n| n.tag_name().name() == "edges" && SaltType::from(*n) == SaltType::Token)
        {
        }

        Ok(())
    }
}
