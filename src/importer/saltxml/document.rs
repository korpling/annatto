use std::{
    collections::BTreeMap,
    convert::{TryFrom, TryInto},
};

use anyhow::{bail, Context, Result};
use graphannis::{
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::ANNIS_NS;
use itertools::Itertools;
use roxmltree::Node;

use super::{
    get_annotations, get_element_id, get_feature_by_qname, resolve_element, SaltObject, SaltType,
};

pub(super) struct DocumentMapper<'a, 'input> {
    nodes: Vec<Node<'a, 'input>>,
    edges: Vec<Node<'a, 'input>>,
    layers: Vec<Node<'a, 'input>>,
    base_texts: BTreeMap<String, String>,
    missing_anno_ns_from_layer: bool,
}

impl<'a, 'input> DocumentMapper<'a, 'input> {
    pub(super) fn read_document(
        input: &'input str,
        missing_anno_ns_from_layer: bool,
        updates: &mut GraphUpdate,
    ) -> Result<()> {
        let doc = roxmltree::Document::parse(input)?;
        let root = doc.root_element();
        if root.tag_name().name() != "SDocumentGraph" {
            bail!("SaltXML document file must start with <SDocumentGraph> tag");
        }

        let nodes = doc
            .root_element()
            .children()
            .filter(|n| n.tag_name().name() == "nodes")
            .collect_vec();

        let edges = doc
            .root_element()
            .children()
            .filter(|n| n.tag_name().name() == "edges")
            .collect_vec();

        let layers = doc
            .root_element()
            .children()
            .filter(|n| n.tag_name().name() == "layers")
            .collect_vec();

        let mut mapper = DocumentMapper {
            base_texts: BTreeMap::new(),
            missing_anno_ns_from_layer,
            nodes,
            edges,
            layers,
        };
        mapper.map_textual_datasources(updates)?;
        mapper.map_tokens(updates)?;

        mapper.map_non_token_nodes(updates)?;

        // TODO map STimeline and STimelineRelation
        // TODO map SOrderRelation for segmentation nodes
        // TODO map SAudioDS and SAudioRelation

        Ok(())
    }

    fn map_textual_datasources(&mut self, updates: &mut GraphUpdate) -> Result<()> {
        for text_node in self
            .nodes
            .iter()
            .filter(|n| SaltType::from_node(n) == SaltType::TextualDs)
        {
            let element_id =
                get_element_id(text_node).context("Missing element ID for textual data source")?;

            if let Some(SaltObject::Text(anno_value)) =
                get_feature_by_qname(text_node, "saltCommon", "SDATA")
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

    fn map_node(&self, n: &Node, updates: &mut GraphUpdate) -> Result<()> {
        let id = get_element_id(n).context("Missing element ID for node")?;
        updates.add_event(UpdateEvent::AddNode {
            node_name: id.clone(),
            node_type: "node".to_string(),
        })?;

        let mut fallback_annotation_namespace = "default_ns".to_string();

        if let Some(layers_attribute) = n.attribute("layers") {
            for layer_ref in layers_attribute.split(' ') {
                let layer_node = resolve_element(layer_ref, "layers", &self.layers)
                    .context("Could not resolve layer")?;
                if let Some(SaltObject::Text(layer_name)) =
                    get_feature_by_qname(&layer_node, "salt", "SNAME")
                {
                    // Use the edge layer as fallback annotation namespace. This is
                    // consistent with e.g. the ANNIS Tree Visualizer handles
                    // annotations without any namespace.
                    if self.missing_anno_ns_from_layer {
                        fallback_annotation_namespace.clone_from(&layer_name);
                    }

                    updates.add_event(UpdateEvent::AddNodeLabel {
                        node_name: id.clone(),
                        anno_ns: ANNIS_NS.to_owned(),
                        anno_name: "layer".to_owned(),
                        anno_value: layer_name,
                    })?;
                }
            }
        }

        for label_node in get_annotations(n) {
            let anno_ns = label_node
                .attribute("namespace")
                .unwrap_or(&fallback_annotation_namespace)
                .to_string();
            let anno_name = label_node
                .attribute("name")
                .context("Missing annotation name for node")?
                .to_string();
            let anno_value =
                SaltObject::from(label_node.attribute("value").unwrap_or_default()).to_string();
            updates.add_event(UpdateEvent::AddNodeLabel {
                node_name: id.clone(),
                anno_ns,
                anno_name,
                anno_value,
            })?;
        }
        Ok(())
    }

    fn map_edge(
        &self,
        rel: &Node,
        component_type: AnnotationComponentType,
        fallback_component_name: &str,
        updates: &mut GraphUpdate,
    ) -> Result<()> {
        let source_att_val = rel.attribute("source").unwrap_or_default();
        let source_element =
            resolve_element(source_att_val, "nodes", &self.nodes).context("Missing source node")?;
        let source_id = get_element_id(&source_element).context("Missing source node ID")?;

        let target_att_val = rel.attribute("target").unwrap_or_default();
        let target_element =
            resolve_element(target_att_val, "nodes", &self.nodes).context("Missing target node")?;
        let target_id = get_element_id(&target_element).context("Missing target node ID")?;

        let component_name = get_feature_by_qname(rel, "salt", "STYPE")
            .map(|t| t.to_string())
            .unwrap_or_else(|| fallback_component_name.to_string());

        let mut component_layer = "default_ns".to_string();
        if let Some(layers_attribute) = rel.attribute("layers") {
            if let Some(first_layer) = layers_attribute.split(' ').next() {
                component_layer = first_layer.to_string();
            }
        }

        updates.add_event(UpdateEvent::AddEdge {
            source_node: source_id.clone(),
            target_node: target_id.clone(),
            layer: component_layer.clone(),
            component_type: component_type.to_string(),
            component_name: component_name.clone(),
        })?;

        if component_type == AnnotationComponentType::Dominance {
            // Also add to the special component with the empty name, which includes all dominance edges from all STypes.
            updates.add_event(UpdateEvent::AddEdge {
                source_node: source_id.clone(),
                target_node: target_id.clone(),
                layer: ANNIS_NS.to_string(),
                component_type: component_type.to_string(),
                component_name: "".to_string(),
            })?;
        }

        let fallback_annotation_namespace = if self.missing_anno_ns_from_layer {
            &component_layer
        } else {
            "default_ns"
        };

        for label_element in get_annotations(rel) {
            let anno_ns = label_element
                .attribute("namespace")
                .unwrap_or(fallback_annotation_namespace)
                .to_string();

            let anno_name = label_element
                .attribute("name")
                .context("Missing annotation name for edge")?
                .to_string();
            let anno_value =
                SaltObject::from(label_element.attribute("value").unwrap_or_default()).to_string();
            updates.add_event(UpdateEvent::AddEdgeLabel {
                source_node: source_id.clone(),
                target_node: target_id.clone(),
                layer: component_layer.clone(),
                component_type: component_type.to_string(),
                component_name: component_name.clone(),
                anno_ns,
                anno_name,
                anno_value,
            })?;
        }
        Ok(())
    }

    fn map_tokens(&self, updates: &mut GraphUpdate) -> Result<()> {
        // Map the token nodes in the same order as in the SaltXML file
        for token_node in self
            .nodes
            .iter()
            .filter(|n| SaltType::from_node(n) == SaltType::Token)
        {
            self.map_node(token_node, updates)?;
        }

        // Order textual relations by their start offset, so we iterate in the
        // actual order of the tokens.
        let sorted_text_rels: BTreeMap<i64, _> = self
            .edges
            .iter()
            .filter(|n| SaltType::from_node(n) == SaltType::TextualRelation)
            .map(|text_rel| {
                let start =
                    get_feature_by_qname(text_rel, "salt", "SSTART").unwrap_or(SaltObject::Null);
                if let SaltObject::Integer(start) = start {
                    (start, *text_rel)
                } else {
                    (-1, *text_rel)
                }
            })
            .collect();

        // Connect the token to the texts by the textual relations
        let mut previous_token = None;
        let mut sorted_text_rels = sorted_text_rels.into_iter().peekable();
        while let Some((_, text_rel)) = sorted_text_rels.next() {
            let source_att_val = text_rel.attribute("source").unwrap_or_default();
            let token =
                resolve_element(source_att_val, "nodes", &self.nodes).with_context(|| {
                    format!("Textual relation source \"{source_att_val}\" could not be resolved")
                })?;

            let target_att_val = text_rel.attribute("target").unwrap_or_default();
            let datasource =
                resolve_element(target_att_val, "nodes", &self.nodes).with_context(|| {
                    format!("Textual relation target \"{target_att_val}\" could not be resolved")
                })?;
            let token_id = get_element_id(&token).context("Missing ID for token")?;
            let datasource_id = get_element_id(&datasource).context("Missing ID for token")?;

            // Get the string for this token
            let matching_base_text = self
                .base_texts
                .get(&datasource_id)
                .with_context(|| format!("Missing base text for token {token_id}"))?;
            // Our indices are refering to characters not bytes
            let matching_base_text = matching_base_text.chars().collect_vec();
            let start =
                get_feature_by_qname(&text_rel, "salt", "SSTART").context("Missing start value")?;
            let end =
                get_feature_by_qname(&text_rel, "salt", "SEND").context("Missing end value")?;
            if let (SaltObject::Integer(start), SaltObject::Integer(end)) = (start, end) {
                let start = usize::try_from(start)?;
                let end = usize::try_from(end)?;
                let covered_text = &matching_base_text[start..end];
                updates.add_event(UpdateEvent::AddNodeLabel {
                    node_name: token_id.clone(),
                    anno_ns: ANNIS_NS.to_string(),
                    anno_name: "tok".to_string(),
                    anno_value: covered_text.iter().collect(),
                })?;

                // Get the whitespace before the first token
                if previous_token.is_none() && start > 0 {
                    let whitespace = &matching_base_text[0..start];
                    updates.add_event(UpdateEvent::AddNodeLabel {
                        node_name: token_id.clone(),
                        anno_ns: ANNIS_NS.to_string(),
                        anno_name: "tok-whitespace-before".to_string(),
                        anno_value: whitespace.iter().collect(),
                    })?;
                }

                // Add whitespace after this token
                let next_token_offset = sorted_text_rels
                    .peek()
                    .map(|(offset, _)| *offset)
                    .unwrap_or_else(|| matching_base_text.len().try_into().unwrap_or(i64::MAX));
                let next_token_offset = usize::try_from(next_token_offset).unwrap_or(0);

                if next_token_offset > end && (next_token_offset - end) >= 1 {
                    let whitespace = &matching_base_text[end..next_token_offset];
                    updates.add_event(UpdateEvent::AddNodeLabel {
                        node_name: token_id.clone(),
                        anno_ns: ANNIS_NS.to_string(),
                        anno_name: "tok-whitespace-after".to_string(),
                        anno_value: whitespace.iter().collect(),
                    })?;
                }
            }
            // Add ordering edges between the tokens for the base token layer
            if let Some(previous_token) = previous_token {
                updates.add_event(UpdateEvent::AddEdge {
                    source_node: previous_token,
                    target_node: token_id.clone(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "".to_string(),
                })?;
            }
            previous_token = Some(token_id);
        }

        Ok(())
    }

    fn map_non_token_nodes(&self, updates: &mut GraphUpdate) -> Result<()> {
        for span_node in self.nodes.iter().filter(|n| {
            let t = SaltType::from_node(n);
            t == SaltType::Span || t == SaltType::Structure
        }) {
            self.map_node(span_node, updates)?;
        }

        // Connect all spans with the token using the spanning relations
        for spanning_rel in self
            .edges
            .iter()
            .filter(|rel| SaltType::from_node(rel) == SaltType::SpanningRelation)
        {
            self.map_edge(spanning_rel, AnnotationComponentType::Coverage, "", updates)?;
        }
        // Add all dominance relations
        for dominance_rel in self
            .edges
            .iter()
            .filter(|rel| SaltType::from_node(rel) == SaltType::DominanceRelation)
        {
            self.map_edge(
                dominance_rel,
                AnnotationComponentType::Dominance,
                "edge",
                updates,
            )?;
        }

        // Add all pointing relations
        for pointing_rel in self
            .edges
            .iter()
            .filter(|rel| SaltType::from_node(rel) == SaltType::PointingRelation)
        {
            self.map_edge(
                pointing_rel,
                AnnotationComponentType::Pointing,
                "edge",
                updates,
            )?;
        }
        Ok(())
    }
}
