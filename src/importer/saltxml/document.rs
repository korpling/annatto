use core::f64;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    convert::{TryFrom, TryInto},
    path::Path,
};

use anyhow::{Context, Result, bail};
use graphannis::{
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::{ANNIS_NS, DEFAULT_NS};
use itertools::Itertools;
use normpath::{BasePathBuf, PathExt};
use roxmltree::Node;
use url::Url;

use super::{
    SaltObject, SaltType, get_annotations, get_element_id, get_feature_by_qname, resolve_element,
};

#[derive(Eq, PartialEq, PartialOrd, Ord, Hash, Clone, Debug)]
pub struct TextProperty {
    segmentation: String,
    val: i64,
}

pub(super) struct DocumentMapper<'a, 'input> {
    nodes: Vec<Node<'a, 'input>>,
    edges: Vec<Node<'a, 'input>>,
    layers: Vec<Node<'a, 'input>>,
    base_texts: BTreeMap<String, String>,
    media_files: BTreeMap<String, String>,
    document_node_name: String,
    token_to_tli: BTreeMap<String, Vec<i64>>,
    missing_anno_ns_from_layer: bool,
    input_directory: BasePathBuf,
}

impl<'input> DocumentMapper<'_, 'input> {
    pub(super) fn read_document(
        input: &'input str,
        input_path: &Path,
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
        let document_node_name =
            get_element_id(&doc.root_element()).context("Missing document ID")?;

        let input_directory = input_path.parent().unwrap_or(input_path).normalize()?;

        let mut mapper = DocumentMapper {
            base_texts: BTreeMap::new(),
            media_files: BTreeMap::new(),
            missing_anno_ns_from_layer,
            nodes,
            edges,
            layers,
            document_node_name,
            token_to_tli: BTreeMap::new(),
            input_directory,
        };

        let timeline = mapper
            .nodes
            .iter()
            .filter(|n| SaltType::from_node(n) == SaltType::Timeline)
            .copied()
            .next();

        mapper.map_textual_datasources(updates)?;
        mapper.map_media_datasources(updates)?;
        mapper.map_tokens(timeline.as_ref(), updates)?;
        if let Some(timeline) = timeline {
            mapper.map_timeline(&timeline, updates)?;
        }
        if !mapper.media_files.is_empty() {
            mapper.map_media_relations(updates)?;
        }

        mapper.map_non_token_nodes(updates)?;

        Ok(())
    }

    fn get_tli_node_name(&self, tli: i64) -> String {
        format!("{}#tli{tli}", self.document_node_name)
    }

    fn map_timeline(&mut self, timeline: &Node, updates: &mut GraphUpdate) -> Result<()> {
        let number_of_tlis = get_feature_by_qname(timeline, "saltCommon", "SDATA")
            .context("Missing SDATA attribute for timeline.")?;
        if let SaltObject::Integer(number_of_tlis) = number_of_tlis {
            let mut previous_tli = None;
            for i in 0..number_of_tlis {
                let tli_node_name = self.get_tli_node_name(i);
                updates.add_event(UpdateEvent::AddNode {
                    node_name: tli_node_name.clone(),
                    node_type: "node".to_string(),
                })?;
                updates.add_event(UpdateEvent::AddNodeLabel {
                    node_name: tli_node_name.clone(),
                    anno_ns: ANNIS_NS.to_string(),
                    anno_name: "tok".to_string(),
                    anno_value: " ".to_string(),
                })?;
                updates.add_event(UpdateEvent::AddEdge {
                    source_node: tli_node_name.clone(),
                    target_node: self.document_node_name.clone(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::PartOf.to_string(),
                    component_name: "".to_string(),
                })?;

                if let Some(previous_tli) = previous_tli {
                    updates.add_event(UpdateEvent::AddEdge {
                        source_node: previous_tli,
                        target_node: tli_node_name.clone(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::Ordering.to_string(),
                        component_name: "".to_string(),
                    })?;
                }

                previous_tli = Some(tli_node_name);
            }
        } else {
            bail!("SDATA attribute for timeline is not a number.")
        }

        // Connect the existing non-timeline tokens with the the timeline tokens
        for timeline_rel in self
            .edges
            .iter()
            .filter(|rel| SaltType::from_node(rel) == SaltType::TimelineRelation)
        {
            let source_att = timeline_rel.attribute("source").unwrap_or_default();
            let token_node = resolve_element(source_att, "nodes", &self.nodes)
                .context("Token referenced in STimelineRelation cannot be resolved")?;
            let token_id = get_element_id(&token_node).context("Token has no ID")?;

            let start = get_feature_by_qname(timeline_rel, "salt", "SSTART")
                .context("Missing SSTART attribute for timeline relation")?;
            let end = get_feature_by_qname(timeline_rel, "salt", "SEND")
                .context("Missing SEND attribute for timeline relation")?;

            if let (SaltObject::Integer(start), SaltObject::Integer(end)) = (start, end) {
                for tli in start..end {
                    updates.add_event(UpdateEvent::AddEdge {
                        source_node: token_id.clone(),
                        target_node: self.get_tli_node_name(tli),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::Coverage.to_string(),
                        component_name: "".to_string(),
                    })?;
                }
                self.token_to_tli
                    .insert(token_id, (start..end).collect_vec());
            } else {
                bail!("SSTART/SEND not an integer")
            }
        }

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

                updates.add_event(UpdateEvent::AddEdge {
                    source_node: element_id.clone(),
                    target_node: self.document_node_name.clone(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::PartOf.to_string(),
                    component_name: "".to_string(),
                })?;
            }
        }
        Ok(())
    }

    fn map_media_datasources(&mut self, updates: &mut GraphUpdate) -> Result<()> {
        for media_node in self
            .nodes
            .iter()
            .filter(|n| SaltType::from_node(n) == SaltType::MediaDs)
        {
            let orig_element_id = get_element_id(media_node)
                .context("Missing element ID for media/audio data source")?;

            if let Some(SaltObject::Url(anno_value)) =
                get_feature_by_qname(media_node, "salt", "SAUDIO_REFERENCE")
            {
                // Parse the file URL with the input file location as base path
                let base_dir = Url::from_directory_path(self.input_directory.canonicalize()?).ok();
                let referenced_url = Url::options()
                    .base_url(base_dir.as_ref())
                    .parse(&anno_value)?;

                let mut element_id = orig_element_id;
                let mut file_path = referenced_url.to_string();
                if referenced_url.scheme() == "file" {
                    // Resolve this file URL against the input directory and
                    // store it relative to the current working directory.
                    let referenced_path = Path::new(referenced_url.path());
                    let referenced_path = pathdiff::diff_paths(
                        referenced_path.normalize()?,
                        &std::env::current_dir()?,
                    )
                    .unwrap_or_else(|| referenced_path.to_path_buf());

                    file_path = referenced_path.to_string_lossy().to_string();

                    // Use the file name as element ID
                    if let Some(file_name) = referenced_path.file_name() {
                        element_id = format!(
                            "{}/{}",
                            self.document_node_name,
                            file_name.to_string_lossy()
                        );
                    }
                };
                updates.add_event(UpdateEvent::AddNode {
                    node_name: element_id.clone(),
                    node_type: "file".to_string(),
                })?;

                self.media_files
                    .insert(element_id.clone(), file_path.clone());

                updates.add_event(UpdateEvent::AddNodeLabel {
                    node_name: element_id.clone(),
                    anno_ns: ANNIS_NS.to_string(),
                    anno_name: "file".to_string(),
                    anno_value: file_path,
                })?;

                updates.add_event(UpdateEvent::AddEdge {
                    source_node: element_id.clone(),
                    target_node: self.document_node_name.clone(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::PartOf.to_string(),
                    component_name: "".to_string(),
                })?;
            }
        }
        Ok(())
    }

    fn map_media_relations(&mut self, updates: &mut GraphUpdate) -> Result<()> {
        // Collect start and end times for all timeline items. SaltXML attaches
        // the time to the token and not timeline items and thus different token
        // can contain more or less fine-grained information about the start/end
        // time. By iterating over all media relations first, we make sure to
        // collect the most fine-grained information.
        let mut tli_to_start: HashMap<i64, f64> = HashMap::new();
        let mut tli_to_end: HashMap<i64, f64> = HashMap::new();
        let mut tlis = BTreeSet::new();

        for media_rel in self
            .edges
            .iter()
            .filter(|n| SaltType::from_node(n) == SaltType::MediaRelation)
        {
            let source_att = media_rel.attribute("source").unwrap_or_default();
            let token_node = resolve_element(source_att, "nodes", &self.nodes)
                .context("Token referenced in SAudioRelation cannot be resolved")?;
            let token_id = get_element_id(&token_node).context("Token has no ID")?;

            let start = get_feature_by_qname(media_rel, "salt", "SSTART")
                .context("Missing SSTART attribute for SAudioRlation")?;
            let end = get_feature_by_qname(media_rel, "salt", "SEND")
                .context("Missing SEND attribute for SAudioRelation")?;

            if let (SaltObject::Float(start), SaltObject::Float(end)) = (start, end) {
                if let Some(covered_tli) = self.token_to_tli.get(&token_id) {
                    if let Some(first_tli) = covered_tli.first() {
                        let start_entry = tli_to_start.entry(*first_tli).or_insert(f64::MAX);
                        *start_entry = start_entry.min(start);
                    }
                    if let Some(last_tli) = covered_tli.last() {
                        let end_entry = tli_to_end.entry(*last_tli).or_insert(f64::MIN);
                        *end_entry = end_entry.max(end);
                    }
                    tlis.extend(covered_tli);
                }
            } else {
                bail!("SSTART/SEND not a float")
            }
        }

        for t in tlis {
            let node_name = self.get_tli_node_name(t);
            let start = tli_to_start.get(&t);
            let end = tli_to_end.get(&t);

            if let (Some(start), Some(end)) = (start, end) {
                updates.add_event(UpdateEvent::AddNodeLabel {
                    node_name,
                    anno_ns: ANNIS_NS.to_string(),
                    anno_name: "time".to_string(),
                    anno_value: format!("{start}-{end}"),
                })?;
            } else if let Some(start) = start {
                updates.add_event(UpdateEvent::AddNodeLabel {
                    node_name,
                    anno_ns: ANNIS_NS.to_string(),
                    anno_name: "time".to_string(),
                    anno_value: format!("{start}-"),
                })?;
            } else if let Some(end) = end {
                updates.add_event(UpdateEvent::AddNodeLabel {
                    node_name,
                    anno_ns: ANNIS_NS.to_string(),
                    anno_name: "time".to_string(),
                    anno_value: format!("-{end}"),
                })?;
            }
        }

        Ok(())
    }

    fn map_node(
        &self,
        n: &Node,
        document_node_name: &str,
        updates: &mut GraphUpdate,
    ) -> Result<()> {
        let id = get_element_id(n).context("Missing element ID for node")?;
        updates.add_event(UpdateEvent::AddNode {
            node_name: id.clone(),
            node_type: "node".to_string(),
        })?;

        updates.add_event(UpdateEvent::AddEdge {
            source_node: id.clone(),
            target_node: document_node_name.to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
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
        overwrite_target_node: Option<String>,
        component_type: AnnotationComponentType,
        fallback_component_name: &str,
        updates: &mut GraphUpdate,
    ) -> Result<()> {
        let source_att_val = rel.attribute("source").unwrap_or_default();
        let source_element =
            resolve_element(source_att_val, "nodes", &self.nodes).context("Missing source node")?;
        let source_id = get_element_id(&source_element).context("Missing source node ID")?;

        let target_id = if let Some(target_id) = overwrite_target_node {
            target_id
        } else {
            let target_att_val = rel.attribute("target").unwrap_or_default();
            let target_element = resolve_element(target_att_val, "nodes", &self.nodes)
                .context("Missing target node")?;
            get_element_id(&target_element).context("Missing target node ID")?
        };

        let component_name = get_feature_by_qname(rel, "salt", "STYPE")
            .map(|t| t.to_string())
            .unwrap_or_else(|| fallback_component_name.to_string());

        let mut component_layer = DEFAULT_NS.to_string();
        if let Some(layers_attribute) = rel.attribute("layers")
            && let Some(first_layer) = layers_attribute.split(' ').next()
        {
            let layer_node = resolve_element(first_layer, "layers", &self.layers)
                .context("Could not resolve layer")?;
            if let Some(SaltObject::Text(layer_name)) =
                get_feature_by_qname(&layer_node, "salt", "SNAME")
            {
                component_layer = layer_name;
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

    fn map_tokens(&self, timeline: Option<&Node>, updates: &mut GraphUpdate) -> Result<()> {
        // Map the token nodes in the same order as in the SaltXML file
        for token_node in self
            .nodes
            .iter()
            .filter(|n| SaltType::from_node(n) == SaltType::Token)
        {
            self.map_node(token_node, &self.document_node_name, updates)?;
        }

        // Order textual relations by their start offset, so we iterate in the
        // actual order of the tokens.
        let sorted_text_rels: BTreeMap<TextProperty, _> = self
            .edges
            .iter()
            .filter(|n| SaltType::from_node(n) == SaltType::TextualRelation)
            .map(|text_rel| {
                let start =
                    get_feature_by_qname(text_rel, "salt", "SSTART").unwrap_or(SaltObject::Null);
                let referenced_text_node = resolve_element(
                    text_rel.attribute("target").unwrap_or_default(),
                    "nodes",
                    &self.nodes,
                )
                .and_then(|n| get_feature_by_qname(&n, "salt", "SNAME"))
                .map(|o| o.to_string())
                .unwrap_or_default();
                let val = if let SaltObject::Integer(start) = start {
                    start
                } else {
                    -1
                };
                let prop = TextProperty {
                    segmentation: referenced_text_node,
                    val,
                };
                (prop, *text_rel)
            })
            .collect();

        // Connect the token to the texts by the textual relations
        let mut previous_token: Option<(TextProperty, String)> = None;
        let mut sorted_text_rels = sorted_text_rels.into_iter().peekable();
        while let Some((text_prop, text_rel)) = sorted_text_rels.next() {
            if let Some(p) = &previous_token
                && p.0.segmentation != text_prop.segmentation
            {
                // If the segmentation changes, there is no previous token
                previous_token = None;
            }

            let source_att_val = text_rel.attribute("source").unwrap_or_default();
            let token =
                resolve_element(source_att_val, "nodes", &self.nodes).with_context(|| {
                    format!("Textual relation source \"{source_att_val}\" could not be resolved")
                })?;
            let token_id = get_element_id(&token).context("Missing ID for token")?;

            let target_att_val = text_rel.attribute("target").unwrap_or_default();
            let datasource =
                resolve_element(target_att_val, "nodes", &self.nodes).with_context(|| {
                    format!("Textual relation target \"{target_att_val}\" could not be resolved")
                })?;
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
                    .map(|(prop, _)| prop.val)
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
                let component_name = if timeline.is_some() {
                    text_prop.segmentation.clone()
                } else {
                    "".to_string()
                };
                updates.add_event(UpdateEvent::AddEdge {
                    source_node: previous_token.1.clone(),
                    target_node: token_id.clone(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name,
                })?;
            }
            previous_token = Some((text_prop, token_id));
        }

        Ok(())
    }

    fn map_non_token_nodes(&self, updates: &mut GraphUpdate) -> Result<()> {
        for span_node in self.nodes.iter().filter(|n| {
            let t = SaltType::from_node(n);
            t == SaltType::Span || t == SaltType::Structure
        }) {
            self.map_node(span_node, &self.document_node_name, updates)?;
        }

        // Connect all spans with the token using the spanning relations
        for spanning_rel in self
            .edges
            .iter()
            .filter(|rel| SaltType::from_node(rel) == SaltType::SpanningRelation)
        {
            self.map_edge(
                spanning_rel,
                None,
                AnnotationComponentType::Coverage,
                "",
                updates,
            )?;
        }
        // Add all dominance relations
        for dominance_rel in self
            .edges
            .iter()
            .filter(|rel| SaltType::from_node(rel) == SaltType::DominanceRelation)
        {
            self.map_edge(
                dominance_rel,
                None,
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
                None,
                AnnotationComponentType::Pointing,
                "edge",
                updates,
            )?;
        }
        Ok(())
    }
}
