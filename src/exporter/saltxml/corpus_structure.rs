use graphannis::AnnotationGraph;
use xml::{writer::XmlEvent, EmitterConfig};

pub(super) struct SaltCorpusStructureMapper {}

impl SaltCorpusStructureMapper {
    pub(super) fn new() -> SaltCorpusStructureMapper {
        SaltCorpusStructureMapper {}
    }

    pub(super) fn map_corpus_structure(
        &self,
        _graph: &AnnotationGraph,
        project_file_path: &std::path::Path,
    ) -> anyhow::Result<()> {
        let output_file = std::fs::File::create(project_file_path)?;
        let mut writer = EmitterConfig::new()
            .perform_indent(true)
            .create_writer(output_file);

        writer.write(XmlEvent::StartDocument {
            version: xml::common::XmlVersion::Version11,
            encoding: Some("UTF-8"),
            standalone: None,
        })?;

        writer.write(
            XmlEvent::start_element("saltCommon:SaltProject")
                .ns("xmi", "http://www.omg.org/XMI")
                .ns("xsi", "http://www.w3.org/2001/XMLSchema-instance")
                .ns("sCorpusStructure", "sCorpusStructure")
                .ns("saltCore", "saltCore")
                .ns("saltCommon", "saltCommon")
                .attr("xsi:version", "2.0"),
        )?;
        writer.write(XmlEvent::end_element())?;

        // let root = doc.root_element();
        // if root.tag_name().name() != "SaltProject" {
        //     return Err(anyhow!(
        //         "SaltXML project file must start with <SaltProject> tag"
        //     ));
        // }

        // let mut documents = BTreeSet::new();

        // // Iterate over all corpus graphs
        // for cg in root
        //     .children()
        //     .filter(|t| t.tag_name().name() == "sCorpusGraphs")
        // {
        //     // Get all nodes
        //     let nodes = cg
        //         .children()
        //         .filter(|t| t.tag_name().name() == "nodes")
        //         .collect_vec();

        //     for node in nodes.iter() {
        //         let salt_type = SaltType::from_node(node);
        //         match salt_type {
        //             SaltType::Corpus | SaltType::Document => {
        //                 // Get the element ID from the label
        //                 let node_name = get_element_id(node)
        //                     .ok_or_else(|| anyhow!("Missing element ID for corpus graph node"))?;
        //                 // Create the element with the collected properties
        //                 updates.add_event(UpdateEvent::AddNode {
        //                     node_name: node_name.to_string(),
        //                     node_type: "corpus".into(),
        //                 })?;

        //                 // Add the document ID to the result
        //                 if SaltType::Document == salt_type {
        //                     documents.insert(node_name.to_string());
        //                 }

        //                 // Add features as annotations
        //                 for feature_node in get_features(node) {
        //                     let annos_ns = feature_node.attribute("namespace");
        //                     let anno_name = feature_node.attribute("name").ok_or_else(|| {
        //                         anyhow!("Missing \"name\" attribute for node \"{node_name}\"")
        //                     })?;
        //                     let anno_value = SaltObject::from(
        //                         feature_node.attribute("value").unwrap_or_default(),
        //                     );

        //                     if annos_ns == Some("salt") {
        //                         if anno_name == "SNAME" {
        //                             // Only map this specific feature as document name
        //                             if salt_type == SaltType::Document {
        //                                 updates.add_event(UpdateEvent::AddNodeLabel {
        //                                     node_name: node_name.to_string(),
        //                                     anno_ns: ANNIS_NS.to_string(),
        //                                     anno_name: "doc".to_string(),
        //                                     anno_value: anno_value.to_string(),
        //                                 })?;
        //                             }
        //                         }
        //                     } else {
        //                         updates.add_event(UpdateEvent::AddNodeLabel {
        //                             node_name: node_name.to_string(),
        //                             anno_ns: annos_ns.unwrap_or_default().to_string(),
        //                             anno_name: anno_name.to_string(),
        //                             anno_value: anno_value.to_string(),
        //                         })?;
        //                     }
        //                 }
        //                 // Add meta annotations
        //                 for anno_node in get_meta_annotations(node) {
        //                     let annos_ns = anno_node.attribute("namespace");
        //                     if annos_ns != Some("salt") {
        //                         let anno_name = anno_node.attribute("name").ok_or_else(|| {
        //                             anyhow!("Missing \"name\" attribute for node \"{node_name}\"")
        //                         })?;
        //                         let anno_value = SaltObject::from(
        //                             anno_node.attribute("value").unwrap_or_default(),
        //                         );

        //                         updates.add_event(UpdateEvent::AddNodeLabel {
        //                             node_name: node_name.to_string(),
        //                             anno_ns: annos_ns.unwrap_or_default().to_string(),
        //                             anno_name: anno_name.to_string(),
        //                             anno_value: anno_value.to_string(),
        //                         })?;
        //                     }
        //                 }
        //             }
        //             _ => {}
        //         }
        //     }

        //     // Add a PartOf Edge between parent corpora and the sub-corpora/documents
        //     for e in cg.children().filter(|n| n.tag_name().name() == "edges") {
        //         match SaltType::from_node(&e) {
        //             SaltType::CorpusRelation | SaltType::DocumentRelation => {
        //                 let source_ref = e.attribute("source").unwrap_or_default();
        //                 let target_ref = e.attribute("target").unwrap_or_default();

        //                 let source_node = resolve_element(source_ref, "nodes", &nodes)
        //                     .and_then(|n| get_element_id(&n));
        //                 let target_node = resolve_element(target_ref, "nodes", &nodes)
        //                     .and_then(|n| get_element_id(&n));

        //                 if let (Some(source_node), Some(target_node)) = (source_node, target_node) {
        //                     // PartOf has the inverse meaning of the corpus and documentation relation in Salt
        //                     updates.add_event(UpdateEvent::AddEdge {
        //                         source_node: target_node,
        //                         target_node: source_node,
        //                         layer: ANNIS_NS.to_string(),
        //                         component_type: AnnotationComponentType::PartOf.to_string(),
        //                         component_name: "".into(),
        //                     })?;
        //                 }
        //             }
        //             _ => {}
        //         }
        //     }
        // }

        Ok(())
    }
}
