use crate::error::Result;
use graphannis::update::{GraphUpdate, UpdateEvent};
use j4rs::{Instance, InvocationArg, Jvm};

use super::{
    super::salt::{get_relation_layer_names, get_text_for_token, node_name},
    is_instance_of, is_null,
};

pub fn map_document_graph(g: Instance, document_id: &str, jvm: &Jvm) -> Result<GraphUpdate> {
    let mut u = GraphUpdate::default();

    // add all nodes and their annotations
    let nodes_iterator: Instance = jvm
        .chain(&jvm.cast(&g, "org.corpus_tools.salt.graph.Graph")?)?
        .invoke("getNodes", &[])?
        .invoke("iterator", &[])?
        .collect();
    while jvm.to_rust::<bool>(jvm.invoke(&nodes_iterator, "hasNext", &[])?)? {
        let node = jvm.invoke(&nodes_iterator, "next", &[])?;
        add_node(node, document_id, &mut u, jvm)?;
    }

    // add ordering edges and special annis:tok label
    add_token_information(&g, document_id, &mut u, jvm)?;

    // TODO: map timeline

    // add spanning, dominance and pointing relations
    let spanning_relations_iterator: Instance = jvm
        .chain(&g)?
        .invoke("getSpanningRelations", &[])?
        .invoke("iterator", &[])?
        .collect();
    while jvm.to_rust::<bool>(jvm.invoke(&spanning_relations_iterator, "hasNext", &[])?)? {
        let relation = jvm.invoke(&spanning_relations_iterator, "next", &[])?;
        add_relation(&relation, "Coverage", "", document_id, &mut u, jvm)?;
    }
    let dominance_relations: Instance = jvm
        .chain(&g)?
        .invoke("getDominanceRelations", &[])?
        .invoke("iterator", &[])?
        .collect();
    while jvm.to_rust::<bool>(jvm.invoke(&dominance_relations, "hasNext", &[])?)? {
        let relation = jvm.invoke(&dominance_relations, "next", &[])?;
        for layer_name in get_relation_layer_names(&relation, jvm)? {
            add_relation(
                &relation,
                "Dominance",
                &layer_name,
                document_id,
                &mut u,
                jvm,
            )?;
        }
    }
    let pointing_relations_iterator: Instance = jvm
        .chain(&g)?
        .invoke("getPointingRelations", &[])?
        .invoke("iterator", &[])?
        .collect();
    while jvm.to_rust::<bool>(jvm.invoke(&pointing_relations_iterator, "hasNext", &[])?)? {
        let relation = jvm.invoke(&pointing_relations_iterator, "next", &[])?;
        for layer_name in get_relation_layer_names(&relation, jvm)? {
            add_relation(&relation, "Pointing", &layer_name, document_id, &mut u, jvm)?;
        }
    }

    Ok(u)
}

fn add_node(n: Instance, document_id: &str, u: &mut GraphUpdate, jvm: &Jvm) -> Result<()> {
    let struct_node_class_name = "org.corpus_tools.salt.common.SStructuredNode";
    if is_instance_of(&n, struct_node_class_name, jvm)? {
        // use the unique name
        let node_name = node_name(&n, document_id, jvm)?;
        u.add_event(UpdateEvent::AddNode {
            node_name: node_name.clone(),
            node_type: "node".to_string(),
        })?;
        // add all annotations
        let annos_iterator: Instance = jvm
            .chain(&jvm.cast(&n, "org.corpus_tools.salt.core.SAnnotationContainer")?)?
            .invoke("getAnnotations", &[])?
            .invoke("iterator", &[])?
            .collect();
        while jvm.to_rust::<bool>(jvm.invoke(&annos_iterator, "hasNext", &[])?)? {
            let anno = jvm.cast(
                &jvm.invoke(&annos_iterator, "next", &[])?,
                "org.corpus_tools.salt.graph.Label",
            )?;
            let anno_ns = jvm.invoke(&anno, "getNamespace", &[])?;
            let anno_ns = if is_null(&anno_ns, jvm)? {
                "".to_string()
            } else {
                jvm.to_rust::<String>(anno_ns)?
            };
            u.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_name.clone(),
                anno_ns,
                anno_name: jvm.to_rust(jvm.invoke(&anno, "getName", &[])?)?,
                anno_value: jvm.to_rust(jvm.invoke(&anno, "getValue", &[])?)?,
            })?;
        }

        // add connection to document node
        u.add_event(UpdateEvent::AddEdge {
            component_type: "PartOf".to_string(),
            layer: "".to_string(),
            component_name: "".to_string(),
            source_node: node_name,
            target_node: document_id.to_string(),
        })?;
    }

    Ok(())
}

fn add_relation(
    rel: &Instance,
    component_type: &str,
    layer: &str,
    document_id: &str,
    u: &mut GraphUpdate,
    jvm: &Jvm,
) -> Result<()> {
    let rel = jvm.cast(rel, "org.corpus_tools.salt.core.SRelation")?;
    // Get the IDs of the source and target nodes
    let source_node = jvm.chain(&rel)?.invoke("getSource", &[])?.collect();
    let source_node = node_name(&source_node, document_id, jvm)?;
    let target_node = jvm.chain(&rel)?.invoke("getTarget", &[])?.collect();
    let target_node = node_name(&target_node, document_id, jvm)?;

    let rel_type = jvm.chain(&rel)?.invoke("getType", &[])?.collect();
    let component_name = if is_null(&rel_type, jvm)? {
        "".to_string()
    } else {
        jvm.to_rust(rel_type)?
    };

    // add edge event
    u.add_event(UpdateEvent::AddEdge {
        source_node: source_node.to_string(),
        target_node: target_node.to_string(),
        component_type: component_type.to_string(),
        component_name: component_name.to_string(),
        layer: layer.to_string(),
    })?;

    // add all annotations
    let annos_iterator: Instance = jvm
        .chain(&jvm.cast(&rel, "org.corpus_tools.salt.core.SAnnotationContainer")?)?
        .invoke("getAnnotations", &[])?
        .invoke("iterator", &[])?
        .collect();
    while jvm.to_rust::<bool>(jvm.invoke(&annos_iterator, "hasNext", &[])?)? {
        let anno = jvm.cast(
            &jvm.invoke(&annos_iterator, "next", &[])?,
            "org.corpus_tools.salt.graph.Label",
        )?;
        let anno_ns = jvm.invoke(&anno, "getNamespace", &[])?;
        let anno_ns = if is_null(&anno_ns, jvm)? {
            "".to_string()
        } else {
            jvm.to_rust::<String>(anno_ns)?
        };
        // add edge annotation
        u.add_event(UpdateEvent::AddEdgeLabel {
            source_node: source_node.to_string(),
            target_node: target_node.to_string(),
            component_type: component_type.to_string(),
            component_name: component_name.to_string(),
            layer: layer.to_string(),
            anno_ns,
            anno_name: jvm.chain(&anno)?.invoke("getName", &[])?.to_rust()?,
            anno_value: jvm.chain(&anno)?.invoke("getValue", &[])?.to_rust()?,
        })?;
    }

    Ok(())
}

fn add_token_information(
    g: &Instance,
    document_id: &str,
    u: &mut GraphUpdate,
    jvm: &Jvm,
) -> Result<()> {
    let sorted_token: Instance = jvm
        .chain(g)?
        .invoke("getSortedTokenByText", &[])?
        .invoke("iterator", &[])?
        .collect();

    let mut last_token_name: Option<String> = None;
    let mut last_text_ds: Option<Instance> = None;
    while jvm.to_rust::<bool>(jvm.invoke(&sorted_token, "hasNext", &[])?)? {
        let token = jvm.cast(
            &jvm.invoke(&sorted_token, "next", &[])?,
            "org.corpus_tools.salt.common.SToken",
        )?;
        let node_name = node_name(&token, document_id, jvm)?;
        let text_ds = get_text_for_token(&token, jvm)?;

        // each token must have it's spanned text as label
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: node_name.clone(),
            anno_ns: "annis".to_string(),
            anno_name: "tok".to_string(),
            anno_value: jvm
                .chain(g)?
                .invoke(
                    "getText",
                    &[InvocationArg::from(
                        jvm.cast(&token, "org.corpus_tools.salt.core.SNode")?,
                    )],
                )?
                .to_rust()?,
        })?;

        // Check that the current text is the same as the previous one.
        // getSortedTokenByText returns all tokens for every text and we don't want to add ordering edges
        // between tokens of different texts.
        if let Some(text_ds) = text_ds {
            if let (Some(last_text_ds), Some(last_token_name)) = (&last_text_ds, &last_token_name) {
                if jvm
                    .chain(&jvm.cast(last_text_ds, "java.lang.Object")?)?
                    .invoke(
                        "equals",
                        &[InvocationArg::from(jvm.cast(&text_ds, "java.lang.Object")?)],
                    )?
                    .to_rust()?
                {
                    // add an explicit Ordering edge between the token
                    u.add_event(UpdateEvent::AddEdge {
                        component_type: "Ordering".to_string(),
                        layer: "annis".to_string(),
                        component_name: "".to_string(),
                        source_node: last_token_name.clone(),
                        target_node: node_name.clone(),
                    })?;
                }
            }
            last_token_name = Some(node_name);
            last_text_ds = Some(text_ds);
        } else {
            last_text_ds = None;
        }
    }
    Ok(())
}