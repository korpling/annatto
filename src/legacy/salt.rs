//! Functions to access to Salt objects

pub mod map_from;
pub mod map_to;

use std::collections::BTreeSet;

use super::{is_instance_of, is_null};
use crate::error::Result;
use j4rs::{Instance, Jvm};

pub fn get_identifier(sdocument: &Instance, jvm: &Jvm) -> Result<Instance> {
    let id = jvm.invoke(
        &jvm.cast(sdocument, "org.corpus_tools.salt.graph.IdentifiableElement")?,
        "getIdentifier",
        &[],
    )?;
    Ok(id)
}

pub fn node_name(node: &Instance, document_id: &str, jvm: &Jvm) -> Result<String> {
    let fragment: String = jvm
        .chain(&jvm.cast(node, "org.corpus_tools.salt.core.SPathElement")?)?
        .invoke("getPath", &[])?
        .invoke("fragment", &[])?
        .to_rust()?;

    if document_id.is_empty() {
        Ok(fragment)
    } else {
        let mut result = String::default();
        result.push_str(document_id);
        result.push('#');
        result.push_str(&fragment);
        Ok(result)
    }
}

pub fn get_text_for_token(token: &Instance, jvm: &Jvm) -> Result<Option<Instance>> {
    // Get all outgoing edges and return the text connected to the first STextualRelation
    let out_relations: Instance = jvm
        .chain(&jvm.cast(&token, "org.corpus_tools.salt.core.SNode")?)?
        .invoke("getOutRelations", &[])?
        .invoke("iterator", &[])?
        .collect();
    while jvm.to_rust::<bool>(jvm.invoke(&out_relations, "hasNext", &[])?)? {
        let rel = jvm.cast(
            &jvm.invoke(&out_relations, "next", &[])?,
            "org.corpus_tools.salt.core.SRelation",
        )?;
        let textrel_class_name = "org.corpus_tools.salt.common.STextualRelation";
        let target_node = jvm.invoke(&rel, "getTarget", &[])?;
        if !is_null(&target_node, jvm)? && is_instance_of(&rel, textrel_class_name, jvm)? {
            return Ok(Some(target_node));
        }
    }
    Ok(None)
}

pub fn get_relation_layer_names(rel: &Instance, jvm: &Jvm) -> Result<BTreeSet<String>> {
    let mut result = BTreeSet::new();

    let layers = jvm.chain(rel)?.invoke("getLayers", &[])?.collect();
    if !is_null(&layers, jvm)? {
        let layers_iterator = jvm.chain(&layers)?.invoke("iterator", &[])?.collect();
        while jvm.to_rust::<bool>(jvm.invoke(&layers_iterator, "hasNext", &[])?)? {
            let layer_name = jvm
                .chain(&layers_iterator)?
                .invoke("next", &[])?
                .invoke("getName", &[])?
                .to_rust()?;
            result.insert(layer_name);
        }
    }

    if result.is_empty() {
        //  add the edge to the default empty layer
        result.insert("".to_string());
    }

    Ok(result)
}
