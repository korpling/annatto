use graphannis::{
    errors::Result,
    update::{GraphUpdate, UpdateEvent},
};
use j4rs::{Instance, InvocationArg, Jvm};
use std::convert::TryFrom;

fn nullable(o: Instance) -> Result<Option<Instance>> {
    let as_jobject = o.java_object();
    let is_null = as_jobject.is_null();
    if !is_null {
        Ok(Option::Some(Instance::from_jobject(as_jobject)?))
    } else {
        Ok(Option::None)
    }
}

fn node_name(node: &Instance, document_id: &str, jvm: &Jvm) -> Result<String> {
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

fn get_text_for_token(token: &Instance, jvm: &Jvm) -> Result<Option<Instance>> {
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
        let textrel_class = jvm.invoke_static(
            "java.lang.Class",
            "forName",
            &[InvocationArg::try_from(textrel_class_name)?],
        )?;
        let target_node = nullable(jvm.invoke(&rel, "getTarget", &[])?)?;
        let args = vec![InvocationArg::from(rel)];
        if jvm
            .chain(&textrel_class)?
            .invoke("isInstance", &args)?
            .to_rust()?
        {
            return Ok(target_node);
        }
    }
    Ok(None)
}

fn add_node(n: Instance, document_id: &str, u: &mut GraphUpdate, jvm: &Jvm) -> Result<()> {
    let struct_node_class_name = "org.corpus_tools.salt.common.SStructuredNode";
    let struct_node_class = jvm.invoke_static(
        "java.lang.Class",
        "forName",
        &[InvocationArg::try_from(struct_node_class_name)?],
    )?;
    let args = vec![InvocationArg::from(n)];
    if jvm
        .chain(&struct_node_class)?
        .invoke("isInstance", &args)?
        .to_rust()?
    {
        if let Some(n) = args.into_iter().next() {
            let n = jvm.cast(&n.instance()?, struct_node_class_name)?;
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
                let anno_ns = nullable(jvm.invoke(&anno, "getNamespace", &[])?)?;
                u.add_event(UpdateEvent::AddNodeLabel {
                    node_name: node_name.clone(),
                    anno_ns: if let Some(ns) = anno_ns {
                        jvm.to_rust::<String>(ns)?
                    } else {
                        "".to_string()
                    },
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
        .chain(&g)?
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

pub fn convert_document_graph(g: Instance, document_id: &str, jvm: &Jvm) -> Result<GraphUpdate> {
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

    // TODO: add coverage information
    // TODO: add dominance relations
    // TODO: add pointing relations

    Ok(u)
}
