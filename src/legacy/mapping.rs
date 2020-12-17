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
                node_name,
                node_type: "node".to_string(),
            })?;
            // TODO: add all annotations
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

    // TODO: add token information
    // TODO: add coverage information
    // TODO: add dominance relations
    // TODO: add pointing relations

    Ok(u)
}