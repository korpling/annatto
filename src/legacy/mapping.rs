use crate::error::Result;
use graphannis::update::{GraphUpdate, UpdateEvent};
use j4rs::{Instance, InvocationArg, Jvm};
use std::convert::TryFrom;

fn nullable(o: Instance) -> Result<Option<Instance>> {
    let as_jobject = o.java_object();
    let check = as_jobject.is_null();
    if check {
        Ok(Option::Some(Instance::from_jobject(as_jobject)?))
    } else {
        Ok(Option::None)
    }
}

pub fn convert_document_graph(g: Instance, jvm: &Jvm) -> Result<GraphUpdate> {
    let mut u = GraphUpdate::default();
    // create the (sub-) corpus and the document nodes
    if let Some(doc_path) = nullable(jvm.invoke(&g, "getPath", &[])?)? {
        if let Some(segments) = nullable(jvm.invoke(&doc_path, "segments", &[])?)? {
            let segments = jvm.invoke_static(
                "java.util.Arrays",
                "asList",
                &[InvocationArg::from(segments)],
            )?;
            let segments_length: i64 = jvm.to_rust(jvm.invoke(&segments, "size", &[])?)?;
            let mut corpus_path = String::default();
            for i in 0..segments_length {
                let s: String =
                    jvm.to_rust(jvm.invoke(&segments, "get", &[InvocationArg::try_from(i)?])?)?;

                if i > 0 {
                    corpus_path.push('/');
                }
                corpus_path.push_str(&s);

                // Add a node of type corpus for this path
                u.add_event(UpdateEvent::AddNode {
                    node_type: "corpus".to_string(),
                    node_name: corpus_path.clone(),
                })?;
                if i == (segments_length - 1) {
                    // The last element is also marked as a document
                    u.add_event(UpdateEvent::AddNodeLabel {
                        node_name: corpus_path.clone(),
                        anno_ns: "annis".to_string(),
                        anno_name: "doc".to_string(),
                        anno_value: s,
                    })?;
                }
            }
        }
    }
    todo!()
}
