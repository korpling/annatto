use crate::{
    error::{AnnattoError, Result},
    exporter::Exporter,
    importer::Importer, workflow::StatusSender,
};
use graphannis::{
    model::AnnotationComponent,
    update::{GraphUpdate, UpdateEvent},
    AnnotationGraph,
};
use graphannis_core::types::{Edge, NodeID};
use std::{
    env::temp_dir,
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};
use tempfile::tempdir_in;

pub mod graphupdate;

fn event_to_string(update_event: &UpdateEvent) -> Result<String> {
    Ok(format!("{:?}", update_event))
}

pub fn write_to_file(updates: &GraphUpdate, path: &std::path::Path) -> Result<()> {
    let mut file = File::create(path)?;
    let it = updates.iter()?;
    for update_event in it {
        let event_tuple = update_event?;
        let event_string = event_to_string(&event_tuple.1)?;
        file.write_all(event_string.as_bytes())?;
        file.write_all(b"\n")?;
    }
    Ok(())
}

/// Get all files with a given extension in a directory.
pub fn get_all_files(
    corpus_root_dir: &Path,
    file_extensions: Vec<&str>,
) -> std::result::Result<Vec<PathBuf>, AnnattoError> {
    let mut paths = Vec::new();
    let flex_path = corpus_root_dir.join("**");
    for ext in file_extensions {
        let ext_path = flex_path.join(format!("*.{ext}"));
        for file_opt in glob::glob(&ext_path.to_string_lossy())? {
            paths.push(file_opt?)
        }
    }
    Ok(paths)
}

pub fn import_as_graphml_string<I, P>(
    importer: I,
    path: P,
    graph_configuration: Option<&str>,
) -> Result<String>
where
    I: Importer,
    P: AsRef<Path>,
{
    import_as_graphml_string_2(importer, path, graph_configuration, false, None)
}

pub fn import_as_graphml_string_2<I, P>(
    importer: I,
    path: P,
    graph_configuration: Option<&str>,
    disk_based: bool,
    tx: Option<StatusSender>
) -> Result<String>
where
    I: Importer,
    P: AsRef<Path>,
{
    let mut u = importer
        .import_corpus(path.as_ref(), importer.step_id(None), tx)
        .map_err(|e| AnnattoError::Import {
            reason: e.to_string(),
            importer: importer.module_name().to_string(),
            path: path.as_ref().to_path_buf(),
        })?;
    let mut g = AnnotationGraph::with_default_graphstorages(disk_based)?;
    g.apply_update(&mut u, |_| {})?;

    let mut buf = BufWriter::new(Vec::new());
    graphannis_core::graph::serialization::graphml::export(
        &g,
        graph_configuration,
        &mut buf,
        |_| {},
    )?;
    let bytes = buf.into_inner()?;
    let actual = String::from_utf8(bytes)?;

    Ok(actual)
}

pub fn export_to_string<E>(
    graph: &AnnotationGraph,
    exporter: E,
    file_extension: &str,
) -> Result<String>
where
    E: Exporter,
{
    let output_path = tempdir_in(temp_dir())?;
    exporter
        .export_corpus(graph, output_path.path(), exporter.step_id(None), None)
        .map_err(|_| AnnattoError::Export {
            reason: "Could not export graph to read its output.".to_string(),
            exporter: exporter.module_name().to_string(),
            path: output_path.path().to_path_buf(),
        })?;
    let mut buffer = String::new();
    for path in get_all_files(output_path.path(), vec![file_extension])? {
        let file_data = fs::read_to_string(path)?;
        buffer.push_str(&file_data);
    }
    Ok(buffer)
}

pub trait Traverse<N, E> {
    /// A node has been reached traversing the given component.
    fn node(
        &self,
        graph: &AnnotationGraph,
        node: NodeID,
        component: &AnnotationComponent,
        buffer: &mut N,
    ) -> Result<()>;

    /// An edge is being processed while traversing the graph in the given component.
    fn edge(
        &self,
        graph: &AnnotationGraph,
        edge: Edge,
        component: &AnnotationComponent,
        buffer: &mut E,
    ) -> Result<()>;

    fn traverse(
        &self,
        graph: &AnnotationGraph,
        node_buffer: &mut N,
        edge_buffer: &mut E,
    ) -> Result<()>;
}
