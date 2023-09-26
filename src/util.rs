use crate::{
    error::{AnnattoError, Result},
    importer::Importer,
};
use graphannis::{
    update::{GraphUpdate, UpdateEvent},
    AnnotationGraph,
};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

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
) -> std::result::Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
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
    let mut u = importer
        .import_corpus(path.as_ref(), importer.step_id(None), None)
        .map_err(|e| AnnattoError::Import {
            reason: e.to_string(),
            importer: importer.module_name().to_string(),
            path: path.as_ref().to_path_buf(),
        })?;
    let mut g = AnnotationGraph::with_default_graphstorages(false)?;
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
