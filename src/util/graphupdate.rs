use std::{
    borrow::Cow,
    collections::BTreeSet,
    ffi::OsStr,
    path::{Path, PathBuf},
};

use crate::{
    Result,
    importer::{GenericImportConfiguration, NamedPaths},
};
use graphannis::{
    model::{AnnotationComponent, AnnotationComponentType},
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::{ANNIS_NS, DEFAULT_NS};
use itertools::Itertools;
use normpath::PathExt;

#[deprecated]
fn add_subcorpora(
    u: &mut GraphUpdate,
    path: &Path,
    parent_corpus: &str,
    file_endings: &[&str],
    document_filter: Option<&BTreeSet<String>>,
) -> Result<Vec<(PathBuf, String)>> {
    let mut result = Vec::new();

    // Get the files and sort them according to their path, to get a predictable
    // order of adding the documents to the graph.

    if path.is_file()
        && file_endings
            .iter()
            .any(|ext| path.extension().unwrap_or_default().to_string_lossy() == *ext)
        && document_filter
            .map(|s| {
                let path_str = path.to_string_lossy();
                let mut contained = s.contains(&*path_str);
                if let Some(stem) = path.file_stem() {
                    contained |= s.contains(&*stem.to_string_lossy());
                }
                contained
            })
            .unwrap_or(true)
    {
        // Add the file itself as document
        let subcorpus_name = path
            .file_stem()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| "document".to_string());
        let node_name = format!("{parent_corpus}/{subcorpus_name}");
        u.add_event(UpdateEvent::AddNode {
            node_name: node_name.clone(),
            node_type: "corpus".to_string(),
        })?;
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: node_name.clone(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "doc".to_string(),
            anno_value: subcorpus_name.to_string(),
        })?;
        u.add_event(UpdateEvent::AddEdge {
            source_node: node_name.clone(),
            target_node: parent_corpus.to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        let result = (path.to_path_buf(), node_name);
        Ok(vec![result])
    } else {
        let read_dir = std::fs::read_dir(path)?;
        let mut entries_in_directory = Vec::with_capacity(read_dir.size_hint().0 + 4);
        for entry in read_dir {
            let entry = entry?;
            let is_dir = entry.path().is_dir();
            let valid_extension = if entry.path().is_file()
                && let Some(extension) = entry.path().extension()
            {
                file_endings
                    .iter()
                    .any(|ext| *ext == extension.to_string_lossy().as_ref())
            } else {
                false
            };
            let file_inclusion_is_licensed = if entry.path().is_file()
                && let Some(file_stem) = entry.path().file_stem().map(OsStr::to_string_lossy)
            {
                document_filter
                    .map(|df| {
                        let long_path = Path::new(parent_corpus).join(&*file_stem);
                        df.contains(&*file_stem) || df.contains(&*long_path.to_string_lossy())
                    })
                    .unwrap_or(true)
            } else {
                false
            };
            if is_dir || (valid_extension && file_inclusion_is_licensed) {
                entries_in_directory.push(entry);
            }
        }
        entries_in_directory.sort_by_key(|dir_entry| dir_entry.path());

        for entry in entries_in_directory {
            let entry_type = entry.file_type()?;
            let entry_path = entry.path();
            let subcorpus_name = if entry_path.is_dir() {
                entry_path
                    .file_name() // do not strip extension!
                    .map(|f| f.to_os_string())
                    .unwrap_or_else(|| entry.file_name())
                    .to_string_lossy()
                    .to_string()
            } else {
                entry_path
                    .file_stem() // strip extension
                    .map(|f| f.to_os_string())
                    .unwrap_or_else(|| entry.file_name())
                    .to_string_lossy()
                    .to_string()
            };
            let node_name = format!("{parent_corpus}/{subcorpus_name}");
            u.add_event(UpdateEvent::AddNode {
                node_name: node_name.clone(),
                node_type: "corpus".to_string(),
            })?;
            u.add_event(UpdateEvent::AddEdge {
                source_node: node_name.clone(),
                target_node: parent_corpus.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string(),
            })?;

            if entry_type.is_dir() {
                result.extend(add_subcorpora(
                    u,
                    &entry.path(),
                    &node_name,
                    file_endings,
                    document_filter,
                )?);
            } else if entry_type.is_file() {
                // Also add the special "annis:doc" label to mark this as document
                u.add_event(UpdateEvent::AddNodeLabel {
                    node_name: node_name.clone(),
                    anno_ns: ANNIS_NS.to_string(),
                    anno_name: "doc".to_string(),
                    anno_value: subcorpus_name.to_string(),
                })?;
                // Only add the corpus graph leafs to the result vector
                result.push((entry.path(), node_name));
            }
        }
        Ok(result)
    }
}

pub fn root_corpus_from_path(root_path: &Path) -> Result<String> {
    let norm_path = root_path.normalize()?;
    let root_name = if norm_path.is_file() {
        // remove extension
        norm_path
            .file_stem()
            .unwrap_or_else(|| OsStr::new("root-corpus"))
            .to_string_lossy()
    } else {
        // keep trailing sequences starting with a "." (e. g. version digits)
        norm_path
            .file_name()
            .unwrap_or_else(|| OsStr::new("root-corpus"))
            .to_string_lossy()
    };
    Ok(root_name.to_string())
}

pub struct ConfiguredUpdate {
    inner: GraphUpdate,
    pub(crate) config: GenericImportConfiguration,
}

impl From<GenericImportConfiguration> for ConfiguredUpdate {
    fn from(config: GenericImportConfiguration) -> Self {
        Self {
            inner: GraphUpdate::default(),
            config,
        }
    }
}

impl ConfiguredUpdate {
    pub(crate) fn node(&mut self, node_name: String, node_type: String) -> Result<()> {
        self.inner.add_event(UpdateEvent::AddNode {
            node_name,
            node_type,
        })?;
        Ok(())
    }

    pub(crate) fn node_label(
        &mut self,
        node_name: String,
        anno_ns: String,
        anno_name: String,
        anno_value: String,
    ) -> Result<()> {
        self.inner.add_event(UpdateEvent::AddNodeLabel {
            node_name,
            anno_ns,
            anno_name,
            anno_value,
        })?;
        Ok(())
    }

    pub(crate) fn labeled_node(
        &mut self,
        node_name: String,
        node_type: String,
        anno_ns: String,
        anno_name: String,
        anno_value: String,
    ) -> Result<()> {
        self.node(node_name.to_string(), node_type)?;
        self.node_label(node_name, anno_ns, anno_name, anno_value)?;
        Ok(())
    }

    pub(crate) fn edge(
        &mut self,
        source_node: String,
        target_node: String,
        component: &AnnotationComponent,
    ) -> Result<()> {
        self.inner.add_event(UpdateEvent::AddEdge {
            source_node,
            target_node,
            layer: component.layer.to_string(),
            component_type: component.get_type().to_string(),
            component_name: component.name.to_string(),
        })?;
        Ok(())
    }

    pub(crate) fn edge_by_parts(
        &mut self,
        source_node: String,
        target_node: String,
        layer: String,
        component_type: AnnotationComponentType,
        component_name: String,
    ) -> Result<AnnotationComponent> {
        self.inner.add_event(UpdateEvent::AddEdge {
            source_node,
            target_node,
            layer: layer.to_string(),
            component_type: component_type.to_string(),
            component_name: component_name.to_string(),
        })?;
        Ok(AnnotationComponent::new(
            component_type,
            layer,
            component_name,
        ))
    }

    pub(crate) fn edge_label(
        &mut self,
        source_node: String,
        target_node: String,
        component: &AnnotationComponent,
        anno_name: String,
        anno_ns: String,
        anno_value: String,
    ) -> Result<()> {
        self.inner.add_event(UpdateEvent::AddEdgeLabel {
            source_node,
            target_node,
            layer: component.layer.to_string(),
            component_type: component.get_type().to_string(),
            component_name: component.name.to_string(),
            anno_ns,
            anno_name,
            anno_value,
        })?;
        Ok(())
    }

    pub(crate) fn labeled_edge(
        &mut self,
        source_node: String,
        target_node: String,
        component: &AnnotationComponent,
        anno_name: String,
        anno_ns: String,
        anno_value: String,
    ) -> Result<()> {
        self.edge(source_node.to_string(), target_node.to_string(), component)?;
        self.edge_label(
            source_node,
            target_node,
            component,
            anno_name,
            anno_ns,
            anno_value,
        )
    }

    pub(crate) fn ordering(
        &mut self,
        source: String,
        target: String,
    ) -> Result<&AnnotationComponent> {
        self.inner.add_event(UpdateEvent::AddEdge {
            source_node: source,
            target_node: target,
            layer: self.config.default_ordering().layer.to_string(),
            component_type: self.config.default_ordering().get_type().to_string(),
            component_name: self.config.default_ordering().name.to_string(),
        })?;
        Ok(self.config.default_ordering())
    }

    pub(crate) fn part_of(
        &mut self,
        source: String,
        target: String,
    ) -> Result<&AnnotationComponent> {
        self.inner.add_event(UpdateEvent::AddEdge {
            source_node: source,
            target_node: target,
            layer: self.config.default_part_of().layer.to_string(),
            component_type: self.config.default_part_of().get_type().to_string(),
            component_name: self.config.default_part_of().name.to_string(),
        })?;
        Ok(self.config.default_part_of())
    }

    pub(crate) fn coverage(
        &mut self,
        source: String,
        target: String,
    ) -> Result<&AnnotationComponent> {
        self.inner.add_event(UpdateEvent::AddEdge {
            source_node: source,
            target_node: target,
            layer: self.config.default_coverage().layer.to_string(),
            component_type: self.config.default_coverage().get_type().to_string(),
            component_name: self.config.default_coverage().name.to_string(),
        })?;
        Ok(self.config.default_coverage())
    }

    pub(crate) fn dominance(
        &mut self,
        source: String,
        target: String,
    ) -> Result<&AnnotationComponent> {
        self.inner.add_event(UpdateEvent::AddEdge {
            source_node: source,
            target_node: target,
            layer: self.config.default_dominance().layer.to_string(),
            component_type: self.config.default_dominance().get_type().to_string(),
            component_name: self.config.default_dominance().name.to_string(),
        })?;
        Ok(self.config.default_dominance())
    }

    pub(crate) fn pointing(
        &mut self,
        source: String,
        target: String,
    ) -> Result<&AnnotationComponent> {
        self.inner.add_event(UpdateEvent::AddEdge {
            source_node: source,
            target_node: target,
            layer: self.config.default_pointing().layer.to_string(),
            component_type: self.config.default_pointing().get_type().to_string(),
            component_name: self.config.default_pointing().name.to_string(),
        })?;
        Ok(self.config.default_pointing())
    }

    pub(crate) fn add_event(&mut self, ue: UpdateEvent) -> Result<()> {
        Ok(self.inner.add_event(ue)?)
    }

    pub(crate) fn build_corpus_graph(&mut self, import_path: &Path) -> Result<NamedPaths> {
        let root_name = self
            .config
            .custom_root_name()
            .unwrap_or(root_corpus_from_path(import_path)?);

        self.node(root_name.clone(), "corpus".to_string())?;

        let mut path_tuples = self.add_subcorpora(import_path, &root_name)?;
        path_tuples.sort();
        Ok(path_tuples)
    }

    fn add_subcorpora(
        &mut self,
        path: &Path,
        parent_corpus: &str,
    ) -> Result<Vec<(PathBuf, String)>> {
        let mut result = Vec::new();

        // Get the files and sort them according to their path, to get a predictable
        // order of adding the documents to the graph.

        if path.is_file()
            && self
                .config
                .extensions()
                .iter()
                .any(|ext| path.extension().unwrap_or_default().to_string_lossy() == *ext)
            && self
                .config
                .document_list()
                .map(|s| {
                    let path_str = path.to_string_lossy();
                    let mut contained = s.contains(&*path_str);
                    if let Some(stem) = path.file_stem() {
                        contained |= s.contains(&*stem.to_string_lossy());
                    }
                    contained
                })
                // if document list is none, we want this to resolve to `true` as this means "no filter applied
                .unwrap_or(true)
        {
            // Add the file itself as document
            let subcorpus_name = path
                .file_stem()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|| "document".to_string());
            let node_name = format!("{parent_corpus}/{subcorpus_name}");
            self.labeled_node(
                node_name.clone(),
                "corpus".to_string(),
                ANNIS_NS.to_string(),
                "doc".to_string(),
                subcorpus_name.to_string(),
            )?;
            self.part_of(node_name.clone(), parent_corpus.to_string());
            Ok(vec![(path.to_path_buf(), node_name)])
        } else {
            let read_dir = std::fs::read_dir(path)?;
            let mut entries_in_directory = Vec::with_capacity(read_dir.size_hint().0 + 4);
            for entry in read_dir {
                let entry = entry?;
                let is_dir = entry.path().is_dir();
                let valid_extension = if entry.path().is_file()
                    && let Some(extension) = entry.path().extension()
                {
                    self.config
                        .extensions()
                        .contains(&extension.to_string_lossy().to_string())
                } else {
                    false
                };
                let file_inclusion_is_licensed = if entry.path().is_file()
                    && let Some(file_stem) = entry.path().file_stem().map(OsStr::to_string_lossy)
                {
                    self.config
                        .document_list()
                        .map(|df| {
                            let long_path = Path::new(parent_corpus).join(&*file_stem);
                            df.contains(&*file_stem) || df.contains(&*long_path.to_string_lossy())
                        })
                        .unwrap_or(true)
                } else {
                    false
                };
                if is_dir || (valid_extension && file_inclusion_is_licensed) {
                    entries_in_directory.push(entry);
                }
            }
            entries_in_directory.sort_by_key(|dir_entry| dir_entry.path());

            for entry in entries_in_directory {
                let entry_type = entry.file_type()?;
                let entry_path = entry.path();
                let subcorpus_name = if entry_path.is_dir() {
                    entry_path
                        .file_name() // do not strip extension!
                        .map(|f| f.to_os_string())
                        .unwrap_or_else(|| entry.file_name())
                        .to_string_lossy()
                        .to_string()
                } else {
                    entry_path
                        .file_stem() // strip extension
                        .map(|f| f.to_os_string())
                        .unwrap_or_else(|| entry.file_name())
                        .to_string_lossy()
                        .to_string()
                };
                let node_name = format!("{parent_corpus}/{subcorpus_name}");
                self.node(node_name.clone(), "corpus".to_string())?;
                self.part_of(node_name.clone(), parent_corpus.to_string())?;
                if entry_type.is_dir() {
                    result.extend(self.add_subcorpora(&entry.path(), &node_name)?);
                } else if entry_type.is_file() {
                    // Also add the special "annis:doc" label to mark this as document
                    self.node_label(
                        node_name.clone(),
                        ANNIS_NS.to_string(),
                        "doc".to_string(),
                        subcorpus_name.to_string(),
                    )?;
                    // Only add the corpus graph leafs to the result vector
                    result.push((entry.path(), node_name));
                }
            }
            Ok(result)
        }
    }
}

impl Into<GraphUpdate> for ConfiguredUpdate {
    fn into(self) -> GraphUpdate {
        self.inner
    }
}

/// Finds all files with a given ending in a directory and map the corpus
/// structure according to the file/directory structure.
///
/// The root directory is mapped as root corpus, sub-directories as sub-corpora
/// and files as documents. The found documents are returned with the
/// corresponding file path and the document node ID as string.
#[deprecated]
pub fn import_corpus_graph_from_files(
    u: &mut GraphUpdate,
    root_path: &Path,
    config: &GenericImportConfiguration,
) -> Result<Vec<(PathBuf, String)>> {
    let root_name = config
        .custom_root_name()
        .unwrap_or(root_corpus_from_path(root_path)?);

    u.add_event(UpdateEvent::AddNode {
        node_name: root_name.clone(),
        node_type: "corpus".to_string(),
    })?;

    let file_endings = config.extensions().iter().map(String::as_str).collect_vec();
    let mut path_tuples = add_subcorpora(
        u,
        root_path,
        &root_name,
        &file_endings,
        config.document_list(),
    )?;
    path_tuples.sort();
    Ok(path_tuples)
}

#[deprecated]
pub fn map_audio_source(
    u: &mut ConfiguredUpdate,
    audio_path: &Path,
    parent_corpus: &str,
    doc_path: &str,
) -> Result<String> {
    let node_name = format!(
        "{}/{}",
        parent_corpus,
        audio_path
            .file_name()
            .map_or_else(|| Cow::from("unknown_file"), |f| f.to_string_lossy())
    );
    // TODO: make sure the file path is relative to the corpus directory. This
    // means we also have to implement copying the linked files to the same
    // location as the GraphML file.
    let audio_path = audio_path.to_string_lossy().to_string();
    u.labeled_node(
        node_name.to_string(),
        "file".to_string(),
        ANNIS_NS.to_string(),
        "file".to_string(),
        audio_path,
    )?;
    u.part_of(node_name.to_string(), doc_path.to_string())?;

    Ok(node_name)
}

#[deprecated]
pub fn add_order_relations<S: AsRef<str>>(
    u: &mut ConfiguredUpdate,
    node_ids: &[S],
    order_name: Option<&str>,
) -> Result<()> {
    let ordering_layer = if order_name.is_none() {
        u.config.default_ordering().layer.to_string()
    } else {
        // this is anachronistic behaviour in view of the configuration options.
        // For now this is done this way as there is no configuration option "alternative layer",
        // as this would have to be defined component-wise.
        DEFAULT_NS.to_string()
    };
    for i in 1..node_ids.len() {
        u.edge_by_parts(
            node_ids[i - 1].as_ref().to_string(),
            node_ids[i].as_ref().to_string(),
            ordering_layer.to_string(),
            u.config.default_ordering().get_type(), // in case the user aims to use a different type for orderings
            order_name.unwrap_or_default().to_string(),
        )?;
    }
    Ok(())
}

pub struct NodeInfo<'a> {
    node_id: &'a str,
    doc_path: &'a str,
    text_node_name: &'a str,
}

impl<'a> NodeInfo<'a> {
    pub fn new(node_id: &'a str, doc_path: &'a str, text_node_name: &'a str) -> NodeInfo<'a> {
        NodeInfo {
            node_id,
            doc_path,
            text_node_name,
        }
    }
}

pub fn map_token_with_ds(
    u: &mut ConfiguredUpdate,
    node: &NodeInfo,
    text_name: Option<&str>,
    value: &str,
    start_time: Option<f64>,
    end_time: Option<f64>,
    add_annis_layer: bool,
) -> Result<String> {
    let tok_id = format!("{}#t{}", node.doc_path, node.node_id);
    u.labeled_node(
        tok_id.clone(),
        "node".to_string(),
        ANNIS_NS.to_string(),
        "tok".to_string(),
        value.to_string(),
    )?;
    if let Some(text_name) = text_name {
        u.node_label(
            tok_id.clone(),
            u.config.default_namespace().to_string(),
            text_name.to_string(),
            value.to_string(),
        )?;
    };
    u.node_label(
        tok_id.clone(),
        ANNIS_NS.to_string(),
        "tok-whitespace-after".to_string(),
        " ".to_string(),
    )?;
    u.part_of(tok_id.clone(), node.text_node_name.to_string())?;
    if let Some(start_time) = start_time {
        let time_code = if let Some(end_time) = end_time {
            if start_time >= end_time {
                return Err(crate::error::AnnattoError::EndTokenTimeLargerThanStart {
                    start: start_time,
                    end: end_time,
                });
            }
            format!("{start_time}-{end_time}")
        } else {
            format!("{start_time}-")
        };
        u.node_label(
            tok_id.clone(),
            ANNIS_NS.to_string(),
            "time".to_string(),
            time_code,
        )?;
    }
    if add_annis_layer {
        u.node_label(
            tok_id.clone(),
            ANNIS_NS.to_string(),
            "layer".to_string(),
            "default_layer".to_string(),
        )?;
    }
    Ok(tok_id)
}

#[deprecated]
pub fn map_annotations<S: AsRef<str>>(
    u: &mut ConfiguredUpdate,
    node: &NodeInfo,
    ns: Option<&str>,
    name: Option<&str>,
    value: Option<&str>,
    targets: &[S],
) -> Result<String> {
    let span_id = format!("{}#sSpan{}", node.doc_path, node.node_id);
    u.labeled_node(
        span_id.clone(),
        "node".to_string(),
        ANNIS_NS.to_string(),
        "layer".to_string(),
        "default_ns".to_string(),
    )?;
    u.part_of(span_id.to_string(), node.text_node_name.to_string())?;
    if let Some(name) = name {
        u.node_label(
            span_id.clone(),
            ns.unwrap_or_default().to_string(),
            name.to_string(),
            value.unwrap_or_default().to_string(),
        )?;
    }
    for target in targets {
        u.coverage(span_id.clone(), target.as_ref().to_string())?;
    }

    Ok(span_id)
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use graphannis::update::GraphUpdate;
    use insta::{assert_debug_snapshot, assert_snapshot};
    use itertools::Itertools;
    use tempfile::TempDir;

    use crate::importer::GenericImportConfiguration;

    use super::import_corpus_graph_from_files;

    #[test]
    fn single_file_import() {
        let mut u = GraphUpdate::new();
        let result = import_corpus_graph_from_files(
            &mut u,
            Path::new("tests/data/import/exmaralda/clean/import/exmaralda/test_doc.exb"),
            &GenericImportConfiguration::new_with_extensions(vec!["exb".to_string()]),
        )
        .unwrap();

        assert_eq!(1, result.len());
        assert_eq!(
            "tests/data/import/exmaralda/clean/import/exmaralda/test_doc.exb",
            result[0].0.to_string_lossy()
        );
        assert_eq!("test_doc/test_doc", result[0].1);

        let created_updates: graphannis_core::errors::Result<Vec<_>> = u.iter().unwrap().collect();
        let created_updates = created_updates.unwrap();

        assert_debug_snapshot!(created_updates);
    }

    #[test]
    fn rename_root() {
        let mut u = GraphUpdate::new();
        let mut result = import_corpus_graph_from_files(
            &mut u,
            Path::new("tests/data/import/exmaralda/"),
            &GenericImportConfiguration::new_with_root_name("custom_root_name".to_string())
                .and_extensions(vec!["exb".to_string()]),
        )
        .unwrap();
        result.sort();
        let path_to_name = result
            .into_iter()
            .map(|(p, s)| format!("{}  ->  {s}", p.to_string_lossy()))
            .collect_vec();

        assert_debug_snapshot!(path_to_name);
    }

    #[test]
    fn node_names_from_paths() {
        let paths = vec![
            "Sophisticated_Corpus_v1.9",
            "Sophisticated_Corpus_v1.9/lang1.1",
            "Sophisticated_Corpus_v1.9/lang2.1",
            "Sophisticated_Corpus_v1.9/lang1.1/doc1.fancyExtension",
            "Sophisticated_Corpus_v1.9/lang1.1/doc2.fancyExtension",
            "Sophisticated_Corpus_v1.9/lang2.1/doc1.fancyExtension",
            "Sophisticated_Corpus_v1.9/lang2.1/doc2.fancyExtension",
        ];
        let tmp_dir = TempDir::new().unwrap();
        assert!(fs::create_dir_all(tmp_dir.path().join(paths[1])).is_ok());
        assert!(fs::create_dir_all(tmp_dir.path().join(paths[2])).is_ok());
        paths[3..].iter().for_each(|p| {
            assert!(
                fs::write(tmp_dir.path().join(p), "".as_bytes()).is_ok(),
                "Error creating: {}",
                p
            )
        });
        let root_path = tmp_dir.path().join(paths[0]);
        let mut update = GraphUpdate::default();
        assert!(
            import_corpus_graph_from_files(
                &mut update,
                &root_path,
                &GenericImportConfiguration::new_with_extensions(vec![
                    "fancyExtension".to_string()
                ])
            )
            .is_ok()
        );
        assert_snapshot!(
            update
                .iter()
                .unwrap()
                .flatten()
                .map(|(_, ue)| match ue {
                    graphannis::update::UpdateEvent::AddNode { node_name, .. } =>
                        node_name.to_string(),
                    _ => "".to_string(),
                })
                .filter(|s| !s.is_empty())
                .join("\n")
        );
    }
}
