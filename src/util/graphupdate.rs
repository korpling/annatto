use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
};

use crate::{
    Result,
    error::AnnattoError,
    importer::{GenericImportConfiguration, NamedPaths},
};
use anyhow::anyhow;
use graphannis::{
    model::{AnnotationComponent, AnnotationComponentType},
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::{ANNIS_NS, DEFAULT_NS};
use normpath::PathExt;

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

pub struct UpdateWriter {
    inner: GraphUpdate,
    pub(crate) config: GenericImportConfiguration,
}

impl From<GenericImportConfiguration> for UpdateWriter {
    fn from(config: GenericImportConfiguration) -> Self {
        Self {
            inner: GraphUpdate::default(),
            config,
        }
    }
}

impl UpdateWriter {
    /// Add a node with the given name and type.
    pub fn node(&mut self, node_name: String, node_type: String) -> Result<()> {
        self.inner.add_event(UpdateEvent::AddNode {
            node_name,
            node_type,
        })?;
        Ok(())
    }

    /// Add a node with the given name and type and attach
    /// it to a corpus node via a `PartOf` edge.
    pub fn node_at(
        &mut self,
        node_name: String,
        node_type: String,
        corpus_node: String,
    ) -> Result<()> {
        self.node(node_name.to_string(), node_type)?;
        self.part_of(node_name, corpus_node)?;
        Ok(())
    }

    /// Delete the node with the given name.
    pub fn delete(&mut self, node_name: String) -> Result<()> {
        self.inner
            .add_event(UpdateEvent::DeleteNode { node_name })?;
        Ok(())
    }

    /// Add a label to a node.
    pub fn node_label(
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

    /// Add a label to a node and use the default namespace.
    pub fn default_node_label(
        &mut self,
        node_name: String,
        anno_name: String,
        anno_value: String,
    ) -> Result<()> {
        self.node_label(
            node_name,
            self.config.default_namespace().to_string(),
            anno_name,
            anno_value,
        )
    }

    /// Create a new node and add a label.
    pub fn labeled_node(
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

    /// Add an edge in the given component.
    pub fn edge(
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

    /// Add an edge by providing its parameters.
    pub fn edge_by_parts(
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

    /// Add a label to an edge.
    pub fn edge_label(
        &mut self,
        source_node: String,
        target_node: String,
        component: &AnnotationComponent,
        anno_ns: String,
        anno_name: String,
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

    /// Add a label to an edge and use the default namespace.
    pub fn default_edge_label(
        &mut self,
        source_node: String,
        target_node: String,
        component: &AnnotationComponent,
        anno_name: String,
        anno_value: String,
    ) -> Result<()> {
        self.edge_label(
            source_node,
            target_node,
            component,
            self.config.default_namespace().to_string(),
            anno_name,
            anno_value,
        )
    }

    /// Add an edge to a component and a label to the new edge.
    pub fn labeled_edge(
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

    /// Add an ordering edge.
    pub fn ordering(&mut self, source: String, target: String) -> Result<&AnnotationComponent> {
        self.inner.add_event(UpdateEvent::AddEdge {
            source_node: source,
            target_node: target,
            layer: self.config.default_ordering().layer.to_string(),
            component_type: self.config.default_ordering().get_type().to_string(),
            component_name: self.config.default_ordering().name.to_string(),
        })?;
        Ok(self.config.default_ordering())
    }

    /// Add a named ordering edge. NOTE that this ignores the name
    /// of the default ordering component.
    pub fn named_ordering(
        &mut self,
        source: String,
        target: String,
        name: String,
    ) -> Result<AnnotationComponent> {
        self.edge_by_parts(
            source,
            target,
            self.config.default_ordering().layer.to_string(),
            self.config.default_ordering().get_type(),
            name,
        )
    }

    /// Add a part of edge.
    pub fn part_of(&mut self, source: String, target: String) -> Result<&AnnotationComponent> {
        self.inner.add_event(UpdateEvent::AddEdge {
            source_node: source,
            target_node: target,
            layer: self.config.default_part_of().layer.to_string(),
            component_type: self.config.default_part_of().get_type().to_string(),
            component_name: self.config.default_part_of().name.to_string(),
        })?;
        Ok(self.config.default_part_of())
    }

    /// Add a coverage edge.
    pub fn coverage(&mut self, source: String, target: String) -> Result<&AnnotationComponent> {
        self.inner.add_event(UpdateEvent::AddEdge {
            source_node: source,
            target_node: target,
            layer: self.config.default_coverage().layer.to_string(),
            component_type: self.config.default_coverage().get_type().to_string(),
            component_name: self.config.default_coverage().name.to_string(),
        })?;
        Ok(self.config.default_coverage())
    }

    /// Add a dominance edge.
    pub fn dominance(&mut self, source: String, target: String) -> Result<&AnnotationComponent> {
        self.inner.add_event(UpdateEvent::AddEdge {
            source_node: source,
            target_node: target,
            layer: self.config.default_dominance().layer.to_string(),
            component_type: self.config.default_dominance().get_type().to_string(),
            component_name: self.config.default_dominance().name.to_string(),
        })?;
        Ok(self.config.default_dominance())
    }

    /// Add a named dominance edge. NOTE that this ignores the name
    /// of the default dominance component.
    pub fn named_dominance(
        &mut self,
        source: String,
        target: String,
        name: String,
    ) -> Result<AnnotationComponent> {
        self.edge_by_parts(
            source,
            target,
            self.config.default_dominance().layer.to_string(),
            self.config.default_dominance().get_type(),
            name,
        )
    }

    /// Add a pointing edge.
    pub fn pointing(&mut self, source: String, target: String) -> Result<&AnnotationComponent> {
        self.inner.add_event(UpdateEvent::AddEdge {
            source_node: source,
            target_node: target,
            layer: self.config.default_pointing().layer.to_string(),
            component_type: self.config.default_pointing().get_type().to_string(),
            component_name: self.config.default_pointing().name.to_string(),
        })?;
        Ok(self.config.default_pointing())
    }

    /// Add a pointing edge with a different name. This overrides the default pointing
    /// component's name.
    pub fn named_pointing(
        &mut self,
        source: String,
        target: String,
        name: String,
    ) -> Result<AnnotationComponent> {
        self.edge_by_parts(
            source,
            target,
            self.config.default_pointing().layer.to_string(),
            self.config.default_pointing().get_type(),
            name,
        )
    }

    /// Forward a raw update event into the `GraphUpdate`.
    pub fn add_event(&mut self, ue: UpdateEvent) -> Result<()> {
        Ok(self.inner.add_event(ue)?)
    }

    /// Finds all files with a given ending in a directory and map the corpus
    /// structure according to the file/directory structure.
    ///
    /// The root directory is mapped as root corpus, sub-directories as sub-corpora
    /// and files as documents. The found documents are returned with the
    /// corresponding file path and the document node ID as string.
    pub fn write_corpus_graph_from_file_tree(&mut self, import_path: &Path) -> Result<NamedPaths> {
        let root_name = self
            .config
            .custom_root_name()
            .unwrap_or(root_corpus_from_path(import_path)?);

        self.node(root_name.clone(), "corpus".to_string())?;

        let mut path_tuples = self.add_subcorpora(import_path, &root_name)?;
        path_tuples.sort();
        Ok(path_tuples)
    }

    pub fn media_source(&mut self, document_node: String, path: &Path) -> Result<String> {
        let media_node_base = path
            .file_name()
            .ok_or::<AnnattoError>(AnnattoError::Anyhow(anyhow!("Filename cannot be derived.")))?
            .to_string_lossy();
        let media_node_name = format!("{}/{media_node_base}", document_node);
        self.labeled_node(
            media_node_name.to_string(),
            "file".to_string(),
            ANNIS_NS.to_string(),
            "file".to_string(),
            path.to_string_lossy().to_string(),
        )?;
        self.part_of(media_node_name.to_string(), document_node)?;
        Ok(media_node_name)
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
            self.part_of(node_name.clone(), parent_corpus.to_string())?;
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

impl From<UpdateWriter> for GraphUpdate {
    fn from(value: UpdateWriter) -> Self {
        value.inner
    }
}

#[deprecated]
pub fn add_order_relations<S: AsRef<str>>(
    u: &mut UpdateWriter,
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
    u: &mut UpdateWriter,
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
    u: &mut UpdateWriter,
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

    use insta::{assert_debug_snapshot, assert_snapshot};
    use itertools::Itertools;
    use tempfile::TempDir;

    use crate::{
        importer::{
            DefaultImportConfiguration, GenericImportConfiguration, exmaralda::ImportEXMARaLDA,
        },
        util::graphupdate::UpdateWriter,
    };

    #[test]
    fn single_file_import() {
        let mut u = UpdateWriter::from(ImportEXMARaLDA::default().default_configuration());
        let result = u
            .write_corpus_graph_from_file_tree(Path::new(
                "tests/data/import/exmaralda/clean/import/exmaralda/test_doc.exb",
            ))
            .unwrap();

        assert_eq!(1, result.len());
        assert_eq!(
            "tests/data/import/exmaralda/clean/import/exmaralda/test_doc.exb",
            result[0].0.to_string_lossy()
        );
        assert_eq!("test_doc/test_doc", result[0].1);

        let created_updates: graphannis_core::errors::Result<Vec<_>> =
            u.inner.iter().unwrap().collect();
        let created_updates = created_updates.unwrap();

        assert_debug_snapshot!(created_updates);
    }

    #[test]
    fn rename_root() {
        let config =
            GenericImportConfiguration::new_with_default_extensions(&ImportEXMARaLDA::default())
                .and_root_name("custom_root_name".to_string());

        let mut u = UpdateWriter::from(config);
        let mut result = u
            .write_corpus_graph_from_file_tree(Path::new("tests/data/import/exmaralda/"))
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
        let config = ImportEXMARaLDA::default()
            .default_configuration()
            .and_extensions(vec!["fancyExtension".to_string()]);
        let mut update = UpdateWriter::from(config);
        assert!(update.write_corpus_graph_from_file_tree(&root_path).is_ok());
        assert_snapshot!(
            update
                .inner
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
