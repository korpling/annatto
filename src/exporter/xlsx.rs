use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap, HashSet},
};

use anyhow::anyhow;
use facet::Facet;
use graphannis::{AnnotationGraph, graph::GraphStorage, model::AnnotationComponentType};
use graphannis_core::{
    annostorage::{NodeAnnotationStorage, ValueSearch},
    graph::{ANNIS_NS, NODE_TYPE_KEY},
    types::{AnnoKey, Component, NodeID},
    util::join_qname,
};
use lazy_static::lazy_static;
use linked_hash_map::LinkedHashMap;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use rust_xlsxwriter::{Format, workbook::Workbook, worksheet::Worksheet};
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;

use crate::{
    progress::ProgressReporter,
    util::token_helper::{TOKEN_KEY, TokenHelper},
};

use super::Exporter;

/// Exports Excel Spreadsheets where each line is a token, the other columns are
/// spans and merged cells can be used for spans that cover more than one token.
#[derive(Facet, Default, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExportXlsx {
    /// If `true`, include the annotation namespace in the column header.
    #[serde(default)]
    include_namespace: bool,
    /// Specify the order of the exported columns as array of annotation keys.
    ///
    /// Example:
    ///
    /// ```toml
    /// [export.config]
    /// annotation_order = ["tok", "lemma", "pos"]
    /// ```
    ///
    /// Has no effect if the vector is empty.
    #[serde(default, with = "crate::estarde::anno_key::in_sequence")]
    annotation_order: Vec<AnnoKey>,
    /// If an output file for a document already exists and the content seems to
    /// be the same, don't overwrite the output file.
    ///
    /// Even with the same content, Excel files will appear as changed for
    /// version control systems because the binary files will be different. When
    /// this configuration value is set, the existing file will read and
    /// compared to the file that will be generated before overwriting it.
    #[serde(default)]
    skip_unchanged_files: bool,
}

fn find_token_roots(
    g: &graphannis::AnnotationGraph,
    token_helper: &TokenHelper,
    ordering_gs: Option<&dyn GraphStorage>,
) -> anyhow::Result<HashSet<NodeID>> {
    let mut roots: HashSet<_> = HashSet::new();
    for n in g
        .get_node_annos()
        .exact_anno_search(Some(ANNIS_NS), "tok", ValueSearch::Any)
    {
        let n = n?;

        // Check that this is an actual token and there are no outgoing coverage edges
        if token_helper.is_token(n.node)?
            && (ordering_gs.is_none()
                || ordering_gs.is_some_and(|gs| gs.get_ingoing_edges(n.node).next().is_none()))
        {
            roots.insert(n.node);
        }
    }
    Ok(roots)
}

fn is_span_column(
    anno_key: &AnnoKey,
    node_annos: &dyn NodeAnnotationStorage,
    token_helper: &TokenHelper,
) -> anyhow::Result<bool> {
    // Check that none of the nodes having this key are token and that there is at least one non-corpus node.
    // Document meta data and annotations inside documents could share the same
    // annotation names, but we only want to include the ones that are used as
    // annotations in a document.
    let mut has_non_corpus_match = false;
    for m in node_annos.exact_anno_search(Some(&anno_key.ns), &anno_key.name, ValueSearch::Any) {
        let m = m?;
        if token_helper.is_token(m.node)? {
            return Ok(false);
        }
        if let Some(node_type) = node_annos.get_value_for_item(&m.node, &NODE_TYPE_KEY)?
            && node_type == "node"
        {
            has_non_corpus_match = true;
        }
    }
    Ok(has_non_corpus_match)
}

fn overwritten_position_for_key(
    anno_key: &AnnoKey,
    position_overwrite: &HashMap<AnnoKey, u16>,
) -> Option<u16> {
    // Try the fully qualified name first, then check if the unspecific name is configured
    position_overwrite
        .get(anno_key)
        .or_else(|| {
            position_overwrite
                .iter()
                .find(|(k, _)| k.name.as_str() == anno_key.name.as_str())
                .map(|(_, ix)| ix)
        })
        .copied()
}

lazy_static! {
    static ref DEFAULT_FORMAT: Format = Format::new();
}

impl ExportXlsx {
    fn export_document(
        &self,
        doc_name: &str,
        doc_node_id: NodeID,
        g: &graphannis::AnnotationGraph,
        output_path: &std::path::Path,
        progress: &ProgressReporter,
    ) -> Result<(), anyhow::Error> {
        let mut workbook = Workbook::new();
        let worksheet = workbook.add_worksheet();

        let token_helper = TokenHelper::new(g)?;
        let ordering_component = Component::new(
            AnnotationComponentType::Ordering,
            ANNIS_NS.into(),
            "".into(),
        );
        let ordering_gs = g.get_graphstorage_as_ref(&ordering_component);

        let token_roots = find_token_roots(g, &token_helper, ordering_gs)?;

        let (token_to_row, has_only_empty_token) =
            self.create_token_colum(g, &token_roots, doc_node_id, worksheet)?;

        let column_offset = if !has_only_empty_token {
            worksheet.write(0, 0, "tok")?;
            1
        } else {
            0
        };
        // Output all spans
        let name_to_column = self.get_span_columns(g, &token_helper, column_offset)?;
        self.create_span_columns(
            g,
            &name_to_column,
            token_to_row,
            &token_helper,
            worksheet,
            progress,
        )?;

        // Add meta data sheet
        let meta_annos = g.get_node_annos().get_annotations_for_item(&doc_node_id)?;
        if !meta_annos.is_empty() {
            let meta_sheet = workbook.add_worksheet();
            meta_sheet.set_name("meta")?;
            meta_sheet.write(0, 0, "Name")?;
            meta_sheet.write(0, 1, "Value")?;

            let mut current_row = 1;
            for a in meta_annos {
                if a.key.ns != ANNIS_NS {
                    meta_sheet.write(current_row, 0, join_qname(&a.key.ns, &a.key.name))?;
                    meta_sheet.write(current_row, 1, a.val)?;
                    current_row += 1;
                }
            }
        }

        let output_path = output_path.join(format!("{doc_name}.xlsx"));

        if self.skip_unchanged_files
            && output_path.is_file()
            && let Some(parent_dir) = output_path.parent()
        {
            // Write the file to a temporary location and use a Excel-diff tool to compare the files
            let tmp_out = NamedTempFile::with_suffix_in(".xlsx", parent_dir)?;
            workbook.save(tmp_out.path())?;
            let diff = sheets_diff::core::diff::Diff::new(
                &output_path.to_string_lossy(),
                &tmp_out.path().to_string_lossy(),
            );
            let contains_changes = !diff.sheet_diff.is_empty() || !diff.cell_diffs.is_empty();
            if contains_changes {
                // Overwrite the output file with the temporary one
                tmp_out.persist(output_path)?;
            }
        } else {
            // Directly write the output file
            workbook.save(output_path)?;
        }

        Ok(())
    }

    /// Get all annotation names for spans and assign them to a spreadsheet column.
    fn get_span_columns(
        &self,
        g: &AnnotationGraph,
        token_helper: &TokenHelper,
        column_offset: u16,
    ) -> anyhow::Result<LinkedHashMap<AnnoKey, u16>> {
        // create a hash map from the configuration value
        let position_overwrite: HashMap<AnnoKey, u16> = self
            .annotation_order
            .iter()
            .enumerate()
            .map(|(idx, anno)| (anno.clone(), (idx as u16) + column_offset))
            .collect();

        let node_annos = g.get_node_annos();
        let mut all_anno_keys = node_annos.annotation_keys()?;

        // order the annotation keys by the configuration
        all_anno_keys.sort_by(|a, b| {
            let a_overwrite = overwritten_position_for_key(a, &position_overwrite);
            let b_overwrite = overwritten_position_for_key(b, &position_overwrite);

            if let Some(a_overwrite) = a_overwrite
                && let Some(b_overwrite) = b_overwrite
            {
                // Compare the configured values
                a_overwrite.cmp(&b_overwrite)
            } else if a_overwrite.is_some() {
                Ordering::Less
            } else if b_overwrite.is_some() {
                Ordering::Greater
            } else {
                // Use lexical comparision of the namespace/name components
                a.cmp(b)
            }
        });

        let mut result = LinkedHashMap::new();

        let mut column_index = column_offset;
        for anno_key in all_anno_keys {
            if anno_key.ns != ANNIS_NS && is_span_column(&anno_key, node_annos, token_helper)? {
                result.insert(anno_key, column_index);
                column_index += 1;
            }
        }

        Ok(result)
    }

    fn create_token_colum(
        &self,
        g: &AnnotationGraph,
        token_roots: &HashSet<NodeID>,
        doc_node_id: NodeID,
        worksheet: &mut rust_xlsxwriter::worksheet::Worksheet,
    ) -> anyhow::Result<(HashMap<NodeID, u32>, bool)> {
        let ordering_component = Component::new(
            AnnotationComponentType::Ordering,
            ANNIS_NS.into(),
            "".into(),
        );
        let ordering_gs = g.get_graphstorage_as_ref(&ordering_component);
        if let Some(gs_part_of) = g.get_graphstorage_as_ref(&Component::new(
            AnnotationComponentType::PartOf,
            ANNIS_NS.into(),
            "".into(),
        )) {
            // Output all token in the first column
            let mut token_roots_for_document = Vec::default();
            for t in token_roots {
                if gs_part_of.is_connected(*t, doc_node_id, 1, std::ops::Bound::Unbounded)? {
                    token_roots_for_document.push(*t);
                }
            }

            let mut has_only_empty_token = true;

            let mut token_to_row = HashMap::new();

            // Start with the first token
            let mut token = token_roots_for_document.into_iter().next();

            // Reserve the first row for the header
            let mut row_index = 1;
            while let Some(current_token) = token {
                if let Some(val) = g
                    .get_node_annos()
                    .get_value_for_item(&current_token, &TOKEN_KEY)?
                    && !val.trim().is_empty()
                {
                    has_only_empty_token = false;
                    worksheet.write(row_index, 0, val)?;
                }

                token_to_row.insert(current_token, row_index);

                token = if let Some(ordering_gs) = ordering_gs
                    && let Some(next_token) = ordering_gs.get_outgoing_edges(current_token).next()
                {
                    let next_token = next_token?;
                    Some(next_token)
                } else {
                    None
                };
                row_index += 1;
            }
            Ok((token_to_row, has_only_empty_token))
        } else {
            Err(anyhow!("Missing PartOf component"))
        }
    }

    fn create_span_columns(
        &self,
        g: &AnnotationGraph,
        name_to_column: &LinkedHashMap<AnnoKey, u16>,
        token_to_row: HashMap<NodeID, u32>,
        token_helper: &TokenHelper,
        worksheet: &mut Worksheet,
        progress: &ProgressReporter,
    ) -> anyhow::Result<()> {
        for span_anno_key in name_to_column.keys() {
            if let Some(column_index) = name_to_column.get(span_anno_key) {
                if self.include_namespace {
                    worksheet.write(
                        0,
                        *column_index,
                        join_qname(&span_anno_key.ns, &span_anno_key.name),
                    )?;
                } else {
                    worksheet.write(0, *column_index, span_anno_key.name.clone())?;
                }
                for span in g.get_node_annos().exact_anno_search(
                    Some(&span_anno_key.ns),
                    &span_anno_key.name,
                    ValueSearch::Any,
                ) {
                    let span = span?;
                    let span_val = g
                        .get_node_annos()
                        .get_value_for_item(&span.node, &span.anno_key)?
                        .unwrap_or_default();

                    let mut spanned_rows = BTreeSet::new();
                    // Find all token covered by the span
                    for gs in token_helper.get_gs_coverage().iter() {
                        for t in gs.get_outgoing_edges(span.node) {
                            let t = t?;
                            if let Some(row) = token_to_row.get(&t) {
                                spanned_rows.insert(row);
                            }
                        }
                    }
                    let first_row = spanned_rows.first();
                    let last_row = spanned_rows.last();

                    if let Some(first) = first_row
                        && let Some(last) = last_row
                    {
                        if *last - *first > 0 {
                            if worksheet
                                .merge_range(
                                    **first,
                                    *column_index,
                                    **last,
                                    *column_index,
                                    &span_val,
                                    &DEFAULT_FORMAT,
                                )
                                .is_err()
                            {
                                progress.warn(format!("Could not write span value {span_val} from row {first} to row {last} in column `{}`. A span already exists.", span_anno_key.name))?;
                            }
                        } else {
                            worksheet.write(**first, *column_index, span_val)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl Exporter for ExportXlsx {
    fn export_corpus(
        &self,
        graph: &graphannis::AnnotationGraph,
        output_path: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Find all documents in the graph
        let doc_annos = graph.get_node_annos().exact_anno_search(
            Some(ANNIS_NS),
            "doc",
            graphannis_core::annostorage::ValueSearch::Any,
        );

        let mut document_names = Vec::new();
        for m in doc_annos {
            let m = m?;
            if let Some(val) = graph
                .get_node_annos()
                .get_value_for_item(&m.node, &m.anno_key)?
            {
                document_names.push((val, m.node));
            }
        }

        let reporter = ProgressReporter::new(tx, step_id, document_names.len())?;

        std::fs::create_dir_all(output_path)?;

        let results: anyhow::Result<Vec<_>> = document_names
            .par_iter()
            .map(|(doc_name, doc_node_id)| {
                self.export_document(doc_name, *doc_node_id, graph, output_path, &reporter)?;
                reporter.worked(1)?;
                Ok(())
            })
            .collect();
        results?;
        Ok(())
    }

    fn file_extension(&self) -> &str {
        "xlsx"
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        path::{Path, PathBuf},
    };

    use insta::assert_snapshot;
    use sha2::{Digest, Sha256};
    use tempfile::TempDir;

    use crate::{
        ExporterStep, ImporterStep, ReadFrom, WriteAs, importer::xlsx::ImportSpreadsheet,
        test_util::compare_graphs,
    };

    use super::*;

    #[test]
    fn serialize() {
        let module = ExportXlsx::default();
        let serialization = toml::to_string(&module);
        assert!(
            serialization.is_ok(),
            "Serialization failed: {:?}",
            serialization.err()
        );
        assert_snapshot!(serialization.unwrap());
    }

    #[test]
    fn serialize_custom() {
        let module = ExportXlsx {
            annotation_order: vec![
                AnnoKey {
                    ns: "text".into(),
                    name: "text".into(),
                },
                AnnoKey {
                    ns: "edition".into(),
                    name: "edition".into(),
                },
            ],
            include_namespace: true,
            skip_unchanged_files: false,
        };
        let serialization = toml::to_string(&module);
        assert!(
            serialization.is_ok(),
            "Serialization failed: {:?}",
            serialization.err()
        );
        assert_snapshot!(serialization.unwrap());
    }

    #[test]
    fn with_segmentation() {
        let importer: ImportSpreadsheet = toml::from_str(
            r#"
        column_map = {"dipl" = ["sentence"], "norm" = ["pos", "lemma", "seg"]}
            "#,
        )
        .unwrap();
        let exporter = ExportXlsx::default();

        // Import an example document
        let path = Path::new("./tests/data/import/xlsx/clean/xlsx/");
        let orig_import_step = ImporterStep {
            module: crate::ReadFrom::Xlsx(importer),
            path: path.to_path_buf(),
            label: None,
        };
        let mut updates = orig_import_step.execute(None).unwrap();
        let mut original_graph = AnnotationGraph::with_default_graphstorages(false).unwrap();
        original_graph.apply_update(&mut updates, |_| {}).unwrap();

        // Export to Excel file, read it again and then compare the annotation graphs
        let tmp_outputdir = TempDir::new().unwrap();
        let output_dir = tmp_outputdir.path().join("xlsx");
        std::fs::create_dir(&output_dir).unwrap();
        let exporter = crate::WriteAs::Xlsx(exporter);
        let export_step = ExporterStep {
            module: exporter,
            path: output_dir.clone(),
            label: None,
        };
        export_step.execute(&original_graph, None).unwrap();

        let importer: ImportSpreadsheet = toml::from_str(
            r#"
        column_map = {"dipl" = ["sentence"], "norm" = ["pos", "lemma", "seg"]}
            "#,
        )
        .unwrap();
        let second_import_step = ImporterStep {
            module: crate::ReadFrom::Xlsx(importer),
            path: output_dir.clone(),
            label: None,
        };
        let mut updates = second_import_step.execute(None).unwrap();
        let mut written_graph = AnnotationGraph::with_default_graphstorages(false).unwrap();

        written_graph.apply_update(&mut updates, |_| {}).unwrap();

        compare_graphs(&original_graph, &written_graph);
    }

    #[test]
    fn with_token() {
        let importer: ImportSpreadsheet = toml::from_str(
            r#"
        column_map = {"tok" = ["lb"]}
            "#,
        )
        .unwrap();
        let exporter = ExportXlsx::default();

        // Import an example document
        let path = Path::new("./tests/data/import/xlsx/sample_sentence/");
        let importer = crate::ReadFrom::Xlsx(importer);
        let orig_import_step = ImporterStep {
            module: importer,
            path: path.to_path_buf(),
            label: None,
        };
        let mut updates = orig_import_step.execute(None).unwrap();
        let mut original_graph = AnnotationGraph::with_default_graphstorages(false).unwrap();
        original_graph.apply_update(&mut updates, |_| {}).unwrap();

        // Export to Excel file and read it again
        let tmp_outputdir = TempDir::new().unwrap();
        let output_dir = tmp_outputdir.path().join("sample_sentence");
        std::fs::create_dir(&output_dir).unwrap();
        let exporter = crate::WriteAs::Xlsx(exporter);
        let export_step = ExporterStep {
            module: exporter,
            path: output_dir.clone(),
            label: None,
        };
        export_step.execute(&original_graph, None).unwrap();

        let importer: ImportSpreadsheet = toml::from_str(
            r#"
        column_map = {"tok" = ["lb"]}
            "#,
        )
        .unwrap();
        let second_import_step = ImporterStep {
            module: crate::ReadFrom::Xlsx(importer),
            path: output_dir.clone(),
            label: None,
        };
        let mut updates = second_import_step.execute(None).unwrap();

        let mut written_graph = AnnotationGraph::with_default_graphstorages(false).unwrap();
        written_graph.apply_update(&mut updates, |_| {}).unwrap();

        // Compare the graphs and make sure the token exist
        compare_graphs(&original_graph, &written_graph);

        let q = graphannis::aql::parse("tok", false).unwrap();
        let it = graphannis::aql::execute_query_on_graph(&written_graph, &q, false, None).unwrap();
        assert_eq!(11, it.count());

        let q = graphannis::aql::parse("lb=\"1\"", false).unwrap();
        let it = graphannis::aql::execute_query_on_graph(&written_graph, &q, false, None).unwrap();
        assert_eq!(1, it.count());

        let q = graphannis::aql::parse("lb=\"2\"", false).unwrap();
        let it = graphannis::aql::execute_query_on_graph(&written_graph, &q, false, None).unwrap();
        assert_eq!(1, it.count());
    }

    #[test]
    fn with_namespace() {
        let importer: ImportSpreadsheet = toml::from_str(
            r#"
        column_map = {"default_ns::text" = ["mynamespace::lb"]}
            "#,
        )
        .unwrap();
        let importer = ReadFrom::Xlsx(importer);
        let mut exporter = ExportXlsx::default();
        exporter.include_namespace = true;
        exporter.annotation_order = vec![AnnoKey {
            ns: "default_ns".into(),
            name: "text".into(),
        }];
        let exporter = WriteAs::Xlsx(exporter);

        // Import an example document
        let path = Path::new("./tests/data/import/xlsx/sample_sentence_with_namespace/");
        let first_import_step = ImporterStep {
            module: importer,
            path: path.to_path_buf(),
            label: None,
        };

        let mut updates = first_import_step.execute(None).unwrap();
        let mut original_graph = AnnotationGraph::with_default_graphstorages(false).unwrap();
        original_graph.apply_update(&mut updates, |_| {}).unwrap();

        // Export to Excel file and read it again
        let tmp_outputdir = TempDir::new().unwrap();
        let output_dir = tmp_outputdir.path().join("sample_sentence_with_namespace");
        std::fs::create_dir(&output_dir).unwrap();
        let export_step = ExporterStep {
            module: exporter,
            path: output_dir.clone(),
            label: None,
        };
        export_step.execute(&original_graph, None).unwrap();

        let importer: ImportSpreadsheet = toml::from_str(
            r#"
        column_map = {"default_ns::text" = ["mynamespace::lb"]}
            "#,
        )
        .unwrap();
        let second_import_step = ImporterStep {
            module: crate::ReadFrom::Xlsx(importer),
            path: output_dir.clone(),
            label: None,
        };
        let mut updates = second_import_step.execute(None).unwrap();

        let mut written_graph = AnnotationGraph::with_default_graphstorages(false).unwrap();

        written_graph.apply_update(&mut updates, |_| {}).unwrap();

        // Compare the graphs and make sure the token exist
        compare_graphs(&original_graph, &written_graph);

        let q = graphannis::aql::parse("tok", false).unwrap();
        let it = graphannis::aql::execute_query_on_graph(&written_graph, &q, false, None).unwrap();
        assert_eq!(11, it.count());

        let q = graphannis::aql::parse("mynamespace:lb=\"1\"", false).unwrap();
        let it = graphannis::aql::execute_query_on_graph(&written_graph, &q, false, None).unwrap();
        assert_eq!(1, it.count());

        let q = graphannis::aql::parse("mynamespace:lb=\"2\"", false).unwrap();
        let it = graphannis::aql::execute_query_on_graph(&written_graph, &q, false, None).unwrap();
        assert_eq!(1, it.count());
    }

    #[test]
    fn with_meta() {
        let importer: ImportSpreadsheet = toml::from_str(
            r#"
        column_map = {"tok" = ["lb"]}
        metasheet = "meta"
        metasheet_skip_rows = 1
            "#,
        )
        .unwrap();
        let exporter = ExportXlsx::default();

        // Import an example document
        let path = Path::new("./tests/data/import/xlsx/sample_sentence/");
        let importer = crate::ReadFrom::Xlsx(importer);
        let orig_import_step = ImporterStep {
            module: importer,
            path: path.to_path_buf(),
            label: None,
        };
        let mut updates = orig_import_step.execute(None).unwrap();
        let mut original_graph = AnnotationGraph::with_default_graphstorages(false).unwrap();
        original_graph.apply_update(&mut updates, |_| {}).unwrap();

        // Export to Excel file and read it again
        let tmp_outputdir = TempDir::new().unwrap();
        let output_dir = tmp_outputdir.path().join("sample_sentence");
        std::fs::create_dir(&output_dir).unwrap();
        let exporter = crate::WriteAs::Xlsx(exporter);
        let export_step = ExporterStep {
            module: exporter,
            path: output_dir.clone(),
            label: None,
        };
        export_step.execute(&original_graph, None).unwrap();

        let importer: ImportSpreadsheet = toml::from_str(
            r#"
        column_map = {"tok" = ["lb"]}
        metasheet = "meta"
        metasheet_skip_rows = 1
            "#,
        )
        .unwrap();
        let second_import_step = ImporterStep {
            module: crate::ReadFrom::Xlsx(importer),
            path: output_dir.clone(),
            label: None,
        };
        let mut updates = second_import_step.execute(None).unwrap();

        let mut written_graph = AnnotationGraph::with_default_graphstorages(false).unwrap();
        written_graph.apply_update(&mut updates, |_| {}).unwrap();

        let q = graphannis::aql::parse("Author=\"Unknown\" _ident_ annis:doc", false).unwrap();
        let it = graphannis::aql::execute_query_on_graph(&written_graph, &q, false, None).unwrap();
        assert_eq!(1, it.count());

        let q = graphannis::aql::parse("Year=\"2024\" _ident_ annis:doc", false).unwrap();
        let it = graphannis::aql::execute_query_on_graph(&written_graph, &q, false, None).unwrap();
        assert_eq!(1, it.count());

        // The header should not be imported
        let q = graphannis::aql::parse("Name _ident_ annis:doc", false).unwrap();
        let it = graphannis::aql::execute_query_on_graph(&written_graph, &q, false, None).unwrap();
        assert_eq!(0, it.count());
    }

    //// Create a tempory corpus directory with a copy of an excel file that we can read and change
    /// Returns the corpus directory, the path to the single document in it and the hash of the document;
    fn create_corpus_folder_and_hash() -> (TempDir, PathBuf, String) {
        let corpus_dir = tempfile::TempDir::new().unwrap();

        let document_path = corpus_dir.path().join("doc1.xlsx");
        std::fs::copy(
            Path::new("./tests/data/import/xlsx/sample_sentence/doc1.xlsx"),
            &document_path,
        )
        .unwrap();
        let mut file = File::open(&document_path).unwrap();
        let mut sha256 = Sha256::new();
        std::io::copy(&mut file, &mut sha256).unwrap();
        let hash_value = format!("{:02X?}", sha256.finalize());

        (corpus_dir, document_path, hash_value)
    }

    #[test]
    fn export_skips_unchanged() {
        let (corpus_dir, document_path, original_hash) = create_corpus_folder_and_hash();

        // Import an example document
        let importer: ImportSpreadsheet = toml::from_str(
            r#"
        column_map = {"tok" = ["lb"]}
        metasheet = "meta"
        metasheet_skip_rows = 1
            "#,
        )
        .unwrap();
        let importer = crate::ReadFrom::Xlsx(importer);
        let orig_import_step = ImporterStep {
            module: importer,
            path: corpus_dir.path().to_path_buf(),
            label: None,
        };
        let mut updates = orig_import_step.execute(None).unwrap();
        let mut graph = AnnotationGraph::with_default_graphstorages(false).unwrap();
        graph.apply_update(&mut updates, |_| {}).unwrap();

        // Export to the same folder and configure exporter to skip unchanged files
        let exporter: ExportXlsx = toml::from_str(
            r#"
            skip_unchanged_files = true
            annotation_order = ["tok", "lb"]
            "#,
        )
        .unwrap();

        let exporter = crate::WriteAs::Xlsx(exporter);
        let export_step = ExporterStep {
            module: exporter,
            path: corpus_dir.path().to_path_buf(),
            label: None,
        };
        export_step.execute(&graph, None).unwrap();

        // Calculate the hash sum of the file again, it should not have changed because the file was not overwritten
        let mut file = File::open(&document_path).unwrap();
        let mut sha256 = Sha256::new();
        std::io::copy(&mut file, &mut sha256).unwrap();
        let hash_after_conversion = format!("{:02X?}", sha256.finalize());

        assert_eq!(original_hash, hash_after_conversion);
    }

    #[test]
    fn export_overwrites_changed() {
        let (corpus_dir, document_path, original_hash) = create_corpus_folder_and_hash();

        // Import an example document
        let importer: ImportSpreadsheet = toml::from_str(
            r#"
        column_map = {"tok" = ["lb"]}
        metasheet = "meta"
        metasheet_skip_rows = 1
            "#,
        )
        .unwrap();
        let importer = crate::ReadFrom::Xlsx(importer);
        let orig_import_step = ImporterStep {
            module: importer,
            path: corpus_dir.path().to_path_buf(),
            label: None,
        };
        let mut updates = orig_import_step.execute(None).unwrap();
        let mut graph = AnnotationGraph::with_default_graphstorages(false).unwrap();
        graph.apply_update(&mut updates, |_| {}).unwrap();

        // Export to the same folder and configure exporter to skip unchanged
        // files but configure the annotation order in a way that the file will
        // be different from the original one
        let exporter: ExportXlsx = toml::from_str(
            r#"
            skip_unchanged_files = true
            annotation_order = ["lb", "tok"]
            "#,
        )
        .unwrap();

        let exporter = crate::WriteAs::Xlsx(exporter);
        let export_step = ExporterStep {
            module: exporter,
            path: corpus_dir.path().to_path_buf(),
            label: None,
        };
        export_step.execute(&graph, None).unwrap();

        // Calculate the hash sum of the file again, it should have changed because the file was overwritten
        let mut file = File::open(&document_path).unwrap();
        let mut sha256 = Sha256::new();
        std::io::copy(&mut file, &mut sha256).unwrap();
        let hash_after_conversion = format!("{:02X?}", sha256.finalize());

        assert_ne!(original_hash, hash_after_conversion);
    }

    #[test]
    fn export_overwrites_unchanged() {
        let (corpus_dir, document_path, original_hash) = create_corpus_folder_and_hash();

        // Import an example document
        let importer: ImportSpreadsheet = toml::from_str(
            r#"
        column_map = {"tok" = ["lb"]}
        metasheet = "meta"
        metasheet_skip_rows = 1
            "#,
        )
        .unwrap();
        let importer = crate::ReadFrom::Xlsx(importer);
        let orig_import_step = ImporterStep {
            module: importer,
            path: corpus_dir.path().to_path_buf(),
            label: Some("custom-xlsx-export-id".to_string()),
        };
        let mut updates = orig_import_step.execute(None).unwrap();
        let mut graph = AnnotationGraph::with_default_graphstorages(false).unwrap();
        graph.apply_update(&mut updates, |_| {}).unwrap();

        // Export to the same folder and configure exporter to overwrite unchanged files
        let exporter: ExportXlsx = toml::from_str(
            r#"
            skip_unchanged_files = false
            annotation_order = ["tok", "lb"]
            "#,
        )
        .unwrap();

        let exporter = crate::WriteAs::Xlsx(exporter);
        let export_step = ExporterStep {
            module: exporter,
            path: corpus_dir.path().to_path_buf(),
            label: None,
        };
        export_step.execute(&graph, None).unwrap();

        // Calculate the hash sum of the file again, it should have changed because the file was overwritten
        let mut file = File::open(&document_path).unwrap();
        let mut sha256 = Sha256::new();
        std::io::copy(&mut file, &mut sha256).unwrap();
        let hash_after_conversion = format!("{:02X?}", sha256.finalize());

        assert_ne!(original_hash, hash_after_conversion);
    }
}
