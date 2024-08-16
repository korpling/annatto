use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Display,
    path::Path,
};

use anyhow::{bail, Context};
use graphannis::{
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::{
    graph::{ANNIS_NS, DEFAULT_NS},
    util::split_qname,
};
use itertools::Itertools;
use serde_derive::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;
use umya_spreadsheet::Cell;

use super::Importer;
use crate::{error::AnnattoError, progress::ProgressReporter, util, StepID};
use documented::{Documented, DocumentedFields};

/// Imports Excel Spreadsheets where each line is a token, the other columns are
/// spans and merged cells can be used for spans that cover more than one token.
#[derive(Default, Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(default, deny_unknown_fields)]
pub struct ImportSpreadsheet {
    /// Maps token columns to annotation columns. If there is more than one
    /// token column, it is assumed that the corpus has multiple segmentations.
    /// In this case, it is necessary to tell the importer which annotation column belongs to which token column.
    ///
    /// Example with the two token columns "dipl" and "norm":
    ///
    /// ```toml
    /// [import.config]
    /// column_map = {"dipl" = ["sentence"], "norm" = ["pos", "lemma", "seg"]}
    /// ```
    /// The column "sentence" must be always be aligned with the "dipl" token
    /// and "pos", "lemma" and "seg" are aligned with the "norm" token.
    column_map: BTreeMap<String, BTreeSet<String>>,
    /// If given, the name of the token column to be used when there is no
    /// explicit mapping given in the `column_map` parameter for this annotation
    /// column.
    ///
    /// Example with two token columns "dipl" and "norm", where all annotation
    /// columns except "lemma" and "pos" are mapped to the "dipl" token column:
    ///
    /// ```toml
    /// [import.config]
    /// column_map = {"dipl" = [], "norm" = ["pos", "lemma"]}
    /// fallback = "dipl"
    /// ```
    fallback: Option<String>,
    /// Optional value of the Excel sheet that contains the data. If not given,
    /// the first sheet is used.
    datasheet: Option<SheetAddress>,
    /// Optional value of the Excel sheet that contains the metadata table. If
    /// no metadata is imported.    
    metasheet: Option<SheetAddress>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(untagged)]
enum SheetAddress {
    Numeric(usize),
    Name(String),
}

impl Display for SheetAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let v = match self {
            SheetAddress::Numeric(n) => n.to_string(),
            SheetAddress::Name(s) => s.to_string(),
        };
        write!(f, "{v}")
    }
}

fn sheet_from_address<'a>(
    book: &'a umya_spreadsheet::Spreadsheet,
    address: &Option<SheetAddress>,
    default: Option<usize>,
) -> Option<&'a umya_spreadsheet::Worksheet> {
    if let Some(addr) = &address {
        match addr {
            SheetAddress::Numeric(n) => book.get_sheet(n),
            SheetAddress::Name(s) => book.get_sheet_by_name(s),
        }
    } else if let Some(default_addr) = &default {
        book.get_sheet(default_addr)
    } else {
        None
    }
}

impl ImportSpreadsheet {
    fn import_datasheet(
        &self,
        doc_path: &str,
        sheet: &umya_spreadsheet::Worksheet,
        update: &mut GraphUpdate,
        progress_reporter: &ProgressReporter,
    ) -> Result<(), AnnattoError> {
        let mut fullmap = self.column_map.clone();
        let known_names = self
            .column_map
            .values()
            .flatten()
            .collect::<BTreeSet<&String>>();
        if let Some(fallback_name) = &self.fallback {
            if fallback_name.is_empty() {
                fullmap.insert("".to_string(), BTreeSet::new());
            }
        }
        let name_to_col_0index = {
            let mut m = BTreeMap::new();
            let header_row = sheet.get_collection_by_row(&1);
            for cell in header_row {
                let name = cell.get_cell_value().get_value().trim().to_string();
                if !name.is_empty() {
                    m.insert(name.to_string(), cell.get_coordinate().get_col_num() - 1);
                    if let Some(fallback_name) = &self.fallback {
                        if !known_names.contains(&name) && !fullmap.contains_key(&name) {
                            if let Some(anno_names) = fullmap.get_mut(fallback_name) {
                                anno_names.insert(name);
                            } else {
                                progress_reporter.warn(&format!(
                                    "`{fallback_name}` is not a valid fallback. Only empty string and keys of the column map are allowed. Column `{name}` will be ignored."))?;
                            }
                        }
                    }
                }
            }
            m
        };
        let merged_cells =
            MergedCellHelper::new(doc_path, sheet, &name_to_col_0index, progress_reporter)?;
        let mut base_tokens = Vec::new();
        for i in 2..=sheet.get_highest_row() {
            let tok_id = format!("{}#t{}", &doc_path, i - 1);
            update.add_event(UpdateEvent::AddNode {
                node_name: tok_id.to_string(),
                node_type: "node".to_string(),
            })?;
            update.add_event(UpdateEvent::AddNodeLabel {
                node_name: tok_id.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: " ".to_string(),
            })?;
            update.add_event(UpdateEvent::AddNodeLabel {
                node_name: tok_id.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "layer".to_string(),
                anno_value: "default_layer".to_string(),
            })?;
            update.add_event(UpdateEvent::AddEdge {
                source_node: tok_id.to_string(),
                target_node: doc_path.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string(),
            })?;
            base_tokens.push(tok_id);
        }
        base_tokens
            .iter()
            .tuple_windows()
            .try_for_each(|(first, second)| {
                update.add_event(UpdateEvent::AddEdge {
                    source_node: first.to_string(),
                    target_node: second.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "".to_string(),
                })?;
                Ok::<(), AnnattoError>(())
            })?;
        for (tok_name, anno_names) in &fullmap {
            let mut names = if tok_name.is_empty() {
                vec![]
            } else {
                vec![tok_name]
            };
            names.extend(anno_names);
            for name in names {
                let index_opt = match name_to_col_0index.get(name) {
                    Some(v) => Some(v),
                    None => {
                        let k = split_qname(name).1;
                        name_to_col_0index.get(k)
                    }
                };
                let mut nodes = Vec::new();
                if let Some(col_0i) = index_opt {
                    let mut row_nums = if let Some(indices) =
                        merged_cells.valid_rows_by_column.get(col_0i)
                    {
                        indices.iter().collect_vec()
                    } else {
                        progress_reporter.warn(format!("Can not determine row indices of column {name}, thus it will be skipped.").as_str())?;
                        continue;
                    };
                    row_nums.sort_unstable();

                    for (start_row, end_row_excl) in row_nums.into_iter().tuple_windows() {
                        let cell = match sheet.get_cell(((col_0i + 1), *start_row)) {
                            Some(cl) => cl,
                            None => continue,
                        };
                        let cell_value = cell.get_value();
                        let value = cell_value.trim();
                        if value.is_empty() {
                            continue;
                        }
                        let base_token_start = *start_row as usize - 2;
                        let base_token_end = *end_row_excl as usize - 2;
                        let overlapped_base_tokens: &[String] =
                            &base_tokens[base_token_start..base_token_end]; // TODO check indices
                        let node_name =
                            format!("{}#{}_{}-{}", &doc_path, tok_name, start_row, end_row_excl);
                        update.add_event(UpdateEvent::AddNode {
                            node_name: node_name.to_string(),
                            node_type: "node".to_string(),
                        })?;
                        update.add_event(UpdateEvent::AddEdge {
                            source_node: node_name.clone(),
                            target_node: doc_path.to_string(),
                            layer: ANNIS_NS.to_string(),
                            component_type: AnnotationComponentType::PartOf.to_string(),
                            component_name: "".to_string(),
                        })?;
                        if !tok_name.is_empty() {
                            update.add_event(UpdateEvent::AddNodeLabel {
                                node_name: node_name.to_string(),
                                anno_ns: ANNIS_NS.to_string(),
                                anno_name: "layer".to_string(),
                                anno_value: tok_name.to_string(),
                            })?;
                            if name == tok_name {
                                update.add_event(UpdateEvent::AddNodeLabel {
                                    node_name: node_name.to_string(),
                                    anno_ns: ANNIS_NS.to_string(),
                                    anno_name: "tok".to_string(),
                                    anno_value: value.to_string(),
                                })?;
                            } else {
                                // Add coverage edges between this span and the
                                // segmentation token. Find all segmentation
                                // token that cover the same rows.
                                if let Some(segmentation_column) = name_to_col_0index.get(tok_name)
                                {
                                    if let Some(segmentation_column_row_nums) =
                                        merged_cells.valid_rows_by_column.get(segmentation_column)
                                    {
                                        for segmentation_start_row in *start_row..*end_row_excl {
                                            // Skip cells that are merged with a previous row
                                            if segmentation_column_row_nums
                                                .contains(&segmentation_start_row)
                                            {
                                                let segmentation_end_row = merged_cells.start_row_by_column
                                                .get(segmentation_column)
                                                .with_context(|| format!("Merged cell helper not found for segmentation column {segmentation_column} ({doc_path})"))?.
                                                get(&segmentation_start_row).unwrap_or(&segmentation_start_row);

                                                let segmentation_node_name = format!(
                                                    "{}#{}_{}-{}",
                                                    &doc_path,
                                                    tok_name,
                                                    segmentation_start_row,
                                                    segmentation_end_row
                                                );

                                                update.add_event(UpdateEvent::AddEdge {
                                                    source_node: node_name.to_string(),
                                                    target_node: segmentation_node_name,
                                                    layer: ANNIS_NS.to_string(),
                                                    component_type:
                                                        AnnotationComponentType::Coverage
                                                            .to_string(),
                                                    component_name: "".to_string(),
                                                })?;
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        let (anno_ns, anno_name) = split_qname(name);

                        update.add_event(UpdateEvent::AddNodeLabel {
                            node_name: node_name.to_string(),
                            anno_ns: anno_ns.unwrap_or(tok_name).to_string(),
                            anno_name: anno_name.to_string(),
                            anno_value: value.to_string(),
                        })?;

                        for target_id in overlapped_base_tokens {
                            update.add_event(UpdateEvent::AddEdge {
                                source_node: node_name.to_string(),
                                target_node: target_id.to_string(),
                                layer: ANNIS_NS.to_string(),
                                component_type: AnnotationComponentType::Coverage.to_string(),
                                component_name: "".to_string(),
                            })?;
                        }
                        if !name.is_empty() && name == tok_name {
                            nodes.push(node_name);
                        }
                    }
                    if !nodes.is_empty() {
                        nodes.iter().tuple_windows().try_for_each(
                            |(first_name, second_name)| {
                                update.add_event(UpdateEvent::AddEdge {
                                    source_node: first_name.to_string(),
                                    target_node: second_name.to_string(),
                                    layer: DEFAULT_NS.to_string(),
                                    component_type: AnnotationComponentType::Ordering.to_string(),
                                    component_name: tok_name.to_string(),
                                })?;
                                Ok::<(), AnnattoError>(())
                            },
                        )?;
                        nodes.clear();
                    }
                } else {
                    progress_reporter.info(&format!("No column `{name}` in file {}", &doc_path))?;
                    continue;
                }
            }
        }
        Ok(())
    }

    fn import_metasheet(
        &self,
        doc_path: &str,
        sheet: &umya_spreadsheet::Worksheet,
        update: &mut GraphUpdate,
    ) -> Result<(), AnnattoError> {
        let max_row_num = sheet.get_highest_row(); // 1-based
        for row_num in 1..max_row_num + 1 {
            let entries = sheet.get_collection_by_row(&row_num); // sorting not necessarily by col number
            let entry_map = entries
                .into_iter()
                .map(|c| (*c.get_coordinate().get_col_num(), c))
                .collect::<BTreeMap<u32, &Cell>>();
            if let Some(key_cell) = entry_map.get(&1) {
                if let Some(value_cell) = entry_map.get(&2) {
                    let kv = key_cell.get_value();
                    let key = kv.trim();
                    if !key.is_empty() {
                        let (ns, name) = split_qname(key);
                        let vv = value_cell.get_value();
                        let value = vv.trim();
                        update.add_event(UpdateEvent::AddNodeLabel {
                            node_name: doc_path.to_string(),
                            anno_ns: ns.map_or("".to_string(), str::to_string),
                            anno_name: name.to_string(),
                            anno_value: value.to_string(),
                        })?;
                    }
                }
            }
        }
        Ok(())
    }

    fn import_workbook(
        &self,
        update: &mut GraphUpdate,
        path: &Path,
        doc_node_name: &str,
        progress_reporter: &ProgressReporter,
    ) -> Result<(), AnnattoError> {
        let book = umya_spreadsheet::reader::xlsx::read(path)?;
        if let Some(sheet) = sheet_from_address(&book, &self.datasheet, Some(0)) {
            self.import_datasheet(doc_node_name, sheet, update, progress_reporter)?;
        }
        if let Some(sheet) = sheet_from_address(&book, &self.metasheet, None) {
            self.import_metasheet(doc_node_name, sheet, update)?;
        }
        Ok(())
    }
}

struct MergedCellHelper {
    /// Set of valid row numbers for each column. The column index is 0-based.
    /// For merged cells, only the first row of the cell is included in the set.
    valid_rows_by_column: BTreeMap<u32, BTreeSet<u32>>,
    /// The actual start row for any merged cell by the column. The column index
    /// is 0-based.
    start_row_by_column: BTreeMap<u32, BTreeMap<u32, u32>>,
}

impl MergedCellHelper {
    /// Get the merged cells of the given sheet and return a helper data
    /// structure that makes it easier to retrieve relevant information about
    /// these merged cells.
    fn new(
        doc_path: &str,
        sheet: &umya_spreadsheet::Worksheet,
        name_to_col_0index: &BTreeMap<String, u32>,
        progress_reporter: &ProgressReporter,
    ) -> anyhow::Result<Self> {
        let mut valid_rows_by_column = BTreeMap::new();
        let mut start_row_by_column: BTreeMap<u32, BTreeMap<u32, u32>> = BTreeMap::new();
        // pre-fill values for each column
        for col_0i in name_to_col_0index.values() {
            valid_rows_by_column.insert(
                *col_0i,
                (2..sheet.get_highest_row() + 2).collect::<BTreeSet<u32>>(),
            );
            start_row_by_column.insert(*col_0i, BTreeMap::new());
        }
        let merged_cells = sheet.get_merge_cells();
        // remove obselete indices of merged cells
        for cell_range in merged_cells {
            let start_col = match cell_range.get_coordinate_start_col() {
                Some(c) => c,
                None => {
                    progress_reporter.warn(&format!(
                        "Could not parse start column of merged cell {}",
                        cell_range.get_range()
                    ))?;
                    continue;
                }
            };
            let col_1i = start_col.get_num();
            let end_col = match cell_range.get_coordinate_end_col() {
                Some(c) => c,
                None => {
                    progress_reporter.info(&format!(
                        "Could not parse end column of merged cell {}, using start column value",
                        cell_range.get_range()
                    ))?;
                    start_col
                }
            };
            if col_1i != end_col.get_num() {
                // cannot handle that kind of stuff
                bail!("Merged cells across multiple columns cannot be mapped.")
            }
            let start_row = match cell_range.get_coordinate_start_row() {
                Some(r) => r,
                None => {
                    progress_reporter.warn(&format!(
                        "Could not parse start row of merged cell {}",
                        cell_range.get_range()
                    ))?;
                    continue;
                }
            };
            let start_1i = start_row.get_num();
            let end_row = match cell_range.get_coordinate_end_row() {
                Some(r) => r,
                None => {
                    progress_reporter.info(&format!(
                        "Could not parse end row of merged cell {}, using start row value",
                        cell_range.get_range()
                    ))?;

                    start_row
                }
            };
            let end_1i = end_row.get_num();
            if let Some(row_set) = valid_rows_by_column.get_mut(&(col_1i - 1)) {
                let obsolete_indices = (*start_1i + 1..*end_1i + 1).collect::<BTreeSet<u32>>();
                obsolete_indices.iter().for_each(|e| {
                    row_set.remove(e);
                });
            } else {
                progress_reporter.warn(&format!(
                    "Merged cells {} ({}) could not be mapped to a known column",
                    cell_range.get_range(),
                    doc_path,
                ))?;
            }
            for i in *start_1i..*end_1i {
                start_row_by_column
                    .entry(*col_1i - 1)
                    .or_default()
                    .insert(i, *start_1i);
            }
        }
        let result = Self {
            valid_rows_by_column,
            start_row_by_column,
        };
        Ok(result)
    }
}

const FILE_EXTENSIONS: [&str; 1] = ["xlsx"];

impl Importer for ImportSpreadsheet {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        step_id: StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut updates = GraphUpdate::default();

        let all_files = util::graphupdate::import_corpus_graph_from_files(
            &mut updates,
            input_path,
            self.file_extensions(),
        )?;
        let number_of_files = all_files.len();
        // Each file is a work step
        let reporter = ProgressReporter::new(tx, step_id.clone(), number_of_files)?;

        all_files.into_iter().try_for_each(|(pb, doc_node_name)| {
            reporter.info(&format!("Importing {}", pb.to_string_lossy()))?;
            self.import_workbook(&mut updates, &pb, &doc_node_name, &reporter)?;
            reporter.worked(1)?;
            Ok::<(), AnnattoError>(())
        })?;

        Ok(updates)
    }

    fn file_extensions(&self) -> &[&str] {
        &FILE_EXTENSIONS
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use graphannis::{
        corpusstorage::{QueryLanguage, SearchQuery},
        AnnotationGraph, CorpusStorage,
    };
    use graphannis_core::{annostorage::ValueSearch, types::AnnoKey};
    use tempfile::tempdir;

    use crate::{workflow::Workflow, ImporterStep, ReadFrom};

    use super::*;

    fn run_spreadsheet_import(
        on_disk: bool,
        fallback: Option<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut col_map = BTreeMap::new();
        col_map.insert(
            "dipl".to_string(),
            vec!["sentence".to_string(), "seg".to_string()]
                .into_iter()
                .collect(),
        );
        col_map.insert(
            "norm".to_string(),
            {
                match fallback {
                    None => vec!["pos".to_string(), "lemma".to_string()],
                    Some(_) => vec!["pos".to_string()],
                }
            }
            .into_iter()
            .collect(),
        );
        let importer = ImportSpreadsheet {
            column_map: col_map,
            fallback: fallback.clone(),
            datasheet: None,
            metasheet: None,
        };
        let importer = ReadFrom::Xlsx(importer);
        let path = Path::new("./tests/data/import/xlsx/clean/xlsx/");
        let import_step = ImporterStep {
            module: importer,
            path: path.to_path_buf(),
        };

        let import = import_step.execute(None);
        let mut u = import?;
        let mut g = AnnotationGraph::with_default_graphstorages(on_disk)?;
        g.apply_update(&mut u, |_| {})?;
        let lemma_count = match &fallback {
            Some(v) => match &v[..] {
                "norm" => 4,
                _ => 0,
            },
            _ => 4,
        };
        let queries_and_results = vec![
            ("dipl", 4),
            ("norm", 4),
            ("dipl _=_ norm", 1),
            ("dipl _l_ norm", 3),
            ("dipl _r_ norm", 3),
            ("dipl:sentence", 1),
            ("dipl:seg", 2),
            ("dipl:sentence _=_ dipl", 0),
            ("dipl:sentence _o_ dipl", 4),
            ("dipl:sentence _l_ dipl", 1),
            ("dipl:sentence _r_ dipl", 1),
            ("dipl:seg _=_ dipl", 1),
            ("dipl:seg _o_ dipl", 4),
            ("dipl:seg _l_ dipl", 2),
            ("dipl:seg _r_ dipl", 2),
            ("norm:pos", 4),
            ("norm:lemma", lemma_count),
            ("norm:pos _=_ norm", 4),
            ("norm:lemma _=_ norm", lemma_count),
            ("annis:doc", 1),
            ("annis:doc=\"test_file\"", 1),
            ("dipl .dipl dipl .dipl dipl .dipl dipl", 1),
            ("norm .norm norm .norm norm .norm norm", 1),
            ("annis:node_name=/.*#t.*/ @ annis:doc", 6),
        ];
        let corpus_name = "current";
        let tmp_dir = tempdir()?;
        g.save_to(&tmp_dir.path().join(corpus_name))?;
        let cs_r = CorpusStorage::with_auto_cache_size(&tmp_dir.path(), true);
        assert!(cs_r.is_ok());
        let cs = cs_r.unwrap();
        for (query_s, expected_result) in queries_and_results {
            let query = SearchQuery {
                corpus_names: &[corpus_name],
                query: query_s,
                query_language: QueryLanguage::AQL,
                timeout: None,
            };
            let count = cs.count(query)?;
            assert_eq!(
                count, expected_result,
                "Result for query `{}` does not match",
                query_s
            );
        }
        Ok(())
    }

    #[test]
    fn spreadsheet_import_in_mem() {
        let import = run_spreadsheet_import(false, None);
        assert!(
            import.is_ok(),
            "Spreadsheet import failed with error: {:?}",
            import.err()
        );
    }

    #[test]
    fn spreadsheet_import_on_disk() {
        let import = run_spreadsheet_import(true, None);
        assert!(
            import.is_ok(),
            "Spreadsheet import failed with error: {:?}",
            import.err()
        );
    }

    #[test]
    fn spreadsheet_import_dirty_fails_and_raises_warnings() {
        let mut col_map = BTreeMap::new();
        col_map.insert(
            "dipl".to_string(),
            vec!["sentence".to_string(), "seg".to_string()]
                .into_iter()
                .collect(),
        );
        col_map.insert(
            "norm".to_string(),
            vec!["pos".to_string(), "lemma".to_string()]
                .into_iter()
                .collect(),
        );
        let importer = ImportSpreadsheet {
            column_map: col_map,
            fallback: None,
            datasheet: None,
            metasheet: None,
        };
        let importer = ReadFrom::Xlsx(importer);
        let path = Path::new("./tests/data/import/xlsx/dirty/xlsx/");
        let import_step = ImporterStep {
            module: importer,
            path: path.to_path_buf(),
        };
        let (sender, receiver) = mpsc::channel();
        let import = import_step.execute(Some(sender));
        assert!(import.is_err());
        assert_ne!(receiver.into_iter().count(), 0);
    }

    #[test]
    fn spreadsheet_import_dirty_passes_with_warnings() {
        let mut col_map = BTreeMap::new();
        col_map.insert(
            "dipl".to_string(),
            vec!["sentence".to_string(), "seg".to_string()]
                .into_iter()
                .collect(),
        );
        col_map.insert(
            "norm".to_string(),
            vec!["pos".to_string(), "lemma".to_string()]
                .into_iter()
                .collect(),
        );
        let importer = ImportSpreadsheet {
            column_map: col_map,
            fallback: None,
            datasheet: None,
            metasheet: None,
        };
        let importer = ReadFrom::Xlsx(importer);
        let path = Path::new("./tests/data/import/xlsx/warnings/xlsx/");
        let import_step = ImporterStep {
            module: importer,
            path: path.to_path_buf(),
        };
        let (sender, receiver) = mpsc::channel();
        let import = import_step.execute(Some(sender));
        assert!(import.is_ok());
        assert_ne!(receiver.into_iter().count(), 0);
    }

    #[test]
    fn spreadsheet_fallback_value_in_mem() {
        let import = run_spreadsheet_import(true, Some("norm".to_string()));
        assert!(
            import.is_ok(),
            "Spreadsheet import failed with error: {:?}",
            import.err()
        );
    }

    #[test]
    fn spreadsheet_fallback_value_on_disk() {
        let import = run_spreadsheet_import(false, Some("norm".to_string()));
        assert!(
            import.is_ok(),
            "Spreadsheet import failed with error: {:?}",
            import.err()
        );
    }

    #[test]
    fn spreadsheet_empty_fallback_value_in_mem() {
        let import = run_spreadsheet_import(true, Some("".to_string()));
        assert!(
            import.is_ok(),
            "Spreadsheet import failed with error: {:?}",
            import.err()
        );
    }

    #[test]
    fn spreadsheet_empty_fallback_value_on_disk() {
        let import = run_spreadsheet_import(false, Some("".to_string()));
        assert!(
            import.is_ok(),
            "Spreadsheet import failed with error: {:?}",
            import.err()
        );
    }

    #[test]
    fn spreadsheet_invalid_fallback_value() {
        let import = run_spreadsheet_import(false, Some("tok".to_string()));
        assert!(
            import.is_ok(),
            "Spreadsheet import failed with error: {:?}",
            import.err()
        );
        let mut col_map = BTreeMap::new();
        col_map.insert(
            "dipl".to_string(),
            vec!["sentence".to_string(), "seg".to_string()]
                .into_iter()
                .collect(),
        );
        col_map.insert(
            "norm".to_string(),
            vec!["pos".to_string()].into_iter().collect(),
        );
        let importer = ImportSpreadsheet {
            column_map: col_map,
            fallback: Some("tok".to_string()),
            datasheet: None,
            metasheet: None,
        };
        let importer = ReadFrom::Xlsx(importer);
        let path = Path::new("./tests/data/import/xlsx/clean/xlsx/");
        let import_step = ImporterStep {
            module: importer,
            path: path.to_path_buf(),
        };
        let (sender, receiver) = mpsc::channel();
        let import = import_step.execute(Some(sender));
        assert!(import.is_ok());
        assert_ne!(receiver.into_iter().count(), 0);
    }

    fn test_with_address(
        book: &umya_spreadsheet::Spreadsheet,
        addr: Option<SheetAddress>,
        default: Option<usize>,
        delivers: bool,
    ) {
        let sh = sheet_from_address(&book, &addr, default);
        assert_eq!(sh.is_some(), delivers);
    }

    #[test]
    fn test_get_sheet_from_name() {
        let path = Path::new("./tests/data/import/xlsx/clean/xlsx/test_file.xlsx");
        let book = umya_spreadsheet::reader::xlsx::read::<&Path>(path);
        assert!(book.is_ok());
        let b = book.unwrap();
        test_with_address(&b, Some(SheetAddress::Name("data".to_string())), None, true);
        test_with_address(&b, None, Some(0), true);
        test_with_address(
            &b,
            Some(SheetAddress::Name("data_".to_string())),
            Some(0),
            false,
        );
        test_with_address(
            &b,
            Some(SheetAddress::Name("data_".to_string())),
            None,
            false,
        );
        test_with_address(&b, None, None, false);
    }

    #[test]
    fn test_get_sheet_from_index() {
        let path = Path::new("./tests/data/import/xlsx/clean/xlsx/test_file.xlsx");
        let book = umya_spreadsheet::reader::xlsx::read::<&Path>(path);
        assert!(book.is_ok());
        let b = book.unwrap();
        test_with_address(&b, Some(SheetAddress::Numeric(0)), None, true);
        test_with_address(&b, None, Some(0), true);
        test_with_address(&b, Some(SheetAddress::Numeric(4)), None, false);
        test_with_address(&b, Some(SheetAddress::Numeric(4)), Some(0), false);
        test_with_address(&b, None, Some(4), false);
    }

    #[test]
    fn test_metadata_in_mem() {
        let r = test_metadata(false);
        assert!(r.is_ok(), "Failed with error: {:?}", r.err());
    }

    #[test]
    fn test_metadata_in_on_disk() {
        let r = test_metadata(true);
        assert!(r.is_ok(), "Failed with error: {:?}", r.err());
    }

    fn test_metadata(on_disk: bool) -> Result<(), Box<dyn std::error::Error>> {
        let mut col_map = BTreeMap::new();
        col_map.insert(
            "dipl".to_string(),
            vec!["sentence".to_string(), "seg".to_string()]
                .into_iter()
                .collect(),
        );
        col_map.insert(
            "norm".to_string(),
            vec!["pos".to_string(), "lemma".to_string()]
                .into_iter()
                .collect(),
        );
        let importer = ImportSpreadsheet {
            column_map: col_map,
            fallback: None,
            datasheet: None,
            metasheet: Some(SheetAddress::Name("meta".to_string())),
        };
        let importer = ReadFrom::Xlsx(importer);
        let path = Path::new("./tests/data/import/xlsx/clean/xlsx/");
        let import_step = ImporterStep {
            module: importer,
            path: path.to_path_buf(),
        };

        let import = import_step.execute(None);
        let mut g = AnnotationGraph::with_default_graphstorages(on_disk)?;
        g.apply_update(&mut import?, |_| {})?;
        let node_annos = g.get_node_annos();
        for (meta_name, exp_value) in [("date", "today"), ("author", "me"), ("key", "value")] {
            let mut matches = node_annos
                .exact_anno_search(None, meta_name, ValueSearch::Any)
                .collect_vec();
            assert_eq!(matches.len(), 1);
            let m = matches.remove(0).unwrap();
            let k = AnnoKey {
                name: meta_name.into(),
                ns: "".into(),
            };
            let value = node_annos.get_value_for_item(&m.node, &k)?;
            assert!(value.is_some());
            assert_eq!(value.unwrap().to_string(), exp_value.to_string());
        }
        Ok(())
    }

    #[test]
    fn parse_spreadsheet_workflow() {
        let workflow: Workflow = toml::from_str(
            r#"
[[import]]
path = "dummy_path"
format = "xlsx"


[import.config]
datasheet = 2
metasheet = "meta"

        "#,
        )
        .unwrap();
        assert_eq!(workflow.import_steps().len(), 1);
        assert_eq!(
            workflow.import_steps()[0].path.to_string_lossy().as_ref(),
            "dummy_path"
        );
        assert!(matches!(
            workflow.import_steps()[0].module,
            ReadFrom::Xlsx(..)
        ));
        if let ReadFrom::Xlsx(importer) = &workflow.import_steps()[0].module {
            assert_eq!(
                importer.metasheet,
                Some(SheetAddress::Name("meta".to_string()))
            );
            assert_eq!(importer.datasheet, Some(SheetAddress::Numeric(2)));
        }
    }
}
