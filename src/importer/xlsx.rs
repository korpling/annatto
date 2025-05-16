use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Display,
    path::PathBuf,
};

use anyhow::{anyhow, bail};
use graphannis::{
    graph::AnnoKey,
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::{
    graph::{ANNIS_NS, DEFAULT_NS},
    util::split_qname,
};
use itertools::Itertools;
use percent_encoding::utf8_percent_encode;
use serde::Serialize;
use serde_derive::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;
use umya_spreadsheet::Cell;

use super::Importer;
use crate::{
    error::AnnattoError, importer::NODE_NAME_ENCODE_SET, progress::ProgressReporter, util, StepID,
};
use documented::{Documented, DocumentedFields};

/// Imports Excel Spreadsheets where each line is a token, the other columns are
/// spans and merged cells can be used for spans that cover more than one token.
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice, Serialize)]
#[serde(deny_unknown_fields)]
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
    #[serde(default)]
    fallback: Option<String>,
    /// Optional value of the Excel sheet that contains the data. If not given,
    /// the first sheet is used.
    #[serde(default)]
    datasheet: Option<SheetAddress>,
    /// Optional value of the Excel sheet that contains the metadata table. If
    /// no metadata is imported.
    #[serde(default)]
    metasheet: Option<SheetAddress>,
    /// Skip the first given rows in the meta data sheet.
    #[serde(default)]
    metasheet_skip_rows: usize,
    /// Map the given annotation columns as token annotations and not as span if possible.
    #[serde(default)]
    token_annos: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
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

/// We implement our own max row function as sheet.get_highest_row() is unreliable
/// and usually outputs a too low number
fn max_row(sheet: &umya_spreadsheet::Worksheet) -> Result<u32, anyhow::Error> {
    sheet
        .get_row_dimensions()
        .iter()
        .map(|r| *r.get_row_num())
        .max()
        .ok_or(anyhow!("Could not determine highest row number."))
}

/// As highest column is computed the same way as highest row by umya_spreadsheet,
/// we play it safe by using our own function to compute the highest column and
/// only do it for the first row, as this is the header
fn max_column(sheet: &umya_spreadsheet::Worksheet) -> Result<u32, anyhow::Error> {
    sheet
        .get_cell_collection()
        .iter()
        .map(|c| c.get_coordinate())
        .filter_map(|xy| {
            if *xy.get_row_num() == 1 {
                Some(*xy.get_col_num())
            } else {
                None
            }
        })
        .max()
        .ok_or(anyhow!("Could not determine highest column number."))
}

struct MetasheetMapper<'a> {
    sheet: &'a umya_spreadsheet::Worksheet,
    skip_rows: usize,
    max_row: u32,
}

impl<'a> MetasheetMapper<'a> {
    fn new(
        sheet: &'a umya_spreadsheet::Worksheet,
        skip_rows: usize,
    ) -> Result<MetasheetMapper<'a>, anyhow::Error> {
        Ok(MetasheetMapper {
            sheet,
            skip_rows,
            max_row: max_row(sheet)?,
        })
    }

    fn import_as_metadata(
        &self,
        doc_node_name: &str,
        update: &mut GraphUpdate,
    ) -> Result<(), AnnattoError> {
        let max_row_num = self.max_row as usize; // 1-based
        for row_num in (self.skip_rows + 1)..max_row_num + 1 {
            let entries = self.sheet.get_collection_by_row(&(row_num as u32)); // sorting not necessarily by col number
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
                            node_name: doc_node_name.to_string(),
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
}

struct DatasheetMapper<'a> {
    sheet: &'a umya_spreadsheet::Worksheet,
    max_row: u32,
    max_col: u32,
    column_map: &'a BTreeMap<String, BTreeSet<String>>,
    reverse_col_map: &'a BTreeMap<String, String>,
    fallback: Option<String>,
    token_annos: &'a [String],
}

impl<'a> DatasheetMapper<'a> {
    fn new(
        sheet: &'a umya_spreadsheet::Worksheet,
        column_map: &'a BTreeMap<String, BTreeSet<String>>,
        reverse_col_map: &'a BTreeMap<String, String>,
        fallback: Option<String>,
        token_annos: &'a [String],
    ) -> Result<DatasheetMapper<'a>, anyhow::Error> {
        Ok(DatasheetMapper {
            sheet,
            max_row: max_row(sheet)?,
            max_col: max_column(sheet)?,
            column_map,
            reverse_col_map,
            fallback,
            token_annos,
        })
    }

    fn import_datasheet(
        &self,
        doc_node_name: &str,
        update: &mut GraphUpdate,
        progress: &ProgressReporter,
    ) -> Result<(), AnnattoError> {
        let expected_names = self
            .reverse_col_map
            .keys()
            .chain(self.reverse_col_map.values())
            .map(|e| e.as_str())
            .collect_vec();
        let mut merged_cells = self.collect_merged_cells(expected_names, progress)?;
        let base_tokens = self.build_tokens(update, doc_node_name)?;
        for col_num in 1..=self.max_col {
            let mc = merged_cells.remove(&col_num).unwrap_or_default();
            self.import_column(
                update,
                doc_node_name,
                col_num,
                mc.into_iter().collect(),
                &base_tokens,
                progress,
            )?;
        }
        Ok(())
    }

    fn import_column(
        &self,
        update: &mut GraphUpdate,
        doc_node_name: &str,
        col_num: u32,
        mut merged_cells: BTreeMap<u32, u32>,
        base_tokens: &[String],
        progress: &ProgressReporter,
    ) -> Result<(), anyhow::Error> {
        let merge_members: BTreeSet<u32> = merged_cells.iter().flat_map(|(s, e)| *s..=*e).collect();
        let col_name_opt = self.sheet.get_cell((col_num, 1));
        let col_name = if let Some(name) = col_name_opt {
            name.get_raw_value().to_string()
        } else {
            return Ok(());
        };
        let (ns, name) = split_qname(&col_name);
        let is_segmentation = self.column_map.contains_key(&name.to_string())
            || self.column_map.contains_key(&col_name);
        let anno_key = if is_segmentation {
            AnnoKey {
                ns: ns.unwrap_or(name).into(), // prefer the namespace in the column header
                name: name.into(),
            }
        } else if let Some(seg_name) = self
            .reverse_col_map
            .get(name)
            .or(self.reverse_col_map.get(&col_name))
            .or(self.fallback.as_ref())
        {
            AnnoKey {
                ns: ns.unwrap_or(seg_name).into(), // prefer the namespace in the column header
                name: name.into(),
            }
        } else {
            let msg = format!(
                "Unknown column {col_name} will not be imported from document {doc_node_name}"
            );
            progress.warn(&msg)?;
            return Ok(());
        };
        let mut last_segment_node = None;
        for row_num in 2..=self.max_row {
            let covered_tokens = if let Some(end_num) = merged_cells.remove(&row_num) {
                if end_num as usize - 2 >= base_tokens.len() {
                    bail!("Row index larger than highest row index in document {doc_node_name}")
                } else {
                    let start = row_num as usize - 2;
                    let end = end_num as usize - 2;
                    &base_tokens[start..=end] // excel uses intervals with inclusive boundaries
                }
            } else if merge_members.contains(&row_num) {
                continue;
            } else {
                if row_num as usize - 2 >= base_tokens.len() {
                    bail!("Row index larger than highest row index in document {doc_node_name}");
                }
                let start = row_num as usize - 2;
                &base_tokens[start..start + 1]
            };
            let cell_value = if let Some(cell) = self
                .sheet
                .get_cell((col_num, row_num))
                .filter(|c| !c.get_raw_value().is_empty())
            {
                cell.get_raw_value().to_string()
            } else {
                continue;
            };
            let node_name = if is_segmentation {
                format!(
                    "{doc_node_name}#{}_{}-{}",
                    &col_name.replace("::", "_"),
                    row_num,
                    row_num as usize + covered_tokens.len()
                )
            } else if self.token_annos.contains(&col_name) {
                // this attempts to recreate the node name of the original segmentation node (if they span different timeline items, the indices will distinguish the names)
                // It falls back to the column name
                let qualifier = self
                    .reverse_col_map
                    .get(name)
                    .or(self.reverse_col_map.get(&col_name))
                    .or(self.fallback.as_ref())
                    .unwrap_or(&col_name);
                format!(
                    "{doc_node_name}#{}_{}-{}",
                    qualifier.replace("::", "_"),
                    row_num,
                    row_num as usize + covered_tokens.len()
                )
            } else {
                format!(
                    "{doc_node_name}#span_{}_{}-{}",
                    utf8_percent_encode(&name.replace("::", "_"), NODE_NAME_ENCODE_SET),
                    row_num,
                    row_num as usize + covered_tokens.len()
                )
            };
            update.add_event(UpdateEvent::AddNode {
                node_name: node_name.to_string(),
                node_type: "node".to_string(),
            })?;
            update.add_event(UpdateEvent::AddEdge {
                source_node: node_name.to_string(),
                target_node: doc_node_name.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string(),
            })?;
            update.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_name.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "layer".to_string(),
                anno_value: anno_key.ns.to_string(),
            })?;
            update.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_name.to_string(),
                anno_ns: anno_key.ns.to_string(),
                anno_name: anno_key.name.to_string(),
                anno_value: cell_value.trim().to_string(),
            })?;
            for tok_id in covered_tokens {
                update.add_event(UpdateEvent::AddEdge {
                    source_node: node_name.to_string(),
                    target_node: tok_id.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Coverage.to_string(),
                    component_name: "".to_string(),
                })?;
            }
            if is_segmentation {
                if let Some(prev_name) = last_segment_node {
                    update.add_event(UpdateEvent::AddEdge {
                        source_node: prev_name,
                        target_node: node_name.to_string(),
                        layer: DEFAULT_NS.to_string(),
                        component_type: AnnotationComponentType::Ordering.to_string(),
                        component_name: anno_key.ns.to_string(),
                    })?;
                }
                last_segment_node = Some(node_name);
            }
        }
        Ok(())
    }

    fn build_tokens(
        &self,
        update: &mut GraphUpdate,
        doc_node_name: &str,
    ) -> Result<Vec<String>, anyhow::Error> {
        let mut base_tokens = Vec::with_capacity(self.max_row as usize);
        for i in 2..=self.max_row as usize {
            // keep spreadsheet row indices in token names for easier debugging
            let tok_id = format!("{doc_node_name}#row{}", i);
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
                target_node: doc_node_name.to_string(),
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
        Ok(base_tokens)
    }

    fn collect_merged_cells(
        &self,
        col_names: Vec<&str>,
        progress: &ProgressReporter,
    ) -> Result<BTreeMap<u32, Vec<(u32, u32)>>, anyhow::Error> {
        let mut cell_map: BTreeMap<u32, Vec<(u32, u32)>> = BTreeMap::default();
        for rng in self.sheet.get_merge_cells() {
            if let (Some(start_col), Some(end_col), Some(start_row), Some(end_row)) = (
                rng.get_coordinate_start_col(),
                rng.get_coordinate_end_col(),
                rng.get_coordinate_start_row(),
                rng.get_coordinate_end_row(),
            ) {
                if start_col != end_col {
                    if (*start_col.get_num()..=*end_col.get_num()).any(|c| {
                        if let Some(cell) = self.sheet.get_cell((c, 1)) {
                            col_names.contains(&cell.get_raw_value().to_string().as_str())
                        } else {
                            false
                        }
                    }) {
                        // At least one of the affected columns of the multi-column merge cell
                        // is a column to be imported, which is forbidden
                        // (this way the user can still import sheet by omitting dirty columns in the column map)
                        bail!("A merge cell affects at least one column that is set to be imported: {:?}", rng);
                    } else {
                        progress.warn(
                            format!(
                                "Merged cell with multiple columns will be ignored: {:?}",
                                rng
                            )
                            .as_str(),
                        )?;
                        continue;
                    }
                }
                cell_map
                    .entry(*start_col.get_num())
                    .or_default()
                    .push((*start_row.get_num(), *end_row.get_num()));
            }
        }
        Ok(cell_map)
    }
}

struct WorkbookMapper<'a> {
    progress: &'a ProgressReporter,
    path: PathBuf,
    column_map: &'a BTreeMap<String, BTreeSet<String>>,
    datasheet: Option<SheetAddress>,
    metasheet: Option<SheetAddress>,
    metasheet_skip_rows: usize,
    fallback: Option<String>,
    token_annos: &'a [String],
    doc_node_name: String,
}

impl WorkbookMapper<'_> {
    fn import_workbook(self, update: &mut GraphUpdate) -> Result<(), AnnattoError> {
        let book = umya_spreadsheet::reader::xlsx::read(&self.path)?;
        let reverse_col_map = self
            .column_map
            .iter()
            .flat_map(|(k, v)| v.iter().map(move |vv| (vv.to_string(), k.to_string())))
            .collect();
        if let Some(sheet) = sheet_from_address(&book, &self.datasheet, Some(0)) {
            let mapper = DatasheetMapper::new(
                sheet,
                self.column_map,
                &reverse_col_map,
                self.fallback.clone(),
                self.token_annos,
            )?;
            mapper.import_datasheet(&self.doc_node_name, update, self.progress)?;
        }
        if let Some(sheet) = sheet_from_address(&book, &self.metasheet, None) {
            let mapper = MetasheetMapper::new(sheet, self.metasheet_skip_rows)?;
            mapper.import_as_metadata(&self.doc_node_name, update)?;
        }
        self.progress.worked(1)?;
        Ok(())
    }
}

const FILE_EXTENSIONS: [&str; 1] = ["xlsx"];

impl ImportSpreadsheet {}

impl Importer for ImportSpreadsheet {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        step_id: StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();

        let all_files = util::graphupdate::import_corpus_graph_from_files(
            &mut update,
            input_path,
            self.file_extensions(),
        )?;
        let number_of_files = all_files.len();
        // Each file is a work step
        let reporter = ProgressReporter::new(tx, step_id.clone(), number_of_files)?;
        let mapper_vec = all_files
            .into_iter()
            .map(|(p, d)| WorkbookMapper {
                progress: &reporter,
                path: p.to_path_buf(),
                column_map: &self.column_map,
                datasheet: self.datasheet.clone(),
                metasheet: self.metasheet.clone(),
                metasheet_skip_rows: self.metasheet_skip_rows,
                fallback: self.fallback.clone(),
                token_annos: &self.token_annos,
                doc_node_name: d.to_string(),
            })
            .collect_vec();
        mapper_vec
            .into_iter()
            .try_for_each(|m| m.import_workbook(&mut update))?;
        Ok(update)
    }

    fn file_extensions(&self) -> &[&str] {
        &FILE_EXTENSIONS
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::mpsc};

    use graphannis::{
        corpusstorage::{QueryLanguage, ResultOrder, SearchQuery},
        AnnotationGraph, CorpusStorage,
    };
    use graphannis_core::{annostorage::ValueSearch, types::AnnoKey};
    use insta::assert_snapshot;
    use tempfile::tempdir;

    use crate::{
        exporter::graphml::GraphMLExporter,
        test_util::export_to_string,
        workflow::{StatusMessage, Workflow},
        ImporterStep, ReadFrom,
    };

    use super::*;

    #[test]
    fn serialize_custom() {
        let module = ImportSpreadsheet {
            column_map: vec![
                (
                    "dipl".to_string(),
                    vec!["sentence".to_string()].into_iter().collect(),
                ),
                (
                    "norm".to_string(),
                    vec!["pos".to_string(), "lemma".to_string()]
                        .into_iter()
                        .collect(),
                ),
            ]
            .into_iter()
            .collect(),
            datasheet: Some(SheetAddress::Name("data".to_string())),
            metasheet: Some(SheetAddress::Numeric(2)),
            fallback: Some("dipl".to_string()),
            metasheet_skip_rows: 1,
            token_annos: vec!["pos".to_string(), "lemma".to_string()],
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
    fn multiple() {
        let path = Path::new("./tests/data/import/xlsx/parallel/");
        let config = "column_map = { txt = [\"sent\", \"clause\", \"pos\", \"lemma\"] }";
        let m: Result<ImportSpreadsheet, _> = toml::from_str(config);
        assert!(m.is_ok(), "Could not deserialize config: {:?}", m.err());
        let import = m.unwrap();
        let (sender, receiver) = mpsc::channel();
        let u = import.import_corpus(
            path,
            StepID {
                module_name: "test_xslx_import".to_string(),
                path: None,
            },
            Some(sender),
        );
        assert!(u.is_ok(), "Failed to import: {:?}", u.err());
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(graph.apply_update(&mut u.unwrap(), |_| {}).is_ok());
        let e: Result<GraphMLExporter, _> = toml::from_str("stable_order = true");
        assert!(e.is_ok(), "Could not deserialize exporter: {:?}", e.err());
        let actual = export_to_string(&graph, e.unwrap());
        assert!(actual.is_ok(), "Could not export: {:?}", actual.err());
        assert_snapshot!(actual.unwrap());
        let progress_report = receiver.into_iter().find(|s| {
            matches!(
                s,
                StatusMessage::Progress {
                    finished_work: 4,
                    ..
                }
            )
        });
        assert!(progress_report.is_some());
    }

    #[test]
    fn snapshot_test() {
        let path = Path::new("./tests/data/import/xlsx/clean/xlsx/");
        let config = "column_map = { dipl = [\"sentence\", \"seg\"], norm = [\"pos\", \"lemma\"] }";
        let m: Result<ImportSpreadsheet, _> = toml::from_str(config);
        assert!(m.is_ok(), "Could not deserialize config: {:?}", m.err());
        let import = m.unwrap();
        let u = import.import_corpus(
            path,
            StepID {
                module_name: "test_xslx_import".to_string(),
                path: None,
            },
            None,
        );
        assert!(u.is_ok(), "Failed to import: {:?}", u.err());
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(graph.apply_update(&mut u.unwrap(), |_| {}).is_ok());
        let e: Result<GraphMLExporter, _> = toml::from_str("stable_order = true");
        assert!(e.is_ok(), "Could not deserialize exporter: {:?}", e.err());
        let actual = export_to_string(&graph, e.unwrap());
        assert!(actual.is_ok(), "Could not export: {:?}", actual.err());
        assert_snapshot!(actual.unwrap());
    }

    #[test]
    fn snapshot_test_with_ns() {
        let path = Path::new("./tests/data/import/xlsx/clean/xlsx-with-ns/");
        let config = "column_map = { dipl = [\"sentence\", \"seg\"], norm = [\"pos\", \"lemma\"] }";
        let m: Result<ImportSpreadsheet, _> = toml::from_str(config);
        assert!(m.is_ok(), "Could not deserialize config: {:?}", m.err());
        let import = m.unwrap();
        let u = import.import_corpus(
            path,
            StepID {
                module_name: "test_xslx_import".to_string(),
                path: None,
            },
            None,
        );
        assert!(u.is_ok(), "Failed to import: {:?}", u.err());
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(graph.apply_update(&mut u.unwrap(), |_| {}).is_ok());
        let e: Result<GraphMLExporter, _> = toml::from_str("stable_order = true");
        assert!(e.is_ok(), "Could not deserialize exporter: {:?}", e.err());
        let actual = export_to_string(&graph, e.unwrap());
        assert!(actual.is_ok(), "Could not export: {:?}", actual.err());
        assert_snapshot!(actual.unwrap());
    }

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
            token_annos: vec![],
            metasheet_skip_rows: 0,
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
            ("annis:node_name=/.*#row.*/ @ annis:doc", 6),
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
            let results_are = cs.find(query, 0, None, ResultOrder::Normal)?;
            assert_eq!(
                results_are.len(),
                expected_result,
                "Result for query `{}` does not match: {:?}",
                query_s,
                results_are
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
            token_annos: vec![],
            metasheet_skip_rows: 0,
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
            datasheet: Some(SheetAddress::Name("Sheet1".to_string())),
            metasheet: None,
            token_annos: vec![],
            metasheet_skip_rows: 0,
        };
        let importer = ReadFrom::Xlsx(importer);
        let path = Path::new("./tests/data/import/xlsx/warnings/xlsx/");
        let import_step = ImporterStep {
            module: importer,
            path: path.to_path_buf(),
        };
        let (sender, receiver) = mpsc::channel();
        let import = import_step.execute(Some(sender));
        assert!(import.is_ok(), "Error occurred: {:?}", import.err());
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
            token_annos: vec![],
            metasheet_skip_rows: 0,
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
            token_annos: vec![],
            metasheet_skip_rows: 0,
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

[import.config.column_map]
text = ["pos", "lemma"]
edition = ["chapter"]

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
