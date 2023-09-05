use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use graphannis::{
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::{graph::ANNIS_NS, util::split_qname};
use itertools::Itertools;
use serde_derive::Deserialize;

use crate::{
    error::AnnattoError,
    util::{get_all_files, insert_corpus_nodes_from_path},
    workflow::{StatusMessage, StatusSender},
    Module,
};

use super::Importer;

pub const MODULE_NAME: &str = "import_spreadsheet";

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct ImportSpreadsheet {
    column_map: BTreeMap<String, BTreeSet<String>>,
}

impl Module for ImportSpreadsheet {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}

fn import_workbook(
    update: &mut GraphUpdate,
    root_path: &Path,
    path: &Path,
    column_map: &BTreeMap<String, BTreeSet<String>>,
    tx: &Option<StatusSender>,
) -> Result<(), Box<dyn std::error::Error>> {
    let doc_path = insert_corpus_nodes_from_path(update, root_path, path)?;
    let book = umya_spreadsheet::reader::xlsx::read(path)?;
    let sheet = book.get_sheet(&0)?;
    let merged_cells = sheet.get_merge_cells();
    let name_to_col_0index = {
        let mut m = BTreeMap::new();
        let header_row = sheet.get_collection_by_row(&1);
        for cell in header_row {
            let name = cell.get_cell_value().get_value().trim().to_string();
            if !name.is_empty() {
                m.insert(name, cell.get_coordinate().get_col_num() - 1);
            }
        }
        m
    };
    let rownums_by_col0i = {
        let mut m = BTreeMap::new();
        for col_0i in name_to_col_0index.values() {
            m.insert(
                *col_0i,
                (2..sheet.get_highest_row() + 2).collect::<BTreeSet<u32>>(),
            );
        }
        for cell_range in merged_cells {
            let start_col = match cell_range.get_coordinate_start_col().as_ref() {
                Some(c) => c,
                None => {
                    if let Some(sender) = tx {
                        let message = StatusMessage::Warning(format!(
                            "Could not parse start column of merged cell {}",
                            cell_range.get_range()
                        ));
                        sender.send(message)?;
                    }
                    continue;
                }
            };
            let col_1i = start_col.get_num();
            let end_col = match cell_range.get_coordinate_end_col().as_ref() {
                Some(c) => c,
                None => {
                    if let Some(sender) = tx {
                        let message = StatusMessage::Info(format!(
                            "Could not parse end column of merged cell {}, using start column value",
                            cell_range.get_range()
                        ));
                        sender.send(message)?;
                    }
                    start_col
                }
            };
            if col_1i != end_col.get_num() {
                // cannot handle that kind of stuff
                let err = AnnattoError::Import {
                    reason: "Merged cells across multiple columns cannot be mapped.".to_string(),
                    importer: MODULE_NAME.to_string(),
                    path: path.into(),
                };
                return Err(Box::new(err));
            }
            let start_row = match cell_range.get_coordinate_start_row().as_ref() {
                Some(r) => r,
                None => {
                    if let Some(sender) = tx {
                        let message = StatusMessage::Warning(format!(
                            "Could not parse start row of merged cell {}",
                            cell_range.get_range()
                        ));
                        sender.send(message)?;
                    }
                    continue;
                }
            };
            let start_1i = start_row.get_num();
            let end_row = match cell_range.get_coordinate_end_row().as_ref() {
                Some(r) => r,
                None => {
                    if let Some(sender) = tx {
                        let message = StatusMessage::Info(format!(
                            "Could not parse end row of merged cell {}, using start row value",
                            cell_range.get_range()
                        ));
                        sender.send(message)?;
                    }
                    start_row
                }
            };
            let end_1i = end_row.get_num();
            if let Some(row_set) = m.get_mut(&(col_1i - 1)) {
                let obsolete_indices = (*start_1i + 1..*end_1i + 1).collect::<BTreeSet<u32>>();
                obsolete_indices.iter().for_each(|e| {
                    row_set.remove(e);
                });
            } else if let Some(sender) = tx {
                let message = StatusMessage::Warning(format!(
                    "Merged cells {} could not be mapped to a known column",
                    cell_range.get_range()
                ));
                sender.send(message)?;
            }
        }
        m
    };
    let mut base_tokens = Vec::new();
    for i in 2..sheet.get_highest_row() + 1 {
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
            Ok::<(), Box<dyn std::error::Error>>(())
        })?;
    for (tok_name, anno_names) in column_map {
        let mut names = vec![tok_name];
        names.extend(anno_names);
        for name in names {
            let index_opt = match name_to_col_0index.get(name) {
                Some(v) => Some(v),
                None => {
                    let k = split_qname(name).1;
                    name_to_col_0index.get(k)
                }
            };
            if let Some(col_0i) = index_opt {
                let mut row_nums = rownums_by_col0i.get(col_0i).unwrap().iter().collect_vec();
                row_nums.sort();
                let mut nodes = Vec::new();
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
                    let overlapped_tokens: &[String] =
                        &base_tokens[*start_row as usize - 2..*end_row_excl as usize - 2]; // TODO check indices
                    let node_name =
                        format!("{}#{}_{}-{}", &doc_path, tok_name, start_row, end_row_excl);
                    update.add_event(UpdateEvent::AddNode {
                        node_name: node_name.to_string(),
                        node_type: "node".to_string(),
                    })?;
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: node_name.to_string(),
                        anno_ns: ANNIS_NS.to_string(),
                        anno_name: "tok".to_string(),
                        anno_value: value.to_string(),
                    })?;
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: node_name.to_string(),
                        anno_ns: ANNIS_NS.to_string(),
                        anno_name: "layer".to_string(),
                        anno_value: tok_name.to_string(),
                    })?;
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: node_name.to_string(),
                        anno_ns: tok_name.to_string(),
                        anno_name: name.to_string(),
                        anno_value: value.to_string(),
                    })?;
                    for target_id in overlapped_tokens {
                        update.add_event(UpdateEvent::AddEdge {
                            source_node: node_name.to_string(),
                            target_node: target_id.to_string(),
                            layer: ANNIS_NS.to_string(),
                            component_type: AnnotationComponentType::Coverage.to_string(),
                            component_name: "".to_string(),
                        })?;
                    }
                    nodes.push(node_name);
                }
                if name == tok_name {
                    nodes.iter().sorted().tuple_windows().try_for_each(
                        |(first_name, second_name)| {
                            update.add_event(UpdateEvent::AddEdge {
                                source_node: first_name.to_string(),
                                target_node: second_name.to_string(),
                                layer: ANNIS_NS.to_string(),
                                component_type: AnnotationComponentType::Ordering.to_string(),
                                component_name: tok_name.to_string(),
                            })?;
                            Ok::<(), Box<dyn std::error::Error>>(())
                        },
                    )?;
                }
            } else {
                // TODO warning
                continue; // no tokenization, no mapping of dependent annotations
            }
        }
    }
    Ok(())
}

impl Importer for ImportSpreadsheet {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let column_map = &self.column_map;
        let all_files = get_all_files(input_path, vec!["xlsx"])?;
        all_files.into_iter().try_for_each(|pb| {
            import_workbook(&mut update, input_path, pb.as_path(), column_map, &tx)
        })?;
        Ok(update)
    }
}

#[cfg(test)]
mod tests {
    use std::{env::temp_dir, sync::mpsc};

    use graphannis::{
        corpusstorage::{QueryLanguage, SearchQuery},
        AnnotationGraph, CorpusStorage,
    };
    use tempfile::tempdir_in;

    use super::*;

    fn run_spreadsheet_import(on_disk: bool) -> Result<(), Box<dyn std::error::Error>> {
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
        };
        let path = Path::new("./tests/data/import/xlsx/clean/xlsx/");
        let import = importer.import_corpus(path, None);
        let mut u = import?;
        let mut g = AnnotationGraph::new(on_disk)?;
        g.apply_update(&mut u, |_| {})?;
        let queries_and_results: [(&str, u64); 19] = [
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
            ("norm:lemma", 4),
            ("norm:pos _=_ norm", 4),
            ("norm:lemma _=_ norm", 4),
        ];
        let corpus_name = "current";
        let tmp_dir = tempdir_in(temp_dir())?;
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
        let import = run_spreadsheet_import(false);
        assert!(
            import.is_ok(),
            "Spreadsheet import failed with error: {:?}",
            import.err()
        );
    }

    #[test]
    fn spreadsheet_import_on_disk() {
        let import = run_spreadsheet_import(true);
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
        };
        let path = Path::new("./tests/data/import/xlsx/dirty/xlsx/");
        let (sender, receiver) = mpsc::channel();
        let import = importer.import_corpus(path, Some(sender));
        assert!(import.is_err());
        assert_ne!(receiver.into_iter().count(), 0);
    }
}
