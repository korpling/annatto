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

use crate::{
    error::AnnattoError,
    util::{get_all_files, insert_corpus_nodes_from_path},
    Module,
};

use super::Importer;

pub const MODULE_NAME: &str = "import_spreadsheet";

#[derive(Default)]
pub struct ImportSpreadsheet {}

impl Module for ImportSpreadsheet {
    fn module_name(&self) -> &str {
        MODULE_NAME
    }
}

fn import_workbook(
    update: &mut GraphUpdate,
    root_path: &Path,
    path: &Path,
    column_map: &BTreeMap<String, Vec<String>>,
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
            m.insert(name, cell.get_coordinate().get_col_num() - 1);
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
            let start_col = cell_range.get_coordinate_start_col().as_ref().unwrap();
            let col_1i = start_col.get_num();
            let end_col = cell_range.get_coordinate_end_col().as_ref().unwrap();
            if col_1i != end_col.get_num() {
                // cannot handle that kind of stuff
                let err = AnnattoError::Import {
                    reason: "Merged cells across multiple columns cannot be mapped.".to_string(),
                    importer: MODULE_NAME.to_string(),
                    path: path.into(),
                };
                return Err(Box::new(err));
            }
            let start_row = cell_range.get_coordinate_start_row().as_ref().unwrap();
            let start_1i = start_row.get_num();
            let end_row = cell_range.get_coordinate_end_row().as_ref().unwrap();
            let end_1i = end_row.get_num();
            let obsolete_indices = (*start_1i + 1..*end_1i + 1).collect::<BTreeSet<u32>>();
            obsolete_indices.iter().for_each(|e| {
                m.get_mut(&(col_1i - 1)).unwrap().remove(e);
            });
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
                    let cell = sheet
                        .get_cell_by_column_and_row(&(col_0i + 1), start_row)
                        .unwrap();
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

fn get_column_map(
    property_val: &String,
) -> Result<BTreeMap<String, Vec<String>>, Box<dyn std::error::Error>> {
    // TODO produce some errors
    let mut column_map = BTreeMap::new();
    for group in property_val.split(';') {
        let (key, names) = match group.trim().split_once('=') {
            None => {
                let err = AnnattoError::InvalidPropertyValue {
                    property: PROP_COLUMN_MAP.to_string(),
                    value: property_val.to_string(),
                };
                return Err(Box::new(err));
            }
            Some((k, v)) => {
                let anno_names = v
                    .replace(['{', '}'], "")
                    .split(',')
                    .map(|name| name.trim().to_string())
                    .collect_vec();
                (k.to_string(), anno_names)
            }
        };
        column_map.insert(key, names);
    }
    Ok(column_map)
}

const PROP_COLUMN_MAP: &str = "column_map";

impl Importer for ImportSpreadsheet {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        properties: &std::collections::BTreeMap<String, String>,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let column_map = if let Some(prop_val) = properties.get(&PROP_COLUMN_MAP.to_string()) {
            get_column_map(prop_val)?
        } else {
            return Err(Box::new(AnnattoError::Import {
                reason: "No column map provided.".to_string(),
                importer: self.module_name().to_string(),
                path: input_path.to_path_buf(),
            }));
        };
        let all_files = get_all_files(input_path, vec!["xlsx"])?;
        all_files.into_iter().try_for_each(|pb| {
            import_workbook(&mut update, input_path, pb.as_path(), &column_map)
        })?;
        Ok(update)
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;

    use graphannis::{
        corpusstorage::{QueryLanguage, SearchQuery},
        AnnotationGraph, CorpusStorage,
    };
    use tempfile::tempdir_in;

    use super::*;

    fn run_spreadsheet_import(on_disk: bool) -> Result<(), Box<dyn std::error::Error>> {
        let importer = ImportSpreadsheet::default();
        let mut props = BTreeMap::default();
        props.insert(
            "column_map".to_string(),
            "dipl={sentence,seg};norm={pos,lemma}".to_string(),
        );
        let path = Path::new("./tests/data/import/xlsx/");
        let import = importer.import_corpus(path, &props, None);
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
        let cs = CorpusStorage::with_auto_cache_size(&tmp_dir.path(), true).unwrap();
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
}
