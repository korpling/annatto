use std::path::Path;

use annatto::{GraphOpDiscriminants, ReadFromDiscriminants, WriteAsDiscriminants};
use itertools::Itertools;
use strum::IntoEnumIterator;

pub(crate) fn create(output_directory: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(output_directory)?;

    std::fs::write(output_directory.join("README.md"), module_list_table())?;

    Ok(())
}

fn module_list_table() -> String {
    let mut table_builder = tabled::builder::Builder::new();
    table_builder.push_record(vec!["Type", "Modules"]);

    let import_row = vec![
        "Import formats".to_string(),
        ReadFromDiscriminants::iter()
            .map(|m| m.as_ref().to_string())
            .join(", "),
    ];
    table_builder.push_record(import_row);

    let export_row = vec![
        "Export formats".to_string(),
        WriteAsDiscriminants::iter()
            .map(|m| m.as_ref().to_string())
            .join(", "),
    ];
    table_builder.push_record(export_row);

    let graph_op_row = vec![
        "Graph operations".to_string(),
        GraphOpDiscriminants::iter()
            .map(|m| m.as_ref().to_string())
            .join(", "),
    ];
    table_builder.push_record(graph_op_row);

    let mut table = table_builder.build();
    table.with(tabled::settings::Style::markdown());

    table.to_string()
}
