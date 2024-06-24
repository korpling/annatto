use std::path::{Path, PathBuf};

use annatto::{GraphOpDiscriminants, ReadFromDiscriminants, WriteAsDiscriminants};
use itertools::Itertools;
use strum::IntoEnumIterator;

pub(crate) fn create(output_directory: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(output_directory)?;
    std::fs::create_dir_all(output_directory.join("importers"))?;
    std::fs::create_dir_all(output_directory.join("exporters"))?;
    std::fs::create_dir_all(output_directory.join("graph_ops"))?;

    // Create an index file with a list of all the modules
    std::fs::write(output_directory.join("README.md"), module_list_table())?;

    // Create a module information for each module of all types
    for m in ReadFromDiscriminants::iter() {
        let module_name = m.as_ref().to_string();
        let path = PathBuf::from(
            output_directory
                .join("importers")
                .join(format!("{module_name}.md")),
        );
        let module_doc = m.module_doc();
        std::fs::write(&path, module_doc)?;
    }

    for m in WriteAsDiscriminants::iter() {
        let module_name = m.as_ref().to_string();
        let path = PathBuf::from(
            output_directory
                .join("exporters")
                .join(format!("{module_name}.md")),
        );
        let module_doc = m.module_doc();
        std::fs::write(&path, module_doc)?;
    }

    for m in GraphOpDiscriminants::iter() {
        let module_name = m.as_ref().to_string();
        let path = PathBuf::from(
            output_directory
                .join("graph_ops")
                .join(format!("{module_name}.md")),
        );
        let module_doc = m.module_doc();
        std::fs::write(&path, module_doc)?;
    }

    Ok(())
}

fn module_list_table() -> String {
    let mut table_builder = tabled::builder::Builder::new();
    table_builder.push_record(vec!["Type", "Modules"]);

    let import_row = vec![
        "Import formats".to_string(),
        ReadFromDiscriminants::iter()
            .map(|m| {
                let module_name = m.as_ref().to_string();
                format!("[{module_name}](importers/{module_name}.md)")
            })
            .join(", "),
    ];
    table_builder.push_record(import_row);

    let export_row = vec![
        "Export formats".to_string(),
        WriteAsDiscriminants::iter()
            .map(|m| {
                let module_name = m.as_ref().to_string();
                format!("[{module_name}](exporters/{module_name}.md)")
            })
            .join(", "),
    ];
    table_builder.push_record(export_row);

    let graph_op_row = vec![
        "Graph operations".to_string(),
        GraphOpDiscriminants::iter()
            .map(|m| {
                let module_name = m.as_ref().to_string();
                format!("[{module_name}](graph_ops/{module_name}.md)")
            })
            .join(", "),
    ];
    table_builder.push_record(graph_op_row);

    let mut table = table_builder.build();
    table.with(tabled::settings::Style::markdown());

    table.to_string()
}
