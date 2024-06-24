use std::{fs::File, io::Write, path::Path};

use annatto::{
    GraphOpDiscriminants, ModuleConfiguration, ReadFromDiscriminants, WriteAsDiscriminants,
};
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
        let path = output_directory
            .join("importers")
            .join(format!("{module_name}.md"));
        let mut output = File::create(path)?;
        writeln!(output, "# {module_name} (importer)")?;
        writeln!(output)?;
        writeln!(output, "{}", m.module_doc())?;
        writeln!(output)?;
        write_module_fields(output, &m.module_configs())?;
    }

    for m in WriteAsDiscriminants::iter() {
        let module_name = m.as_ref().to_string();
        let path = output_directory
            .join("exporters")
            .join(format!("{module_name}.md"));
        let mut output = File::create(path)?;
        writeln!(output, "# {module_name} (exporter)")?;
        writeln!(output)?;
        writeln!(output, "{}", m.module_doc())?;
        writeln!(output)?;
        write_module_fields(output, &m.module_configs())?;
    }

    for m in GraphOpDiscriminants::iter() {
        let module_name = m.as_ref().to_string();
        let path = output_directory
            .join("graph_ops")
            .join(format!("{module_name}.md"));
        let mut output = File::create(path)?;
        writeln!(output, "# {module_name} (graph_operation)")?;
        writeln!(output)?;
        writeln!(output, "{}", m.module_doc())?;
        writeln!(output)?;
        write_module_fields(output, &m.module_configs())?;
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

fn write_module_fields<W>(mut output: W, fields: &[ModuleConfiguration]) -> anyhow::Result<()>
where
    W: Write,
{
    if fields.is_empty() {
        writeln!(output, "*No Configuration*")?;
    } else {
        writeln!(output, "## Configuration")?;
        writeln!(output)?;

        for f in fields {
            writeln!(output, "###  {}", f.name)?;
            writeln!(output)?;

            if f.description.is_empty() {
                writeln!(output, "*No description*")?;
            } else {
                writeln!(output, "{}", f.description)?;
            }
            writeln!(output)?;
        }
    }

    Ok(())
}
