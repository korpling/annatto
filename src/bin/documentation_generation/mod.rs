use std::{fs::File, io::Write, path::Path};

use annatto::{GraphOp, ModuleConfiguration, ReadFrom, WriteAs, util::documentation::ModuleInfo};

use facet::{Facet, Variant};
use facet_reflect::peek_enum_variants;
use itertools::Itertools;

pub(crate) fn create(output_directory: &Path) -> anyhow::Result<()> {
    let importers = peek_enum_variants(ReadFrom::SHAPE).unwrap_or_default();
    let exporters = peek_enum_variants(WriteAs::SHAPE).unwrap_or_default();
    let graph_ops = peek_enum_variants(GraphOp::SHAPE).unwrap_or_default();

    // Create an index file with a list of all the modules
    write_module_list_table(output_directory, importers, exporters, graph_ops)?;

    // Create a module information for each module of all types
    write_importer_files(importers, output_directory)?;
    write_exporter_files(exporters, output_directory)?;
    write_graph_op_files(graph_ops, output_directory)?;

    Ok(())
}

fn write_module_list_table(
    output_directory: &Path,
    importers: &[Variant],
    exporters: &[Variant],
    graph_ops: &[Variant],
) -> anyhow::Result<()> {
    let mut table_builder = tabled::builder::Builder::new();
    table_builder.push_record(vec!["Type", "Modules"]);

    let import_row = vec![
        "Import formats".to_string(),
        importers
            .iter()
            .map(|m| {
                let module_name = m.name.to_lowercase();
                format!("[{module_name}](importers/{module_name}.md)")
            })
            .join(", "),
    ];
    table_builder.push_record(import_row);

    let export_row = vec![
        "Export formats".to_string(),
        exporters
            .iter()
            .map(|m| {
                let module_name = m.name.to_lowercase();
                format!("[{module_name}](exporters/{module_name}.md)")
            })
            .join(", "),
    ];
    table_builder.push_record(export_row);

    let graph_op_row = vec![
        "Graph operations".to_string(),
        graph_ops
            .iter()
            .map(|m| {
                let module_name = m.name.to_lowercase();
                format!("[{module_name}](graph_ops/{module_name}.md)")
            })
            .join(", "),
    ];
    table_builder.push_record(graph_op_row);

    let mut table = table_builder.build();
    table.with(tabled::settings::Style::markdown());

    std::fs::create_dir_all(output_directory)?;
    std::fs::write(output_directory.join("README.md"), table.to_string())?;
    Ok(())
}

fn write_importer_files(importers: &[Variant], output_directory: &Path) -> anyhow::Result<()> {
    let importers_directory = output_directory.join("importers");
    std::fs::create_dir_all(&importers_directory)?;

    for m in importers {
        let ModuleInfo { name, doc, configs } = ModuleInfo::from(m);
        let path = importers_directory.join(format!("{name}.md"));
        let mut output = File::create(path)?;
        writeln!(output, "# {name} (importer)")?;
        writeln!(output)?;
        writeln!(output, "{doc}")?;
        writeln!(output)?;
        write_module_fields(output, &configs)?;
    }

    Ok(())
}

fn write_exporter_files(exporters: &[Variant], output_directory: &Path) -> anyhow::Result<()> {
    let exporters_directory = output_directory.join("exporters");
    std::fs::create_dir_all(&exporters_directory)?;

    for m in exporters {
        let ModuleInfo { name, doc, configs } = ModuleInfo::from(m);
        let path = exporters_directory.join(format!("{name}.md"));
        let mut output = File::create(path)?;
        writeln!(output, "# {name} (exporter)")?;
        writeln!(output)?;
        writeln!(output, "{doc}")?;
        writeln!(output)?;
        write_module_fields(output, &configs)?;
    }

    Ok(())
}
fn write_graph_op_files(graph_ops: &[Variant], output_directory: &Path) -> anyhow::Result<()> {
    let graph_ops_directory = output_directory.join("graph_ops");
    std::fs::create_dir_all(&graph_ops_directory)?;

    for m in graph_ops {
        let ModuleInfo { name, doc, configs } = ModuleInfo::from(m);
        let path = graph_ops_directory.join(format!("{name}.md"));
        let mut output = File::create(path)?;
        writeln!(output, "# {name} (graph_operation)")?;
        writeln!(output)?;
        writeln!(output, "{doc}")?;
        writeln!(output)?;
        write_module_fields(output, &configs)?;
    }

    Ok(())
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

#[cfg(test)]
mod tests;
