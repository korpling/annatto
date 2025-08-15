use annatto::{
    GraphOp, ModuleConfiguration, ReadFrom, StepID, WriteAs,
    error::AnnattoError,
    util::documentation::{self, ModuleInfo},
    workflow::{StatusMessage, Workflow, execute_from_file},
};
use facet::{Facet, Type, UserType};
use facet_reflect::peek_enum_variants;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use clap::Parser;
use itertools::Itertools;
use lazy_static::lazy_static;
use std::{
    collections::HashMap, convert::TryFrom, path::PathBuf, sync::mpsc, thread, time::Duration,
};
use tabled::{
    Table,
    settings::{Modify, Width, object::Segment, themes::ColumnNames},
};
use tracing_subscriber::filter::EnvFilter;

lazy_static! {
    static ref USE_ANSI_COLORS: bool = std::env::var("NO_COLOR").is_err();
}

/// Define a conversion operation
#[derive(Parser)]
#[command(version, about)]
enum Cli {
    /// Run a conversion pipeline from a workflow file.
    Run {
        /// The path to the workflow file.
        #[clap(value_parser)]
        workflow_file: std::path::PathBuf,
        /// Adding this argument resolves environmental variables in the provided workflow file.
        #[clap(long)]
        env: bool,
        /// If this argument is given, store temporary annotation graphs in main
        /// memory instead of on disk. This is faster, but if the corpus is too
        /// large to fit into main memory, the pipeline will fail. Can also set
        /// by setting the environment variable `ANNATTO_IN_MEMORY` to `true`.
        #[clap(long, env = "ANNATTO_IN_MEMORY", default_value = "false")]
        in_memory: bool,
        /// If a file name is provided the workflow is exported again. All environmental variables will be resolved first.
        /// If the provided path exists, the file will be overwritten.
        #[clap(long)]
        save: Option<PathBuf>,
    },
    /// Only check if a workflow file can be loaded. Invalid workflow files will lead to a non-zero exit code.
    Validate {
        /// The path to the workflow file.
        #[clap(value_parser)]
        workflow_file: std::path::PathBuf,
    },
    /// List all supported formats (importer, exporter) and graph operations.
    List,
    /// Show information about modules for the given format or graph operations having this name.
    Info { name: String },
    /// Create a documentation of the modules by creating markdown files in given directory.
    Documentation {
        #[clap(value_parser)]
        output_directory: std::path::PathBuf,
    },
}

fn print_markdown(text: &str) {
    if *USE_ANSI_COLORS {
        termimad::print_text(text);
    } else {
        print!("{text}");
    }
}

fn markdown_text(text: &str) -> String {
    if *USE_ANSI_COLORS {
        termimad::text(text).to_string()
    } else {
        text.to_string()
    }
}

fn set_terminal_table_style(
    table: &mut Table,
    first_column_width: usize,
    second_column_width: usize,
) {
    table
        .with(tabled::settings::Style::modern())
        .with(
            Modify::new(Segment::new(.., 0..1)).with(Width::wrap(first_column_width).keep_words()),
        )
        .with(
            Modify::new(Segment::new(.., 1..2)).with(Width::wrap(second_column_width).keep_words()),
        )
        .with(ColumnNames::default());
}

pub fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::from_default_env().add_directive("annatto=trace".parse()?);
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .compact()
        .init();

    let args = Parser::parse();
    match args {
        Cli::Run {
            workflow_file,
            env,
            in_memory,
            save,
        } => convert(workflow_file, env, in_memory, save)?,
        Cli::Validate { workflow_file } => {
            Workflow::try_from((workflow_file, false))?;
        }
        Cli::List => list_modules(),
        Cli::Info { name } => module_info(&name),
        Cli::Documentation { output_directory } => {
            documentation_generation::create(&output_directory)?
        }
    };
    Ok(())
}

/// Execute the conversion in the background and show the status to the user
fn convert(
    workflow_file: PathBuf,
    read_env: bool,
    in_memory: bool,
    save: Option<PathBuf>,
) -> Result<(), AnnattoError> {
    let (tx, rx) = mpsc::channel();
    let result = thread::spawn(move || {
        execute_from_file(&workflow_file, read_env, in_memory, Some(tx.clone()), save)
    });

    let mut all_bars: HashMap<StepID, ProgressBar> = HashMap::new();

    let not_started_style = ProgressStyle::default_bar()
        .template("{prefix} [{bar:30.blue}] {percent}% {msg}")
        .expect("Could not parse progress bar template")
        .progress_chars("=> ");

    let in_progress_bar_style = ProgressStyle::default_bar()
        .template("{prefix} [{bar:30.blue}] {percent}% {msg}  [{elapsed_precise}/est. {duration}]")
        .expect("Could not parse progress bar template")
        .progress_chars("=> ");

    let in_progress_spinner_style = ProgressStyle::default_bar()
        .template("{prefix} [{spinner:^30}] {msg}  [{elapsed_precise}]")
        .expect("Could not parse progress bar template")
        .tick_strings(&["∙∙∙", "●∙∙", "∙●∙", "∙∙●", " "]);

    let finished_style = ProgressStyle::default_bar()
        .template("{prefix} [{bar:30.blue}] {percent}% {msg}  [{elapsed_precise}]")
        .expect("Could not parse progress bar template")
        .progress_chars("=> ");

    let multi_bar = MultiProgress::new();

    for status_update in rx {
        match status_update {
            StatusMessage::StepsCreated(steps) => {
                if steps.is_empty() {
                    multi_bar.println("No steps in workflow file")?;
                } else {
                    // Add a progress bar for all steps
                    for (idx, s) in steps.into_iter().enumerate() {
                        let idx = idx + 1;

                        let pb = multi_bar.insert_from_back(0, ProgressBar::new(100));
                        pb.set_style(not_started_style.clone());
                        pb.set_prefix(format!("#{idx:<2}"));
                        pb.set_message(s.to_string());
                        pb.enable_steady_tick(Duration::from_secs(1));
                        all_bars.insert(s, pb);
                    }
                }
            }
            StatusMessage::Info(msg) => {
                multi_bar.println(msg)?;
            }
            StatusMessage::Warning(msg) => {
                let msg = format!("[WARNING] {}", &msg);
                if *USE_ANSI_COLORS {
                    multi_bar.println(console::style(msg).red().to_string())?;
                } else {
                    multi_bar.println(msg)?;
                }
            }
            StatusMessage::Progress {
                id,
                total_work,
                finished_work,
            } => {
                if let Some(pb) = all_bars.get(&id) {
                    if let Some(total_work) = total_work {
                        let progress: f32 = (finished_work as f32 / total_work as f32) * 100.0;
                        let pos = progress.round() as u64;
                        pb.set_style(in_progress_bar_style.clone());
                        pb.set_position(pos);
                    } else {
                        pb.set_style(in_progress_spinner_style.clone());
                        pb.tick();
                    }
                }
            }
            StatusMessage::StepDone { id } => {
                // Finish this progress bar and reset all other non-finished ones
                for (pb_id, pb) in all_bars.iter() {
                    if pb_id == &id {
                        pb.set_style(finished_style.clone());
                        pb.finish();
                    } else if !pb.is_finished() {
                        pb.reset_elapsed();
                    }
                }
            }
        }
    }
    // Join the finished thread
    let result = result.join().map_err(|_e| AnnattoError::JoinHandle)?;
    match result {
        Ok(_) => {
            multi_bar.println("Conversion successful")?;
            Ok(())
        }
        Err(e) => Err(e),
    }
}

fn list_modules() {
    let type_col_with: usize = 16;
    let (term_width, _) = termimad::terminal_size();
    let term_width = term_width as usize;
    let list_col_width = term_width.saturating_sub(type_col_with).saturating_sub(7);

    // Create a table where each row is one type of module
    let mut table_builder = tabled::builder::Builder::new();
    table_builder.push_record(vec!["Type", "Modules"]);
    let import_row = vec![
        "Import formats".to_string(),
        peek_enum_variants(ReadFrom::SHAPE)
            .unwrap_or_default()
            .iter()
            .map(|v| v.name.to_lowercase())
            .join(", "),
    ];
    table_builder.push_record(import_row);

    let export_row = vec![
        "Export formats".to_string(),
        peek_enum_variants(WriteAs::SHAPE)
            .unwrap_or_default()
            .iter()
            .map(|v| v.name.to_lowercase())
            .join(", "),
    ];
    table_builder.push_record(export_row);

    let graph_op_row = vec![
        "Graph operations".to_string(),
        peek_enum_variants(GraphOp::SHAPE)
            .unwrap_or_default()
            .iter()
            .map(|v| v.name.to_lowercase())
            .join(", "),
    ];
    table_builder.push_record(graph_op_row);

    let mut table = table_builder.build();

    if *USE_ANSI_COLORS {
        set_terminal_table_style(&mut table, type_col_with, list_col_width);
    } else {
        table.with(tabled::settings::Style::markdown());
    }

    println!("{table}\n");

    print_markdown(
        "Use `annatto info <name>` to get more information about one of the formats or graph operations.\n\n",
    );
}

fn module_info(name: &str) {
    let matching_importers: Vec<_> = peek_enum_variants(ReadFrom::SHAPE)
        .unwrap_or_default()
        .iter()
        .filter(|m| m.name.to_lowercase() == name.to_lowercase())
        .collect();
    let matching_exporters: Vec<_> = peek_enum_variants(WriteAs::SHAPE)
        .unwrap_or_default()
        .iter()
        .filter(|m| m.name.to_lowercase() == name.to_lowercase())
        .collect();

    let matching_graph_ops: Vec<_> = peek_enum_variants(GraphOp::SHAPE)
        .unwrap_or_default()
        .iter()
        .filter(|m| m.name.to_lowercase() == name.to_lowercase())
        .collect();

    if matching_importers.is_empty()
        && matching_exporters.is_empty()
        && matching_graph_ops.is_empty()
    {
        println!(
            "No module with name {name} found. Run the `annotto list` command to get a list of all modules."
        )
    }

    if !matching_importers.is_empty() {
        print_markdown("# Importers\n\n");
        for m in matching_importers {
            let ModuleInfo { name, doc, configs } = documentation::ModuleInfo::from(m);
            print_markdown(&format!("## {name} (importer)\n\n{doc}\n\n"));
            print_module_fields(configs);
        }
    }

    if !matching_exporters.is_empty() {
        print_markdown("# Exporters\n\n");
        for m in matching_exporters {
            let ModuleInfo { name, doc, configs } = documentation::ModuleInfo::from(m);
            print_markdown(&format!("## {name} (exporter)\n\n{doc}\n\n"));
            print_module_fields(configs);
        }
    }

    if !matching_graph_ops.is_empty() {
        print_markdown("# Graph operations\n\n");
        for m in matching_graph_ops {
            // The name of the module is taken from the wrapper enum
            let module_name = m.name.to_lowercase();
            // Get the inner type wrapped by the graph operations enum and use
            // its documentation and fields
            if let Some(inner_field) = m.data.fields.first().map(|m| m.shape())
                && let Type::User(module_type) = inner_field.ty
                && let UserType::Struct(module_impl) = module_type
            {
                let module_doc = documentation::clean_string(inner_field.doc);
                print_markdown(&format!(
                    "## {module_name} (graph operation)\n\n{module_doc}\n\n"
                ));

                let fields = module_impl
                    .fields
                    .iter()
                    .map(|f| ModuleConfiguration {
                        name: f.name.to_lowercase(),
                        description: documentation::clean_string(f.doc),
                    })
                    .collect();
                print_module_fields(fields);
            }
        }
    }
}

fn print_module_fields(mut fields: Vec<ModuleConfiguration>) {
    if fields.is_empty() {
        print_markdown("*No Configuration*\n\n");
    } else {
        // Replace all descriptions with markdown

        for f in &mut fields {
            f.description = markdown_text(&f.description);
        }

        let name_col_width: usize = fields.iter().map(|f| f.name.len()).max().unwrap_or(5);
        let (term_width, _) = termimad::terminal_size();
        let term_width = term_width as usize;
        let description_col_width = term_width.saturating_sub(name_col_width).saturating_sub(7);

        print_markdown("*Configuration*\n\n");
        let mut table = Table::new(fields);

        if *USE_ANSI_COLORS {
            set_terminal_table_style(&mut table, name_col_width, description_col_width);
        } else {
            table.with(tabled::settings::Style::markdown());
        }
        println!("{table}\n");
    }
}

mod documentation_generation;
