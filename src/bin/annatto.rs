use annatto::{
    error::AnnattoError,
    workflow::{execute_from_file, StatusMessage, Workflow},
    GraphOpDiscriminants, ModuleConfiguration, ReadFromDiscriminants, StepID, WriteAsDiscriminants,
};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use clap::Parser;
use itertools::Itertools;
use lazy_static::lazy_static;
use std::{
    collections::HashMap, convert::TryFrom, path::PathBuf, sync::mpsc, thread, time::Duration,
};
use strum::IntoEnumIterator;
use tabled::{settings::themes::ColumnNames, Table};
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
        #[structopt(long)]
        env: bool,
    },
    /// Only check if a workflow file can be imported. Invalid workflow files will lead to a non-zero exit code.
    Validate {
        /// The path to the workflow file.
        #[clap(value_parser)]
        workflow_file: std::path::PathBuf,
    },
    /// List all supported formats (importer, exporter) and graph operations.
    List,
    /// Show information about modules for the given format or graph operations having this name.
    Info { name: String },
}

fn print_markdown(text: &str) {
    if *USE_ANSI_COLORS {
        termimad::print_text(text);
    } else {
        print!("{text}");
    }
}

pub fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::from_default_env().add_directive("annatto=trace".parse()?);
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .compact()
        .init();

    let args = Parser::parse();
    match args {
        Cli::Run { workflow_file, env } => convert(workflow_file, env)?,
        Cli::Validate { workflow_file } => {
            Workflow::try_from((workflow_file, false))?;
        }
        Cli::List => list_modules(),
        Cli::Info { name } => module_info(&name),
    };
    Ok(())
}

/// Execute the conversion in the background and show the status to the user
fn convert(workflow_file: PathBuf, read_env: bool) -> Result<(), AnnattoError> {
    let (tx, rx) = mpsc::channel();
    let result =
        thread::spawn(move || execute_from_file(&workflow_file, read_env, Some(tx.clone())));

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
                        pb.enable_steady_tick(Duration::from_millis(250));
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
    // Create a markdown styled table where each row is one type of module
    let mut table = String::default();
    table.push_str("|:-:|:-:|\n");
    let importer_list = ReadFromDiscriminants::iter()
        .map(|m| m.as_ref().to_string())
        .join(", ");
    table.push_str("| Import formats | ");
    table.push_str(&importer_list);
    table.push_str("|\n");
    table.push_str("|:-:|:-:|\n");

    let exporter_list = WriteAsDiscriminants::iter()
        .map(|m| m.as_ref().to_string())
        .join(", ");
    table.push_str("| Export formats | ");
    table.push_str(&exporter_list);
    table.push_str("|\n");
    table.push_str("|:-:|:-:|\n");

    let graph_op_list = GraphOpDiscriminants::iter()
        .map(|m| m.as_ref().to_string())
        .join(", ");
    table.push_str("| Graph operations | ");
    table.push_str(&graph_op_list);
    table.push_str("|\n");
    table.push_str("|-\n");

    print_markdown(&table);
    print_markdown("\nUse `annatto info <name>` to get more information about one of the formats or graph operations.\n\n");
}

fn module_info(name: &str) {
    let matching_importers: Vec<_> = ReadFromDiscriminants::iter()
        .filter(|m| m.as_ref() == name.to_lowercase())
        .collect();
    let matching_exporters: Vec<_> = WriteAsDiscriminants::iter()
        .filter(|m| m.as_ref() == name.to_lowercase())
        .collect();
    let matching_graph_ops: Vec<_> = GraphOpDiscriminants::iter()
        .filter(|m| m.as_ref() == name.to_lowercase())
        .collect();

    if matching_importers.is_empty()
        && matching_exporters.is_empty()
        && matching_graph_ops.is_empty()
    {
        println!("No module with name {name} found. Run the `annotto list` command to get a list of all modules.")
    }

    if !matching_importers.is_empty() {
        print_markdown("# Importers\n\n");
        for m in matching_importers {
            let module_doc = m.module_doc();
            print_markdown(&format!("## {} (importer)\n\n{module_doc}\n\n", m.as_ref()));
            print_module_fields(m.module_configs());
        }
    }

    if !matching_exporters.is_empty() {
        print_markdown("# Exporters\n\n");
        for m in matching_exporters {
            let module_doc = m.module_doc();
            print_markdown(&format!("## {} (exporter)\n\n{module_doc}\n\n", m.as_ref()));
            print_module_fields(m.module_configs());
        }
    }

    if !matching_graph_ops.is_empty() {
        print_markdown("# Graph operations\n\n");
        for m in matching_graph_ops {
            let module_doc = m.module_doc();
            print_markdown(&format!(
                "## {} (graph operation)\n\n{module_doc}\n\n",
                m.as_ref()
            ));
            print_module_fields(m.module_configs());
        }
    }
}

fn print_module_fields(fields: Vec<ModuleConfiguration>) {
    print_markdown("*Configuration*\n\n");
    if fields.is_empty() {
        print_markdown("*None*\n\n");
    } else {
        let mut table = Table::new(fields);

        table
            .with(tabled::settings::Style::modern())
            .with(ColumnNames::default());

        println!("{}\n", table);
    }
}
