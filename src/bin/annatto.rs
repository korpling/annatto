use annatto::{
    error::AnnattoError,
    exporter::{exmaralda::ExportExmaralda, graphml::ExportGraphML, xlsx::XlsxExporter},
    importer::{
        conllu::ImportCoNLLU, exmaralda::ImportEXMARaLDA, file_nodes::CreateFileNodes,
        graphml::GraphMLImporter, meta::AnnotateCorpus, none::CreateEmptyCorpus,
        opus::ImportOpusLinks, ptb::ImportPTB, textgrid::ImportTextgrid,
        treetagger::ImportTreeTagger, xlsx::ImportSpreadsheet, xml::ImportXML,
    },
    workflow::{execute_from_file, StatusMessage, Workflow},
    GraphOpDiscriminants, ReadFromDiscriminants, StepID, WriteAsDiscriminants,
};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use clap::Parser;
use documented::Documented;
use itertools::Itertools;
use std::{
    collections::HashMap, convert::TryFrom, path::PathBuf, sync::mpsc, thread, time::Duration,
};
use strum::IntoEnumIterator;
use tracing_subscriber::filter::EnvFilter;

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
                multi_bar.println(console::style(msg).red().to_string())?;
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
    let importer_list = ReadFromDiscriminants::iter()
        .map(|m| m.as_ref().to_string())
        .join(", ");
    println!("Importer formats: {}", importer_list);

    let exporter_list = WriteAsDiscriminants::iter()
        .map(|m| m.as_ref().to_string())
        .join(", ");
    println!("Exporter formats: {}", exporter_list);

    let graph_op_list = GraphOpDiscriminants::iter()
        .map(|m| m.as_ref().to_string())
        .join(", ");
    println!("Graph operations: {}", graph_op_list);
}

fn module_info(name: &str) {
    let matching_importers: Vec<_> = ReadFromDiscriminants::iter()
        .filter(|m| m.as_ref() == name.to_lowercase())
        .collect();
    let matching_exporters: Vec<_> = WriteAsDiscriminants::iter()
        .filter(|m| m.as_ref() == name.to_lowercase())
        .collect();
    let _matching_graph_ops: Vec<_> = GraphOpDiscriminants::iter()
        .filter(|m| m.as_ref() == name.to_lowercase())
        .collect();

    if !matching_importers.is_empty() {
        termimad::print_text("# Importers");
        for m in matching_importers {
            print_importer_info(m);
        }
    }

    if !matching_exporters.is_empty() {
        termimad::print_text("# Exporters");
        for m in matching_exporters {
            print_exporter_info(m);
        }
    }
}

fn print_importer_info(m: ReadFromDiscriminants) {
    let module_doc = match m {
        ReadFromDiscriminants::CoNLLU => ImportCoNLLU::DOCS,
        ReadFromDiscriminants::EXMARaLDA => ImportEXMARaLDA::DOCS,
        ReadFromDiscriminants::GraphML => GraphMLImporter::DOCS,
        ReadFromDiscriminants::Meta => AnnotateCorpus::DOCS,
        ReadFromDiscriminants::None => CreateEmptyCorpus::DOCS,
        ReadFromDiscriminants::Opus => ImportOpusLinks::DOCS,
        ReadFromDiscriminants::Path => CreateFileNodes::DOCS,
        ReadFromDiscriminants::PTB => ImportPTB::DOCS,
        ReadFromDiscriminants::TextGrid => ImportTextgrid::DOCS,
        ReadFromDiscriminants::TreeTagger => ImportTreeTagger::DOCS,
        ReadFromDiscriminants::Xlsx => ImportSpreadsheet::DOCS,
        ReadFromDiscriminants::Xml => ImportXML::DOCS,
    };
    termimad::print_text(&format!("## {} (importer)\n\n{module_doc}\n\n", m.as_ref()));
}

fn print_exporter_info(m: WriteAsDiscriminants) {
    let module_doc = match m {
        WriteAsDiscriminants::GraphML => ExportGraphML::DOCS,
        WriteAsDiscriminants::EXMARaLDA => ExportExmaralda::DOCS,
        WriteAsDiscriminants::Xlsx => XlsxExporter::DOCS,
    };
    termimad::print_text(&format!("## {} (exporter)\n\n{module_doc}\n\n", m.as_ref()));
}
