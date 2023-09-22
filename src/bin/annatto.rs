#[cfg(feature = "embed-documentation")]
use annatto::documentation_server;
use annatto::{
    error::AnnattoError,
    workflow::{execute_from_file, StatusMessage, Workflow},
    StepID,
};
use indicatif::{ProgressBar, ProgressStyle};

use std::{collections::HashMap, convert::TryFrom, path::PathBuf, sync::mpsc, thread};
use structopt::StructOpt;

/// Define a conversion operation
#[derive(StructOpt)]
enum Cli {
    /// Run a conversion pipeline from a workflow file.
    Run {
        /// The path to the workflow file.
        #[structopt(parse(from_os_str))]
        workflow_file: std::path::PathBuf,
        /// Adding this argument resolves environmental variables in the provided workflow file.
        #[structopt(long)]
        env: bool,
    },
    /// Only check if a workflow files can be imported. Invalid workflow files will lead to a non-zero exit code.
    Validate {
        /// The path to the workflow file.
        #[structopt(parse(from_os_str))]
        workflow_file: std::path::PathBuf,
    },
    #[cfg(feature = "embed-documentation")]
    /// Show the documentation for this version of Annatto in the browser.
    ShowDocumentation,
}

pub fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Cli::from_args();
    match args {
        Cli::Run { workflow_file, env } => convert(workflow_file, env)?,
        #[cfg(feature = "embed-documentation")]
        Cli::ShowDocumentation => documentation_server::start_server()?,
        Cli::Validate { workflow_file } => {
            Workflow::try_from((workflow_file, false))?;
        }
    };
    Ok(())
}

/// Execute the conversion in the background and show the status to the user
fn convert(workflow_file: PathBuf, read_env: bool) -> Result<(), AnnattoError> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(
        move || match execute_from_file(&workflow_file, read_env, Some(tx.clone())) {
            Ok(_) => {}
            Err(e) => tx
                .send(StatusMessage::Failed(e))
                .expect("Could not send failure message"),
        },
    );

    let mut steps_progress: HashMap<StepID, f32> = HashMap::new();

    let bar = ProgressBar::new(1000);
    bar.set_style(ProgressStyle::default_bar().template("[{elapsed}] [{bar:40}] {percent}% {msg}"));
    let mut errors = Vec::new();
    for status_update in rx {
        match status_update {
            StatusMessage::Failed(e) => {
                errors.push(e);
            }
            StatusMessage::StepsCreated(steps) => {
                if steps.is_empty() {
                    bar.println("No steps in workflow file")
                } else {
                    // Print all steps and insert empty progress for each step
                    bar.println(format!("Conversion starts with {} steps", steps.len()));
                    bar.println("-------------------------------");
                    for s in steps {
                        bar.println(format!("{}", &s));
                        steps_progress.entry(s).or_default();
                    }
                    bar.println("-------------------------------");
                }
                bar.println("");
            }
            StatusMessage::Info(msg) => {
                bar.println(msg);
            }
            StatusMessage::Warning(msg) => {
                bar.println(format!("[WARNING] {}", &msg));
            }
            StatusMessage::Progress {
                id,
                total_work,
                finished_work,
            } => {
                let progress: f32 = finished_work as f32 / total_work as f32;
                *steps_progress.entry(id.clone()).or_default() = progress;
                // Sum up all steps
                let progress_sum: f32 = steps_progress.values().sum();
                let num_entries: f32 = steps_progress.len() as f32;
                let progress_percent = (progress_sum / num_entries) * 100.0;
                bar.set_position((progress_percent * 10.0) as u64);
                bar.set_message(format!("Running {}", id));
            }
            StatusMessage::StepDone { id } => {
                *steps_progress.entry(id.clone()).or_default() = 1.0;
                // Sum up all steps
                let progress_sum: f32 = steps_progress.values().sum();
                let num_entries: f32 = steps_progress.len() as f32;
                let progress_percent = (progress_sum / num_entries) * 100.0;
                bar.set_position((progress_percent * 10.0) as u64);
                bar.set_message(format!("Finished {}", id));
            }
        }
    }
    if errors.is_empty() {
        bar.finish_with_message("Conversion successful");
        Ok(())
    } else {
        Err(AnnattoError::ConversionFailed { errors })
    }
}
