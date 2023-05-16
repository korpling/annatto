use annatto::{
    error::AnnattoError,
    workflow::{execute_from_file, StatusMessage},
    StepID,
};
use indicatif::{ProgressBar, ProgressStyle};

use std::{collections::HashMap, path::PathBuf, sync::mpsc, thread};
use structopt::StructOpt;

/// Define a conversion operation
#[derive(StructOpt)]
enum Cli {
    /// Run a conversion pipeline from a workflow file.
    Run {
        /// The path to the workflow file.
        #[structopt(parse(from_os_str))]
        workflow_file: std::path::PathBuf,
    },
    #[cfg(feature = "embed-documentation")]
    /// Show the documentation for this version of Annatto in the browser.
    ShowDocumentation,
}

pub fn main() -> anyhow::Result<()> {
    let args = Cli::from_args();

    match args {
        Cli::Run { workflow_file } => convert(workflow_file)?,
        #[cfg(feature = "embed-documentation")]
        Cli::ShowDocumentation => documentation_server::start_server()?,
    };
    Ok(())
}

/// Execute the conversion in the background and show the status to the user
fn convert(workflow_file: PathBuf) -> Result<(), AnnattoError> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(
        move || match execute_from_file(&workflow_file, Some(tx.clone())) {
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

#[cfg(feature = "embed-documentation")]
mod documentation_server {

    use anyhow::Ok;
    use rust_embed::RustEmbed;

    #[derive(RustEmbed)]
    #[folder = "docs/book/"]
    struct CompiledDocumentation;

    pub fn start_server() -> anyhow::Result<()> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(server_documentation_files())?;
        Ok(())
    }

    async fn server_documentation_files() -> anyhow::Result<()> {
        println!("Hello World from async");
        Ok(())
    }
}
