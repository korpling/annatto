use annatto::{
    error::AnnattoError,
    workflow::{execute_from_file, StatusMessage, Workflow},
    StepID,
};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use std::{
    collections::HashMap, convert::TryFrom, path::PathBuf, sync::mpsc, thread, time::Duration,
};
use structopt::StructOpt;
use tracing_subscriber::{filter::LevelFilter, EnvFilter};

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
    /// Only check if a workflow file can be imported. Invalid workflow files will lead to a non-zero exit code.
    Validate {
        /// The path to the workflow file.
        #[structopt(parse(from_os_str))]
        workflow_file: std::path::PathBuf,
    },
}

pub fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env()?
        .add_directive("annatto=trace".parse()?);
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .compact()
        .init();
    let args = Cli::from_args();
    match args {
        Cli::Run { workflow_file, env } => convert(workflow_file, env)?,
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

    let mut errors = Vec::new();
    for status_update in rx {
        match status_update {
            StatusMessage::Failed(e) => {
                errors.push(e);
            }

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
    if errors.is_empty() {
        multi_bar.println("Conversion successful")?;
        Ok(())
    } else {
        Err(AnnattoError::ConversionFailed { errors })
    }
}
