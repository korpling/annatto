use std::{collections::HashMap, sync::mpsc, thread};

use pepper::{
    error::PepperError,
    workflow::{execute_from_file, StatusMessage},
    StepID,
};
use structopt::StructOpt;

use indicatif::{ProgressBar, ProgressStyle};

/// Define a conversion operation
#[derive(StructOpt)]
struct Cli {
    /// The path to the workflow file
    #[structopt(parse(from_os_str))]
    workflow_file: std::path::PathBuf,
}

pub fn main() -> Result<(), PepperError> {
    let args = Cli::from_args();

    // Execute the conversion in the background and show the status to the user
    let (tx, rx) = mpsc::channel();
    thread::spawn(
        move || match execute_from_file(&args.workflow_file, Some(tx.clone())) {
            Ok(_) => {}
            Err(e) => tx
                .send(StatusMessage::Failed(e))
                .expect("Could not send failure message"),
        },
    );

    let mut steps_progress: HashMap<StepID, f32> = HashMap::new();

    let bar = ProgressBar::new(1000);
    bar.set_style(ProgressStyle::default_bar().template("[{elapsed}] [{bar:40}] {percent}% {msg}"));

    for status_update in rx {
        match status_update {
            StatusMessage::Failed(e) => {
                return Err(e);
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
                bar.println(&msg);
            }
            StatusMessage::Warning(msg) => {
                bar.println(&format!("[WARNING] {}", &msg));
            }
            StatusMessage::Progress { id, progress } => {
                *steps_progress.entry(id.clone()).or_default() = progress;
                // Sum up all steps
                let progress_sum: f32 = steps_progress.iter().map(|(_, p)| p).sum();
                let num_entries: f32 = steps_progress.len() as f32;
                let progress_percent = (progress_sum / num_entries) * 100.0;
                bar.set_position((progress_percent * 10.0) as u64);
                bar.set_message(&format!("{}", id));
            }
        }
    }

    bar.finish_with_message("Conversion successful");
    Ok(())
}
