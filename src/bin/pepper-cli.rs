use std::{sync::mpsc, thread};

use pepper::{
    error::PepperError,
    workflow::{execute_from_file, StatusMessage},
};
use structopt::StructOpt;

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

    for status_update in rx {
        // TODO: print progress updates as a nice progress bar, e.g. with the progressing crate
        match status_update {
            StatusMessage::Failed(e) => {
                return Err(e);
            }
            _ => {
                println!("{:?}", status_update);
            }
        }
    }

    Ok(())
}
