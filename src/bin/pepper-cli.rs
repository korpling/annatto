use pepper::{error::PepperError, workflow::execute_from_file};
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
    execute_from_file(&args.workflow_file)?;
    Ok(())
}
