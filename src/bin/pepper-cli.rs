use pepper::execution::execute_workflow;
use structopt::StructOpt;

/// Define a conversion operation
#[derive(StructOpt)]
struct Cli {
    /// The path to the workflow file
    #[structopt(parse(from_os_str))]
    workflow_file: std::path::PathBuf,
}

pub fn main() {
    let args = Cli::from_args();
    execute_workflow(&args.workflow_file);
}