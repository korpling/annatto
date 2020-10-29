pub mod donothing;
pub mod error;
pub mod exporter;
pub mod importer;
pub mod manipulator;
pub mod workflow;


pub trait Module: Sync {
    fn module_name(&self) -> String;
}
