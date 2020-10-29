use crate::{
    error::PepperError, error::Result, exporter::Exporter, importer::Importer,
    manipulator::Manipulator,
};

mod donothing;
mod graphml;

pub fn importer_by_name(name: &str) -> Result<Box<dyn Importer>> {
    match name {
        "GraphMLImporter" => Ok(Box::new(graphml::GraphMLImporter::new())),
        "DoNothingImporter" => Ok(Box::new(donothing::DoNothingImporter::new())),
        _ => Err(PepperError::NoSuchModule(name.to_string())),
    }
}

pub fn manipulator_by_name(name: &str) -> Result<Box<dyn Manipulator>> {
    match name {
        "DoNothingManipulator" => Ok(Box::new(donothing::DoNothingManipulator::new())),
        _ => Err(PepperError::NoSuchModule(name.to_string())),
    }
}

pub fn exporter_by_name(name: &str) -> Result<Box<dyn Exporter>> {
    match name {
        "DoNothingExporter" => Ok(Box::new(donothing::DoNothingExporter::new())),
        _ => Err(PepperError::NoSuchModule(name.to_string())),
    }
}
