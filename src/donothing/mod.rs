use crate::exporter::Exporter;
use crate::importer::Importer;
use crate::manipulator::Manipulator;
use crate::Module;

pub struct DoNothingImporter {
    name: String,
}

impl DoNothingImporter {
    pub fn new() -> DoNothingImporter {
        DoNothingImporter {
            name: String::from("DoNothingImporter"),
        }
    }
}

impl Importer for DoNothingImporter {
    fn import_corpus(
        &self,
        path: &std::path::Path,
        properties: &std::collections::HashMap<String, String>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        todo!()
    }
}

impl Module for DoNothingImporter {
    fn module_name(&self) -> String {
        self.name.clone()
    }
}
pub struct DoNothingManipulator {
    name: String,
}

impl DoNothingManipulator {
    pub fn new() -> DoNothingManipulator {
        DoNothingManipulator {
            name: String::from("DoNothingManipulator"),
        }
    }
}

impl Manipulator for DoNothingManipulator {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        properties: &std::collections::HashMap<String, String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }
}

impl Module for DoNothingManipulator {
    fn module_name(&self) -> String {
        self.name.clone()
    }
}

pub struct DoNothingExporter {
    name: String,
}

impl DoNothingExporter {
    pub fn new() -> DoNothingExporter {
        DoNothingExporter {
            name: String::from("DoNothingExporter"),
        }
    }
}

impl Exporter for DoNothingExporter {
    fn export_corpus(
        &self,
        graph: &graphannis::AnnotationGraph,
        properties: &std::collections::HashMap<String, String>,
        output_path: &std::path::Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }
}

impl Module for DoNothingExporter {
    fn module_name(&self) -> String {
        self.name.clone()
    }
}
