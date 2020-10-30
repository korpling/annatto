use crate::error::PepperError;

pub struct ExmaraldaExporter {
    jvm : j4rs::Jvm,
}

impl ExmaraldaExporter {
    pub fn new() -> Result<ExmaraldaExporter, PepperError> {
        let jvm = j4rs::JvmBuilder::new().build()?;
        let exporter = ExmaraldaExporter {
            jvm,
        };
        Ok(exporter)
    }
}