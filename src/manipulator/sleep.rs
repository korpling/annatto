use documented::{Documented, DocumentedFields};
use serde::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use super::Manipulator;

/// This operation pauses the conversion process. As a regular user, you usually do not need to use this feature.
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(deny_unknown_fields)]
pub struct Sleep {
    /// Time to sleep in seconds.
    #[serde(default)]
    seconds: u64,
}

impl Manipulator for Sleep {
    fn manipulate_corpus(
        &self,
        graph: &mut graphannis::AnnotationGraph,
        _workflow_directory: &std::path::Path,
        _step_id: crate::StepID,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        std::thread::sleep(std::time::Duration::from_secs(self.seconds));
        graph.ensure_loaded_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn deserialize() {
        let toml_str = "seconds = 10";
        let r: Result<super::Sleep, _> = toml::from_str(toml_str);
        assert!(r.is_ok());
        assert_eq!(r.unwrap().seconds, 10);
    }
}
