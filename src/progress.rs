use crate::{error::PepperError, workflow::StatusMessage, workflow::StatusSender, Module, StepID};
use log::{info, warn};
use std::path::Path;

pub struct ProgressReporter {
    tx: Option<StatusSender>,
    step_id: StepID,
}

impl ProgressReporter {
    pub fn new(
        tx: Option<StatusSender>,
        module: &dyn Module,
        path: Option<&Path>,
    ) -> ProgressReporter {
        let step_id = module.step_id(path);
        ProgressReporter { tx, step_id }
    }

    pub fn info(&self, msg: &str) -> Result<(), PepperError> {
        if let Some(ref tx) = self.tx {
            tx.send(StatusMessage::Info(msg.to_string()))?;
        } else {
            info!("{}", msg);
        }
        Ok(())
    }

    pub fn warn(&self, msg: &str) -> Result<(), PepperError> {
        if let Some(ref tx) = self.tx {
            tx.send(StatusMessage::Warning(msg.to_string()))?;
        } else {
            warn!("{}", msg);
        }
        Ok(())
    }

    pub fn set_progress(&self, progress: f32) -> Result<(), PepperError> {
        if let Some(ref tx) = self.tx {
            tx.send(StatusMessage::Progress {
                id: self.step_id.clone(),
                progress,
            })?;
        }
        Ok(())
    }
}
