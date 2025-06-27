use crate::{StepID, error::AnnattoError, workflow::StatusMessage, workflow::StatusSender};
use log::{info, warn};
use std::sync::{Arc, Mutex};

struct ProgressState {
    tx: Option<StatusSender>,
    accumulated_finished_work: usize,
}

pub struct ProgressReporter {
    state: Arc<Mutex<ProgressState>>,
    total_work: Option<usize>,
    step_id: StepID,
}

impl ProgressReporter {
    pub fn new(
        tx: Option<StatusSender>,
        step_id: StepID,
        total_work: usize,
    ) -> Result<ProgressReporter, AnnattoError> {
        let state = ProgressState {
            tx,
            accumulated_finished_work: 0,
        };
        let reporter = ProgressReporter {
            state: Arc::new(Mutex::new(state)),
            step_id,
            total_work: Some(total_work),
        };
        // Send a first status report so any listener can get the total number of steps to perform
        reporter.worked(0)?;
        Ok(reporter)
    }

    pub fn new_unknown_total_work(
        tx: Option<StatusSender>,
        step_id: StepID,
    ) -> Result<ProgressReporter, AnnattoError> {
        let state = ProgressState {
            tx,
            accumulated_finished_work: 0,
        };
        let reporter = ProgressReporter {
            state: Arc::new(Mutex::new(state)),
            step_id,
            total_work: None,
        };
        // Send a first status report so any listener can get the total number of steps to perform
        reporter.worked(0)?;
        Ok(reporter)
    }

    pub fn info(&self, msg: &str) -> Result<(), AnnattoError> {
        let state = self.state.lock()?;
        if let Some(ref tx) = state.tx {
            tx.send(StatusMessage::Info(msg.to_string()))?;
        } else {
            info!("{msg}");
        }
        Ok(())
    }

    pub fn warn(&self, msg: &str) -> Result<(), AnnattoError> {
        let state = self.state.lock()?;
        if let Some(ref tx) = state.tx {
            tx.send(StatusMessage::Warning(msg.to_string()))?;
        } else {
            warn!("{msg}");
        }
        Ok(())
    }

    pub fn worked(&self, finished_work: usize) -> Result<(), AnnattoError> {
        let mut state = self.state.lock()?;
        state.accumulated_finished_work += finished_work;

        if let Some(ref tx) = state.tx {
            tx.send(StatusMessage::Progress {
                id: self.step_id.clone(),
                total_work: self.total_work,
                finished_work: state.accumulated_finished_work,
            })?;
        }
        Ok(())
    }
}
