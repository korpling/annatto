use graphannis::{update::GraphUpdate, AnnotationGraph};
use lazy_static::lazy_static;

use crate::{error::AnnattoError, progress::ProgressReporter, workflow::StatusSender, StepID};

lazy_static! {
    static ref FALLBACK_STEP_ID: StepID = {
        StepID {
            module_name: "(undefined)".to_string(),
            path: None,
        }
    };
}

/// This method applies updates to a graph without re-calculating the statistics.
/// Additionally, the statistics of the graph are set to `None` to indicate that
/// the statistics need to be computed if needed.
pub(crate) fn update_graph(
    graph: &mut AnnotationGraph,
    update: &mut GraphUpdate,
    step_id: Option<StepID>,
    tx: Option<StatusSender>,
) -> Result<(), anyhow::Error> {
    let step_id = step_id.unwrap_or(FALLBACK_STEP_ID.clone());
    let progress = ProgressReporter::new(tx.clone(), step_id.clone(), update.len()?)?;
    graph
        .apply_update_keep_statistics(update, |msg| {
            if let Err(e) = progress.info(msg) {
                log::error!("{e}");
            }
        })
        .map_err(|reason| AnnattoError::UpdateGraph(reason.to_string()))?;
    if let Some(sender) = tx {
        sender.send(crate::workflow::StatusMessage::StepDone { id: step_id })?;
    };
    if graph.global_statistics.is_some() {
        graph.global_statistics = None;
    }
    Ok(())
}

/// This method applies updates to a graph without re-calculating the statistics.
/// Additionally, the statistics of the graph are set to `None` to indicate that
/// the statistics need to be computed if needed.
pub(crate) fn update_graph_silent(
    graph: &mut AnnotationGraph,
    update: &mut GraphUpdate,
) -> Result<(), anyhow::Error> {
    update_graph(graph, update, None, None)
}
