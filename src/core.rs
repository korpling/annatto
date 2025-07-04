use graphannis::{AnnotationGraph, update::GraphUpdate};
use lazy_static::lazy_static;

use crate::{StepID, error::AnnattoError, progress::ProgressReporter, workflow::StatusSender};

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
#[allow(clippy::disallowed_methods)]
pub(crate) fn update_graph(
    graph: &mut AnnotationGraph,
    update: &mut GraphUpdate,
    step_id: Option<StepID>,
    tx: Option<StatusSender>,
) -> Result<(), anyhow::Error> {
    let step_id = step_id.unwrap_or(FALLBACK_STEP_ID.clone());
    let update_size = update.len()?;
    let progress = ProgressReporter::new(tx.clone(), step_id.clone(), update_size)?;
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
    if graph.global_statistics.is_some() && update_size > 0 {
        // reset statistics if update was non-empty
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

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use graphannis::{AnnotationGraph, update::GraphUpdate};
    use insta::assert_snapshot;
    use itertools::Itertools;

    use crate::{core::update_graph, util::example_generator};

    #[test]
    fn is_effective() {
        let g = AnnotationGraph::with_default_graphstorages(false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        let mut u = GraphUpdate::default();
        example_generator::create_corpus_structure_simple(&mut u);
        let (sender, receiver) = mpsc::channel();
        assert!(update_graph(&mut graph, &mut u, None, Some(sender)).is_ok());
        let messages = receiver
            .into_iter()
            .map(|m| match m {
                crate::workflow::StatusMessage::StepsCreated(_) => "".to_string(),
                crate::workflow::StatusMessage::Info(msg) => msg,
                crate::workflow::StatusMessage::Warning(w) => w,
                crate::workflow::StatusMessage::Progress { id, .. } => id.module_name,
                crate::workflow::StatusMessage::StepDone { id } => id.module_name,
            })
            .join("\n");
        assert_snapshot!(messages);
        assert!(graph.global_statistics.is_none());
    }
}
