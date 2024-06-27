use graphannis::update::GraphUpdate;

use crate::progress::ProgressReporter;

pub(super) struct DocumentMapper {
    reporter: ProgressReporter,
}

impl DocumentMapper {
    pub(super) fn new(reporter: ProgressReporter) -> DocumentMapper {
        DocumentMapper { reporter }
    }

    pub(super) fn read_document<R: std::io::Read>(
        &self,
        _input: &mut R,
        _document_node_name: &str,
        _updates: &mut GraphUpdate,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
