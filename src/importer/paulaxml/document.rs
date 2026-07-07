use std::path::Path;

use anyhow::Result;
use graphannis::update::GraphUpdate;

pub(super) struct DocumentMapper {}

impl<'input> DocumentMapper {
    pub(super) fn read_document(
        _input_directory: &Path,
        _doc_node_name: &str,
        _updates: &mut GraphUpdate,
    ) -> Result<()> {
        // let doc = roxmltree::Document::parse(input)?;
        // let root = doc.root_element();
        // if root.tag_name().name() != "paula" {
        //     bail!("PAULA XML document file must start with <paula> tag");
        // }

        // let nodes = doc
        //     .root_element()
        //     .children()
        //     .filter(|n| n.tag_name().name() == "nodes")
        //     .collect_vec();

        let _mapper = DocumentMapper {};

        todo!()
    }
}
