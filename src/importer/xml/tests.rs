use std::path::Path;

use insta::assert_snapshot;

use crate::{importer::xml::ImportXML, util::import_as_graphml_string};

#[test]
fn test_generic_xml() {
    let actual = import_as_graphml_string(
        ImportXML {
            default_ordering: "w".to_string(),
            named_orderings: vec!["w".to_string()].into_iter().collect(),
            skip_names: vec![
                "document".to_string(),
                "P".to_string(),
                "AUTHOR".to_string(),
                "URL".to_string(),
                "QUOTE".to_string(),
                "TITLE".to_string(),
                "TRANSLATOR".to_string(),
            ]
            .into_iter()
            .collect(),
            use_ids: true,
        },
        Path::new("tests/data/import/generic_xml/"),
        None,
    )
    .unwrap();

    assert_snapshot!(actual);
}
