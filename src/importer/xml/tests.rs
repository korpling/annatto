use std::{collections::BTreeSet, path::Path};

use insta::assert_snapshot;

use crate::{importer::xml::ImportXML, util::import_as_graphml_string};

#[test]
fn test_generic_xml() {
    let actual = import_as_graphml_string(
        ImportXML {
            default_ordering: "w".to_string(),
            named_orderings: vec!["w".to_string()].into_iter().collect::<BTreeSet<String>>(),
            skip_names: BTreeSet::new()
        },
        Path::new("tests/data/import/generic_xml/"),
        None,
    )
    .unwrap();

    assert_snapshot!(actual);
}
