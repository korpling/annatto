use crate::importer::textgrid;

use super::*;

#[test]
fn parse_sequence() {
    let textgrid = OoTextfileParser::parse(Rule::textgrid, &include_str!("empty.TextGrid"))
        .unwrap()
        .next()
        .unwrap();

    // TextGrid fields are only a *flat* sequence of numbers, text and flags
    let pairs: Vec<_> = textgrid.into_inner().collect();
    assert_eq!(7, pairs.len());

    assert_eq!(Rule::number, pairs[0].as_rule());
    assert_eq!("123", pairs[0].as_str());

    assert_eq!(Rule::number, pairs[1].as_rule());
    assert_eq!("2045.144149659864", pairs[1].as_str());

    assert_eq!(Rule::flag, pairs[2].as_rule());
    assert_eq!("<exists>", pairs[2].as_str());

    assert_eq!(Rule::number, pairs[3].as_rule());
    assert_eq!("0", pairs[3].as_str());

    assert_eq!(Rule::text, pairs[4].as_rule());
    assert_eq!("\"value\"", pairs[4].as_str());

    assert_eq!(Rule::text, pairs[5].as_rule());
    assert_eq!("\"value\"", pairs[5].as_str());

    assert_eq!(Rule::EOI, pairs[6].as_rule());
}

#[test]
fn parse_mary_john() {
    let textgrid = TextGrid::parse(include_str!("maryjohn.TextGrid")).unwrap();
    assert_eq!(0.0, textgrid.xmin);
    assert_eq!(2.3, textgrid.xmax);
}
