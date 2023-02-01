use anyhow::bail;

use super::*;

#[test]
fn parse_empty() {
    let textgrid = OoTextfileParser::parse(Rule::textgrid, &include_str!("empty.TextGrid"))
        .unwrap()
        .next()
        .unwrap();

    // Check top-level sequence
    let mut textgrid = textgrid.into_inner();
    let xmin = textgrid.next().unwrap();
    assert_eq!(xmin.as_rule(), Rule::xmin);
    let xmax = textgrid.next().unwrap();
    assert_eq!(xmax.as_rule(), Rule::xmax);
    let item_size = textgrid.next().unwrap();
    assert_eq!(item_size.as_rule(), Rule::item_size);

    // Check that the xmin, xmax and item_size values have been parsed
    let xmin_val = xmin.into_inner().next().unwrap();
    assert_eq!(xmin_val.as_str(), "123");
    let xmax_val = xmax.into_inner().next().unwrap();
    assert_eq!(xmax_val.as_str(), "2045.144149659864");
    let item_size_val = item_size.into_inner().next().unwrap();
    assert_eq!(item_size_val.as_str(), "0");
}
