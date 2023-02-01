use super::*;

#[test]
fn parse_good() {
    let minimal = OoTextfileParser::parse(Rule::textgrid, &include_str!("minimal.TextGrid")).unwrap();
    
}
