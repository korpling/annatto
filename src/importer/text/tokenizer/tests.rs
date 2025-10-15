use itertools::Itertools;

use super::*;

#[test]
fn tokenize_english() {
    let text = "O.K., so the answer's obvious. Don't feed the trolls...";
    let tokenizer = TreeTaggerTokenizer::new(Language::English).unwrap();
    let tokens = tokenizer
        .tokenize(text.as_bytes())
        .unwrap()
        .into_iter()
        .map(|t| t.value)
        .collect_vec();

    assert_eq!(
        vec![
            "O.K.", ",", "so", "the", "answer", "'s", "obvious", ".", "Do", "n't", "feed", "the",
            "trolls", "..."
        ],
        tokens,
    );
}

#[test]
fn tokenize_parenthesis() {
    let text = "No(t) ((this)) (time)!";
    let tokenizer = TreeTaggerTokenizer::new(Language::English).unwrap();
    let tokens = tokenizer
        .tokenize(text.as_bytes())
        .unwrap()
        .into_iter()
        .map(|t| t.value)
        .collect_vec();

    assert_eq!(vec!["No(t)", "((this))", "(", "time", ")", "!"], tokens);
}

#[test]
fn tokenize_sgml() {
    let text = r#"
        <doc title="Test" date="24.11.2025">
       A <b>test</b> for <span ordering='1.0'>SGML spans</span>.
       </doc>
       "#;
    let tokenizer = TreeTaggerTokenizer::new(Language::Unknown).unwrap();
    let tokens = tokenizer
        .tokenize(text.as_bytes())
        .unwrap()
        .into_iter()
        .map(|t| t.value)
        .collect_vec();

    assert_eq!(
        vec![
            "<doc title=\"Test\" date=\"24.11.2025\">",
            "A",
            "<b>",
            "test",
            "</b>",
            "for",
            "<span ordering='1.0'>",
            "SGML",
            "spans",
            "</span>",
            ".",
            "</doc>"
        ],
        tokens,
    );
}

#[test]
fn tokenize_clitics_french() {
    let text = "ou ceux-là mêmes qu'il s'affirmaient";
    let tokenizer = TreeTaggerTokenizer::new(Language::French).unwrap();
    let tokens = tokenizer
        .tokenize(text.as_bytes())
        .unwrap()
        .into_iter()
        .map(|t| t.value)
        .collect_vec();

    assert_eq!(
        vec![
            "ou",
            "ceux",
            "-là",
            "mêmes",
            "qu'",
            "il",
            "s'",
            "affirmaient"
        ],
        tokens,
    );
}

#[test]
fn tokenize_clitics_italian() {
    let text = "Riuscire all'università";
    let tokenizer = TreeTaggerTokenizer::new(Language::Italian).unwrap();
    let tokens = tokenizer
        .tokenize(text.as_bytes())
        .unwrap()
        .into_iter()
        .map(|t| t.value)
        .collect_vec();

    assert_eq!(tokens, vec!["Riuscire", "all'", "università"]);
}
