use itertools::Itertools;

use super::*;

#[test]
fn tokenize_unknown() {
    let text = "Riuscire all'università";
    let tokenizer = TreeTaggerTokenizer::new("unknown".into()).unwrap();
    let tokens = tokenizer
        .tokenize(text.as_bytes())
        .unwrap()
        .into_iter()
        .map(|t| t.value)
        .collect_vec();

    assert_eq!(tokens, vec!["Riuscire", "all'università"]);
}

#[test]
fn tokenize_english() {
    let text = "O.K., so the answer's obvious. Don't feed the trolls...";
    let tokenizer = TreeTaggerTokenizer::new("en".into()).unwrap();
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
fn tokenize_french() {
    let text = "ou ceux-là mêmes qu'il s'affirmaient";
    let tokenizer = TreeTaggerTokenizer::new("fr".into()).unwrap();
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
fn tokenize_italian() {
    let text = "Riuscire all'università";
    let tokenizer = TreeTaggerTokenizer::new("it".into()).unwrap();
    let tokens = tokenizer
        .tokenize(text.as_bytes())
        .unwrap()
        .into_iter()
        .map(|t| t.value)
        .collect_vec();

    assert_eq!(tokens, vec!["Riuscire", "all'", "università"]);
}

#[test]
fn tokenize_romanian() {
    let text = "Toate ființele umane se nasc libere și egale în demnitate și în drepturi.";
    let tokenizer = TreeTaggerTokenizer::new("ro".into()).unwrap();
    let tokens = tokenizer
        .tokenize(text.as_bytes())
        .unwrap()
        .into_iter()
        .map(|t| t.value)
        .collect_vec();

    assert_eq!(
        tokens,
        vec![
            "Toate",
            "ființele",
            "umane",
            "se",
            "nasc",
            "libere",
            "și",
            "egale",
            "în",
            "demnitate",
            "și",
            "în",
            "drepturi",
            "."
        ]
    );
}

#[test]
fn tokenize_portuguese() {
    let text = "Todos os seres humanos nascem livres e iguais em dignidade e em direitos.";
    let tokenizer = TreeTaggerTokenizer::new("pt".into()).unwrap();
    let tokens = tokenizer
        .tokenize(text.as_bytes())
        .unwrap()
        .into_iter()
        .map(|t| t.value)
        .collect_vec();

    assert_eq!(
        tokens,
        vec![
            "Todos",
            "os",
            "seres",
            "humanos",
            "nascem",
            "livres",
            "e",
            "iguais",
            "em",
            "dignidade",
            "e",
            "em",
            "direitos",
            "."
        ]
    );
}

#[test]
fn tokenize_galician() {
    let text = "Como te chamas?";
    let tokenizer = TreeTaggerTokenizer::new("gl".into()).unwrap();
    let tokens = tokenizer
        .tokenize(text.as_bytes())
        .unwrap()
        .into_iter()
        .map(|t| t.value)
        .collect_vec();

    assert_eq!(tokens, vec!["Como", "te", "chamas", "?",]);
}

#[test]
fn tokenize_catalan() {
    let text =
        "No existia, no existí mai entre nosaltres, una comunitat d'interessos, d'afeccions.";
    let tokenizer = TreeTaggerTokenizer::new("ca".into()).unwrap();
    let tokens = tokenizer
        .tokenize(text.as_bytes())
        .unwrap()
        .into_iter()
        .map(|t| t.value)
        .collect_vec();

    assert_eq!(
        tokens,
        vec![
            "No",
            "existia",
            ",",
            "no",
            "existí",
            "mai",
            "entre",
            "nosaltres",
            ",",
            "una",
            "comunitat",
            "d'",
            "interessos",
            ",",
            "d'",
            "afeccions",
            "."
        ]
    );
}

#[test]
fn tokenize_parenthesis() {
    let text = "No(t) ((this)) (time)!";
    let tokenizer = TreeTaggerTokenizer::new("en".into()).unwrap();
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
fn tokenize_trailing_punctuation() {
    let text = "What!!?!.";
    let tokenizer = TreeTaggerTokenizer::new(Language::Unknown).unwrap();
    let tokens = tokenizer
        .tokenize(text.as_bytes())
        .unwrap()
        .into_iter()
        .map(|t| t.value)
        .collect_vec();

    assert_eq!(vec!["What", "!", "!", "?", "!", "."], tokens);
}
