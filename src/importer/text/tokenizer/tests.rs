#[test]
fn tokenize_english() {
    let text = "O.K., so the answer's obvious. Don't feed the trolls...";
    let tokens = super::tokenize(text.as_bytes(), super::Language::English).unwrap();

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
    let tokens = super::tokenize(text.as_bytes(), super::Language::English).unwrap();

    assert_eq!(vec!["No(t)", "((this))", "(", "time", ")", "!"], tokens,);
}

#[test]
fn tokenize_sgml() {
    let text = r#"
        <doc title="Test" date="24.11.2025">
       A <b>test</b> for <span ordering='1.0'>SGML spans</span>.
       </doc>
       "#;
    let tokens = super::tokenize(text.as_bytes(), super::Language::English).unwrap();

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
    let tokens = super::tokenize(text.as_bytes(), super::Language::French).unwrap();

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
    let tokens = super::tokenize(text.as_bytes(), super::Language::Italian).unwrap();

    assert_eq!(tokens, vec!["Riuscire", "all'", "università"]);
}
