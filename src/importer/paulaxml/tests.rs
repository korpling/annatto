use super::*;

#[test]
fn paula_documents_by_header_type() {
    let paula_dir =
        PaulaDirectory::open_directory("tests/data/import/paulaxml/rootCorpus/subCorpus1/doc1")
            .unwrap();
    let paula_doc = PaulaDocument::from_directory(&paula_dir).unwrap();

    let texts = paula_doc.by_header_type("text");
    assert_eq!(1, texts.len());
    assert_eq!(
        "doc1.text",
        texts[0]
            .root_element()
            .children()
            .filter(|n| n.has_tag_name("header"))
            .next()
            .unwrap()
            .attribute("paula_id")
            .unwrap()
    );
}

#[test]
fn paula_documents_by_id() {
    let paula_dir =
        PaulaDirectory::open_directory("tests/data/import/paulaxml/rootCorpus/subCorpus1/doc1")
            .unwrap();
    let paula_doc = PaulaDocument::from_directory(&paula_dir).unwrap();

    let result = paula_doc.by_paula_id("doc1.tok_lemma");
    assert_eq!(true, result.is_some());
}
