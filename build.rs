use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=docs/");

    // Make sure that the docs/book directory exists, otherwise include_dir will
    // fail.
    let root_dir = std::env::var_os("CARGO_MANIFEST_DIR").unwrap();
    let book_dir = Path::new(&root_dir).join("docs/book/");

    std::fs::create_dir_all(&book_dir).unwrap();

    // In order to test the documentation server, we need at least an index.html
    // file in the output folder.
    let doc_index = book_dir.join("index.html");
    if !doc_index.exists() {
        println!(
            "cargo:warning=Creating almost empty file {} for tests.",
            doc_index.to_string_lossy()
        );
        std::fs::write(
            doc_index,
            r#"
        <!DOCTYPE HTML>
        <html lang="en" class="sidebar-visible no-js light">
            <head>
                <!-- Book generated using mdBook -->
                <meta charset="UTF-8">
                <title>Introduction - Annatto Documentation</title>
            </head>
            <body>
            Compile the documentation with "mdbook build docs/"
            </body>
        </html>
        "#,
        )
        .unwrap();
    }
}
