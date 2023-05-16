use std::path::Path;

fn main() {
    // Make sure that the docs/book directory exists, otherwise include_dir will
    // fail.
    let root_dir = std::env::var_os("CARGO_MANIFEST_DIR").unwrap();
    let book_dir = Path::new(&root_dir).join("docs/book/");
    std::fs::create_dir_all(book_dir).unwrap();

    // If we are executed using `cargo dist`, we want to compile the book before
    // starting the build. The special `dist` profile can be used to check this
    // condition.
    if std::env::var("PROFILE").unwrap_or_default().as_str() == "dist" {
        println!("cargo:warning=Compiling documentation with mdbook. This can take some time.");
        std::process::Command::new("mdbook")
            .arg("build")
            .arg("docs/")
            .output()
            .expect("Could not execute mdbook build docs/");
    }
}
