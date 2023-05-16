use std::path::Path;

fn main() {
    // Make sure that the docs/book directory exists, otherwise include_dir will
    // fail.
    let root_dir = std::env::var_os("CARGO_MANIFEST_DIR").unwrap();
    let book_dir = Path::new(&root_dir).join("docs/book/");
    std::fs::create_dir_all(book_dir).unwrap();
}
