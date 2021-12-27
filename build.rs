use std::path::PathBuf;

use cached_path::cached_path;
fn main() {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let pepper_dir = out_dir.join("pepper").join("plugins");
    println!("cargo:rerun-if-changed={:#?}", &pepper_dir);
    let exists = pepper_dir.is_dir();
    if !exists {
        std::fs::create_dir_all(&out_dir).unwrap();

        // download Pepper distribution and unzip its plugin folder to the output directory
        let pepper_dist_path = cached_path("https://korpling.german.hu-berlin.de/saltnpepper/pepper/download/stable/Pepper_2020.09.02.zip").unwrap();
        let pepper_dist_zip = std::fs::File::open(&pepper_dist_path).unwrap();
        let mut archive = zip::ZipArchive::new(pepper_dist_zip).unwrap();

        for i in 0..archive.len() {
            let mut file = archive.by_index(i).unwrap();
            let file_name = file.name().trim().replace('\\', "/");
            if file_name.starts_with("pepper/plugins/") {
                let output_path = out_dir.join(file_name);
                if file.is_dir() {
                    std::fs::create_dir_all(output_path).unwrap();
                } else {
                    // Extract file
                    let mut outfile = std::fs::File::create(&output_path).unwrap();
                    std::io::copy(&mut file, &mut outfile).unwrap();
                }
            }
        }
    }
}
