use std::env::set_var;
use std::fs::create_dir_all;
use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=protos");
    println!("cargo:rerun-if-changed=src/protos");

    println!("cargo:rerun-if-env-changed=BUILD_NUMBER");

    println!(
        "cargo:rustc-env=BUILD_NUMBER={}",
        std::env::var("BUILD_NUMBER").unwrap_or("0".to_owned())
    );

    set_var("OUT_DIR", "src/protos/");

    let path = Path::new("./src/protos");
    if !path.exists() {
        let _ = create_dir_all(path);
    }
    let content = r#"
    mod objects;
    pub use objects::*;
    "#;
    let mut config = prost_build::Config::new();
    config.default_package_filename("objects");
    config.compile_protos(&["protos/objects.proto"],
                          &["protos"]).unwrap();
    std::fs::write("src/protos/mod.rs", content).unwrap();
}
