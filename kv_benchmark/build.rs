fn main() -> Result<(), Box<dyn std::error::Error>> {
    // This tells prost-build to compile the proto file and generate Rust code.
    prost_build::compile_protos(&["src/benchmark.proto"], &["src/"])?;

    // This tells flatc to compile the fbs schema and generate Rust code.
    let fbs_file = "src/benchmark.fbs";
    println!("cargo:rerun-if-changed={}", fbs_file);
    flatc_rust::run(flatc_rust::Args {
        lang: "rust",
        inputs: &[std::path::Path::new(fbs_file)],
        out_dir: &std::path::Path::new(&std::env::var("OUT_DIR").unwrap()),
        ..Default::default()
    })?;

    Ok(())
}