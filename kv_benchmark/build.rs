fn main() -> Result<(), Box<dyn std::error::Error>> {
    // This tells prost-build to compile the proto file and generate Rust code.
    // The output will be placed in the `OUT_DIR` directory, which Cargo manages.
    prost_build::compile_protos(&["src/benchmark.proto"], &["src/"])?;
    Ok(())
}