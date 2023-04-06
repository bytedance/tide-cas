use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("descriptor.bin"))
        .compile(
            &["proto/build/bazel/remote/execution/v2/remote_execution.proto",
                "proto/google/bytestream/bytestream.proto",
            ],
            &["proto/"],
        )?;
    Ok(())
}