fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/grpc_backend.proto")?;
    tonic_build::compile_protos("proto/redpy.proto")?;
    Ok(())
}
