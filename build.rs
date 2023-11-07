fn main() {
    let proto_file = "proto/log.proto";
    tonic_build::configure()
        .type_attribute("Record", "#[serde_as]")
        .type_attribute("Record", "#[derive(serde::Deserialize, serde::Serialize)]")
        .field_attribute("value", r#"#[serde_as(as = "Base64")]"#)
        .field_attribute("offset", r#"#[serde(skip_deserializing)]"#)
        .compile(&[proto_file], &["."])
        .unwrap_or_else(|e| panic!("protobuf compile error: {}", e));
}
