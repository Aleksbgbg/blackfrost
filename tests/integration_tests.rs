use blackfrost::SnowflakeGenerator;

#[tokio::test]
async fn generate_id() {
  let generator = SnowflakeGenerator::new(0);

  let id = generator.generate_id().await;

  assert!(id > 0);
}
