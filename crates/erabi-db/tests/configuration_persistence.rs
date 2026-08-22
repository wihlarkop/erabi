use erabi_db::{
    BootstrapConfiguration, ErabiDatabase, LocalDataOwnership, MigrationRunner,
    PersistedDestination, PersistedSetting, SecretEnvironmentVariableName, SettingScope,
    repositories::ConfigurationRepository,
};
use erabi_domain::LayerValue;

#[tokio::test]
async fn configuration_ordinary_tri_state_settings_round_trip_exactly()
-> Result<(), Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    let repository = ConfigurationRepository::new(&database);
    let scope = SettingScope::Crawler {
        crawler_id: "crawler-1".into(),
    };

    for value in [
        LayerValue::Inherit,
        LayerValue::Custom(serde_json::json!(250)),
        LayerValue::ResetToBuiltIn,
    ] {
        let setting = PersistedSetting::new(
            scope.clone(),
            "request_delay_ms",
            value,
            "2026-08-23T00:00:00Z",
        )?;
        repository.save_setting(&setting).await?;
        assert_eq!(
            repository.setting(&scope, "request_delay_ms").await?,
            Some(setting)
        );
    }
    Ok(())
}

#[tokio::test]
async fn configuration_secrets_are_rejected_and_destination_keeps_only_env_name()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(
        PersistedSetting::new(
            SettingScope::Global,
            "crawl4ai_api_token",
            LayerValue::Custom(serde_json::json!("never-persist-this")),
            "2026-08-23T00:00:00Z",
        )
        .is_err()
    );
    assert!(
        PersistedDestination::new(
            "destination-1",
            "Remote Turso",
            "TURSO",
            serde_json::json!({"api_token": "never-persist-this"}),
            None,
            "2026-08-23T00:00:00Z",
            "2026-08-23T00:00:00Z",
        )
        .is_err()
    );

    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    let repository = ConfigurationRepository::new(&database);
    let destination = PersistedDestination::new(
        "destination-1",
        "Remote Turso",
        "TURSO",
        serde_json::json!({"url": "https://example.turso.io"}),
        Some(SecretEnvironmentVariableName::new("TURSO_AUTH_TOKEN")?),
        "2026-08-23T00:00:00Z",
        "2026-08-23T00:00:00Z",
    )?;
    repository.save_destination(&destination).await?;
    let stored = repository.destination("destination-1").await?;
    assert_eq!(stored, Some(destination));
    assert_eq!(
        stored
            .and_then(|destination| destination.secret_environment_variable_name)
            .map(|name| name.as_str().to_owned()),
        Some("TURSO_AUTH_TOKEN".into())
    );
    Ok(())
}

#[tokio::test]
async fn configuration_local_data_ownership_metadata_round_trips_without_runtime_locking()
-> Result<(), Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    let repository = ConfigurationRepository::new(&database);
    let ownership = LocalDataOwnership::new(
        "C:/controlled/data",
        42,
        "2026-08-23T00:00:00Z",
        "0.1.0",
        "127.0.0.1:7878",
        "2026-08-23T00:00:00Z",
    )?;
    repository.save_local_data_ownership(&ownership).await?;
    assert_eq!(
        repository
            .local_data_ownership("C:/controlled/data")
            .await?,
        Some(ownership)
    );
    Ok(())
}

#[test]
fn configuration_bootstrap_defaults_to_telemetry_off_and_secret_names_only()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(!BootstrapConfiguration::default().telemetry_enabled);
    assert!(SecretEnvironmentVariableName::new("not_a_secret_value").is_err());
    assert_eq!(
        SecretEnvironmentVariableName::new("ERABI_ACCESS_TOKEN")?.as_str(),
        "ERABI_ACCESS_TOKEN"
    );
    Ok(())
}
