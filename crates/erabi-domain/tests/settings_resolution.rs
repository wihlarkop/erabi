use erabi_domain::{LayerValue, ResolvedValue, SettingLayers, SettingSource};

fn all_inherit() -> SettingLayers<u64> {
    SettingLayers {
        per_run: LayerValue::Inherit,
        run_profile: Some(LayerValue::Inherit),
        crawler: Some(LayerValue::Inherit),
        collection: Some(LayerValue::Inherit),
        global: LayerValue::Inherit,
    }
}

#[test]
fn settings_every_applicable_custom_layer_wins_in_canonical_precedence_order() {
    let cases = [
        (SettingSource::PerRunOverride, 11),
        (SettingSource::RunProfile, 12),
        (SettingSource::CrawlerOperationalDefault, 13),
        (SettingSource::CollectionOverride, 14),
        (SettingSource::GlobalSetting, 15),
    ];

    for (expected_source, value) in cases {
        let mut layers = all_inherit();
        match expected_source {
            SettingSource::PerRunOverride => layers.per_run = LayerValue::Custom(value),
            SettingSource::RunProfile => layers.run_profile = Some(LayerValue::Custom(value)),
            SettingSource::CrawlerOperationalDefault => {
                layers.crawler = Some(LayerValue::Custom(value));
            }
            SettingSource::CollectionOverride => {
                layers.collection = Some(LayerValue::Custom(value));
            }
            SettingSource::GlobalSetting => layers.global = LayerValue::Custom(value),
            SettingSource::BuiltInDefault => unreachable!(),
        }

        assert_eq!(
            layers.resolve(99),
            ResolvedValue {
                value,
                source: expected_source,
            }
        );
    }
}

#[test]
fn settings_reset_at_every_applicable_layer_stops_inheritance_and_reports_built_in() {
    let reset_sources = [
        SettingSource::PerRunOverride,
        SettingSource::RunProfile,
        SettingSource::CrawlerOperationalDefault,
        SettingSource::CollectionOverride,
        SettingSource::GlobalSetting,
    ];

    for reset_source in reset_sources {
        let mut layers = all_inherit();
        match reset_source {
            SettingSource::PerRunOverride => {
                layers.per_run = LayerValue::ResetToBuiltIn;
                layers.run_profile = Some(LayerValue::Custom(22));
            }
            SettingSource::RunProfile => {
                layers.run_profile = Some(LayerValue::ResetToBuiltIn);
                layers.crawler = Some(LayerValue::Custom(33));
            }
            SettingSource::CrawlerOperationalDefault => {
                layers.crawler = Some(LayerValue::ResetToBuiltIn);
                layers.collection = Some(LayerValue::Custom(44));
            }
            SettingSource::CollectionOverride => {
                layers.collection = Some(LayerValue::ResetToBuiltIn);
                layers.global = LayerValue::Custom(55);
            }
            SettingSource::GlobalSetting => layers.global = LayerValue::ResetToBuiltIn,
            SettingSource::BuiltInDefault => unreachable!(),
        }

        assert_eq!(
            layers.resolve(99),
            ResolvedValue {
                value: 99,
                source: SettingSource::BuiltInDefault,
            }
        );
    }
}

#[test]
fn settings_quick_scrape_skips_inapplicable_crawler_and_run_profile_layers() {
    let layers = SettingLayers::quick_scrape(
        LayerValue::Inherit,
        Some(LayerValue::Custom(44)),
        LayerValue::Custom(55),
    );

    assert_eq!(
        layers.resolve(99),
        ResolvedValue {
            value: 44,
            source: SettingSource::CollectionOverride,
        }
    );
}

#[test]
fn settings_fully_inherited_setting_uses_the_built_in_value_and_source() {
    assert_eq!(
        all_inherit().resolve(99),
        ResolvedValue {
            value: 99,
            source: SettingSource::BuiltInDefault,
        }
    );
}
