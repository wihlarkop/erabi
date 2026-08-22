//! Explicit inheritable operational-setting resolution.

/// A setting value at one applicable configuration layer.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LayerValue<T> {
    /// Continue to the next lower-precedence applicable layer.
    #[default]
    Inherit,
    /// Use this layer's value.
    Custom(T),
    /// Stop inheritance and use the product's built-in value.
    ResetToBuiltIn,
}

/// The configuration layer that supplied a resolved setting value.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SettingSource {
    PerRunOverride,
    RunProfile,
    CrawlerOperationalDefault,
    CollectionOverride,
    GlobalSetting,
    BuiltInDefault,
}

/// A resolved value together with the layer that effectively supplied it.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ResolvedValue<T> {
    pub value: T,
    pub source: SettingSource,
}

/// Values for one setting across the operational precedence chain.
///
/// `None` means the layer is not applicable to this run; it is deliberately
/// different from [`LayerValue::Inherit`], which represents an applicable
/// layer that defers to the next one.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SettingLayers<T> {
    pub per_run: LayerValue<T>,
    pub run_profile: Option<LayerValue<T>>,
    pub crawler: Option<LayerValue<T>>,
    pub collection: Option<LayerValue<T>>,
    pub global: LayerValue<T>,
}

impl<T> SettingLayers<T> {
    /// Builds the layer chain used by an ad-hoc Quick Scrape.
    ///
    /// Quick Scrape has no Crawler or Run Profile, so those layers cannot
    /// participate in resolution.
    #[must_use]
    pub const fn quick_scrape(
        per_run: LayerValue<T>,
        collection: Option<LayerValue<T>>,
        global: LayerValue<T>,
    ) -> Self {
        Self {
            per_run,
            run_profile: None,
            crawler: None,
            collection,
            global,
        }
    }

    /// Resolves this setting using the canonical operational precedence.
    #[must_use]
    pub fn resolve(&self, built_in: T) -> ResolvedValue<T>
    where
        T: Clone,
    {
        let layers = [
            (SettingSource::PerRunOverride, Some(&self.per_run)),
            (SettingSource::RunProfile, self.run_profile.as_ref()),
            (
                SettingSource::CrawlerOperationalDefault,
                self.crawler.as_ref(),
            ),
            (SettingSource::CollectionOverride, self.collection.as_ref()),
            (SettingSource::GlobalSetting, Some(&self.global)),
        ];

        for (source, layer_value) in layers {
            match layer_value {
                Some(LayerValue::Custom(value)) => {
                    return ResolvedValue {
                        value: value.clone(),
                        source,
                    };
                }
                Some(LayerValue::ResetToBuiltIn) => {
                    return ResolvedValue {
                        value: built_in,
                        source: SettingSource::BuiltInDefault,
                    };
                }
                Some(LayerValue::Inherit) | None => {}
            }
        }

        ResolvedValue {
            value: built_in,
            source: SettingSource::BuiltInDefault,
        }
    }
}
