use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub(crate) struct CrawlRequest {
    pub(crate) urls: Vec<String>,
    pub(crate) browser_config: TypedConfig<BrowserConfig>,
    pub(crate) crawler_config: TypedConfig<CrawlerRunConfig>,
}

#[derive(Debug, Serialize)]
pub(crate) struct TypedConfig<T> {
    #[serde(rename = "type")]
    pub(crate) config_type: &'static str,
    pub(crate) params: T,
}

#[derive(Debug, Serialize)]
pub(crate) struct BrowserConfig {
    pub(crate) headless: bool,
    pub(crate) user_agent: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct CrawlerRunConfig {
    pub(crate) cache_mode: TypedConfig<&'static str>,
    pub(crate) page_timeout: u64,
    pub(crate) stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) wait_for: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) scan_full_page: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) scroll_delay: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max_scroll_steps: Option<u32>,
    pub(crate) screenshot: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) force_viewport_screenshot: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct HealthResponse {
    pub(crate) status: String,
    pub(crate) version: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CrawlResponse {
    pub(crate) success: bool,
    pub(crate) results: Vec<CrawlResult>,
    pub(crate) server_processing_time_s: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CrawlResult {
    pub(crate) url: String,
    pub(crate) html: String,
    pub(crate) success: bool,
    pub(crate) cleaned_html: Option<String>,
    #[serde(default)]
    pub(crate) links: ProviderLinks,
    pub(crate) markdown: Option<ProviderMarkdown>,
    pub(crate) screenshot: Option<String>,
    pub(crate) response_headers: Option<BTreeMap<String, serde_json::Value>>,
    pub(crate) status_code: Option<i64>,
    pub(crate) redirected_url: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ProviderLink {
    pub(crate) href: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct ProviderLinks {
    #[serde(default)]
    pub(crate) internal: Vec<ProviderLink>,
    #[serde(default)]
    pub(crate) external: Vec<ProviderLink>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum ProviderMarkdown {
    Structured(ProviderMarkdownResult),
    Text(String),
}

#[derive(Debug, Deserialize)]
pub(crate) struct ProviderMarkdownResult {
    pub(crate) raw_markdown: String,
}
