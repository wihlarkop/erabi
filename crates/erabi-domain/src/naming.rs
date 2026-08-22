#[must_use]
pub fn derive_source_name(
    page_title: Option<&str>,
    og_title: Option<&str>,
    url: &url::Url,
) -> String {
    page_title
        .filter(|value| !value.trim().is_empty())
        .or_else(|| og_title.filter(|value| !value.trim().is_empty()))
        .map(str::trim)
        .map_or_else(
            || {
                let domain = url.host_str().unwrap_or_default();
                let path = url.path().trim_matches('/');
                if !domain.is_empty() && !path.is_empty() {
                    format!("{domain}/{path}")
                } else if !domain.is_empty() {
                    domain.to_owned()
                } else {
                    "Untitled Source".to_owned()
                }
            },
            str::to_owned,
        )
}
#[must_use]
pub fn derive_dataset_name(source_name: &str, page_type_name: &str) -> String {
    format!("{} {}", source_name.trim(), page_type_name.trim())
        .trim()
        .to_owned()
}
