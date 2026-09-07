//! `Crawl4AI` adapter boundary; it does not expose engine details to the domain.

mod client;
mod config;
mod dto;

pub use client::Crawl4AiAdapter;
pub use config::{Crawl4AiConfig, Crawl4AiConfigError};
