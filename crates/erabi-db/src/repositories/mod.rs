//! Bounded repositories for Plan 02-owned persistence contracts.

mod artifact;
mod configuration;
mod crawler;
mod run;

pub use artifact::ArtifactRepository;
pub use configuration::ConfigurationRepository;
pub use crawler::{CrawlerPointers, CrawlerRepository};
pub use run::CrawlRunRepository;
