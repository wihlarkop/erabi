//! Bounded repositories for Plan 02-owned persistence contracts.

mod artifact;
mod crawler;
mod run;

pub use artifact::ArtifactRepository;
pub use crawler::{CrawlerPointers, CrawlerRepository};
pub use run::CrawlRunRepository;
