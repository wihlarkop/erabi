//! Bounded repositories for Plan 02-owned persistence contracts.

mod crawler;
mod run;

pub use crawler::{CrawlerPointers, CrawlerRepository};
pub use run::CrawlRunRepository;
