use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use crate::{SqliteConnection as Connection, SqliteRow as Row, SqliteValue};
use erabi_domain::{
    CanonicalizationPolicy, Crawler, CrawlerId, CrawlerVersion, CrawlerVersionGuardrails,
    CrawlerVersionId, CrawlerVersionState, DiscoveryTransition, DiscoveryTransitionId,
    DomainScopePolicy, OperationalOverrides, PageType, PageTypeId, UrlMatcher,
    VersionValidationContext, VersionValidationRegistry, VersionValidationReport, canonical_sha256,
};
use rusqlite::TransactionBehavior;
use serde::Serialize;
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::{DatabaseFailure, DbError, ErabiDatabase};

macro_rules! finish_transaction {
    ($transaction:expr, $result:expr) => {{
        match $result {
            Ok(value) => $transaction
                .commit()
                .map_err(CrawlerRepositoryError::database)
                .map(|()| value),
            Err(error) => {
                let _ = $transaction.rollback();
                Err(error)
            }
        }
    }};
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CrawlerPointers {
    pub active_published_version_id: Option<String>,
    pub active_draft_version_id: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum CrawlerRepositoryError {
    #[error("Crawler was not found")]
    CrawlerNotFound,
    #[error("CrawlerVersion was not found")]
    CrawlerVersionNotFound,
    #[error("CrawlerVersion does not belong to the requested Crawler")]
    VersionNotOwnedByCrawler,
    #[error("Crawler already has an active Draft")]
    ActiveDraftExists,
    #[error("CrawlerVersion is not a Draft")]
    VersionNotDraft,
    #[error("CrawlerVersion is not the active Draft")]
    VersionNotActiveDraft,
    #[error("CrawlerVersion is not Published")]
    VersionNotPublished,
    #[error("Published CrawlerVersion is immutable")]
    PublishedVersionImmutable,
    #[error("PageType was not found")]
    PageTypeNotFound,
    #[error("PageType does not belong to the requested CrawlerVersion")]
    PageTypeNotOwnedByVersion,
    #[error("PageType is still referenced by durable crawler semantics")]
    PageTypeInUse,
    #[error("URLMatcher was not found")]
    UrlMatcherNotFound,
    #[error("URLMatcher does not belong to the requested PageType")]
    UrlMatcherNotOwnedByPageType,
    #[error("URLMatcher definition is invalid")]
    InvalidUrlMatcherDefinition,
    #[error("DiscoveryTransition was not found")]
    DiscoveryTransitionNotFound,
    #[error("DiscoveryTransition does not belong to the requested CrawlerVersion")]
    TransitionNotOwnedByVersion,
    #[error("DiscoveryTransition source PageType was not found")]
    TransitionSourcePageTypeNotFound,
    #[error("DiscoveryTransition target PageType was not found")]
    TransitionTargetPageTypeNotFound,
    #[error("DiscoveryTransition is invalid")]
    InvalidDiscoveryTransition,
    #[error("canonicalization policy is invalid")]
    InvalidCanonicalizationPolicy,
    #[error("Domain Scope policy is invalid")]
    InvalidDomainScope,
    #[error("crawler guardrails are invalid")]
    InvalidCrawlGuardrails,
    #[error("PageType budget is invalid")]
    InvalidPageTypeBudget,
    #[error("transition budget is invalid")]
    InvalidTransitionBudget,
    #[error("CrawlerVersion lifecycle transition is invalid")]
    InvalidLifecycleTransition,
    #[error("CrawlerVersion lifecycle transition conflicted with another request")]
    ConcurrentVersionTransition,
    #[error("durable Crawler state is invalid")]
    CorruptState,
    #[error("CrawlerVersion publication validation failed")]
    PublicationValidationFailed(VersionValidationReport),
    #[error("CrawlerVersion publication validation infrastructure failed")]
    PublicationValidationInfrastructure,
    #[error("database operation failed")]
    Database(#[source] DbError),
}

impl CrawlerRepositoryError {
    fn database(error: impl Into<DbError>) -> Self {
        Self::Database(error.into())
    }
}

impl From<DbError> for CrawlerRepositoryError {
    fn from(error: DbError) -> Self {
        Self::Database(error)
    }
}

impl From<rusqlite::Error> for CrawlerRepositoryError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(DbError::from(error))
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct CrawlerAuditMetadata {
    pub actor: Option<String>,
    pub occurred_at: Option<String>,
    pub config_hash: Option<String>,
    pub warning_summary: Vec<String>,
    pub base_version_id: Option<CrawlerVersionId>,
}

#[derive(Clone, Debug, Serialize)]
pub struct CrawlerVersionRecord {
    pub version: CrawlerVersion,
    pub audit: CrawlerAuditMetadata,
}

/// Immutable semantic input captured for one Test Lab evaluation.
#[derive(Clone, Debug)]
pub struct CrawlerSemanticSnapshot {
    pub version: CrawlerVersion,
    pub page_types: Vec<PageTypeRecord>,
    pub transitions: Vec<DiscoveryTransitionRecord>,
    pub config_hash: String,
}

/// Draft and optional Published semantics captured from one database snapshot.
#[derive(Clone, Debug)]
pub struct CrawlerEvaluationSnapshot {
    pub draft: CrawlerSemanticSnapshot,
    pub active_published_version_id: Option<CrawlerVersionId>,
    pub published: Option<CrawlerSemanticSnapshot>,
}

/// A durable Page Type projection with validated URL matchers.
#[derive(Clone, Debug)]
pub struct PageTypeRecord {
    pub id: PageTypeId,
    pub crawler_version_id: CrawlerVersionId,
    pub name: String,
    pub priority: i32,
    pub matchers: Vec<UrlMatcherRecord>,
}

impl PageTypeRecord {
    #[must_use]
    pub fn domain_page_type(&self) -> PageType {
        PageType {
            id: self.id,
            name: self.name.clone(),
            priority: self.priority,
            matchers: self
                .matchers
                .iter()
                .map(|matcher| matcher.matcher.clone())
                .collect(),
        }
    }
}

/// A durable URL matcher projection. `ordinal` is presentation order only.
#[derive(Clone, Debug)]
pub struct UrlMatcherRecord {
    pub id: String,
    pub page_type_id: PageTypeId,
    pub ordinal: i64,
    pub matcher: UrlMatcher,
}

/// A durable typed `DiscoveryTransition` projection.
#[derive(Clone, Debug)]
pub struct DiscoveryTransitionRecord {
    pub crawler_version_id: CrawlerVersionId,
    pub transition: DiscoveryTransition,
}

#[derive(Clone, Copy, Debug)]
pub struct CrawlerRepository<'database> {
    database: &'database ErabiDatabase,
}

#[allow(clippy::missing_errors_doc)]
impl<'database> CrawlerRepository<'database> {
    #[must_use]
    pub const fn new(database: &'database ErabiDatabase) -> Self {
        Self { database }
    }

    fn database(error: impl Into<DbError>) -> CrawlerRepositoryError {
        CrawlerRepositoryError::database(error)
    }

    pub async fn create(&self, crawler: &Crawler) -> Result<(), DbError> {
        let defaults = serialize(crawler.operational_defaults())?;
        let id = crawler.id().to_string();
        let name = crawler.name.clone();
        self.database
            .call(move |raw| {
                let connection = Connection::new(raw);
                connection.execute(
                "INSERT INTO crawlers (id, name, collection_id, operational_defaults_json, active_published_version_id, active_draft_version_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                (
                    id,
                    name,
                    SqliteValue::Null,
                    defaults,
                    SqliteValue::Null,
                    SqliteValue::Null,
                ),
                )
                .map(|_| ())
                .map_err(DbError::from)
            })
            .await
    }

    pub async fn list(&self) -> Result<Vec<Crawler>, CrawlerRepositoryError> {
        self.database.call(move |raw| {
        let connection = Connection::new(raw);
        let mut rows = connection
            .query(
                "SELECT id, name, collection_id, operational_defaults_json, active_published_version_id, active_draft_version_id FROM crawlers ORDER BY name COLLATE BINARY, id",
                (),
            )

            .map_err(Self::database)?;
        let mut crawlers = Vec::new();
        while let Some(row) = rows.next().map_err(Self::database)? {
            let crawler = crawler_from_row(row)?;
            ensure_pointer_consistency(&connection, crawler.id())?;
            crawlers.push(crawler);
        }
        Ok(crawlers)
        }).await
    }

    pub async fn get(&self, crawler_id: CrawlerId) -> Result<Crawler, CrawlerRepositoryError> {
        self.database.call(move |raw| {
        let connection = Connection::new(raw);
        let mut rows = connection
            .query(
                "SELECT id, name, collection_id, operational_defaults_json, active_published_version_id, active_draft_version_id FROM crawlers WHERE id = ?1",
                [crawler_id.to_string()],
            )

            .map_err(Self::database)?;
        let Some(row) = rows.next().map_err(Self::database)? else {
            return Err(CrawlerRepositoryError::CrawlerNotFound);
        };
        let crawler = crawler_from_row(row)?;
        ensure_pointer_consistency(&connection, crawler.id())?;
        Ok(crawler)
        }).await
    }

    pub async fn list_versions(
        &self,
        crawler_id: CrawlerId,
    ) -> Result<Vec<CrawlerVersionRecord>, CrawlerRepositoryError> {
        self.database.call(move |raw| {
        let connection = Connection::new(raw);
        ensure_crawler_exists(&connection, crawler_id)?;
        ensure_pointer_consistency(&connection, crawler_id)?;
        let mut rows = connection
            .query(
                "SELECT id, crawler_id, state, semantic_configuration_json FROM crawler_versions WHERE crawler_id = ?1 ORDER BY id",
                [crawler_id.to_string()],
            )

            .map_err(Self::database)?;
        let mut versions = Vec::new();
        while let Some(row) = rows.next().map_err(Self::database)? {
            let version = version_from_row(row)?;
            validate_seed_projection(&connection, &version)?;
            load_transition_records(&connection, &version, true)?;
            let audit = audit_metadata(&connection, version.id())?;
            versions.push(CrawlerVersionRecord { version, audit });
        }
        Ok(versions)
        }).await
    }

    pub async fn version(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
    ) -> Result<CrawlerVersionRecord, CrawlerRepositoryError> {
        self.database.call(move |raw| {
        let connection = Connection::new(raw);
        ensure_crawler_exists(&connection, crawler_id)?;
        ensure_pointer_consistency(&connection, crawler_id)?;
        let mut rows = connection
            .query(
                "SELECT id, crawler_id, state, semantic_configuration_json FROM crawler_versions WHERE id = ?1",
                [version_id.to_string()],
            )

            .map_err(Self::database)?;
        let Some(row) = rows.next().map_err(Self::database)? else {
            return Err(CrawlerRepositoryError::CrawlerVersionNotFound);
        };
        let version = version_from_row(row)?;
        if version.crawler_id() != crawler_id {
            return Err(CrawlerRepositoryError::VersionNotOwnedByCrawler);
        }
        validate_seed_projection(&connection, &version)?;
        load_transition_records(&connection, &version, true)?;
        let audit = audit_metadata(&connection, version.id())?;
        Ok(CrawlerVersionRecord { version, audit })
        }).await
    }

    pub async fn create_draft(
        &self,
        crawler_id: CrawlerId,
        actor: &str,
        occurred_at: &str,
    ) -> Result<CrawlerVersion, CrawlerRepositoryError> {
        let version = CrawlerVersion::draft(crawler_id);
        for attempt in 0..LIFECYCLE_CONTENTION_ATTEMPTS {
            let actor = actor.to_owned();
            let occurred_at = occurred_at.to_owned();
            let version_for_attempt = version.clone();
            let result = self
                .database
                .call(move |raw| {
                    let mut connection = Connection::new(raw);
                    match connection.transaction_with_behavior(TransactionBehavior::Immediate) {
                        Ok(transaction) => {
                            let result = insert_draft_in_transaction(
                                &transaction,
                                &version_for_attempt,
                                None,
                                &actor,
                                &occurred_at,
                            );
                            finish_transaction!(transaction, result)
                        }
                        Err(error) => Err(CrawlerRepositoryError::database(error)),
                    }
                })
                .await;
            match result {
                Ok(()) => return Ok(version),
                Err(CrawlerRepositoryError::Database(DbError::Database(failure)))
                    if matches!(
                        failure,
                        DatabaseFailure::Busy | DatabaseFailure::BusySnapshot
                    ) =>
                {
                    self.retry_lifecycle_contention(crawler_id, attempt, failure)
                        .await?;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("the bounded lifecycle contention loop always returns")
    }

    pub async fn create_draft_from_published(
        &self,
        crawler_id: CrawlerId,
        source_version_id: CrawlerVersionId,
        actor: &str,
        occurred_at: &str,
    ) -> Result<CrawlerVersion, CrawlerRepositoryError> {
        for attempt in 0..LIFECYCLE_CONTENTION_ATTEMPTS {
            let actor = actor.to_owned();
            let occurred_at = occurred_at.to_owned();
            let result = self
                .database
                .call(move |raw| {
                    let mut connection = Connection::new(raw);
                    match connection.transaction_with_behavior(TransactionBehavior::Immediate) {
                        Ok(transaction) => {
                            let result = clone_draft_in_transaction(
                                &transaction,
                                crawler_id,
                                source_version_id,
                                &actor,
                                &occurred_at,
                            );
                            finish_transaction!(transaction, result)
                        }
                        Err(error) => Err(CrawlerRepositoryError::database(error)),
                    }
                })
                .await;
            match result {
                Ok(version) => return Ok(version),
                Err(CrawlerRepositoryError::Database(DbError::Database(failure)))
                    if matches!(
                        failure,
                        DatabaseFailure::Busy | DatabaseFailure::BusySnapshot
                    ) =>
                {
                    self.retry_lifecycle_contention(crawler_id, attempt, failure)
                        .await?;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("the bounded lifecycle contention loop always returns")
    }

    pub async fn save_draft(
        &self,
        version: &CrawlerVersion,
        actor: &str,
        occurred_at: &str,
    ) -> Result<(), DbError> {
        match self.save_draft_typed(version, actor, occurred_at).await {
            Err(CrawlerRepositoryError::CrawlerNotFound) => {
                // Preserve the original compatibility seam's FK behavior for
                // callers that use `save_draft` directly. The typed authoring
                // APIs above reject this case before attempting a write.
                let configuration = serialize(version)?;
                let id = version.id().to_string();
                let crawler_id = version.crawler_id().to_string();
                self.database
                    .call(move |raw| {
                    let connection = Connection::new(raw);
                    connection.execute(
                        "INSERT INTO crawler_versions (id, crawler_id, state, semantic_configuration_json) VALUES (?1, ?2, 'DRAFT', ?3)",
                        (id, crawler_id, configuration),
                    )

                    .map(|_| ())
                    .map_err(DbError::from)
                    })
                    .await
            }
            result => result.map_err(repository_error_as_db),
        }
    }

    async fn save_draft_typed(
        &self,
        version: &CrawlerVersion,
        actor: &str,
        occurred_at: &str,
    ) -> Result<(), CrawlerRepositoryError> {
        if version.state() != CrawlerVersionState::Draft {
            return Err(CrawlerRepositoryError::PublishedVersionImmutable);
        }
        version
            .validate_semantic_contract()
            .map_err(|error| map_semantic_error(error.code))?;
        let configuration = serialize(version).map_err(CrawlerRepositoryError::database)?;
        let version = version.clone();
        let actor = actor.to_owned();
        let occurred_at = occurred_at.to_owned();
        self.database
            .call(move |raw| {
                let mut connection = Connection::new(raw);
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(Self::database)?;
                let result = save_draft_in_transaction(
                    &transaction,
                    &version,
                    configuration.as_str(),
                    &actor,
                    &occurred_at,
                );
                finish_transaction!(transaction, result)
            })
            .await
    }

    pub async fn publish(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        actor: &str,
        occurred_at: &str,
    ) -> Result<CrawlerVersionRecord, CrawlerRepositoryError> {
        let actor = actor.to_owned();
        let occurred_at = occurred_at.to_owned();
        let registry = self.database.version_validation_registry().clone();
        self.database
            .call(move |raw| {
                let mut connection = Connection::new(raw);
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(Self::database)?;
                let result = publish_in_transaction(
                    &transaction,
                    crawler_id,
                    version_id,
                    &actor,
                    &occurred_at,
                    &registry,
                );
                finish_transaction!(transaction, result)
            })
            .await
    }

    /// Runs the configured publication registry against one immutable Draft
    /// snapshot without mutating the database. The result is advisory; the
    /// publication transaction always runs validation again.
    pub async fn publish_validation(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
    ) -> Result<VersionValidationReport, CrawlerRepositoryError> {
        let registry = self.database.version_validation_registry().clone();
        self.database
            .call(move |raw| {
                let mut connection = Connection::new(raw);
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Deferred)
                    .map_err(Self::database)?;
                let result = publication_validation_in_transaction(
                    &transaction,
                    crawler_id,
                    version_id,
                    &registry,
                );
                match result {
                    Ok(report) => transaction
                        .commit()
                        .map_err(Self::database)
                        .map(|()| report),
                    Err(error) => {
                        let _ = transaction.rollback();
                        Err(error)
                    }
                }
            })
            .await
    }

    pub async fn publish_and_activate(
        &self,
        crawler: &Crawler,
        version: &CrawlerVersion,
        actor: &str,
        occurred_at: &str,
    ) -> Result<(), DbError> {
        if version.crawler_id() != crawler.id() || version.state() != CrawlerVersionState::Published
        {
            return Err(DbError::Invariant(
                "only a published version belonging to the Crawler can be activated".into(),
            ));
        }
        self.publish(crawler.id(), version.id(), actor, occurred_at)
            .await
            .map(|_| ())
            .map_err(repository_error_as_db)
    }

    pub async fn reactivate_published_typed(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        actor: &str,
        occurred_at: &str,
    ) -> Result<(), CrawlerRepositoryError> {
        let actor = actor.to_owned();
        let occurred_at = occurred_at.to_owned();
        self.database
            .call(move |raw| {
                let mut connection = Connection::new(raw);
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(Self::database)?;
                let result = reactivate_in_transaction(
                    &transaction,
                    crawler_id,
                    version_id,
                    &actor,
                    &occurred_at,
                );
                finish_transaction!(transaction, result)
            })
            .await
    }

    pub async fn reactivate_published(
        &self,
        crawler: &Crawler,
        version: &CrawlerVersion,
        actor: &str,
        occurred_at: &str,
    ) -> Result<(), DbError> {
        if version.state() != CrawlerVersionState::Published || version.crawler_id() != crawler.id()
        {
            return Err(DbError::Invariant(
                "only a published version belonging to the Crawler can be reactivated".into(),
            ));
        }
        self.reactivate_published_typed(crawler.id(), version.id(), actor, occurred_at)
            .await
            .map_err(repository_error_as_db)
    }

    pub async fn pointers(&self, crawler: &Crawler) -> Result<CrawlerPointers, DbError> {
        let crawler_id = crawler.id().to_string();
        self.database.call(move |raw| {
        let connection = Connection::new(raw);
        let row = connection
            .prepare(
                "SELECT active_published_version_id, active_draft_version_id FROM crawlers WHERE id = ?1",
            )
            ?
            .query_row([crawler_id])
            ?;
        Ok(CrawlerPointers {
            active_published_version_id: row.get(0)?,
            active_draft_version_id: row.get(1)?,
        })
        }).await
    }

    pub async fn audit_event_count(&self, entity_id: &str) -> Result<i64, DbError> {
        let entity_id = entity_id.to_owned();
        self.database
            .call(move |raw| {
                let connection = Connection::new(raw);
                let row = connection
                    .prepare("SELECT COUNT(*) FROM audit_events WHERE entity_id = ?1")?
                    .query_row([entity_id])?;
                Ok(row.get(0)?)
            })
            .await
    }

    pub async fn configuration_hash(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
    ) -> Result<String, CrawlerRepositoryError> {
        self.database
            .call(move |raw| {
                let connection = Connection::new(raw);
                ensure_crawler_exists(&connection, crawler_id)?;
                ensure_pointer_consistency(&connection, crawler_id)?;
                let version = load_version(&connection, version_id)?;
                if version.crawler_id() != crawler_id {
                    return Err(CrawlerRepositoryError::VersionNotOwnedByCrawler);
                }
                validate_seed_projection(&connection, &version)?;
                load_transition_records(&connection, &version, true)?;
                semantic_hash(&connection, &version)
            })
            .await
    }

    /// Captures all mutable semantic inputs needed by Test Lab from one
    /// database read transaction. The returned values are safe to freeze in
    /// an application-layer evaluation snapshot.
    pub async fn evaluation_snapshot(
        &self,
        crawler_id: CrawlerId,
        draft_version_id: CrawlerVersionId,
        include_published: bool,
    ) -> Result<CrawlerEvaluationSnapshot, CrawlerRepositoryError> {
        self.database
            .call(move |raw| {
                let mut connection = Connection::new(raw);
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Deferred)
                    .map_err(Self::database)?;
                let result = evaluation_snapshot_in_transaction(
                    &transaction,
                    crawler_id,
                    draft_version_id,
                    include_published,
                );
                match result {
                    Ok(snapshot) => transaction
                        .commit()
                        .map_err(Self::database)
                        .map(|()| snapshot),
                    Err(error) => {
                        let _ = transaction.rollback();
                        Err(error)
                    }
                }
            })
            .await
    }

    /// Captures one exact Published `CrawlerVersion` and all of its semantic
    /// children from a coherent read transaction. Production submission and
    /// execution address this immutable version directly; no active-pointer or
    /// latest-version lookup participates in the result.
    ///
    /// # Errors
    /// Returns `VersionNotPublished` for a Draft and preserves the existing
    /// typed ownership/corruption failures for all other invalid state.
    pub async fn published_semantic_snapshot(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
    ) -> Result<CrawlerSemanticSnapshot, CrawlerRepositoryError> {
        self.database
            .call(move |raw| {
                let mut connection = Connection::new(raw);
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Deferred)
                    .map_err(Self::database)?;
                let result = (|| {
                    ensure_crawler_exists(&transaction, crawler_id)?;
                    ensure_pointer_consistency(&transaction, crawler_id)?;
                    let snapshot = load_semantic_snapshot(&transaction, crawler_id, version_id)?;
                    if snapshot.version.state() != CrawlerVersionState::Published {
                        return Err(CrawlerRepositoryError::VersionNotPublished);
                    }
                    Ok(snapshot)
                })();
                finish_transaction!(transaction, result)
            })
            .await
    }

    /// Reads the selected version's typed canonicalization policy.
    pub async fn canonicalization_policy(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
    ) -> Result<CanonicalizationPolicy, CrawlerRepositoryError> {
        Ok(self
            .version(crawler_id, version_id)
            .await?
            .version
            .canonicalization_policy()
            .clone())
    }

    /// Updates canonicalization semantics in the active Draft transactionally.
    pub async fn update_canonicalization_policy(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        policy: &CanonicalizationPolicy,
        actor: &str,
        occurred_at: &str,
    ) -> Result<CanonicalizationPolicy, CrawlerRepositoryError> {
        policy
            .validate()
            .map_err(|_| CrawlerRepositoryError::InvalidCanonicalizationPolicy)?;
        let policy = policy.clone();
        let updated = self
            .update_semantic_version(
                crawler_id,
                version_id,
                actor,
                occurred_at,
                "CANONICALIZATION_POLICY_UPDATED",
                "canonicalization",
                |version| {
                    version
                        .set_canonicalization_policy(policy)
                        .map_err(|error| map_semantic_error(error.code))
                },
            )
            .await?;
        Ok(updated.canonicalization_policy().clone())
    }

    /// Reads the selected version's typed Domain Scope policy.
    pub async fn domain_scope_policy(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
    ) -> Result<DomainScopePolicy, CrawlerRepositoryError> {
        Ok(self
            .version(crawler_id, version_id)
            .await?
            .version
            .domain_scope()
            .clone())
    }

    /// Updates Domain Scope semantics in the active Draft transactionally.
    pub async fn update_domain_scope_policy(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        policy: &DomainScopePolicy,
        actor: &str,
        occurred_at: &str,
    ) -> Result<DomainScopePolicy, CrawlerRepositoryError> {
        policy
            .validate()
            .map_err(|_| CrawlerRepositoryError::InvalidDomainScope)?;
        let policy = policy.clone();
        let updated = self
            .update_semantic_version(
                crawler_id,
                version_id,
                actor,
                occurred_at,
                "DOMAIN_SCOPE_UPDATED",
                "domain_scope",
                |version| {
                    version
                        .set_domain_scope(policy)
                        .map_err(|error| map_semantic_error(error.code))
                },
            )
            .await?;
        Ok(updated.domain_scope().clone())
    }

    /// Reads the selected version's mandatory semantic guardrail baseline.
    pub async fn crawler_version_guardrails(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
    ) -> Result<CrawlerVersionGuardrails, CrawlerRepositoryError> {
        Ok(self
            .version(crawler_id, version_id)
            .await?
            .version
            .guardrails()
            .clone())
    }

    /// Updates crawler and `PageType` guardrails in the active Draft transactionally.
    pub async fn update_crawler_version_guardrails(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        guardrails: &CrawlerVersionGuardrails,
        actor: &str,
        occurred_at: &str,
    ) -> Result<CrawlerVersionGuardrails, CrawlerRepositoryError> {
        guardrails
            .validate()
            .map_err(|error| map_semantic_error(error.code))?;
        let guardrails = guardrails.clone();
        let updated = self
            .update_semantic_version(
                crawler_id,
                version_id,
                actor,
                occurred_at,
                "CRAWLER_GUARDRAILS_UPDATED",
                "guardrails",
                |version| {
                    version
                        .set_guardrails(guardrails)
                        .map_err(|error| map_semantic_error(error.code))
                },
            )
            .await?;
        Ok(updated.guardrails().clone())
    }

    #[allow(clippy::too_many_arguments)]
    async fn update_semantic_version<F>(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        actor: &str,
        occurred_at: &str,
        event_type: &str,
        entity_id: &str,
        update: F,
    ) -> Result<CrawlerVersion, CrawlerRepositoryError>
    where
        F: FnOnce(&mut CrawlerVersion) -> Result<(), CrawlerRepositoryError> + Send + 'static,
    {
        let actor = actor.to_owned();
        let occurred_at = occurred_at.to_owned();
        let event_type = event_type.to_owned();
        let entity_id = entity_id.to_owned();
        self.database
            .call(move |raw| {
                let mut connection = Connection::new(raw);
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(Self::database)?;
                let result = (|| {
                    let mut version = load_mutation_version(&transaction, crawler_id, version_id)?;
                    update(&mut version)?;
                    version
                        .validate_semantic_contract()
                        .map_err(|error| map_semantic_error(error.code))?;
                    let configuration =
                        serialize(&version).map_err(CrawlerRepositoryError::database)?;
                    update_version_configuration(&transaction, &version, &configuration)?;
                    let hash = semantic_hash(&transaction, &version)?;
                    insert_semantic_mutation_audit(
                        &transaction,
                        event_type.as_str(),
                        actor.as_str(),
                        occurred_at.as_str(),
                        version_id,
                        entity_id.as_str(),
                        hash.as_str(),
                    )?;
                    Ok::<CrawlerVersion, CrawlerRepositoryError>(version)
                })();
                finish_transaction!(transaction, result)
            })
            .await
    }

    /// Lists typed transitions in deterministic presentation order.
    pub async fn list_discovery_transitions(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
    ) -> Result<Vec<DiscoveryTransitionRecord>, CrawlerRepositoryError> {
        let version = self.version(crawler_id, version_id).await?;
        self.database
            .call(move |raw| {
                let connection = Connection::new(raw);
                load_transition_records(&connection, &version.version, true)
            })
            .await
    }

    /// Reads one typed transition while validating version ownership.
    pub async fn discovery_transition(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        transition_id: DiscoveryTransitionId,
    ) -> Result<DiscoveryTransitionRecord, CrawlerRepositoryError> {
        let transitions = self
            .list_discovery_transitions(crawler_id, version_id)
            .await?;
        if let Some(record) = transitions
            .into_iter()
            .find(|record| record.transition.id == transition_id)
        {
            return Ok(record);
        }
        self.database
            .call(move |raw| {
                let connection = Connection::new(raw);
                ensure_transition_belongs_to_version(&connection, version_id, transition_id)
            })
            .await?;
        Err(CrawlerRepositoryError::CorruptState)
    }

    /// Creates a directed transition and declares it in the same Draft transaction.
    pub async fn create_discovery_transition(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        transition: &DiscoveryTransition,
        actor: &str,
        occurred_at: &str,
    ) -> Result<DiscoveryTransitionRecord, CrawlerRepositoryError> {
        transition
            .validate()
            .map_err(|_| CrawlerRepositoryError::InvalidDiscoveryTransition)?;
        let transition_id = transition.id;
        let transition = transition.clone();
        let actor = actor.to_owned();
        let occurred_at = occurred_at.to_owned();
        self.database
            .call(move |raw| {
                let mut connection = Connection::new(raw);
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(Self::database)?;
                let result = (|| {
                    let version = load_mutation_version(&transaction, crawler_id, version_id)?;
                    ensure_transition_page_type(
                        &transaction,
                        version_id,
                        transition.source_page_type_id,
                        true,
                    )?;
                    ensure_transition_page_type(
                        &transaction,
                        version_id,
                        transition.target_page_type_id,
                        false,
                    )?;
                    if transition_row_exists(&transaction, transition.id)? {
                        return Err(CrawlerRepositoryError::CorruptState);
                    }
                    let mut updated = version.clone();
                    let mut ids = updated.transition_ids().to_vec();
                    if ids.contains(&transition.id) {
                        return Err(CrawlerRepositoryError::CorruptState);
                    }
                    ids.push(transition.id);
                    updated
                        .set_transition_ids(ids)
                        .map_err(|error| map_semantic_error(error.code))?;
                    let version_configuration =
                        serialize(&updated).map_err(CrawlerRepositoryError::database)?;
                    let transition_configuration =
                        serialize(&transition).map_err(CrawlerRepositoryError::database)?;
                    transaction
                        .execute(
                            "INSERT INTO discovery_transitions (id, crawler_version_id, configuration_json) VALUES (?1, ?2, ?3)",
                            (
                                transition.id.to_string(),
                                version_id.to_string(),
                                transition_configuration,
                            ),
                        )
                        .map_err(CrawlerRepositoryError::database)?;
                    update_version_configuration(&transaction, &updated, &version_configuration)?;
                    let hash = semantic_hash(&transaction, &updated)?;
                    insert_semantic_mutation_audit(
                        &transaction,
                        "DISCOVERY_TRANSITION_CREATED",
                        actor.as_str(),
                        occurred_at.as_str(),
                        version_id,
                        transition.id.to_string().as_str(),
                        hash.as_str(),
                    )?;
                    Ok::<(), CrawlerRepositoryError>(())
                })();
                finish_transaction!(transaction, result)
            })
            .await?;
        self.discovery_transition(crawler_id, version_id, transition_id)
            .await
    }

    /// Replaces one typed transition without changing its identity.
    pub async fn update_discovery_transition(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        transition_id: DiscoveryTransitionId,
        transition: &DiscoveryTransition,
        actor: &str,
        occurred_at: &str,
    ) -> Result<DiscoveryTransitionRecord, CrawlerRepositoryError> {
        if transition.id != transition_id {
            return Err(CrawlerRepositoryError::InvalidDiscoveryTransition);
        }
        transition
            .validate()
            .map_err(|_| CrawlerRepositoryError::InvalidDiscoveryTransition)?;
        let transition_configuration =
            serialize(transition).map_err(CrawlerRepositoryError::database)?;
        let source_page_type_id = transition.source_page_type_id;
        let target_page_type_id = transition.target_page_type_id;
        let actor = actor.to_owned();
        let occurred_at = occurred_at.to_owned();
        self.database
            .call(move |raw| {
                let mut connection = Connection::new(raw);
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(Self::database)?;
                let result = (|| {
                    let version = load_mutation_version(&transaction, crawler_id, version_id)?;
                    ensure_transition_belongs_to_version(&transaction, version_id, transition_id)?;
                    ensure_transition_page_type(
                        &transaction,
                        version_id,
                        source_page_type_id,
                        true,
                    )?;
                    ensure_transition_page_type(
                        &transaction,
                        version_id,
                        target_page_type_id,
                        false,
                    )?;
                    let updated = transaction
                        .execute(
                            "UPDATE discovery_transitions SET configuration_json = ?1 WHERE id = ?2 AND crawler_version_id = ?3",
                            (
                                transition_configuration.as_str(),
                                transition_id.to_string(),
                                version_id.to_string(),
                            ),
                        )
                        .map_err(CrawlerRepositoryError::database)?;
                    if updated != 1 {
                        return Err(CrawlerRepositoryError::CorruptState);
                    }
                    let hash = semantic_hash(&transaction, &version)?;
                    insert_semantic_mutation_audit(
                        &transaction,
                        "DISCOVERY_TRANSITION_UPDATED",
                        actor.as_str(),
                        occurred_at.as_str(),
                        version_id,
                        transition_id.to_string().as_str(),
                        hash.as_str(),
                    )
                })();
                finish_transaction!(transaction, result)
            })
            .await?;
        self.discovery_transition(crawler_id, version_id, transition_id)
            .await
    }

    /// Deletes a transition row and its declared ID atomically.
    pub async fn delete_discovery_transition(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        transition_id: DiscoveryTransitionId,
        actor: &str,
        occurred_at: &str,
    ) -> Result<(), CrawlerRepositoryError> {
        let actor = actor.to_owned();
        let occurred_at = occurred_at.to_owned();
        self.database
            .call(move |raw| {
                let mut connection = Connection::new(raw);
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(Self::database)?;
                let result = (|| {
                    let version = load_mutation_version(&transaction, crawler_id, version_id)?;
                    ensure_transition_belongs_to_version(&transaction, version_id, transition_id)?;
                    let mut updated = version.clone();
                    updated
                        .set_transition_ids(
                            updated
                                .transition_ids()
                                .iter()
                                .copied()
                                .filter(|id| *id != transition_id)
                                .collect(),
                        )
                        .map_err(|error| map_semantic_error(error.code))?;
                    let version_configuration =
                        serialize(&updated).map_err(CrawlerRepositoryError::database)?;
                    let deleted = transaction
                        .execute(
                            "DELETE FROM discovery_transitions WHERE id = ?1 AND crawler_version_id = ?2",
                            (transition_id.to_string(), version_id.to_string()),
                        )
                        .map_err(CrawlerRepositoryError::database)?;
                    if deleted != 1 {
                        return Err(CrawlerRepositoryError::CorruptState);
                    }
                    update_version_configuration(&transaction, &updated, &version_configuration)?;
                    let hash = semantic_hash(&transaction, &updated)?;
                    insert_semantic_mutation_audit(
                        &transaction,
                        "DISCOVERY_TRANSITION_DELETED",
                        actor.as_str(),
                        occurred_at.as_str(),
                        version_id,
                        transition_id.to_string().as_str(),
                        hash.as_str(),
                    )
                })();
                finish_transaction!(transaction, result)
            })
            .await
    }

    /// Lists Page Types in deterministic presentation order for a selected
    /// version. The order is never used by the matching service as a winner
    /// tie-breaker.
    pub async fn list_page_types(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
    ) -> Result<Vec<PageTypeRecord>, CrawlerRepositoryError> {
        let version = self.version(crawler_id, version_id).await?;
        self.database
            .call(move |raw| {
                let connection = Connection::new(raw);
                load_page_type_records(&connection, &version.version)
            })
            .await
    }

    /// Reads one Page Type while validating its version ownership.
    pub async fn page_type(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        page_type_id: PageTypeId,
    ) -> Result<PageTypeRecord, CrawlerRepositoryError> {
        let version = self.version(crawler_id, version_id).await?;
        self.database
            .call(move |raw| {
                let connection = Connection::new(raw);
                ensure_page_type_belongs_to_version(&connection, version_id, page_type_id)?;
                load_page_type_records(&connection, &version.version)?
                    .into_iter()
                    .find(|page_type| page_type.id == page_type_id)
                    .ok_or(CrawlerRepositoryError::CorruptState)
            })
            .await
    }

    /// Creates a Page Type and updates the version's declared Page Type IDs
    /// in the same Draft transaction.
    pub async fn create_page_type(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        name: &str,
        priority: i32,
        actor: &str,
        occurred_at: &str,
    ) -> Result<PageTypeRecord, CrawlerRepositoryError> {
        let page_type_id = PageTypeId::new();
        let name = name.to_owned();
        let actor = actor.to_owned();
        let occurred_at = occurred_at.to_owned();
        self.database
            .call(move |raw| {
                let mut connection = Connection::new(raw);
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(Self::database)?;
                let result = (|| {
                    let version = load_mutation_version(&transaction, crawler_id, version_id)?;
                    let mut updated = version.clone();
                    let mut ids = updated.page_type_ids().to_vec();
                    ids.push(page_type_id);
                    updated
                        .set_page_type_ids(ids)
                        .map_err(|error| map_semantic_error(error.code))?;
                    let configuration =
                        serialize(&updated).map_err(CrawlerRepositoryError::database)?;
                    transaction
                        .execute(
                            "INSERT INTO page_types (id, crawler_version_id, name, priority, configuration_json) VALUES (?1, ?2, ?3, ?4, ?5)",
                            (
                                page_type_id.to_string(),
                                version_id.to_string(),
                                name.as_str(),
                                i64::from(priority),
                                "{}",
                            ),
                        )
                        .map_err(CrawlerRepositoryError::database)?;
                    update_version_configuration(&transaction, &updated, &configuration)?;
                    let hash = semantic_hash(&transaction, &updated)?;
                    insert_semantic_mutation_audit(
                        &transaction,
                        "PAGE_TYPE_CREATED",
                        actor.as_str(),
                        occurred_at.as_str(),
                        version_id,
                        page_type_id.to_string().as_str(),
                        hash.as_str(),
                    )?;
                    Ok::<(), CrawlerRepositoryError>(())
                })();
                finish_transaction!(transaction, result)
            })
            .await?;
        self.page_type(crawler_id, version_id, page_type_id).await
    }

    /// Updates only Task 2-owned Page Type fields and preserves opaque
    /// configuration JSON.
    #[allow(clippy::too_many_arguments)]
    pub async fn update_page_type(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        page_type_id: PageTypeId,
        name: &str,
        priority: i32,
        actor: &str,
        occurred_at: &str,
    ) -> Result<PageTypeRecord, CrawlerRepositoryError> {
        let name = name.to_owned();
        let actor = actor.to_owned();
        let occurred_at = occurred_at.to_owned();
        self.database
            .call(move |raw| {
                let mut connection = Connection::new(raw);
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(Self::database)?;
                let result = (|| {
                    let version = load_mutation_version(&transaction, crawler_id, version_id)?;
                    ensure_page_type_belongs_to_version(&transaction, version_id, page_type_id)?;
                    let updated = transaction
                        .execute(
                            "UPDATE page_types SET name = ?1, priority = ?2 WHERE id = ?3 AND crawler_version_id = ?4",
                            (name.as_str(), i64::from(priority), page_type_id.to_string(), version_id.to_string()),
                        )
                        .map_err(CrawlerRepositoryError::database)?;
                    if updated != 1 {
                        return Err(CrawlerRepositoryError::CorruptState);
                    }
                    let hash = semantic_hash(&transaction, &version)?;
                    insert_semantic_mutation_audit(
                        &transaction,
                        "PAGE_TYPE_UPDATED",
                        actor.as_str(),
                        occurred_at.as_str(),
                        version_id,
                        page_type_id.to_string().as_str(),
                        hash.as_str(),
                    )
                })();
                finish_transaction!(transaction, result)
            })
            .await?;
        self.page_type(crawler_id, version_id, page_type_id).await
    }

    /// Deletes a Page Type, its owned matchers, and its declared version ID
    /// atomically. Known seed/opaque transition references block deletion.
    pub async fn delete_page_type(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        page_type_id: PageTypeId,
        actor: &str,
        occurred_at: &str,
    ) -> Result<(), CrawlerRepositoryError> {
        let actor = actor.to_owned();
        let occurred_at = occurred_at.to_owned();
        self.database
            .call(move |raw| {
                let mut connection = Connection::new(raw);
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(Self::database)?;
                let result = (|| {
                    let version = load_mutation_version(&transaction, crawler_id, version_id)?;
                    ensure_page_type_belongs_to_version(&transaction, version_id, page_type_id)?;
                    if page_type_is_in_use(&transaction, &version, page_type_id)? {
                        return Err(CrawlerRepositoryError::PageTypeInUse);
                    }
                    let mut updated = version.clone();
                    updated
                        .set_page_type_ids(
                            updated
                                .page_type_ids()
                                .iter()
                                .copied()
                                .filter(|id| *id != page_type_id)
                                .collect(),
                        )
                        .map_err(|error| {
                            if error.code == erabi_domain::ErrorCode::InvalidPageTypeBudget {
                                CrawlerRepositoryError::PageTypeInUse
                            } else {
                                map_semantic_error(error.code)
                            }
                        })?;
                    let configuration =
                        serialize(&updated).map_err(CrawlerRepositoryError::database)?;
                    transaction
                        .execute(
                            "DELETE FROM url_matchers WHERE page_type_id = ?1",
                            [page_type_id.to_string()],
                        )
                        .map_err(CrawlerRepositoryError::database)?;
                    let deleted = transaction
                        .execute(
                            "DELETE FROM page_types WHERE id = ?1 AND crawler_version_id = ?2",
                            (page_type_id.to_string(), version_id.to_string()),
                        )
                        .map_err(CrawlerRepositoryError::database)?;
                    if deleted != 1 {
                        return Err(CrawlerRepositoryError::CorruptState);
                    }
                    update_version_configuration(&transaction, &updated, &configuration)?;
                    let hash = semantic_hash(&transaction, &updated)?;
                    insert_semantic_mutation_audit(
                        &transaction,
                        "PAGE_TYPE_DELETED",
                        actor.as_str(),
                        occurred_at.as_str(),
                        version_id,
                        page_type_id.to_string().as_str(),
                        hash.as_str(),
                    )
                })();
                finish_transaction!(transaction, result)
            })
            .await
    }

    /// Lists URL matchers in deterministic authoring order.
    pub async fn list_url_matchers(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        page_type_id: PageTypeId,
    ) -> Result<Vec<UrlMatcherRecord>, CrawlerRepositoryError> {
        Ok(self
            .page_type(crawler_id, version_id, page_type_id)
            .await?
            .matchers)
    }

    /// Reads one URL matcher while validating the full ownership chain.
    pub async fn url_matcher(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        page_type_id: PageTypeId,
        matcher_id: &str,
    ) -> Result<UrlMatcherRecord, CrawlerRepositoryError> {
        let page_type = self.page_type(crawler_id, version_id, page_type_id).await?;
        if let Some(matcher) = page_type
            .matchers
            .into_iter()
            .find(|matcher| matcher.id == matcher_id)
        {
            return Ok(matcher);
        }
        let matcher_id = matcher_id.to_owned();
        let owner = self
            .database
            .call(move |raw| {
                let connection = Connection::new(raw);
                let mut rows = connection
                    .query(
                        "SELECT page_type_id FROM url_matchers WHERE id = ?1",
                        [matcher_id.as_str()],
                    )
                    .map_err(Self::database)?;
                let Some(row) = rows.next().map_err(Self::database)? else {
                    return Err(CrawlerRepositoryError::UrlMatcherNotFound);
                };
                let owner: String = row.get(0).map_err(Self::database)?;
                Ok(owner)
            })
            .await?;
        if owner != page_type_id.to_string() {
            return Err(CrawlerRepositoryError::UrlMatcherNotOwnedByPageType);
        }
        Err(CrawlerRepositoryError::CorruptState)
    }

    /// Creates a URL matcher and allocates its presentation ordinal inside the
    /// same immediate transaction.
    pub async fn create_url_matcher(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        page_type_id: PageTypeId,
        matcher: &UrlMatcher,
        actor: &str,
        occurred_at: &str,
    ) -> Result<UrlMatcherRecord, CrawlerRepositoryError> {
        let matcher_id = new_opaque_id();
        matcher
            .validate_definition()
            .map_err(|_| CrawlerRepositoryError::InvalidUrlMatcherDefinition)?;
        let matcher_json = serde_json::to_string(matcher).map_err(|error| {
            CrawlerRepositoryError::database(DbError::Serialization(error.to_string()))
        })?;
        let result_matcher_id = matcher_id.clone();
        let actor = actor.to_owned();
        let occurred_at = occurred_at.to_owned();
        self.database
            .call(move |raw| {
                let mut connection = Connection::new(raw);
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(Self::database)?;
                let result = (|| {
                    let version = load_mutation_version(&transaction, crawler_id, version_id)?;
                    ensure_page_type_belongs_to_version(&transaction, version_id, page_type_id)?;
                    let row = transaction
                        .prepare(
                            "SELECT COALESCE(MAX(ordinal), -1) + 1 FROM url_matchers WHERE page_type_id = ?1",
                        )
                        .map_err(CrawlerRepositoryError::database)?
                        .query_row([page_type_id.to_string()])
                        .map_err(CrawlerRepositoryError::database)?;
                    let ordinal: i64 = row.get(0).map_err(CrawlerRepositoryError::database)?;
                    transaction
                        .execute(
                            "INSERT INTO url_matchers (id, page_type_id, ordinal, matcher_json) VALUES (?1, ?2, ?3, ?4)",
                            (
                                matcher_id.as_str(),
                                page_type_id.to_string(),
                                ordinal,
                                matcher_json.as_str(),
                            ),
                        )
                        .map_err(CrawlerRepositoryError::database)?;
                    let hash = semantic_hash(&transaction, &version)?;
                    insert_semantic_mutation_audit(
                        &transaction,
                        "URL_MATCHER_CREATED",
                        actor.as_str(),
                        occurred_at.as_str(),
                        version_id,
                        matcher_id.as_str(),
                        hash.as_str(),
                    )?;
                    Ok::<i64, CrawlerRepositoryError>(ordinal)
                })();
                finish_transaction!(transaction, result)
            })
            .await?;
        self.url_matcher(
            crawler_id,
            version_id,
            page_type_id,
            result_matcher_id.as_str(),
        )
        .await
    }

    /// Replaces a URL matcher definition without changing its presentation
    /// ordinal.
    #[allow(clippy::too_many_arguments)]
    pub async fn update_url_matcher(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        page_type_id: PageTypeId,
        matcher_id: &str,
        matcher: &UrlMatcher,
        actor: &str,
        occurred_at: &str,
    ) -> Result<UrlMatcherRecord, CrawlerRepositoryError> {
        matcher
            .validate_definition()
            .map_err(|_| CrawlerRepositoryError::InvalidUrlMatcherDefinition)?;
        let matcher_json = serde_json::to_string(matcher).map_err(|error| {
            CrawlerRepositoryError::database(DbError::Serialization(error.to_string()))
        })?;
        let matcher_id = matcher_id.to_owned();
        let result_matcher_id = matcher_id.clone();
        let actor = actor.to_owned();
        let occurred_at = occurred_at.to_owned();
        self.database
            .call(move |raw| {
                let mut connection = Connection::new(raw);
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(Self::database)?;
                let result = (|| {
                    let version = load_mutation_version(&transaction, crawler_id, version_id)?;
                    ensure_page_type_belongs_to_version(&transaction, version_id, page_type_id)?;
                    let existing = transaction
                        .execute(
                            "UPDATE url_matchers SET matcher_json = ?1 WHERE id = ?2 AND page_type_id = ?3",
                            (matcher_json.as_str(), matcher_id.as_str(), page_type_id.to_string()),
                        )
                        .map_err(CrawlerRepositoryError::database)?;
                    if existing != 1 {
                        ensure_matcher_belongs_to_page_type(
                            &transaction,
                            matcher_id.as_str(),
                            page_type_id,
                        )?;
                        return Err(CrawlerRepositoryError::CorruptState);
                    }
                    let hash = semantic_hash(&transaction, &version)?;
                    insert_semantic_mutation_audit(
                        &transaction,
                        "URL_MATCHER_UPDATED",
                        actor.as_str(),
                        occurred_at.as_str(),
                        version_id,
                        matcher_id.as_str(),
                        hash.as_str(),
                    )
                })();
                finish_transaction!(transaction, result)
            })
            .await?;
        self.url_matcher(
            crawler_id,
            version_id,
            page_type_id,
            result_matcher_id.as_str(),
        )
        .await
    }

    /// Deletes a URL matcher from an active Draft.
    pub async fn delete_url_matcher(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        page_type_id: PageTypeId,
        matcher_id: &str,
        actor: &str,
        occurred_at: &str,
    ) -> Result<(), CrawlerRepositoryError> {
        let matcher_id = matcher_id.to_owned();
        let actor = actor.to_owned();
        let occurred_at = occurred_at.to_owned();
        self.database
            .call(move |raw| {
                let mut connection = Connection::new(raw);
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(Self::database)?;
                let result = (|| {
                    let version = load_mutation_version(&transaction, crawler_id, version_id)?;
                    ensure_page_type_belongs_to_version(&transaction, version_id, page_type_id)?;
                    let deleted = transaction
                        .execute(
                            "DELETE FROM url_matchers WHERE id = ?1 AND page_type_id = ?2",
                            (matcher_id.as_str(), page_type_id.to_string()),
                        )
                        .map_err(CrawlerRepositoryError::database)?;
                    if deleted != 1 {
                        ensure_matcher_belongs_to_page_type(
                            &transaction,
                            matcher_id.as_str(),
                            page_type_id,
                        )?;
                        return Err(CrawlerRepositoryError::UrlMatcherNotFound);
                    }
                    let hash = semantic_hash(&transaction, &version)?;
                    insert_semantic_mutation_audit(
                        &transaction,
                        "URL_MATCHER_DELETED",
                        actor.as_str(),
                        occurred_at.as_str(),
                        version_id,
                        matcher_id.as_str(),
                        hash.as_str(),
                    )
                })();
                finish_transaction!(transaction, result)
            })
            .await
    }

    async fn retry_lifecycle_contention(
        &self,
        crawler_id: CrawlerId,
        attempt: usize,
        original_failure: DatabaseFailure,
    ) -> Result<(), CrawlerRepositoryError> {
        tokio::time::sleep(LIFECYCLE_CONTENTION_BACKOFFS[attempt]).await;
        let result = self
            .database
            .call(move |raw| {
                let connection = Connection::new(raw);
                active_draft_for(&connection, crawler_id)
            })
            .await;
        match result {
            Ok(Some(_)) => Err(CrawlerRepositoryError::ActiveDraftExists),
            Ok(None)
            | Err(CrawlerRepositoryError::Database(DbError::Database(
                DatabaseFailure::Busy | DatabaseFailure::BusySnapshot,
            ))) if attempt + 1 < LIFECYCLE_CONTENTION_ATTEMPTS => Ok(()),
            Ok(None) | Err(CrawlerRepositoryError::Database(DbError::Database(_))) => Err(
                CrawlerRepositoryError::database(DbError::Database(original_failure)),
            ),
            Err(error) => Err(error),
        }
    }
}

const LIFECYCLE_CONTENTION_ATTEMPTS: usize = 5;
const LIFECYCLE_CONTENTION_BACKOFFS: [Duration; LIFECYCLE_CONTENTION_ATTEMPTS] = [
    Duration::from_millis(1),
    Duration::from_millis(2),
    Duration::from_millis(4),
    Duration::from_millis(8),
    Duration::from_millis(16),
];

fn insert_draft_in_transaction(
    connection: &impl crate::SqliteExecutor,
    version: &CrawlerVersion,
    base_version_id: Option<CrawlerVersionId>,
    actor: &str,
    occurred_at: &str,
) -> Result<(), CrawlerRepositoryError> {
    version
        .validate_semantic_contract()
        .map_err(|error| map_semantic_error(error.code))?;
    let crawler_id = version.crawler_id().to_string();
    let mut pointers = connection
        .query(
            "SELECT active_draft_version_id FROM crawlers WHERE id = ?1",
            [crawler_id.as_str()],
        )
        .map_err(CrawlerRepositoryError::database)?;
    let Some(pointer_row) = pointers.next().map_err(CrawlerRepositoryError::database)? else {
        return Err(CrawlerRepositoryError::CrawlerNotFound);
    };
    let active_draft: Option<String> = pointer_row
        .get(0)
        .map_err(CrawlerRepositoryError::database)?;
    if active_draft.is_some() {
        return Err(CrawlerRepositoryError::ActiveDraftExists);
    }

    let configuration = serialize(version).map_err(CrawlerRepositoryError::database)?;
    connection
        .execute(
            "INSERT INTO crawler_versions (id, crawler_id, state, semantic_configuration_json) VALUES (?1, ?2, 'DRAFT', ?3)",
            (version.id().to_string(), crawler_id.as_str(), configuration),
        )

        .map_err(CrawlerRepositoryError::database)?;
    sync_seed_rows(connection, version)?;
    let activated = connection
        .execute(
            "UPDATE crawlers SET active_draft_version_id = ?1 WHERE id = ?2 AND active_draft_version_id IS NULL",
            (version.id().to_string(), crawler_id.as_str()),
        )

        .map_err(CrawlerRepositoryError::database)?;
    if activated != 1 {
        return Err(CrawlerRepositoryError::ConcurrentVersionTransition);
    }
    insert_audit_event(
        connection,
        format!("draft:{}:{}", version.id(), occurred_at),
        "CRAWLER_DRAFT_CREATED",
        actor,
        occurred_at,
        &version.id().to_string(),
        audit_payload(version.id(), base_version_id, None, &[]),
    )
}

fn save_draft_in_transaction(
    connection: &impl crate::SqliteExecutor,
    version: &CrawlerVersion,
    configuration: &str,
    actor: &str,
    occurred_at: &str,
) -> Result<(), CrawlerRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT crawler_id, state FROM crawler_versions WHERE id = ?1",
            [version.id().to_string()],
        )
        .map_err(CrawlerRepositoryError::database)?;
    let existing = rows.next().map_err(CrawlerRepositoryError::database)?;
    if let Some(row) = existing {
        let owner: String = row.get(0).map_err(CrawlerRepositoryError::database)?;
        let state: String = row.get(1).map_err(CrawlerRepositoryError::database)?;
        if owner != version.crawler_id().to_string() {
            return Err(CrawlerRepositoryError::VersionNotOwnedByCrawler);
        }
        if state == "PUBLISHED" {
            return Err(CrawlerRepositoryError::PublishedVersionImmutable);
        }
        if state != "DRAFT" {
            return Err(CrawlerRepositoryError::InvalidLifecycleTransition);
        }
        let pointer = active_draft_for(connection, version.crawler_id())?;
        if pointer.as_deref() != Some(version.id().to_string().as_str()) {
            return Err(CrawlerRepositoryError::InvalidLifecycleTransition);
        }
        connection
            .execute(
                "UPDATE crawler_versions SET semantic_configuration_json = ?1 WHERE id = ?2 AND state = 'DRAFT'",
                (configuration, version.id().to_string()),
            )

            .map_err(CrawlerRepositoryError::database)?;
        sync_seed_rows(connection, version)?;
        return Ok(());
    }

    insert_draft_in_transaction(connection, version, None, actor, occurred_at)
}

#[allow(clippy::too_many_lines)]
fn clone_draft_in_transaction(
    connection: &impl crate::SqliteExecutor,
    crawler_id: CrawlerId,
    source_version_id: CrawlerVersionId,
    actor: &str,
    occurred_at: &str,
) -> Result<CrawlerVersion, CrawlerRepositoryError> {
    let mut crawler_rows = connection
        .query(
            "SELECT active_draft_version_id FROM crawlers WHERE id = ?1",
            [crawler_id.to_string()],
        )
        .map_err(CrawlerRepositoryError::database)?;
    let Some(crawler_row) = crawler_rows
        .next()
        .map_err(CrawlerRepositoryError::database)?
    else {
        return Err(CrawlerRepositoryError::CrawlerNotFound);
    };
    let active_draft: Option<String> = crawler_row
        .get(0)
        .map_err(CrawlerRepositoryError::database)?;
    if active_draft.is_some() {
        return Err(CrawlerRepositoryError::ActiveDraftExists);
    }

    let source = load_version(connection, source_version_id)?;
    if source.crawler_id() != crawler_id {
        return Err(CrawlerRepositoryError::VersionNotOwnedByCrawler);
    }
    if source.state() != CrawlerVersionState::Published {
        return Err(CrawlerRepositoryError::VersionNotPublished);
    }
    let clone = source
        .draft_from_published()
        .map_err(|_| CrawlerRepositoryError::InvalidLifecycleTransition)?;

    let mut page_map = declared_child_id_map(
        source
            .page_type_ids()
            .iter()
            .zip(clone.page_type_ids())
            .map(|(source, target)| (source.to_string(), target.to_string())),
    );
    let mut transition_map = declared_child_id_map(
        source
            .transition_ids()
            .iter()
            .zip(clone.transition_ids())
            .map(|(source, target)| (source.to_string(), target.to_string())),
    );
    augment_child_id_map(
        connection,
        source_version_id,
        &mut page_map,
        "SELECT id FROM page_types WHERE crawler_version_id = ?1",
    )?;
    augment_child_id_map(
        connection,
        source_version_id,
        &mut transition_map,
        "SELECT id FROM discovery_transitions WHERE crawler_version_id = ?1",
    )?;
    let seed_map = source
        .seeds()
        .iter()
        .zip(clone.seeds())
        .map(|(source, target)| (source.id.to_string(), target.id.to_string()))
        .collect::<BTreeMap<_, _>>();

    let configuration = serialize(&clone).map_err(CrawlerRepositoryError::database)?;
    connection
        .execute(
            "INSERT INTO crawler_versions (id, crawler_id, state, semantic_configuration_json) VALUES (?1, ?2, 'DRAFT', ?3)",
            (clone.id().to_string(), crawler_id.to_string(), configuration),
        )

        .map_err(CrawlerRepositoryError::database)?;
    clone_child_rows(
        connection,
        source_version_id,
        clone.id(),
        &seed_map,
        &page_map,
        &transition_map,
    )?;
    let activated = connection
        .execute(
            "UPDATE crawlers SET active_draft_version_id = ?1 WHERE id = ?2 AND active_draft_version_id IS NULL",
            (clone.id().to_string(), crawler_id.to_string()),
        )

        .map_err(CrawlerRepositoryError::database)?;
    if activated != 1 {
        return Err(CrawlerRepositoryError::ConcurrentVersionTransition);
    }
    let hash = semantic_hash(connection, &clone)?;
    insert_audit_event(
        connection,
        format!("draft:{}:{}", clone.id(), occurred_at),
        "CRAWLER_DRAFT_CREATED",
        actor,
        occurred_at,
        &clone.id().to_string(),
        audit_payload(
            clone.id(),
            Some(source_version_id),
            Some(hash.as_str()),
            &[],
        ),
    )?;
    Ok(clone)
}

fn publish_in_transaction(
    connection: &impl crate::SqliteExecutor,
    crawler_id: CrawlerId,
    version_id: CrawlerVersionId,
    actor: &str,
    occurred_at: &str,
    registry: &VersionValidationRegistry,
) -> Result<CrawlerVersionRecord, CrawlerRepositoryError> {
    let context = publication_validation_context(connection, crawler_id, version_id)?;
    let report = registry
        .validate(&context)
        .map_err(|_| CrawlerRepositoryError::PublicationValidationInfrastructure)?;
    if !report.is_publishable() {
        return Err(CrawlerRepositoryError::PublicationValidationFailed(report));
    }
    let draft = context.version().clone();
    let hash = report.config_hash.clone();
    let warning_summary = report.warning_summary();
    let mut published = draft.clone();
    published
        .publish()
        .map_err(|_| CrawlerRepositoryError::PublishedVersionImmutable)?;
    let configuration = serialize(&published).map_err(CrawlerRepositoryError::database)?;
    let updated = connection
        .execute(
            "UPDATE crawler_versions SET state = 'PUBLISHED', semantic_configuration_json = ?1 WHERE id = ?2 AND crawler_id = ?3 AND state = 'DRAFT'",
            (configuration, version_id.to_string(), crawler_id.to_string()),
        )

        .map_err(CrawlerRepositoryError::database)?;
    if updated != 1 {
        return Err(CrawlerRepositoryError::ConcurrentVersionTransition);
    }
    let pointers = connection
        .execute(
            "UPDATE crawlers SET active_published_version_id = ?1, active_draft_version_id = NULL WHERE id = ?2 AND active_draft_version_id = ?1",
            (version_id.to_string(), crawler_id.to_string()),
        )

        .map_err(CrawlerRepositoryError::database)?;
    if pointers != 1 {
        return Err(CrawlerRepositoryError::ConcurrentVersionTransition);
    }
    let base_version_id = base_version_from_audit(connection, version_id)?;
    insert_audit_event(
        connection,
        format!("publish:{version_id}:{occurred_at}"),
        "CRAWLER_VERSION_PUBLISHED",
        actor,
        occurred_at,
        &version_id.to_string(),
        audit_payload(
            version_id,
            base_version_id,
            Some(hash.as_str()),
            &warning_summary,
        ),
    )?;
    Ok(CrawlerVersionRecord {
        version: published,
        audit: CrawlerAuditMetadata {
            actor: Some(actor.to_owned()),
            occurred_at: Some(occurred_at.to_owned()),
            config_hash: Some(hash),
            warning_summary,
            base_version_id,
        },
    })
}

fn publication_validation_in_transaction(
    connection: &impl crate::SqliteExecutor,
    crawler_id: CrawlerId,
    version_id: CrawlerVersionId,
    registry: &VersionValidationRegistry,
) -> Result<VersionValidationReport, CrawlerRepositoryError> {
    let context = publication_validation_context(connection, crawler_id, version_id)?;
    registry
        .validate(&context)
        .map_err(|_| CrawlerRepositoryError::PublicationValidationInfrastructure)
}

fn publication_validation_context(
    connection: &impl crate::SqliteExecutor,
    crawler_id: CrawlerId,
    version_id: CrawlerVersionId,
) -> Result<VersionValidationContext, CrawlerRepositoryError> {
    ensure_crawler_exists(connection, crawler_id)?;
    ensure_pointer_consistency(connection, crawler_id)?;
    let version = load_publication_version(connection, version_id)?;
    if version.crawler_id() != crawler_id {
        return Err(CrawlerRepositoryError::VersionNotOwnedByCrawler);
    }
    if version.state() != CrawlerVersionState::Draft {
        return Err(CrawlerRepositoryError::VersionNotDraft);
    }
    let active_draft = active_draft_for(connection, crawler_id)?;
    if active_draft.as_deref() != Some(version_id.to_string().as_str()) {
        return Err(if active_draft.is_some() {
            CrawlerRepositoryError::InvalidLifecycleTransition
        } else {
            CrawlerRepositoryError::VersionNotDraft
        });
    }
    let page_types = load_page_type_records(connection, &version)?;
    let transitions = load_transition_records(connection, &version, false)?;
    let config_hash = semantic_hash_with_semantic_validation(connection, &version, false)?;
    let evidence = super::test_evidence::load_for_version_in_transaction(
        connection,
        version_id,
        config_hash.as_str(),
    )
    .map_err(|error| match error {
        super::test_evidence::TestEvidenceRepositoryError::Database(error) => {
            CrawlerRepositoryError::Database(error)
        }
        _ => CrawlerRepositoryError::CorruptState,
    })?;
    Ok(VersionValidationContext::new(
        version,
        page_types
            .into_iter()
            .map(|page_type| page_type.domain_page_type())
            .collect(),
        transitions
            .into_iter()
            .map(|transition| transition.transition)
            .collect(),
        evidence,
        config_hash,
    ))
}

fn reactivate_in_transaction(
    connection: &impl crate::SqliteExecutor,
    crawler_id: CrawlerId,
    version_id: CrawlerVersionId,
    actor: &str,
    occurred_at: &str,
) -> Result<(), CrawlerRepositoryError> {
    let version = load_version(connection, version_id)?;
    if version.crawler_id() != crawler_id {
        return Err(CrawlerRepositoryError::VersionNotOwnedByCrawler);
    }
    if version.state() != CrawlerVersionState::Published {
        return Err(CrawlerRepositoryError::VersionNotPublished);
    }
    let changed = connection
        .execute(
            "UPDATE crawlers SET active_published_version_id = ?1 WHERE id = ?2 AND EXISTS (SELECT 1 FROM crawler_versions WHERE id = ?1 AND crawler_id = ?2 AND state = 'PUBLISHED')",
            (version_id.to_string(), crawler_id.to_string()),
        )

        .map_err(CrawlerRepositoryError::database)?;
    if changed != 1 {
        return Err(CrawlerRepositoryError::ConcurrentVersionTransition);
    }
    insert_audit_event(
        connection,
        format!("reactivate:{version_id}:{occurred_at}"),
        "CRAWLER_VERSION_REACTIVATED",
        actor,
        occurred_at,
        &version_id.to_string(),
        audit_payload(version_id, None, None, &[]),
    )
}

fn ensure_crawler_exists(
    connection: &impl crate::SqliteExecutor,
    crawler_id: CrawlerId,
) -> Result<(), CrawlerRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT 1 FROM crawlers WHERE id = ?1",
            [crawler_id.to_string()],
        )
        .map_err(CrawlerRepositoryError::database)?;
    if rows
        .next()
        .map_err(CrawlerRepositoryError::database)?
        .is_none()
    {
        return Err(CrawlerRepositoryError::CrawlerNotFound);
    }
    Ok(())
}

fn ensure_pointer_consistency(
    connection: &impl crate::SqliteExecutor,
    crawler_id: CrawlerId,
) -> Result<(), CrawlerRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT 1 FROM crawlers AS crawler WHERE crawler.id = ?1 AND ((crawler.active_draft_version_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM crawler_versions AS version WHERE version.id = crawler.active_draft_version_id AND version.crawler_id = crawler.id AND version.state = 'DRAFT')) OR (crawler.active_published_version_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM crawler_versions AS version WHERE version.id = crawler.active_published_version_id AND version.crawler_id = crawler.id AND version.state = 'PUBLISHED'))) LIMIT 1",
            [crawler_id.to_string()],
        )

        .map_err(CrawlerRepositoryError::database)?;
    if rows
        .next()
        .map_err(CrawlerRepositoryError::database)?
        .is_some()
    {
        return Err(CrawlerRepositoryError::CorruptState);
    }
    Ok(())
}

fn active_draft_for(
    connection: &impl crate::SqliteExecutor,
    crawler_id: CrawlerId,
) -> Result<Option<String>, CrawlerRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT active_draft_version_id FROM crawlers WHERE id = ?1",
            [crawler_id.to_string()],
        )
        .map_err(CrawlerRepositoryError::database)?;
    let Some(row) = rows.next().map_err(CrawlerRepositoryError::database)? else {
        return Err(CrawlerRepositoryError::CrawlerNotFound);
    };
    row.get(0).map_err(CrawlerRepositoryError::database)
}

fn evaluation_snapshot_in_transaction(
    connection: &impl crate::SqliteExecutor,
    crawler_id: CrawlerId,
    draft_version_id: CrawlerVersionId,
    include_published: bool,
) -> Result<CrawlerEvaluationSnapshot, CrawlerRepositoryError> {
    ensure_crawler_exists(connection, crawler_id)?;
    ensure_pointer_consistency(connection, crawler_id)?;

    let pointers = connection
        .prepare(
            "SELECT active_published_version_id, active_draft_version_id FROM crawlers WHERE id = ?1",
        )

        .map_err(CrawlerRepositoryError::database)?
        .query_row([crawler_id.to_string()])

        .map_err(CrawlerRepositoryError::database)?;
    let active_published_version_id = pointers
        .get::<Option<String>>(0)
        .map_err(CrawlerRepositoryError::database)?
        .map(|value| parse_version_id(&value))
        .transpose()?;
    let draft = load_semantic_snapshot(connection, crawler_id, draft_version_id)?;
    if draft.version.state() != CrawlerVersionState::Draft {
        return Err(CrawlerRepositoryError::VersionNotDraft);
    }
    let active_draft_version = pointers
        .get::<Option<String>>(1)
        .map_err(CrawlerRepositoryError::database)?;
    if active_draft_version.as_deref() != Some(draft_version_id.to_string().as_str()) {
        return Err(CrawlerRepositoryError::VersionNotActiveDraft);
    }

    let published = if include_published {
        if let Some(version_id) = active_published_version_id {
            Some(load_semantic_snapshot(connection, crawler_id, version_id)?)
        } else {
            None
        }
    } else {
        None
    };
    Ok(CrawlerEvaluationSnapshot {
        draft,
        active_published_version_id,
        published,
    })
}

fn load_semantic_snapshot(
    connection: &impl crate::SqliteExecutor,
    crawler_id: CrawlerId,
    version_id: CrawlerVersionId,
) -> Result<CrawlerSemanticSnapshot, CrawlerRepositoryError> {
    let version = load_version(connection, version_id)?;
    if version.crawler_id() != crawler_id {
        return Err(CrawlerRepositoryError::VersionNotOwnedByCrawler);
    }
    validate_seed_projection(connection, &version)?;
    let page_types = load_page_type_records(connection, &version)?;
    let transitions = load_transition_records(connection, &version, true)?;
    let config_hash = semantic_hash_with_semantic_validation(connection, &version, true)?;
    Ok(CrawlerSemanticSnapshot {
        version,
        page_types,
        transitions,
        config_hash,
    })
}

fn load_mutation_version(
    connection: &impl crate::SqliteExecutor,
    crawler_id: CrawlerId,
    version_id: CrawlerVersionId,
) -> Result<CrawlerVersion, CrawlerRepositoryError> {
    ensure_crawler_exists(connection, crawler_id)?;
    let version = load_version(connection, version_id)?;
    if version.crawler_id() != crawler_id {
        return Err(CrawlerRepositoryError::VersionNotOwnedByCrawler);
    }
    if version.state() != CrawlerVersionState::Draft {
        return Err(CrawlerRepositoryError::PublishedVersionImmutable);
    }
    if active_draft_for(connection, crawler_id)?.as_deref() != Some(version_id.to_string().as_str())
    {
        return Err(CrawlerRepositoryError::VersionNotActiveDraft);
    }
    // A mutation must not compound an existing mismatch between the declared
    // version projection and typed child rows.
    load_page_type_records(connection, &version)?;
    load_transition_records(connection, &version, true)?;
    Ok(version)
}

#[derive(Debug, Eq, PartialEq)]
struct SeedProjection {
    original_url: String,
    canonical_url: String,
    enabled: bool,
    label: Option<String>,
    entry_page_type_hint: Option<String>,
}

fn validate_seed_projection(
    connection: &impl crate::SqliteExecutor,
    version: &CrawlerVersion,
) -> Result<(), CrawlerRepositoryError> {
    let expected = version
        .seeds()
        .iter()
        .map(|seed| {
            (
                seed.id.to_string(),
                SeedProjection {
                    original_url: seed.original_url.as_str().to_owned(),
                    canonical_url: seed.canonical_url.as_str().to_owned(),
                    enabled: seed.enabled,
                    label: seed.label.clone(),
                    entry_page_type_hint: seed.entry_page_type_hint.map(|id| id.to_string()),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    if expected.len() != version.seeds().len()
        || version.seeds().iter().any(|seed| {
            seed.entry_page_type_hint
                .is_some_and(|hint| !version.page_type_ids().contains(&hint))
        })
    {
        return Err(CrawlerRepositoryError::CorruptState);
    }

    let mut rows = connection
        .query(
            "SELECT id, original_url, canonical_url, enabled, label, entry_page_type_hint_id FROM seeds WHERE crawler_version_id = ?1",
            [version.id().to_string()],
        )

        .map_err(CrawlerRepositoryError::database)?;
    let mut actual = BTreeMap::new();
    while let Some(row) = rows.next().map_err(CrawlerRepositoryError::database)? {
        let id: String = row.get(0).map_err(CrawlerRepositoryError::database)?;
        let original_url: String = row.get(1).map_err(CrawlerRepositoryError::database)?;
        let canonical_url: String = row.get(2).map_err(CrawlerRepositoryError::database)?;
        let enabled = match row
            .get::<i64>(3)
            .map_err(CrawlerRepositoryError::database)?
        {
            0 => false,
            1 => true,
            _ => return Err(CrawlerRepositoryError::CorruptState),
        };
        let label = row
            .get::<Option<String>>(4)
            .map_err(CrawlerRepositoryError::database)?;
        let hint = row
            .get::<Option<String>>(5)
            .map_err(CrawlerRepositoryError::database)?;
        if Uuid::parse_str(&id).map_or(true, |value| value.get_version_num() != 7)
            || hint
                .as_deref()
                .is_some_and(|value| parse_page_type_id(value).is_err())
            || actual
                .insert(
                    id,
                    SeedProjection {
                        original_url,
                        canonical_url,
                        enabled,
                        label,
                        entry_page_type_hint: hint,
                    },
                )
                .is_some()
        {
            return Err(CrawlerRepositoryError::CorruptState);
        }
    }
    if actual != expected {
        return Err(CrawlerRepositoryError::CorruptState);
    }
    Ok(())
}

fn load_page_type_records(
    connection: &impl crate::SqliteExecutor,
    version: &CrawlerVersion,
) -> Result<Vec<PageTypeRecord>, CrawlerRepositoryError> {
    let declared = version
        .page_type_ids()
        .iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    if declared.len() != version.page_type_ids().len() {
        return Err(CrawlerRepositoryError::CorruptState);
    }

    let mut rows = connection
        .query(
            "SELECT id, crawler_version_id, name, priority, configuration_json FROM page_types WHERE crawler_version_id = ?1 ORDER BY name COLLATE BINARY, id",
            [version.id().to_string()],
        )

        .map_err(CrawlerRepositoryError::database)?;
    let mut records = Vec::new();
    while let Some(row) = rows.next().map_err(CrawlerRepositoryError::database)? {
        let id: PageTypeId = parse_page_type_id(
            &row.get::<String>(0)
                .map_err(CrawlerRepositoryError::database)?,
        )?;
        let crawler_version_id = parse_version_id(
            &row.get::<String>(1)
                .map_err(CrawlerRepositoryError::database)?,
        )?;
        if crawler_version_id != version.id() || !declared.contains(&id.to_string()) {
            return Err(CrawlerRepositoryError::CorruptState);
        }
        let configuration: String = row.get(4).map_err(CrawlerRepositoryError::database)?;
        serde_json::from_str::<Value>(&configuration)
            .map_err(|_| CrawlerRepositoryError::CorruptState)?;
        let priority = i32::try_from(
            row.get::<i64>(3)
                .map_err(CrawlerRepositoryError::database)?,
        )
        .map_err(|_| CrawlerRepositoryError::CorruptState)?;
        records.push(PageTypeRecord {
            id,
            crawler_version_id,
            name: row.get(2).map_err(CrawlerRepositoryError::database)?,
            priority,
            matchers: load_matchers_for_page_type(connection, id)?,
        });
    }
    if records
        .iter()
        .map(|record| record.id.to_string())
        .collect::<BTreeSet<_>>()
        != declared
    {
        return Err(CrawlerRepositoryError::CorruptState);
    }
    Ok(records)
}

fn load_transition_records(
    connection: &impl crate::SqliteExecutor,
    version: &CrawlerVersion,
    validate_semantics: bool,
) -> Result<Vec<DiscoveryTransitionRecord>, CrawlerRepositoryError> {
    let declared = version
        .transition_ids()
        .iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    if declared.len() != version.transition_ids().len() {
        return Err(CrawlerRepositoryError::CorruptState);
    }

    let mut rows = connection
        .query(
            "SELECT id, crawler_version_id, configuration_json FROM discovery_transitions WHERE crawler_version_id = ?1 ORDER BY id",
            [version.id().to_string()],
        )

        .map_err(CrawlerRepositoryError::database)?;
    let mut records = Vec::new();
    while let Some(row) = rows.next().map_err(CrawlerRepositoryError::database)? {
        let id = parse_transition_id(
            &row.get::<String>(0)
                .map_err(CrawlerRepositoryError::database)?,
        )?;
        let owner = parse_version_id(
            &row.get::<String>(1)
                .map_err(CrawlerRepositoryError::database)?,
        )?;
        if owner != version.id() || !declared.contains(&id.to_string()) {
            return Err(CrawlerRepositoryError::CorruptState);
        }
        let configuration: String = row.get(2).map_err(CrawlerRepositoryError::database)?;
        let transition: DiscoveryTransition = serde_json::from_str(&configuration)
            .map_err(|_| CrawlerRepositoryError::CorruptState)?;
        if transition.id != id {
            return Err(CrawlerRepositoryError::CorruptState);
        }
        if validate_semantics {
            if !version
                .page_type_ids()
                .contains(&transition.source_page_type_id)
                || !version
                    .page_type_ids()
                    .contains(&transition.target_page_type_id)
            {
                return Err(CrawlerRepositoryError::CorruptState);
            }
            ensure_page_type_belongs_to_version(
                connection,
                version.id(),
                transition.source_page_type_id,
            )
            .map_err(|_| CrawlerRepositoryError::CorruptState)?;
            ensure_page_type_belongs_to_version(
                connection,
                version.id(),
                transition.target_page_type_id,
            )
            .map_err(|_| CrawlerRepositoryError::CorruptState)?;
            transition
                .validate()
                .map_err(|_| CrawlerRepositoryError::CorruptState)?;
        } else {
            for page_type_id in [
                transition.source_page_type_id,
                transition.target_page_type_id,
            ] {
                if !version.page_type_ids().contains(&page_type_id) {
                    ensure_missing_transition_page_type_is_absent(connection, page_type_id)?;
                }
            }
        }
        records.push(DiscoveryTransitionRecord {
            crawler_version_id: owner,
            transition,
        });
    }
    if records
        .iter()
        .map(|record| record.transition.id.to_string())
        .collect::<BTreeSet<_>>()
        != declared
    {
        return Err(CrawlerRepositoryError::CorruptState);
    }
    Ok(records)
}

fn ensure_missing_transition_page_type_is_absent(
    connection: &impl crate::SqliteExecutor,
    page_type_id: PageTypeId,
) -> Result<(), CrawlerRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT crawler_version_id FROM page_types WHERE id = ?1",
            [page_type_id.to_string()],
        )
        .map_err(CrawlerRepositoryError::database)?;
    if rows
        .next()
        .map_err(CrawlerRepositoryError::database)?
        .is_some()
    {
        return Err(CrawlerRepositoryError::CorruptState);
    }
    Ok(())
}

fn load_matchers_for_page_type(
    connection: &impl crate::SqliteExecutor,
    page_type_id: PageTypeId,
) -> Result<Vec<UrlMatcherRecord>, CrawlerRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT id, page_type_id, ordinal, matcher_json FROM url_matchers WHERE page_type_id = ?1 ORDER BY ordinal, id",
            [page_type_id.to_string()],
        )

        .map_err(CrawlerRepositoryError::database)?;
    let mut matchers = Vec::new();
    while let Some(row) = rows.next().map_err(CrawlerRepositoryError::database)? {
        let id: String = row.get(0).map_err(CrawlerRepositoryError::database)?;
        if Uuid::parse_str(&id).map_or(true, |value| value.get_version_num() != 7) {
            return Err(CrawlerRepositoryError::CorruptState);
        }
        let stored_page_type = parse_page_type_id(
            &row.get::<String>(1)
                .map_err(CrawlerRepositoryError::database)?,
        )?;
        if stored_page_type != page_type_id {
            return Err(CrawlerRepositoryError::CorruptState);
        }
        let matcher_json: String = row.get(3).map_err(CrawlerRepositoryError::database)?;
        let matcher = serde_json::from_str::<UrlMatcher>(&matcher_json)
            .map_err(|_| CrawlerRepositoryError::CorruptState)?;
        matchers.push(UrlMatcherRecord {
            id,
            page_type_id: stored_page_type,
            ordinal: row.get(2).map_err(CrawlerRepositoryError::database)?,
            matcher,
        });
    }
    Ok(matchers)
}

fn ensure_page_type_belongs_to_version(
    connection: &impl crate::SqliteExecutor,
    version_id: CrawlerVersionId,
    page_type_id: PageTypeId,
) -> Result<(), CrawlerRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT crawler_version_id FROM page_types WHERE id = ?1",
            [page_type_id.to_string()],
        )
        .map_err(CrawlerRepositoryError::database)?;
    let Some(row) = rows.next().map_err(CrawlerRepositoryError::database)? else {
        return Err(CrawlerRepositoryError::PageTypeNotFound);
    };
    let owner = parse_version_id(
        &row.get::<String>(0)
            .map_err(CrawlerRepositoryError::database)?,
    )?;
    if owner != version_id {
        return Err(CrawlerRepositoryError::PageTypeNotOwnedByVersion);
    }
    Ok(())
}

fn ensure_transition_belongs_to_version(
    connection: &impl crate::SqliteExecutor,
    version_id: CrawlerVersionId,
    transition_id: DiscoveryTransitionId,
) -> Result<(), CrawlerRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT crawler_version_id FROM discovery_transitions WHERE id = ?1",
            [transition_id.to_string()],
        )
        .map_err(CrawlerRepositoryError::database)?;
    let Some(row) = rows.next().map_err(CrawlerRepositoryError::database)? else {
        return Err(CrawlerRepositoryError::DiscoveryTransitionNotFound);
    };
    let owner = parse_version_id(
        &row.get::<String>(0)
            .map_err(CrawlerRepositoryError::database)?,
    )?;
    if owner != version_id {
        return Err(CrawlerRepositoryError::TransitionNotOwnedByVersion);
    }
    Ok(())
}

fn ensure_transition_page_type(
    connection: &impl crate::SqliteExecutor,
    version_id: CrawlerVersionId,
    page_type_id: PageTypeId,
    source: bool,
) -> Result<(), CrawlerRepositoryError> {
    ensure_page_type_belongs_to_version(connection, version_id, page_type_id).map_err(|error| {
        match error {
            CrawlerRepositoryError::PageTypeNotFound => {
                if source {
                    CrawlerRepositoryError::TransitionSourcePageTypeNotFound
                } else {
                    CrawlerRepositoryError::TransitionTargetPageTypeNotFound
                }
            }
            CrawlerRepositoryError::PageTypeNotOwnedByVersion => {
                CrawlerRepositoryError::TransitionNotOwnedByVersion
            }
            other => other,
        }
    })
}

fn transition_row_exists(
    connection: &impl crate::SqliteExecutor,
    transition_id: DiscoveryTransitionId,
) -> Result<bool, CrawlerRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT 1 FROM discovery_transitions WHERE id = ?1",
            [transition_id.to_string()],
        )
        .map_err(CrawlerRepositoryError::database)?;
    Ok(rows
        .next()
        .map_err(CrawlerRepositoryError::database)?
        .is_some())
}

fn ensure_matcher_belongs_to_page_type(
    connection: &impl crate::SqliteExecutor,
    matcher_id: &str,
    page_type_id: PageTypeId,
) -> Result<(), CrawlerRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT page_type_id FROM url_matchers WHERE id = ?1",
            [matcher_id],
        )
        .map_err(CrawlerRepositoryError::database)?;
    let Some(row) = rows.next().map_err(CrawlerRepositoryError::database)? else {
        return Err(CrawlerRepositoryError::UrlMatcherNotFound);
    };
    let owner = parse_page_type_id(
        &row.get::<String>(0)
            .map_err(CrawlerRepositoryError::database)?,
    )?;
    if owner != page_type_id {
        return Err(CrawlerRepositoryError::UrlMatcherNotOwnedByPageType);
    }
    Ok(())
}

fn page_type_is_in_use(
    connection: &impl crate::SqliteExecutor,
    version: &CrawlerVersion,
    page_type_id: PageTypeId,
) -> Result<bool, CrawlerRepositoryError> {
    if version
        .guardrails()
        .page_types
        .iter()
        .any(|guardrail| guardrail.page_type_id == page_type_id)
    {
        return Ok(true);
    }
    let page_type_id = page_type_id.to_string();
    let mut seeds = connection
        .query(
            "SELECT 1 FROM seeds WHERE crawler_version_id = ?1 AND entry_page_type_hint_id = ?2 LIMIT 1",
            (version.id().to_string(), page_type_id.as_str()),
        )

        .map_err(CrawlerRepositoryError::database)?;
    if seeds
        .next()
        .map_err(CrawlerRepositoryError::database)?
        .is_some()
    {
        return Ok(true);
    }

    let mut opaque_rows = connection
        .query(
            "SELECT configuration_json FROM discovery_transitions WHERE crawler_version_id = ?1 UNION ALL SELECT configuration_json FROM page_types WHERE crawler_version_id = ?1 AND id <> ?2",
            (version.id().to_string(), page_type_id.as_str()),
        )

        .map_err(CrawlerRepositoryError::database)?;
    while let Some(row) = opaque_rows
        .next()
        .map_err(CrawlerRepositoryError::database)?
    {
        let configuration: String = row.get(0).map_err(CrawlerRepositoryError::database)?;
        let value = serde_json::from_str::<Value>(&configuration)
            .map_err(|_| CrawlerRepositoryError::CorruptState)?;
        if json_contains_string(&value, &page_type_id) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn json_contains_string(value: &Value, needle: &str) -> bool {
    match value {
        Value::Array(values) => values
            .iter()
            .any(|value| json_contains_string(value, needle)),
        Value::Object(values) => values
            .values()
            .any(|value| json_contains_string(value, needle)),
        Value::String(value) => value == needle,
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

fn update_version_configuration(
    connection: &impl crate::SqliteExecutor,
    version: &CrawlerVersion,
    configuration: &str,
) -> Result<(), CrawlerRepositoryError> {
    let updated = connection
        .execute(
            "UPDATE crawler_versions SET semantic_configuration_json = ?1 WHERE id = ?2 AND state = 'DRAFT'",
            (configuration, version.id().to_string()),
        )

        .map_err(CrawlerRepositoryError::database)?;
    if updated != 1 {
        return Err(CrawlerRepositoryError::ConcurrentVersionTransition);
    }
    Ok(())
}

fn insert_semantic_mutation_audit(
    connection: &impl crate::SqliteExecutor,
    event_type: &str,
    actor: &str,
    occurred_at: &str,
    version_id: CrawlerVersionId,
    entity_id: &str,
    config_hash: &str,
) -> Result<(), CrawlerRepositoryError> {
    insert_audit_event(
        connection,
        new_opaque_id(),
        event_type,
        actor,
        occurred_at,
        &version_id.to_string(),
        serde_json::json!({
            "version_id": version_id.to_string(),
            "entity_id": entity_id,
            "config_hash": config_hash,
        })
        .to_string(),
    )
}

fn load_version(
    connection: &impl crate::SqliteExecutor,
    version_id: CrawlerVersionId,
) -> Result<CrawlerVersion, CrawlerRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT id, crawler_id, state, semantic_configuration_json FROM crawler_versions WHERE id = ?1",
            [version_id.to_string()],
        )

        .map_err(CrawlerRepositoryError::database)?;
    let Some(row) = rows.next().map_err(CrawlerRepositoryError::database)? else {
        return Err(CrawlerRepositoryError::CrawlerVersionNotFound);
    };
    let version = version_from_row(row)?;
    validate_seed_projection(connection, &version)?;
    Ok(version)
}

fn load_publication_version(
    connection: &impl crate::SqliteExecutor,
    version_id: CrawlerVersionId,
) -> Result<CrawlerVersion, CrawlerRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT id, crawler_id, state, semantic_configuration_json FROM crawler_versions WHERE id = ?1",
            [version_id.to_string()],
        )

        .map_err(CrawlerRepositoryError::database)?;
    let Some(row) = rows.next().map_err(CrawlerRepositoryError::database)? else {
        return Err(CrawlerRepositoryError::CrawlerVersionNotFound);
    };
    let version = version_from_row_with_semantic_validation(row, false)?;
    validate_seed_projection(connection, &version)?;
    Ok(version)
}

fn crawler_from_row(row: &Row) -> Result<Crawler, CrawlerRepositoryError> {
    let id: String = row.get(0).map_err(CrawlerRepositoryError::database)?;
    let id = parse_crawler_id(&id)?;
    let name: String = row.get(1).map_err(CrawlerRepositoryError::database)?;
    let collection_id: Option<String> = row.get(2).map_err(CrawlerRepositoryError::database)?;
    let defaults_json: String = row.get(3).map_err(CrawlerRepositoryError::database)?;
    let defaults: OperationalOverrides =
        serde_json::from_str(&defaults_json).map_err(|_| CrawlerRepositoryError::CorruptState)?;
    let active_published: Option<String> = row.get(4).map_err(CrawlerRepositoryError::database)?;
    let active_draft: Option<String> = row.get(5).map_err(CrawlerRepositoryError::database)?;
    Ok(Crawler::from_persisted(
        id,
        name,
        collection_id
            .as_deref()
            .map(parse_collection_id)
            .transpose()?,
        defaults,
        active_published
            .as_deref()
            .map(parse_version_id)
            .transpose()?,
        active_draft.as_deref().map(parse_version_id).transpose()?,
    ))
}

fn version_from_row(row: &Row) -> Result<CrawlerVersion, CrawlerRepositoryError> {
    version_from_row_with_semantic_validation(row, true)
}

fn version_from_row_with_semantic_validation(
    row: &Row,
    validate_semantics: bool,
) -> Result<CrawlerVersion, CrawlerRepositoryError> {
    let row_id: String = row.get(0).map_err(CrawlerRepositoryError::database)?;
    let crawler_id: String = row.get(1).map_err(CrawlerRepositoryError::database)?;
    let state: String = row.get(2).map_err(CrawlerRepositoryError::database)?;
    let configuration: String = row.get(3).map_err(CrawlerRepositoryError::database)?;
    let version: CrawlerVersion =
        serde_json::from_str(&configuration).map_err(|_| CrawlerRepositoryError::CorruptState)?;
    let id = parse_version_id(&row_id)?;
    let owner = parse_crawler_id(&crawler_id)?;
    let expected_state = match state.as_str() {
        "DRAFT" => CrawlerVersionState::Draft,
        "PUBLISHED" => CrawlerVersionState::Published,
        _ => return Err(CrawlerRepositoryError::CorruptState),
    };
    if version.id() != id || version.crawler_id() != owner || version.state() != expected_state {
        return Err(CrawlerRepositoryError::CorruptState);
    }
    if validate_semantics {
        version
            .validate_semantic_contract()
            .map_err(|_| CrawlerRepositoryError::CorruptState)?;
    }
    Ok(version)
}

fn audit_metadata(
    connection: &impl crate::SqliteExecutor,
    version_id: CrawlerVersionId,
) -> Result<CrawlerAuditMetadata, CrawlerRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT event_type, actor, occurred_at, payload_json FROM audit_events WHERE entity_type = 'CRAWLER_VERSION' AND entity_id = ?1 ORDER BY occurred_at, id",
            [version_id.to_string()],
        )

        .map_err(CrawlerRepositoryError::database)?;
    let mut metadata = CrawlerAuditMetadata::default();
    while let Some(row) = rows.next().map_err(CrawlerRepositoryError::database)? {
        let event_type: String = row.get(0).map_err(CrawlerRepositoryError::database)?;
        let actor: String = row.get(1).map_err(CrawlerRepositoryError::database)?;
        let occurred_at: String = row.get(2).map_err(CrawlerRepositoryError::database)?;
        let payload_json: String = row.get(3).map_err(CrawlerRepositoryError::database)?;
        let payload: Value = serde_json::from_str(&payload_json)
            .map_err(|_| CrawlerRepositoryError::CorruptState)?;
        if event_type == "CRAWLER_DRAFT_CREATED" || event_type == "CRAWLER_VERSION_PUBLISHED" {
            // This DTO projects the creation/publication record, not the latest
            // lifecycle event. Reactivation remains durably auditable without
            // mixing its actor/time with publication-only metadata.
            metadata.actor = Some(actor);
            metadata.occurred_at = Some(occurred_at);
            metadata.config_hash = payload
                .get("config_hash")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .or(metadata.config_hash);
            metadata.base_version_id = payload
                .get("base_version_id")
                .and_then(Value::as_str)
                .map(parse_version_id)
                .transpose()?;
            metadata.warning_summary = audit_warning_summary(payload.get("warning_summary"))?;
        }
    }
    Ok(metadata)
}

fn audit_warning_summary(value: Option<&Value>) -> Result<Vec<String>, CrawlerRepositoryError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let Some(values) = value.as_array() else {
        return Err(CrawlerRepositoryError::CorruptState);
    };
    if values.len() > erabi_domain::MAX_VALIDATION_ISSUES {
        return Err(CrawlerRepositoryError::CorruptState);
    }
    let mut summary = Vec::with_capacity(values.len());
    for value in values {
        let Some(value) = value.as_str() else {
            return Err(CrawlerRepositoryError::CorruptState);
        };
        if value.is_empty()
            || value.chars().count() > erabi_domain::MAX_VALIDATION_MESSAGE_CHARS
            || value.chars().any(char::is_control)
        {
            return Err(CrawlerRepositoryError::CorruptState);
        }
        summary.push(value.to_owned());
    }
    if summary.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(CrawlerRepositoryError::CorruptState);
    }
    Ok(summary)
}

fn base_version_from_audit(
    connection: &impl crate::SqliteExecutor,
    version_id: CrawlerVersionId,
) -> Result<Option<CrawlerVersionId>, CrawlerRepositoryError> {
    Ok(audit_metadata(connection, version_id)?.base_version_id)
}

fn audit_payload(
    version_id: CrawlerVersionId,
    base_version_id: Option<CrawlerVersionId>,
    config_hash: Option<&str>,
    warning_summary: &[String],
) -> String {
    serde_json::json!({
        "version_id": version_id.to_string(),
        "base_version_id": base_version_id.map(|id| id.to_string()),
        "config_hash": config_hash,
        "warning_summary": warning_summary,
    })
    .to_string()
}

fn insert_audit_event(
    connection: &impl crate::SqliteExecutor,
    id: String,
    event_type: &str,
    actor: &str,
    occurred_at: &str,
    entity_id: &str,
    payload_json: String,
) -> Result<(), CrawlerRepositoryError> {
    connection
        .execute(
            "INSERT INTO audit_events (id, event_type, actor, occurred_at, entity_type, entity_id, payload_json) VALUES (?1, ?2, ?3, ?4, 'CRAWLER_VERSION', ?5, ?6)",
            (id, event_type, actor, occurred_at, entity_id, payload_json),
        )

        .map_err(CrawlerRepositoryError::database)
        .map(|_| ())
}

fn sync_seed_rows(
    connection: &impl crate::SqliteExecutor,
    version: &CrawlerVersion,
) -> Result<(), CrawlerRepositoryError> {
    connection
        .execute(
            "DELETE FROM seeds WHERE crawler_version_id = ?1",
            [version.id().to_string()],
        )
        .map_err(CrawlerRepositoryError::database)?;
    for seed in version.seeds() {
        connection
            .execute(
                "INSERT INTO seeds (id, crawler_version_id, original_url, canonical_url, enabled, label, entry_page_type_hint_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                (
                    seed.id.to_string(),
                    version.id().to_string(),
                    seed.original_url.as_str(),
                    seed.canonical_url.as_str(),
                    i64::from(seed.enabled),
                    seed.label.clone(),
                    seed.entry_page_type_hint.map(|id| id.to_string()),
                ),
            )

            .map_err(CrawlerRepositoryError::database)?;
    }
    Ok(())
}

fn declared_child_id_map(
    pairs: impl Iterator<Item = (String, String)>,
) -> BTreeMap<String, String> {
    pairs.collect()
}

fn augment_child_id_map(
    connection: &impl crate::SqliteExecutor,
    source_version_id: CrawlerVersionId,
    map: &mut BTreeMap<String, String>,
    sql: &str,
) -> Result<(), CrawlerRepositoryError> {
    let mut rows = connection
        .query(sql, [source_version_id.to_string()])
        .map_err(CrawlerRepositoryError::database)?;
    while let Some(row) = rows.next().map_err(CrawlerRepositoryError::database)? {
        let id: String = row.get(0).map_err(CrawlerRepositoryError::database)?;
        map.entry(id).or_insert_with(new_opaque_id);
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn clone_child_rows(
    connection: &impl crate::SqliteExecutor,
    source_version_id: CrawlerVersionId,
    target_version_id: CrawlerVersionId,
    seed_map: &BTreeMap<String, String>,
    page_map: &BTreeMap<String, String>,
    transition_map: &BTreeMap<String, String>,
) -> Result<(), CrawlerRepositoryError> {
    let mut seeds = connection
        .query(
            "SELECT id, original_url, canonical_url, enabled, label, entry_page_type_hint_id FROM seeds WHERE crawler_version_id = ?1 ORDER BY id",
            [source_version_id.to_string()],
        )

        .map_err(CrawlerRepositoryError::database)?;
    let mut seed_rows = Vec::new();
    while let Some(row) = seeds.next().map_err(CrawlerRepositoryError::database)? {
        let old_id: String = row.get(0).map_err(CrawlerRepositoryError::database)?;
        let hint: Option<String> = row.get(5).map_err(CrawlerRepositoryError::database)?;
        let hint = hint
            .map(|id| {
                page_map
                    .get(&id)
                    .cloned()
                    .ok_or(CrawlerRepositoryError::CorruptState)
            })
            .transpose()?;
        seed_rows.push((
            seed_map.get(&old_id).cloned().unwrap_or_else(new_opaque_id),
            row.get::<String>(1)
                .map_err(CrawlerRepositoryError::database)?,
            row.get::<String>(2)
                .map_err(CrawlerRepositoryError::database)?,
            row.get::<i64>(3)
                .map_err(CrawlerRepositoryError::database)?,
            row.get::<Option<String>>(4)
                .map_err(CrawlerRepositoryError::database)?,
            hint,
        ));
    }
    for (id, original_url, canonical_url, enabled, label, hint) in seed_rows {
        connection
            .execute(
                "INSERT INTO seeds (id, crawler_version_id, original_url, canonical_url, enabled, label, entry_page_type_hint_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                (id, target_version_id.to_string(), original_url, canonical_url, enabled, label, hint),
            )

            .map_err(CrawlerRepositoryError::database)?;
    }

    let mut pages = connection
        .query(
            "SELECT id, name, priority, configuration_json FROM page_types WHERE crawler_version_id = ?1 ORDER BY id",
            [source_version_id.to_string()],
        )

        .map_err(CrawlerRepositoryError::database)?;
    while let Some(row) = pages.next().map_err(CrawlerRepositoryError::database)? {
        let old_id: String = row.get(0).map_err(CrawlerRepositoryError::database)?;
        let configuration: String = row.get(3).map_err(CrawlerRepositoryError::database)?;
        let configuration =
            remap_json_references(&configuration, &[seed_map, page_map, transition_map])?;
        let new_id = page_map.get(&old_id).cloned().unwrap_or_else(new_opaque_id);
        connection
            .execute(
                "INSERT INTO page_types (id, crawler_version_id, name, priority, configuration_json) VALUES (?1, ?2, ?3, ?4, ?5)",
                (
                    new_id,
                    target_version_id.to_string(),
                    row.get::<String>(1).map_err(CrawlerRepositoryError::database)?,
                    row.get::<i64>(2).map_err(CrawlerRepositoryError::database)?,
                    configuration,
                ),
            )

            .map_err(CrawlerRepositoryError::database)?;
    }

    let mut matchers = connection
        .query(
            "SELECT url_matchers.page_type_id, url_matchers.ordinal, url_matchers.matcher_json FROM url_matchers JOIN page_types ON page_types.id = url_matchers.page_type_id WHERE page_types.crawler_version_id = ?1 ORDER BY url_matchers.page_type_id, url_matchers.ordinal, url_matchers.id",
            [source_version_id.to_string()],
        )

        .map_err(CrawlerRepositoryError::database)?;
    while let Some(row) = matchers.next().map_err(CrawlerRepositoryError::database)? {
        let page_id: String = row.get(0).map_err(CrawlerRepositoryError::database)?;
        let matcher_json: String = row.get(2).map_err(CrawlerRepositoryError::database)?;
        let matcher_json =
            remap_json_references(&matcher_json, &[seed_map, page_map, transition_map])?;
        let target_page = page_map
            .get(&page_id)
            .cloned()
            .ok_or(CrawlerRepositoryError::CorruptState)?;
        connection
            .execute(
                "INSERT INTO url_matchers (id, page_type_id, ordinal, matcher_json) VALUES (?1, ?2, ?3, ?4)",
                (
                    new_opaque_id(),
                    target_page,
                    row.get::<i64>(1).map_err(CrawlerRepositoryError::database)?,
                    matcher_json,
                ),
            )

            .map_err(CrawlerRepositoryError::database)?;
    }

    let mut transitions = connection
        .query(
            "SELECT id, configuration_json FROM discovery_transitions WHERE crawler_version_id = ?1 ORDER BY id",
            [source_version_id.to_string()],
        )

        .map_err(CrawlerRepositoryError::database)?;
    while let Some(row) = transitions
        .next()
        .map_err(CrawlerRepositoryError::database)?
    {
        let old_id: String = row.get(0).map_err(CrawlerRepositoryError::database)?;
        let configuration: String = row.get(1).map_err(CrawlerRepositoryError::database)?;
        let configuration =
            remap_json_references(&configuration, &[seed_map, page_map, transition_map])?;
        let mut transition: DiscoveryTransition = serde_json::from_str(&configuration)
            .map_err(|_| CrawlerRepositoryError::CorruptState)?;
        transition.latest_test_evidence_id = None;
        let configuration = serialize(&transition).map_err(CrawlerRepositoryError::database)?;
        let new_id = transition_map
            .get(&old_id)
            .cloned()
            .unwrap_or_else(new_opaque_id);
        connection
            .execute(
                "INSERT INTO discovery_transitions (id, crawler_version_id, configuration_json) VALUES (?1, ?2, ?3)",
                (new_id, target_version_id.to_string(), configuration),
            )

            .map_err(CrawlerRepositoryError::database)?;
    }
    Ok(())
}

/// Revalidates the active Draft's complete semantic projection using the
/// caller's transaction. This is intentionally crate-visible so evidence
/// persistence can close its hash-check/insert TOCTOU window.
pub(crate) fn current_draft_semantic_hash_in_transaction(
    connection: &impl crate::SqliteExecutor,
    crawler_id: CrawlerId,
    version_id: CrawlerVersionId,
) -> Result<String, CrawlerRepositoryError> {
    ensure_crawler_exists(connection, crawler_id)?;
    ensure_pointer_consistency(connection, crawler_id)?;
    if active_draft_for(connection, crawler_id)?.as_deref() != Some(version_id.to_string().as_str())
    {
        return Err(CrawlerRepositoryError::VersionNotActiveDraft);
    }
    let snapshot = load_semantic_snapshot(connection, crawler_id, version_id)?;
    if snapshot.version.state() != CrawlerVersionState::Draft {
        return Err(CrawlerRepositoryError::VersionNotDraft);
    }
    Ok(snapshot.config_hash)
}

pub(crate) fn semantic_hash_for_version_in_connection(
    connection: &impl crate::SqliteExecutor,
    crawler_id: CrawlerId,
    version_id: CrawlerVersionId,
) -> Result<String, CrawlerRepositoryError> {
    ensure_crawler_exists(connection, crawler_id)?;
    ensure_pointer_consistency(connection, crawler_id)?;
    Ok(load_semantic_snapshot(connection, crawler_id, version_id)?.config_hash)
}

#[allow(clippy::too_many_lines)]
fn semantic_hash(
    connection: &impl crate::SqliteExecutor,
    version: &CrawlerVersion,
) -> Result<String, CrawlerRepositoryError> {
    semantic_hash_with_semantic_validation(connection, version, true)
}

#[allow(clippy::too_many_lines)]
fn semantic_hash_with_semantic_validation(
    connection: &impl crate::SqliteExecutor,
    version: &CrawlerVersion,
    validate_semantics: bool,
) -> Result<String, CrawlerRepositoryError> {
    if validate_semantics {
        version
            .validate_semantic_contract()
            .map_err(|_| CrawlerRepositoryError::CorruptState)?;
    }
    load_transition_records(connection, version, validate_semantics)?;
    let mut version_json = serde_json::to_value(version).map_err(|error| {
        CrawlerRepositoryError::database(DbError::Serialization(error.to_string()))
    })?;
    if let Value::Object(object) = &mut version_json {
        object.remove("id");
        object.remove("crawler_id");
        object.remove("state");
    }

    let seeds = child_values(
        connection,
        "SELECT id, original_url, canonical_url, enabled, label, entry_page_type_hint_id FROM seeds WHERE crawler_version_id = ?1",
        version.id(),
        |row| {
            let mut object = Map::new();
            object.insert(
                "id".into(),
                Value::String(row.get(0).map_err(CrawlerRepositoryError::database)?),
            );
            object.insert(
                "original_url".into(),
                Value::String(row.get(1).map_err(CrawlerRepositoryError::database)?),
            );
            object.insert(
                "canonical_url".into(),
                Value::String(row.get(2).map_err(CrawlerRepositoryError::database)?),
            );
            object.insert(
                "enabled".into(),
                Value::from(
                    row.get::<i64>(3)
                        .map_err(CrawlerRepositoryError::database)?,
                ),
            );
            object.insert(
                "label".into(),
                row.get::<Option<String>>(4)
                    .map_err(CrawlerRepositoryError::database)?
                    .map_or(Value::Null, Value::String),
            );
            object.insert(
                "entry_page_type_hint".into(),
                row.get::<Option<String>>(5)
                    .map_err(CrawlerRepositoryError::database)?
                    .map_or(Value::Null, Value::String),
            );
            Ok(Value::Object(object))
        },
    )?;
    let pages = child_values(
        connection,
        "SELECT id, name, priority, configuration_json FROM page_types WHERE crawler_version_id = ?1",
        version.id(),
        |row| {
            let configuration: String = row.get(3).map_err(CrawlerRepositoryError::database)?;
            let configuration = serde_json::from_str::<Value>(&configuration)
                .map_err(|_| CrawlerRepositoryError::CorruptState)?;
            let mut object = Map::new();
            object.insert(
                "id".into(),
                Value::String(row.get(0).map_err(CrawlerRepositoryError::database)?),
            );
            object.insert(
                "name".into(),
                Value::String(row.get(1).map_err(CrawlerRepositoryError::database)?),
            );
            object.insert(
                "priority".into(),
                Value::from(
                    row.get::<i64>(2)
                        .map_err(CrawlerRepositoryError::database)?,
                ),
            );
            object.insert("configuration".into(), configuration);
            Ok(Value::Object(object))
        },
    )?;
    let matchers = child_values(
        connection,
        "SELECT url_matchers.id, url_matchers.page_type_id, url_matchers.matcher_json FROM url_matchers JOIN page_types ON page_types.id = url_matchers.page_type_id WHERE page_types.crawler_version_id = ?1",
        version.id(),
        |row| {
            let matcher_json: String = row.get(2).map_err(CrawlerRepositoryError::database)?;
            let matcher = serde_json::from_str::<UrlMatcher>(&matcher_json)
                .map_err(|_| CrawlerRepositoryError::CorruptState)?;
            let mut object = Map::new();
            object.insert(
                "id".into(),
                Value::String(row.get(0).map_err(CrawlerRepositoryError::database)?),
            );
            object.insert(
                "page_type_id".into(),
                Value::String(row.get(1).map_err(CrawlerRepositoryError::database)?),
            );
            object.insert(
                "matcher".into(),
                serde_json::to_value(matcher).map_err(|error| {
                    CrawlerRepositoryError::database(DbError::Serialization(error.to_string()))
                })?,
            );
            Ok(Value::Object(object))
        },
    )?;
    let transitions = child_values(
        connection,
        "SELECT id, configuration_json FROM discovery_transitions WHERE crawler_version_id = ?1",
        version.id(),
        |row| {
            let id = parse_transition_id(
                &row.get::<String>(0)
                    .map_err(CrawlerRepositoryError::database)?,
            )?;
            let configuration: String = row.get(1).map_err(CrawlerRepositoryError::database)?;
            let transition: DiscoveryTransition = serde_json::from_str(&configuration)
                .map_err(|_| CrawlerRepositoryError::CorruptState)?;
            if transition.id != id {
                return Err(CrawlerRepositoryError::CorruptState);
            }
            Ok(serde_json::json!({
                "id": id.to_string(),
                "configuration": transition_semantic_value(&transition)?,
            }))
        },
    )?;

    let template = SemanticHashTemplate {
        version: version_json,
        seeds,
        page_types: pages,
        url_matchers: matchers,
        transitions,
    };
    let canonicalization = refine_semantic_labels(&template, version);
    let payload = remapped_semantic_payload(&template, &canonicalization);
    canonical_sha256(&payload).map_err(|error| {
        CrawlerRepositoryError::database(DbError::Serialization(error.to_string()))
    })
}

fn transition_semantic_value(
    transition: &DiscoveryTransition,
) -> Result<Value, CrawlerRepositoryError> {
    let mut value = serde_json::to_value(transition).map_err(|error| {
        CrawlerRepositoryError::database(DbError::Serialization(error.to_string()))
    })?;
    if let Value::Object(object) = &mut value {
        // Test Evidence is confidence metadata owned by the later Test Lab;
        // attaching the latest evidence must not change crawler semantics.
        object.remove("latest_test_evidence_id");
    }
    Ok(value)
}

struct SemanticHashTemplate {
    version: Value,
    seeds: Vec<Value>,
    page_types: Vec<Value>,
    url_matchers: Vec<Value>,
    transitions: Vec<Value>,
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
enum SemanticEntityKind {
    Seed,
    PageType,
    UrlMatcher,
    Transition,
}

impl SemanticEntityKind {
    const fn label_prefix(self) -> &'static str {
        match self {
            Self::Seed => "seed",
            Self::PageType => "page_type",
            Self::UrlMatcher => "url_matcher",
            Self::Transition => "transition",
        }
    }
}

struct SemanticEntity {
    id: String,
    kind: SemanticEntityKind,
    value: Value,
}

struct SemanticCanonicalization {
    known_kinds: BTreeMap<String, SemanticEntityKind>,
    labels: BTreeMap<String, String>,
    incoming: BTreeMap<String, Vec<Value>>,
}

fn refine_semantic_labels(
    template: &SemanticHashTemplate,
    version: &CrawlerVersion,
) -> SemanticCanonicalization {
    // Refinement is bounded by the number of semantic children. Equal final
    // labels deliberately remain a multiset in the payload; no UUID is chosen
    // to break a structurally symmetric class.
    let entities = semantic_entities(template, version);
    let known_kinds = entities
        .iter()
        .map(|entity| (entity.id.clone(), entity.kind))
        .collect::<BTreeMap<_, _>>();
    let mut labels = labels_for_entities(
        &entities,
        &known_kinds,
        None,
        &incoming_incidence(&entities, &known_kinds, None),
    );
    for _ in 0..entities.len() {
        let incoming = incoming_incidence(&entities, &known_kinds, Some(&labels));
        let refined = labels_for_entities(&entities, &known_kinds, Some(&labels), &incoming);
        if same_semantic_partition(&labels, &refined) {
            labels = refined;
            break;
        }
        labels = refined;
    }
    let incoming = incoming_incidence(&entities, &known_kinds, Some(&labels));
    SemanticCanonicalization {
        known_kinds,
        labels,
        incoming,
    }
}

fn semantic_entities(
    template: &SemanticHashTemplate,
    version: &CrawlerVersion,
) -> Vec<SemanticEntity> {
    let mut entities = Vec::new();
    extend_semantic_entities(
        &mut entities,
        SemanticEntityKind::Seed,
        version.seeds().iter().map(|seed| seed.id.to_string()),
        &template.seeds,
    );
    extend_semantic_entities(
        &mut entities,
        SemanticEntityKind::PageType,
        version.page_type_ids().iter().map(ToString::to_string),
        &template.page_types,
    );
    extend_semantic_entities(
        &mut entities,
        SemanticEntityKind::UrlMatcher,
        std::iter::empty(),
        &template.url_matchers,
    );
    extend_semantic_entities(
        &mut entities,
        SemanticEntityKind::Transition,
        version.transition_ids().iter().map(ToString::to_string),
        &template.transitions,
    );
    entities
}

fn extend_semantic_entities(
    entities: &mut Vec<SemanticEntity>,
    kind: SemanticEntityKind,
    declared_ids: impl Iterator<Item = String>,
    rows: &[Value],
) {
    let mut values = rows
        .iter()
        .filter_map(|row| {
            row.get("id")
                .and_then(Value::as_str)
                .map(|id| (id.to_owned(), row.clone()))
        })
        .collect::<BTreeMap<_, _>>();
    for id in declared_ids {
        values
            .entry(id.clone())
            .or_insert_with(|| serde_json::json!({"id": id}));
    }
    entities.extend(
        values
            .into_iter()
            .map(|(id, value)| SemanticEntity { id, kind, value }),
    );
}

fn labels_for_entities(
    entities: &[SemanticEntity],
    known_kinds: &BTreeMap<String, SemanticEntityKind>,
    previous_labels: Option<&BTreeMap<String, String>>,
    incoming: &BTreeMap<String, Vec<Value>>,
) -> BTreeMap<String, String> {
    let mut signatures = BTreeMap::<SemanticEntityKind, Vec<(String, String)>>::new();
    for entity in entities {
        let mut value = entity.value.clone();
        remap_semantic_references(
            &mut value,
            known_kinds,
            previous_labels,
            Some((&entity.id, entity.kind)),
        );
        let signature = canonical_sort_key(&serde_json::json!({
            "incoming": sorted_array(incoming.get(&entity.id).cloned().unwrap_or_default()),
            "kind": entity.kind.label_prefix(),
            "value": value,
        }));
        signatures
            .entry(entity.kind)
            .or_default()
            .push((entity.id.clone(), signature));
    }
    let mut labels = BTreeMap::new();
    for (kind, signatures) in signatures {
        let unique_signatures = signatures
            .iter()
            .map(|(_, signature)| signature.clone())
            .collect::<BTreeSet<_>>();
        let ranks = unique_signatures
            .into_iter()
            .enumerate()
            .map(|(rank, signature)| (signature, rank))
            .collect::<BTreeMap<_, _>>();
        for (id, signature) in signatures {
            let rank = ranks.get(&signature).copied().unwrap_or_default();
            labels.insert(id, format!("@{}:{rank}", kind.label_prefix()));
        }
    }
    labels
}

fn incoming_incidence(
    entities: &[SemanticEntity],
    known_kinds: &BTreeMap<String, SemanticEntityKind>,
    labels: Option<&BTreeMap<String, String>>,
) -> BTreeMap<String, Vec<Value>> {
    let mut incoming = BTreeMap::<String, Vec<Value>>::new();
    for entity in entities {
        let source = labels
            .and_then(|labels| labels.get(&entity.id))
            .cloned()
            .unwrap_or_else(|| format!("@{}", entity.kind.label_prefix()));
        for reference in semantic_references(&entity.value, known_kinds, &entity.id) {
            incoming
                .entry(reference.target)
                .or_default()
                .push(serde_json::json!({
                    "path": reference.path,
                    "source": source,
                }));
        }
    }
    incoming
}

struct SemanticReference {
    target: String,
    path: String,
}

fn semantic_references(
    value: &Value,
    known_kinds: &BTreeMap<String, SemanticEntityKind>,
    owner_id: &str,
) -> Vec<SemanticReference> {
    let mut references = Vec::new();
    collect_semantic_references(value, known_kinds, owner_id, "", &mut references);
    references
}

fn collect_semantic_references(
    value: &Value,
    known_kinds: &BTreeMap<String, SemanticEntityKind>,
    owner_id: &str,
    path: &str,
    references: &mut Vec<SemanticReference>,
) {
    match value {
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                collect_semantic_references(
                    value,
                    known_kinds,
                    owner_id,
                    &format!("{path}/{index}"),
                    references,
                );
            }
        }
        Value::Object(values) => {
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            for key in keys {
                if let Some(value) = values.get(key) {
                    collect_semantic_references(
                        value,
                        known_kinds,
                        owner_id,
                        &format!("{path}/{}", json_pointer_segment(key)),
                        references,
                    );
                }
            }
        }
        Value::String(value) => {
            if known_kinds.contains_key(value) && !(path == "/id" && value == owner_id) {
                references.push(SemanticReference {
                    target: value.clone(),
                    path: path.to_owned(),
                });
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn json_pointer_segment(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

fn same_semantic_partition(
    left: &BTreeMap<String, String>,
    right: &BTreeMap<String, String>,
) -> bool {
    // IDs are used only to compare two refinement partitions of this one
    // in-memory graph. They never select or appear in canonical output.
    let groups = |labels: &BTreeMap<String, String>| {
        let mut groups = BTreeMap::<String, BTreeSet<String>>::new();
        for (id, label) in labels {
            groups.entry(label.clone()).or_default().insert(id.clone());
        }
        groups.into_values().collect::<BTreeSet<_>>()
    };
    groups(left) == groups(right)
}

fn remapped_semantic_payload(
    template: &SemanticHashTemplate,
    canonicalization: &SemanticCanonicalization,
) -> Value {
    let mut version = template.version.clone();
    remap_semantic_references(
        &mut version,
        &canonicalization.known_kinds,
        Some(&canonicalization.labels),
        None,
    );
    sort_version_collections(&mut version);
    let mut payload = BTreeMap::new();
    payload.insert("version", version);
    payload.insert(
        "seeds",
        remapped_sorted_array(&template.seeds, SemanticEntityKind::Seed, canonicalization),
    );
    payload.insert(
        "page_types",
        remapped_sorted_array(
            &template.page_types,
            SemanticEntityKind::PageType,
            canonicalization,
        ),
    );
    payload.insert(
        "url_matchers",
        remapped_sorted_array(
            &template.url_matchers,
            SemanticEntityKind::UrlMatcher,
            canonicalization,
        ),
    );
    payload.insert(
        "transitions",
        remapped_sorted_array(
            &template.transitions,
            SemanticEntityKind::Transition,
            canonicalization,
        ),
    );
    payload.insert(
        "incoming_incidence_profiles",
        anonymous_incidence_profiles(canonicalization),
    );
    serde_json::to_value(payload).unwrap_or(Value::Null)
}

fn anonymous_incidence_profiles(canonicalization: &SemanticCanonicalization) -> Value {
    let profiles = canonicalization
        .labels
        .iter()
        .filter_map(|(id, label)| {
            canonicalization.known_kinds.get(id).map(|kind| {
                serde_json::json!({
                    "incoming": sorted_array(
                        canonicalization.incoming.get(id).cloned().unwrap_or_default(),
                    ),
                    "kind": kind.label_prefix(),
                    "label": label,
                })
            })
        })
        .collect::<Vec<_>>();
    sorted_array(profiles)
}

fn remapped_sorted_array(
    values: &[Value],
    kind: SemanticEntityKind,
    canonicalization: &SemanticCanonicalization,
) -> Value {
    let mut values = values.to_vec();
    for value in &mut values {
        let owner = value
            .get("id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        remap_semantic_references(
            value,
            &canonicalization.known_kinds,
            Some(&canonicalization.labels),
            owner.as_deref().map(|id| (id, kind)),
        );
    }
    sorted_array(values)
}

fn sort_version_collections(value: &mut Value) {
    let Value::Object(object) = value else {
        return;
    };
    for field in ["seeds", "page_type_ids", "transition_ids"] {
        if let Some(Value::Array(values)) = object.get_mut(field) {
            values.sort_by_key(canonical_sort_key);
        }
    }
    if let Some(Value::Object(guardrails)) = object.get_mut("guardrails")
        && let Some(Value::Array(page_types)) = guardrails.get_mut("page_types")
    {
        page_types.sort_by_key(canonical_sort_key);
    }
}

fn child_values<F>(
    connection: &impl crate::SqliteExecutor,
    sql: &str,
    version_id: CrawlerVersionId,
    mut map: F,
) -> Result<Vec<Value>, CrawlerRepositoryError>
where
    F: FnMut(&Row) -> Result<Value, CrawlerRepositoryError>,
{
    let mut rows = connection
        .query(sql, [version_id.to_string()])
        .map_err(CrawlerRepositoryError::database)?;
    let mut values = Vec::new();
    while let Some(row) = rows.next().map_err(CrawlerRepositoryError::database)? {
        values.push(map(row)?);
    }
    Ok(values)
}

fn remap_json_references(
    serialized: &str,
    maps: &[&BTreeMap<String, String>],
) -> Result<String, CrawlerRepositoryError> {
    // Task 1 treats child JSON as opaque: only exact values matching known
    // version-local identities are remapped. Future typed schemas own any
    // field-aware reference handling rather than guessing at arbitrary keys.
    let mut value: Value =
        serde_json::from_str(serialized).map_err(|_| CrawlerRepositoryError::CorruptState)?;
    remap_json_value(&mut value, maps);
    serde_json::to_string(&value).map_err(|_| CrawlerRepositoryError::CorruptState)
}

fn remap_json_value(value: &mut Value, maps: &[&BTreeMap<String, String>]) {
    match value {
        Value::Array(values) => values
            .iter_mut()
            .for_each(|value| remap_json_value(value, maps)),
        Value::Object(values) => values
            .values_mut()
            .for_each(|value| remap_json_value(value, maps)),
        Value::String(string) => {
            for map in maps {
                if let Some(replacement) = map.get(string) {
                    *string = replacement.clone();
                    break;
                }
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn remap_semantic_references(
    value: &mut Value,
    known_kinds: &BTreeMap<String, SemanticEntityKind>,
    labels: Option<&BTreeMap<String, String>>,
    owner: Option<(&str, SemanticEntityKind)>,
) {
    let mut local_references = BTreeMap::new();
    let mut next_reference_index = BTreeMap::new();
    remap_semantic_references_in_scope(
        value,
        known_kinds,
        labels,
        owner,
        &mut local_references,
        &mut next_reference_index,
    );
}

fn remap_semantic_references_in_scope(
    value: &mut Value,
    known_kinds: &BTreeMap<String, SemanticEntityKind>,
    labels: Option<&BTreeMap<String, String>>,
    owner: Option<(&str, SemanticEntityKind)>,
    local_references: &mut BTreeMap<String, String>,
    next_reference_index: &mut BTreeMap<String, usize>,
) {
    match value {
        Value::Array(values) => {
            for value in values {
                remap_semantic_references_in_scope(
                    value,
                    known_kinds,
                    labels,
                    owner,
                    local_references,
                    next_reference_index,
                );
            }
        }
        Value::Object(values) => {
            let mut keys = values.keys().cloned().collect::<Vec<_>>();
            keys.sort_unstable();
            for key in keys {
                if let Some(value) = values.get_mut(&key) {
                    remap_semantic_references_in_scope(
                        value,
                        known_kinds,
                        labels,
                        owner,
                        local_references,
                        next_reference_index,
                    );
                }
            }
        }
        Value::String(string) => {
            if let Some((owner_id, owner_kind)) = owner
                && string == owner_id
            {
                *string = format!("@self:{}", owner_kind.label_prefix());
            } else if let Some(kind) = known_kinds.get(string) {
                let label = labels
                    .and_then(|labels| labels.get(string))
                    .cloned()
                    .unwrap_or_else(|| format!("@{}", kind.label_prefix()));
                let next = next_reference_index.entry(label.clone()).or_insert(0);
                let replacement = local_references.entry(string.clone()).or_insert_with(|| {
                    let replacement = format!("{label}#{next}");
                    *next += 1;
                    replacement
                });
                string.clone_from(replacement);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn sorted_array(mut values: Vec<Value>) -> Value {
    values.sort_by_key(canonical_sort_key);
    Value::Array(values)
}

fn canonical_sort_key(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

fn repository_error_as_db(error: CrawlerRepositoryError) -> DbError {
    match error {
        CrawlerRepositoryError::Database(error) => error,
        other => DbError::Invariant(other.to_string()),
    }
}

fn map_semantic_error(code: erabi_domain::ErrorCode) -> CrawlerRepositoryError {
    match code {
        erabi_domain::ErrorCode::InvalidCanonicalizationPolicy => {
            CrawlerRepositoryError::InvalidCanonicalizationPolicy
        }
        erabi_domain::ErrorCode::InvalidDomainScope
        | erabi_domain::ErrorCode::InvalidDomainScopeRule
        | erabi_domain::ErrorCode::RegistrableDomainUnavailable => {
            CrawlerRepositoryError::InvalidDomainScope
        }
        erabi_domain::ErrorCode::InvalidCrawlGuardrails => {
            CrawlerRepositoryError::InvalidCrawlGuardrails
        }
        erabi_domain::ErrorCode::InvalidPageTypeBudget => {
            CrawlerRepositoryError::InvalidPageTypeBudget
        }
        erabi_domain::ErrorCode::InvalidTransitionBudget => {
            CrawlerRepositoryError::InvalidTransitionBudget
        }
        erabi_domain::ErrorCode::InvalidDiscoveryTransition => {
            CrawlerRepositoryError::InvalidDiscoveryTransition
        }
        erabi_domain::ErrorCode::Conflict => CrawlerRepositoryError::PublishedVersionImmutable,
        _ => CrawlerRepositoryError::CorruptState,
    }
}

fn new_opaque_id() -> String {
    Uuid::now_v7().to_string()
}

fn parse_crawler_id(value: &str) -> Result<CrawlerId, CrawlerRepositoryError> {
    Uuid::parse_str(value)
        .ok()
        .and_then(CrawlerId::from_uuid)
        .ok_or(CrawlerRepositoryError::CorruptState)
}

fn parse_version_id(value: &str) -> Result<CrawlerVersionId, CrawlerRepositoryError> {
    Uuid::parse_str(value)
        .ok()
        .and_then(CrawlerVersionId::from_uuid)
        .ok_or(CrawlerRepositoryError::CorruptState)
}

fn parse_page_type_id(value: &str) -> Result<PageTypeId, CrawlerRepositoryError> {
    Uuid::parse_str(value)
        .ok()
        .and_then(PageTypeId::from_uuid)
        .ok_or(CrawlerRepositoryError::CorruptState)
}

fn parse_transition_id(value: &str) -> Result<DiscoveryTransitionId, CrawlerRepositoryError> {
    Uuid::parse_str(value)
        .ok()
        .and_then(DiscoveryTransitionId::from_uuid)
        .ok_or(CrawlerRepositoryError::CorruptState)
}

fn parse_collection_id(value: &str) -> Result<erabi_domain::CollectionId, CrawlerRepositoryError> {
    Uuid::parse_str(value)
        .ok()
        .and_then(erabi_domain::CollectionId::from_uuid)
        .ok_or(CrawlerRepositoryError::CorruptState)
}

fn serialize(value: &impl serde::Serialize) -> Result<String, DbError> {
    serde_json::to_string(value).map_err(|error| DbError::Serialization(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MigrationRunner;
    use erabi_domain::{
        CanonicalizationPolicyId, CrawlerVersion, DiscoveryTransition, DiscoveryTransitionId,
        DomainScopeId, PageTypeId, Seed, TransitionBudget, UrlMatcher, resolve_page_type,
    };

    async fn database() -> Result<ErabiDatabase, Box<dyn std::error::Error>> {
        let database = ErabiDatabase::in_memory().await?;
        MigrationRunner::default().apply(&database).await?;
        Ok(database)
    }

    fn is_lifecycle_contention(error: &rusqlite::Error) -> bool {
        matches!(
            crate::classify_rusqlite_error(error),
            DatabaseFailure::Busy | DatabaseFailure::BusySnapshot
        )
    }

    #[test]
    fn lifecycle_contention_recognizes_only_sqlite_write_contention() {
        assert!(is_lifecycle_contention(&rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
            None,
        )));
        assert!(is_lifecycle_contention(&rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(517),
            None,
        )));
        assert!(!is_lifecycle_contention(&rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT),
            None,
        )));
        assert!(!is_lifecycle_contention(&rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CORRUPT),
            None,
        )));
    }

    async fn execute_sql<T, F>(
        database: &ErabiDatabase,
        operation: F,
    ) -> Result<T, Box<dyn std::error::Error>>
    where
        T: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> Result<T, rusqlite::Error> + Send + 'static,
    {
        Ok(crate::test_call(database, operation).await?)
    }

    async fn count_by_crawler(
        database: &ErabiDatabase,
        sql: &str,
        crawler_id: CrawlerId,
    ) -> Result<i64, Box<dyn std::error::Error>> {
        let sql = sql.to_owned();
        let crawler_id = crawler_id.to_string();
        execute_sql(database, move |connection| {
            connection.query_row(&sql, [crawler_id], |row| row.get(0))
        })
        .await
    }

    async fn count_all(
        database: &ErabiDatabase,
        sql: &str,
    ) -> Result<i64, Box<dyn std::error::Error>> {
        let sql = sql.to_owned();
        execute_sql(database, move |connection| {
            connection.query_row(&sql, [], |row| row.get(0))
        })
        .await
    }

    #[derive(Clone, Copy, Eq, PartialEq)]
    enum PageReference {
        Source,
        Target,
    }

    #[derive(Clone, Copy, Eq, PartialEq)]
    enum TransitionTarget {
        Target,
        Source,
    }

    #[derive(Clone, Copy, Eq, PartialEq)]
    enum ChildInsertionOrder {
        Forward,
        Reverse,
    }

    #[derive(Clone, Copy)]
    struct GraphShape {
        seed_hint: PageReference,
        transition_target: TransitionTarget,
        configuration_reference: PageReference,
        child_insertion: ChildInsertionOrder,
    }

    const fn standard_graph_shape() -> GraphShape {
        GraphShape {
            seed_hint: PageReference::Source,
            transition_target: TransitionTarget::Target,
            configuration_reference: PageReference::Source,
            child_insertion: ChildInsertionOrder::Forward,
        }
    }

    fn persisted_transition(
        id: DiscoveryTransitionId,
        source_page_type_id: PageTypeId,
        target_page_type_id: PageTypeId,
        name: &str,
    ) -> DiscoveryTransition {
        DiscoveryTransition {
            id,
            source_page_type_id,
            target_page_type_id,
            name: name.to_owned(),
            enabled: true,
            link_selector: "a[href]".to_owned(),
            url_constraints: None,
            priority: 10,
            budget: TransitionBudget {
                max_links_per_source_page: 10,
                total_budget: Some(100),
                depth_contribution: 1,
            },
            deduplicate: true,
            latest_test_evidence_id: None,
        }
    }

    async fn graph_version(
        database: &ErabiDatabase,
        name: &str,
        shape: GraphShape,
    ) -> Result<(Crawler, CrawlerVersion), Box<dyn std::error::Error>> {
        let repository = CrawlerRepository::new(database);
        let crawler = Crawler::new(name);
        repository.create(&crawler).await?;

        let source_page = PageTypeId::new();
        let target_page = PageTypeId::new();
        let transition = DiscoveryTransitionId::new();
        let mut seed = Seed::new(
            "https://example.test/catalog".parse()?,
            "https://example.test/catalog".parse()?,
        );
        seed.entry_page_type_hint = Some(if shape.seed_hint == PageReference::Target {
            target_page
        } else {
            source_page
        });
        let mut version = CrawlerVersion::draft(crawler.id());
        version.set_page_type_ids(vec![source_page, target_page])?;
        version.set_transition_ids(vec![transition])?;
        version.add_seed(seed)?;
        repository
            .save_draft(&version, "operator", "2026-08-25T00:00:00Z")
            .await?;

        let page_rows = if shape.child_insertion == ChildInsertionOrder::Reverse {
            [(target_page, "Target"), (source_page, "Source")]
        } else {
            [(source_page, "Source"), (target_page, "Target")]
        };
        let configuration_target = if shape.configuration_reference == PageReference::Target {
            target_page
        } else {
            source_page
        };
        let version_id = version.id().to_string();
        let transition_id = transition.to_string();
        let configuration_target = configuration_target.to_string();
        let transition_json = serde_json::to_string(&persisted_transition(
            transition,
            source_page,
            if shape.transition_target == TransitionTarget::Source {
                source_page
            } else {
                target_page
            },
            "graph transition",
        ))?;
        let page_inserts = page_rows
            .into_iter()
            .map(|(id, page_name)| {
                (
                    id.to_string(),
                    page_name.to_owned(),
                    serde_json::json!({"related_page_type_id": configuration_target.clone()})
                        .to_string(),
                )
            })
            .collect::<Vec<_>>();
        execute_sql(database, move |connection| {
            for (id, page_name, configuration_json) in page_inserts {
                connection.execute(
                    "INSERT INTO page_types (id, crawler_version_id, name, priority, configuration_json) VALUES (?1, ?2, ?3, 10, ?4)",
                    (id, version_id.as_str(), page_name, configuration_json),
                )?;
            }
            connection.execute(
                "INSERT INTO discovery_transitions (id, crawler_version_id, configuration_json) VALUES (?1, ?2, ?3)",
                (transition_id, version_id, transition_json),
            )?;
            Ok(())
        }).await?;
        Ok((crawler, version))
    }

    #[derive(Clone, Copy)]
    enum SymmetricGraphShape {
        SelfTransition,
        DistinctTransition,
        MatchersShareOwner,
        MatchersSplitOwners,
    }

    #[allow(clippy::too_many_lines)]
    async fn symmetric_graph_version(
        database: &ErabiDatabase,
        name: &str,
        shape: SymmetricGraphShape,
        swap_members: bool,
        reverse_insertion: bool,
    ) -> Result<(Crawler, CrawlerVersion), Box<dyn std::error::Error>> {
        let repository = CrawlerRepository::new(database);
        let crawler = Crawler::new(name);
        repository.create(&crawler).await?;

        let first_page = PageTypeId::new();
        let second_page = PageTypeId::new();
        let (source_page, target_page) = if swap_members {
            (second_page, first_page)
        } else {
            (first_page, second_page)
        };
        let transition_id = DiscoveryTransitionId::new();

        let mut version = CrawlerVersion::draft(crawler.id());
        let mut page_type_ids = vec![first_page, second_page];
        if reverse_insertion {
            page_type_ids.reverse();
        }
        version.set_page_type_ids(page_type_ids)?;
        if matches!(
            shape,
            SymmetricGraphShape::SelfTransition | SymmetricGraphShape::DistinctTransition
        ) {
            version.set_transition_ids(vec![transition_id])?;
        }
        repository
            .save_draft(&version, "operator", "2026-08-25T00:00:00Z")
            .await?;

        let mut page_rows = vec![first_page, second_page];
        if reverse_insertion {
            page_rows.reverse();
        }
        let version_id = version.id().to_string();
        let page_inserts = page_rows
            .into_iter()
            .map(|page_id| (page_id.to_string(), version_id.clone()))
            .collect::<Vec<_>>();
        let mut transition_insert = None;
        let mut matcher_inserts = Vec::new();
        match shape {
            SymmetricGraphShape::SelfTransition | SymmetricGraphShape::DistinctTransition => {
                let target = if matches!(shape, SymmetricGraphShape::SelfTransition) {
                    source_page
                } else {
                    target_page
                };
                transition_insert = Some((
                    transition_id.to_string(),
                    version_id.clone(),
                    serde_json::to_string(&persisted_transition(
                        transition_id,
                        source_page,
                        target,
                        "symmetric transition",
                    ))?,
                ));
            }
            SymmetricGraphShape::MatchersShareOwner | SymmetricGraphShape::MatchersSplitOwners => {
                let second_owner = if matches!(shape, SymmetricGraphShape::MatchersShareOwner) {
                    source_page
                } else {
                    target_page
                };
                let mut matchers = vec![
                    (source_page, 0_i64, "https://example.test/first"),
                    (second_owner, 1_i64, "https://example.test/second"),
                ];
                if reverse_insertion {
                    matchers.reverse();
                }
                for (page_type_id, ordinal, pattern) in matchers {
                    let matcher = UrlMatcher::exact_url(pattern.parse()?);
                    matcher_inserts.push((
                        new_opaque_id(),
                        page_type_id.to_string(),
                        ordinal,
                        serde_json::to_string(&matcher)?,
                    ));
                }
            }
        }
        execute_sql(database, move |connection| {
            for (page_id, version_id) in page_inserts {
                connection.execute(
                    "INSERT INTO page_types (id, crawler_version_id, name, priority, configuration_json) VALUES (?1, ?2, 'Symmetric', 10, '{}')",
                    (page_id, version_id),
                )?;
            }
            if let Some((transition_id, version_id, configuration_json)) = transition_insert {
                connection.execute(
                    "INSERT INTO discovery_transitions (id, crawler_version_id, configuration_json) VALUES (?1, ?2, ?3)",
                    (transition_id, version_id, configuration_json),
                )?;
            }
            for (matcher_id, page_type_id, ordinal, matcher_json) in matcher_inserts {
                connection.execute(
                    "INSERT INTO url_matchers (id, page_type_id, ordinal, matcher_json) VALUES (?1, ?2, ?3, ?4)",
                    (matcher_id, page_type_id, ordinal, matcher_json),
                )?;
            }
            Ok(())
        }).await?;
        Ok((crawler, version))
    }

    #[tokio::test]
    async fn symmetric_page_types_preserve_same_member_transition_references()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let repository = CrawlerRepository::new(&database);
        let (self_crawler, self_version) = symmetric_graph_version(
            &database,
            "Symmetric self transition",
            SymmetricGraphShape::SelfTransition,
            false,
            false,
        )
        .await?;
        let (distinct_crawler, distinct_version) = symmetric_graph_version(
            &database,
            "Symmetric distinct transition",
            SymmetricGraphShape::DistinctTransition,
            false,
            false,
        )
        .await?;

        assert_ne!(
            repository
                .configuration_hash(self_crawler.id(), self_version.id())
                .await?,
            repository
                .configuration_hash(distinct_crawler.id(), distinct_version.id())
                .await?
        );
        Ok(())
    }

    #[tokio::test]
    async fn symmetric_page_types_preserve_matcher_ownership_grouping()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let repository = CrawlerRepository::new(&database);
        let (shared_crawler, shared_version) = symmetric_graph_version(
            &database,
            "Symmetric shared matcher owner",
            SymmetricGraphShape::MatchersShareOwner,
            false,
            false,
        )
        .await?;
        let (split_crawler, split_version) = symmetric_graph_version(
            &database,
            "Symmetric split matcher owners",
            SymmetricGraphShape::MatchersSplitOwners,
            false,
            false,
        )
        .await?;

        assert_ne!(
            repository
                .configuration_hash(shared_crawler.id(), shared_version.id())
                .await?,
            repository
                .configuration_hash(split_crawler.id(), split_version.id())
                .await?
        );
        Ok(())
    }

    #[tokio::test]
    async fn symmetric_graph_hash_ignores_member_uuid_renaming()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let repository = CrawlerRepository::new(&database);
        let (source_crawler, source_version) = symmetric_graph_version(
            &database,
            "Symmetric UUID source",
            SymmetricGraphShape::MatchersShareOwner,
            false,
            false,
        )
        .await?;
        let (renamed_crawler, renamed_version) = symmetric_graph_version(
            &database,
            "Symmetric UUID renamed",
            SymmetricGraphShape::MatchersShareOwner,
            true,
            false,
        )
        .await?;

        assert_eq!(
            repository
                .configuration_hash(source_crawler.id(), source_version.id())
                .await?,
            repository
                .configuration_hash(renamed_crawler.id(), renamed_version.id())
                .await?
        );
        Ok(())
    }

    #[tokio::test]
    async fn symmetric_graph_hash_ignores_reverse_insertion_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let repository = CrawlerRepository::new(&database);
        let (forward_crawler, forward_version) = symmetric_graph_version(
            &database,
            "Symmetric insertion forward",
            SymmetricGraphShape::MatchersShareOwner,
            false,
            false,
        )
        .await?;
        let (reverse_crawler, reverse_version) = symmetric_graph_version(
            &database,
            "Symmetric insertion reverse",
            SymmetricGraphShape::MatchersShareOwner,
            false,
            true,
        )
        .await?;

        assert_eq!(
            repository
                .configuration_hash(forward_crawler.id(), forward_version.id())
                .await?,
            repository
                .configuration_hash(reverse_crawler.id(), reverse_version.id())
                .await?
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn semantic_hash_preserves_reference_topology_without_version_local_ids()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let repository = CrawlerRepository::new(&database);

        let (clone_crawler, source) =
            graph_version(&database, "Clone equivalent", standard_graph_shape()).await?;
        let source = repository
            .publish(
                clone_crawler.id(),
                source.id(),
                "publisher",
                "2026-08-25T00:01:00Z",
            )
            .await?
            .version;
        let source_hash = repository
            .configuration_hash(clone_crawler.id(), source.id())
            .await?;
        let cloned = repository
            .create_draft_from_published(
                clone_crawler.id(),
                source.id(),
                "operator",
                "2026-08-25T00:02:00Z",
            )
            .await?;
        assert_ne!(source.id(), cloned.id());
        assert_ne!(source.seeds()[0].id, cloned.seeds()[0].id);
        assert_ne!(source.page_type_ids(), cloned.page_type_ids());
        assert_ne!(source.transition_ids(), cloned.transition_ids());
        assert_eq!(
            source_hash,
            repository
                .configuration_hash(clone_crawler.id(), cloned.id())
                .await?
        );

        let (source_hint_crawler, source_hint) =
            graph_version(&database, "Hint A", standard_graph_shape()).await?;
        let (target_hint_crawler, target_hint) = graph_version(
            &database,
            "Hint B",
            GraphShape {
                seed_hint: PageReference::Target,
                ..standard_graph_shape()
            },
        )
        .await?;
        assert_ne!(
            repository
                .configuration_hash(source_hint_crawler.id(), source_hint.id())
                .await?,
            repository
                .configuration_hash(target_hint_crawler.id(), target_hint.id())
                .await?
        );

        let (directed_transition_crawler, directed_transition) =
            graph_version(&database, "Transition A to B", standard_graph_shape()).await?;
        let (self_transition_crawler, self_transition) = graph_version(
            &database,
            "Transition A to A",
            GraphShape {
                transition_target: TransitionTarget::Source,
                ..standard_graph_shape()
            },
        )
        .await?;
        assert_ne!(
            repository
                .configuration_hash(directed_transition_crawler.id(), directed_transition.id())
                .await?,
            repository
                .configuration_hash(self_transition_crawler.id(), self_transition.id())
                .await?
        );

        let (source_reference_crawler, source_reference) = graph_version(
            &database,
            "Configuration reference A",
            standard_graph_shape(),
        )
        .await?;
        let (target_reference_crawler, target_reference) = graph_version(
            &database,
            "Configuration reference B",
            GraphShape {
                configuration_reference: PageReference::Target,
                ..standard_graph_shape()
            },
        )
        .await?;
        assert_ne!(
            repository
                .configuration_hash(source_reference_crawler.id(), source_reference.id())
                .await?,
            repository
                .configuration_hash(target_reference_crawler.id(), target_reference.id())
                .await?
        );

        let (first_scope_crawler, mut first_scope) = graph_version(
            &database,
            "Canonicalization and scope A",
            standard_graph_shape(),
        )
        .await?;
        first_scope.set_canonicalization_policy_id(Some(CanonicalizationPolicyId::new()))?;
        first_scope.set_domain_scope_id(Some(DomainScopeId::new()))?;
        repository
            .save_draft(&first_scope, "operator", "2026-08-25T00:00:01Z")
            .await?;
        let (second_scope_crawler, mut second_scope) = graph_version(
            &database,
            "Canonicalization and scope B",
            standard_graph_shape(),
        )
        .await?;
        second_scope.set_canonicalization_policy_id(Some(CanonicalizationPolicyId::new()))?;
        second_scope.set_domain_scope_id(Some(DomainScopeId::new()))?;
        repository
            .save_draft(&second_scope, "operator", "2026-08-25T00:00:01Z")
            .await?;
        assert_ne!(
            repository
                .configuration_hash(first_scope_crawler.id(), first_scope.id())
                .await?,
            repository
                .configuration_hash(second_scope_crawler.id(), second_scope.id())
                .await?
        );

        let (forward_insertion_crawler, forward_insertion) =
            graph_version(&database, "Insertion independent A", standard_graph_shape()).await?;
        let (reversed_insertion_crawler, reversed_insertion) = graph_version(
            &database,
            "Insertion independent B",
            GraphShape {
                child_insertion: ChildInsertionOrder::Reverse,
                ..standard_graph_shape()
            },
        )
        .await?;
        assert_eq!(
            repository
                .configuration_hash(forward_insertion_crawler.id(), forward_insertion.id())
                .await?,
            repository
                .configuration_hash(reversed_insertion_crawler.id(), reversed_insertion.id())
                .await?
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn crawler_authoring_lifecycle_clones_children_and_preserves_history()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let repository = CrawlerRepository::new(&database);
        let crawler = Crawler::new("Catalog");
        repository.create(&crawler).await?;

        let page_type_id = PageTypeId::new();
        let transition_id = DiscoveryTransitionId::new();
        let mut initial = CrawlerVersion::draft(crawler.id());
        initial.set_page_type_ids(vec![page_type_id])?;
        initial.set_transition_ids(vec![transition_id])?;
        initial.add_seed(Seed::new(
            "https://example.test/catalog".parse()?,
            "https://example.test/catalog".parse()?,
        ))?;
        repository
            .save_draft(&initial, "operator", "2026-08-25T00:00:00Z")
            .await?;
        let page_type_id_string = page_type_id.to_string();
        let initial_id = initial.id().to_string();
        let configuration_json = serde_json::json!({
            "extract": "links",
            "page_type_id": page_type_id_string.clone(),
        })
        .to_string();
        let matcher_json = serde_json::to_string(&UrlMatcher::path_prefix(None, "/catalog"))?;
        let transition_json = serde_json::to_string(&persisted_transition(
            transition_id,
            page_type_id,
            page_type_id,
            "catalog links",
        ))?;
        execute_sql(&database, move |connection| {
            connection.execute(
                "INSERT INTO page_types (id, crawler_version_id, name, priority, configuration_json) VALUES (?1, ?2, 'catalog', 10, '{\"extract\":\"links\"}')",
                (page_type_id_string.as_str(), initial_id.as_str()),
            )?;
            connection.execute(
                "UPDATE page_types SET configuration_json = ?1 WHERE id = ?2",
                (configuration_json, page_type_id_string.as_str()),
            )?;
            connection.execute(
                "INSERT INTO url_matchers (id, page_type_id, ordinal, matcher_json) VALUES (?1, ?2, 0, ?3)",
                (new_opaque_id(), page_type_id_string.as_str(), matcher_json),
            )?;
            connection.execute(
                "INSERT INTO discovery_transitions (id, crawler_version_id, configuration_json) VALUES (?1, ?2, ?3)",
                (transition_id.to_string(), initial_id.as_str(), transition_json),
            )?;
            Ok(())
        })
        .await?;

        initial.publish()?;
        repository
            .publish_and_activate(&crawler, &initial, "operator", "2026-08-25T00:01:00Z")
            .await?;
        let source_hash = repository
            .configuration_hash(crawler.id(), initial.id())
            .await?;
        assert_eq!(source_hash.len(), 64);

        let cloned = repository
            .create_draft_from_published(
                crawler.id(),
                initial.id(),
                "operator",
                "2026-08-25T00:02:00Z",
            )
            .await?;
        assert_ne!(cloned.id(), initial.id());
        assert_ne!(cloned.page_type_ids(), initial.page_type_ids());
        assert_ne!(cloned.transition_ids(), initial.transition_ids());
        assert_ne!(cloned.seeds()[0].id, initial.seeds()[0].id);
        assert_eq!(
            source_hash,
            repository
                .configuration_hash(crawler.id(), cloned.id())
                .await?
        );

        let cloned_id = cloned.id().to_string();
        let cloned_page_type_id = cloned.page_type_ids()[0].to_string();
        let child_counts: (i64, i64, i64, i64) = execute_sql(&database, move |connection| {
            Ok((
                connection.query_row(
                    "SELECT COUNT(*) FROM seeds WHERE crawler_version_id = ?1",
                    [&cloned_id],
                    |row| row.get(0),
                )?,
                connection.query_row(
                    "SELECT COUNT(*) FROM page_types WHERE crawler_version_id = ?1",
                    [&cloned_id],
                    |row| row.get(0),
                )?,
                connection.query_row(
                    "SELECT COUNT(*) FROM url_matchers WHERE page_type_id = ?1",
                    [&cloned_page_type_id],
                    |row| row.get(0),
                )?,
                connection.query_row(
                    "SELECT COUNT(*) FROM discovery_transitions WHERE crawler_version_id = ?1",
                    [&cloned_id],
                    |row| row.get(0),
                )?,
            ))
        })
        .await?;
        assert_eq!(child_counts, (1, 1, 1, 1));
        let cloned_id = cloned.id().to_string();
        let cloned_page_configuration: String = execute_sql(&database, move |connection| {
            connection.query_row(
                "SELECT configuration_json FROM page_types WHERE crawler_version_id = ?1",
                [cloned_id],
                |row| row.get(0),
            )
        })
        .await?;
        assert!(cloned_page_configuration.contains(&cloned.page_type_ids()[0].to_string()));
        assert!(!cloned_page_configuration.contains(&page_type_id.to_string()));

        let mut edited = cloned.clone();
        edited.add_seed(Seed::new(
            "https://example.test/second".parse()?,
            "https://example.test/second".parse()?,
        ))?;
        repository
            .save_draft(&edited, "operator", "2026-08-25T00:03:00Z")
            .await?;
        let initial_id = initial.id().to_string();
        let source_seed_count: i64 = execute_sql(&database, move |connection| {
            connection.query_row(
                "SELECT COUNT(*) FROM seeds WHERE crawler_version_id = ?1",
                [initial_id],
                |row| row.get(0),
            )
        })
        .await?;
        assert_eq!(source_seed_count, 1);

        let published_clone = repository
            .publish(
                crawler.id(),
                cloned.id(),
                "operator",
                "2026-08-25T00:03:30Z",
            )
            .await?;
        assert_eq!(published_clone.audit.base_version_id, Some(initial.id()));
        let older_draft = repository
            .create_draft_from_published(
                crawler.id(),
                initial.id(),
                "operator",
                "2026-08-25T00:03:45Z",
            )
            .await?;
        assert_ne!(older_draft.id(), published_clone.version.id());
        assert_eq!(
            repository
                .version(crawler.id(), older_draft.id())
                .await?
                .audit
                .base_version_id,
            Some(initial.id())
        );

        repository
            .reactivate_published_typed(
                crawler.id(),
                initial.id(),
                "operator",
                "2026-08-25T00:04:00Z",
            )
            .await?;
        let pointers = repository.pointers(&crawler).await?;
        assert_eq!(
            pointers.active_published_version_id,
            Some(initial.id().to_string())
        );
        assert_eq!(
            pointers.active_draft_version_id,
            Some(older_draft.id().to_string())
        );
        let initial_id = initial.id().to_string();
        assert!(
            execute_sql(&database, move |connection| Ok(connection
                .execute(
                    "UPDATE seeds SET label = 'published-edit' WHERE crawler_version_id = ?1",
                    [initial_id],
                )
                .is_err()))
            .await?
        );
        Ok(())
    }

    #[tokio::test]
    async fn publication_audit_metadata_is_not_mixed_with_reactivation()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let repository = CrawlerRepository::new(&database);
        let crawler = Crawler::new("Audit projection");
        repository.create(&crawler).await?;
        let mut initial = repository
            .create_draft(crawler.id(), "author-a", "2026-08-25T00:00:00Z")
            .await?;
        initial.add_seed(Seed::new(
            "https://example.test/".parse()?,
            "https://example.test/".parse()?,
        ))?;
        repository
            .save_draft(&initial, "author-a", "2026-08-25T00:00:00Z")
            .await?;
        let initial = repository
            .publish(
                crawler.id(),
                initial.id(),
                "author-a",
                "2026-08-25T00:01:00Z",
            )
            .await?
            .version;
        let draft = repository
            .create_draft_from_published(
                crawler.id(),
                initial.id(),
                "author-a",
                "2026-08-25T00:02:00Z",
            )
            .await?;
        let published = repository
            .publish(crawler.id(), draft.id(), "author-a", "2026-08-25T00:03:00Z")
            .await?
            .version;
        let published_hash = repository
            .configuration_hash(crawler.id(), published.id())
            .await?;

        repository
            .reactivate_published_typed(
                crawler.id(),
                published.id(),
                "operator-b",
                "2026-08-25T00:04:00Z",
            )
            .await?;
        let read = repository.version(crawler.id(), published.id()).await?;
        assert_eq!(read.audit.actor.as_deref(), Some("author-a"));
        assert_eq!(
            read.audit.occurred_at.as_deref(),
            Some("2026-08-25T00:03:00Z")
        );
        assert_eq!(
            read.audit.config_hash.as_deref(),
            Some(published_hash.as_str())
        );
        assert_eq!(read.audit.base_version_id, Some(initial.id()));

        let (reactivation_actor, reactivation_at) = execute_sql(&database, move |connection| {
            connection.query_row(
                "SELECT actor, occurred_at FROM audit_events WHERE entity_id = ?1 AND event_type = 'CRAWLER_VERSION_REACTIVATED'",
                [published.id().to_string()],
                |row| Ok::<(String, String), rusqlite::Error>((row.get(0)?, row.get(1)?)),
            )
        })
        .await?;
        assert_eq!(reactivation_actor, "operator-b");
        assert_eq!(reactivation_at, "2026-08-25T00:04:00Z");
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn semantic_hash_refinement_scales_with_realistic_child_counts()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let repository = CrawlerRepository::new(&database);
        let crawler = Crawler::new("Scalable semantic hash");
        repository.create(&crawler).await?;
        let page_ids = (0..12).map(|_| PageTypeId::new()).collect::<Vec<_>>();
        let transition_ids = (0..6)
            .map(|_| DiscoveryTransitionId::new())
            .collect::<Vec<_>>();
        let mut seed = Seed::new(
            "https://example.test/catalog".parse()?,
            "https://example.test/catalog".parse()?,
        );
        seed.entry_page_type_hint = page_ids.first().copied();
        let mut version = CrawlerVersion::draft(crawler.id());
        version.set_page_type_ids(page_ids.clone())?;
        version.set_transition_ids(transition_ids.clone())?;
        version.add_seed(seed)?;
        repository
            .save_draft(&version, "operator", "2026-08-25T00:00:00Z")
            .await?;

        let version_id = version.id().to_string();
        let mut page_inserts = Vec::new();
        let mut matcher_inserts = Vec::new();
        for index in (0..page_ids.len()).rev() {
            let page_id = page_ids[index];
            let next_page_id = page_ids[(index + 1) % page_ids.len()];
            page_inserts.push((
                page_id.to_string(),
                version_id.clone(),
                format!("Page {index}"),
                i64::try_from(index)?,
                serde_json::json!({"next_page_type_id": next_page_id.to_string()}).to_string(),
            ));
            for ordinal in 0..2_i64 {
                let matcher = UrlMatcher::path_prefix(None, format!("/page-{index}/{ordinal}"));
                matcher_inserts.push((
                    new_opaque_id(),
                    page_id.to_string(),
                    ordinal,
                    serde_json::to_string(&matcher)?,
                ));
            }
        }
        let mut transition_inserts = Vec::new();
        for (index, transition_id) in transition_ids.iter().enumerate() {
            transition_inserts.push((
                transition_id.to_string(),
                version_id.clone(),
                serde_json::json!({
                    "id": transition_id.to_string(),
                    "source_page_type_id": page_ids[index].to_string(),
                    "target_page_type_id": page_ids[(index + 1) % page_ids.len()].to_string(),
                    "name": format!("transition {index}"),
                    "enabled": true,
                    "link_selector": "a[href]",
                    "url_constraints": null,
                    "priority": 10,
                    "budget": {
                        "max_links_per_source_page": 10,
                        "total_budget": 100,
                        "depth_contribution": 1
                    },
                    "deduplicate": true,
                    "latest_test_evidence_id": null
                })
                .to_string(),
            ));
        }
        execute_sql(&database, move |connection| {
            for (page_id, version_id, name, priority, configuration_json) in page_inserts {
                connection.execute(
                    "INSERT INTO page_types (id, crawler_version_id, name, priority, configuration_json) VALUES (?1, ?2, ?3, ?4, ?5)",
                    (page_id, version_id, name, priority, configuration_json),
                )?;
            }
            for (matcher_id, page_id, ordinal, matcher_json) in matcher_inserts {
                connection.execute(
                    "INSERT INTO url_matchers (id, page_type_id, ordinal, matcher_json) VALUES (?1, ?2, ?3, ?4)",
                    (matcher_id, page_id, ordinal, matcher_json),
                )?;
            }
            for (transition_id, version_id, configuration_json) in transition_inserts {
                connection.execute(
                    "INSERT INTO discovery_transitions (id, crawler_version_id, configuration_json) VALUES (?1, ?2, ?3)",
                    (transition_id, version_id, configuration_json),
                )?;
            }
            Ok(())
        })
        .await?;

        let first = repository
            .configuration_hash(crawler.id(), version.id())
            .await?;
        let second = repository
            .configuration_hash(crawler.id(), version.id())
            .await?;
        assert_eq!(first, second);
        assert_eq!(
            count_all(&database, "SELECT COUNT(*) FROM page_types").await?,
            12
        );
        assert_eq!(
            count_all(&database, "SELECT COUNT(*) FROM url_matchers").await?,
            24
        );
        assert_eq!(
            count_all(&database, "SELECT COUNT(*) FROM discovery_transitions").await?,
            6
        );
        Ok(())
    }

    #[tokio::test]
    async fn serialized_initial_draft_winner_is_classified_without_external_transaction()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let crawler = Crawler::new("Serialized initial contention");
        let repository = CrawlerRepository::new(&database);
        repository.create(&crawler).await?;
        let winner = repository
            .create_draft(crawler.id(), "winner", "2026-08-25T00:00:00Z")
            .await?;
        let loser = repository
            .create_draft(crawler.id(), "loser", "2026-08-25T00:00:01Z")
            .await;
        assert!(matches!(
            loser,
            Err(CrawlerRepositoryError::ActiveDraftExists
                | CrawlerRepositoryError::ConcurrentVersionTransition)
        ));

        assert_eq!(
            repository.pointers(&crawler).await?.active_draft_version_id,
            Some(winner.id().to_string())
        );
        assert_eq!(
            count_by_crawler(
                &database,
                "SELECT COUNT(*) FROM crawler_versions WHERE crawler_id = ?1",
                crawler.id()
            )
            .await?,
            1
        );
        assert_eq!(count_all(&database, "SELECT COUNT(*) FROM seeds").await?, 0);
        assert_eq!(
            count_all(&database, "SELECT COUNT(*) FROM page_types").await?,
            0
        );
        assert_eq!(
            count_all(&database, "SELECT COUNT(*) FROM url_matchers").await?,
            0
        );
        assert_eq!(
            count_all(&database, "SELECT COUNT(*) FROM discovery_transitions").await?,
            0
        );
        assert_eq!(
            count_all(
                &database,
                "SELECT COUNT(*) FROM audit_events WHERE entity_type = 'CRAWLER_VERSION'"
            )
            .await?,
            1
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn serialized_published_clone_winner_is_classified_without_external_transaction()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let repository = CrawlerRepository::new(&database);
        let crawler = Crawler::new("Delayed clone contention");
        repository.create(&crawler).await?;
        let mut published = repository
            .create_draft(crawler.id(), "author", "2026-08-25T00:00:00Z")
            .await?;
        published.add_seed(Seed::new(
            "https://example.test/".parse()?,
            "https://example.test/".parse()?,
        ))?;
        repository
            .save_draft(&published, "author", "2026-08-25T00:00:00Z")
            .await?;
        repository
            .publish(
                crawler.id(),
                published.id(),
                "author",
                "2026-08-25T00:00:01Z",
            )
            .await?;

        let winner = repository
            .create_draft_from_published(
                crawler.id(),
                published.id(),
                "winner",
                "2026-08-25T00:00:02Z",
            )
            .await?;
        let loser = repository
            .create_draft_from_published(
                crawler.id(),
                published.id(),
                "loser",
                "2026-08-25T00:00:03Z",
            )
            .await;
        assert!(matches!(
            loser,
            Err(CrawlerRepositoryError::ActiveDraftExists
                | CrawlerRepositoryError::ConcurrentVersionTransition)
        ));
        assert_eq!(
            repository.pointers(&crawler).await?.active_draft_version_id,
            Some(winner.id().to_string())
        );
        assert_eq!(
            count_by_crawler(
                &database,
                "SELECT COUNT(*) FROM crawler_versions WHERE crawler_id = ?1",
                crawler.id()
            )
            .await?,
            2
        );
        assert_eq!(count_all(&database, "SELECT COUNT(*) FROM seeds").await?, 2);
        assert_eq!(
            count_all(&database, "SELECT COUNT(*) FROM page_types").await?,
            0
        );
        assert_eq!(
            count_all(&database, "SELECT COUNT(*) FROM url_matchers").await?,
            0
        );
        assert_eq!(
            count_all(&database, "SELECT COUNT(*) FROM discovery_transitions").await?,
            0
        );
        assert_eq!(
            count_all(
                &database,
                "SELECT COUNT(*) FROM audit_events WHERE entity_type = 'CRAWLER_VERSION'"
            )
            .await?,
            3
        );
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_initial_draft_creation_has_one_winner()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let crawler = Crawler::new("Concurrent");
        CrawlerRepository::new(&database).create(&crawler).await?;
        let left_database = database.clone();
        let right_database = database.clone();
        let left = CrawlerRepository::new(&left_database);
        let right = CrawlerRepository::new(&right_database);
        let (left, right) = tokio::join!(
            left.create_draft(crawler.id(), "left", "2026-08-25T00:00:00Z"),
            right.create_draft(crawler.id(), "right", "2026-08-25T00:00:01Z"),
        );
        assert_eq!(
            usize::from(u8::from(left.is_ok())) + usize::from(u8::from(right.is_ok())),
            1
        );
        assert!(
            left.is_ok()
                || matches!(
                    left,
                    Err(CrawlerRepositoryError::ActiveDraftExists
                        | CrawlerRepositoryError::ConcurrentVersionTransition)
                )
        );
        assert!(
            right.is_ok()
                || matches!(
                    right,
                    Err(CrawlerRepositoryError::ActiveDraftExists
                        | CrawlerRepositoryError::ConcurrentVersionTransition)
                )
        );
        let repository = CrawlerRepository::new(&database);
        assert!(
            repository
                .pointers(&crawler)
                .await?
                .active_draft_version_id
                .is_some()
        );
        assert_eq!(
            count_by_crawler(
                &database,
                "SELECT COUNT(*) FROM crawler_versions WHERE crawler_id = ?1",
                crawler.id()
            )
            .await?,
            1
        );
        assert_eq!(count_all(&database, "SELECT COUNT(*) FROM seeds").await?, 0);
        assert_eq!(
            count_all(&database, "SELECT COUNT(*) FROM page_types").await?,
            0
        );
        assert_eq!(
            count_all(&database, "SELECT COUNT(*) FROM url_matchers").await?,
            0
        );
        assert_eq!(
            count_all(&database, "SELECT COUNT(*) FROM discovery_transitions").await?,
            0
        );
        assert_eq!(
            count_all(
                &database,
                "SELECT COUNT(*) FROM audit_events WHERE entity_type = 'CRAWLER_VERSION'"
            )
            .await?,
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_draft_from_published_creation_has_one_winner()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let crawler = Crawler::new("Clone concurrency");
        let repository = CrawlerRepository::new(&database);
        repository.create(&crawler).await?;
        let mut initial = repository
            .create_draft(crawler.id(), "operator", "2026-08-25T00:00:00Z")
            .await?;
        initial.add_seed(Seed::new(
            "https://example.test/".parse()?,
            "https://example.test/".parse()?,
        ))?;
        repository
            .save_draft(&initial, "operator", "2026-08-25T00:00:00Z")
            .await?;
        repository
            .publish(
                crawler.id(),
                initial.id(),
                "operator",
                "2026-08-25T00:00:01Z",
            )
            .await?;

        let left_database = database.clone();
        let right_database = database.clone();
        let left_repository = CrawlerRepository::new(&left_database);
        let right_repository = CrawlerRepository::new(&right_database);
        let (left, right) = tokio::join!(
            left_repository.create_draft_from_published(
                crawler.id(),
                initial.id(),
                "left",
                "2026-08-25T00:00:02Z",
            ),
            right_repository.create_draft_from_published(
                crawler.id(),
                initial.id(),
                "right",
                "2026-08-25T00:00:03Z",
            ),
        );
        assert_eq!(
            usize::from(u8::from(left.is_ok())) + usize::from(u8::from(right.is_ok())),
            1
        );
        assert!(
            left.is_ok()
                || matches!(
                    left,
                    Err(CrawlerRepositoryError::ActiveDraftExists
                        | CrawlerRepositoryError::ConcurrentVersionTransition)
                )
        );
        assert!(
            right.is_ok()
                || matches!(
                    right,
                    Err(CrawlerRepositoryError::ActiveDraftExists
                        | CrawlerRepositoryError::ConcurrentVersionTransition)
                )
        );

        let pointers = repository.pointers(&crawler).await?;
        assert!(pointers.active_draft_version_id.is_some());
        assert_eq!(
            count_by_crawler(
                &database,
                "SELECT COUNT(*) FROM crawler_versions WHERE crawler_id = ?1",
                crawler.id()
            )
            .await?,
            2
        );
        assert_eq!(count_all(&database, "SELECT COUNT(*) FROM seeds").await?, 2);
        assert_eq!(
            count_all(&database, "SELECT COUNT(*) FROM page_types").await?,
            0
        );
        assert_eq!(
            count_all(&database, "SELECT COUNT(*) FROM url_matchers").await?,
            0
        );
        assert_eq!(
            count_all(&database, "SELECT COUNT(*) FROM discovery_transitions").await?,
            0
        );
        assert_eq!(
            count_all(
                &database,
                "SELECT COUNT(*) FROM audit_events WHERE entity_type = 'CRAWLER_VERSION'"
            )
            .await?,
            3
        );
        Ok(())
    }

    #[tokio::test]
    async fn corrupt_semantic_configuration_is_not_not_found()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let repository = CrawlerRepository::new(&database);
        let crawler = Crawler::new("Corrupt");
        repository.create(&crawler).await?;
        let draft = repository
            .create_draft(crawler.id(), "operator", "2026-08-25T00:00:00Z")
            .await?;
        let draft_id = draft.id().to_string();
        execute_sql(&database, move |connection| {
            connection.execute(
                "UPDATE crawler_versions SET semantic_configuration_json = '{bad-json}' WHERE id = ?1",
                [draft_id],
            )
        })
        .await?;
        assert!(matches!(
            repository.version(crawler.id(), draft.id()).await,
            Err(CrawlerRepositoryError::CorruptState)
        ));
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn page_type_and_matcher_authoring_is_atomic_and_hashes_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let repository = CrawlerRepository::new(&database);
        let crawler = Crawler::new("Page Type authoring");
        repository.create(&crawler).await?;
        let draft = repository
            .create_draft(crawler.id(), "operator", "2026-08-25T01:00:00Z")
            .await?;

        let page = repository
            .create_page_type(
                crawler.id(),
                draft.id(),
                "Products",
                1,
                "operator",
                "2026-08-25T01:01:00Z",
            )
            .await?;
        assert_eq!(
            repository
                .version(crawler.id(), draft.id())
                .await?
                .version
                .page_type_ids(),
            &[page.id]
        );
        let first_hash = repository
            .configuration_hash(crawler.id(), draft.id())
            .await?;

        let page_id = page.id.to_string();
        execute_sql(&database, move |connection| {
            connection.execute(
                "UPDATE page_types SET configuration_json = ?1 WHERE id = ?2",
                (r#"{"future":{"flag":true}}"#, page_id),
            )
        })
        .await?;
        let matcher = repository
            .create_url_matcher(
                crawler.id(),
                draft.id(),
                page.id,
                &UrlMatcher::path_prefix(Some("example.test".into()), "/products"),
                "operator",
                "2026-08-25T01:02:00Z",
            )
            .await?;
        assert_eq!(matcher.ordinal, 0);
        let second_hash = repository
            .configuration_hash(crawler.id(), draft.id())
            .await?;
        assert_ne!(first_hash, second_hash);

        let updated = repository
            .update_page_type(
                crawler.id(),
                draft.id(),
                page.id,
                "Products v2",
                9,
                "operator",
                "2026-08-25T01:03:00Z",
            )
            .await?;
        assert_eq!(updated.name, "Products v2");
        assert_eq!(updated.priority, 9);
        let page_id = page.id.to_string();
        let preserved_configuration: String = execute_sql(&database, move |connection| {
            connection.query_row(
                "SELECT configuration_json FROM page_types WHERE id = ?1",
                [page_id],
                |row| row.get(0),
            )
        })
        .await?;
        assert_eq!(preserved_configuration, r#"{"future":{"flag":true}}"#);
        let third_hash = repository
            .configuration_hash(crawler.id(), draft.id())
            .await?;
        assert_ne!(second_hash, third_hash);

        repository
            .update_url_matcher(
                crawler.id(),
                draft.id(),
                page.id,
                &matcher.id,
                &UrlMatcher::exact_url("https://example.test/products/1".parse()?),
                "operator",
                "2026-08-25T01:04:00Z",
            )
            .await?;
        let fourth_hash = repository
            .configuration_hash(crawler.id(), draft.id())
            .await?;
        assert_ne!(third_hash, fourth_hash);
        repository
            .delete_url_matcher(
                crawler.id(),
                draft.id(),
                page.id,
                &matcher.id,
                "operator",
                "2026-08-25T01:05:00Z",
            )
            .await?;
        repository
            .delete_page_type(
                crawler.id(),
                draft.id(),
                page.id,
                "operator",
                "2026-08-25T01:06:00Z",
            )
            .await?;
        assert!(
            repository
                .list_page_types(crawler.id(), draft.id())
                .await?
                .is_empty()
        );
        assert!(
            repository
                .version(crawler.id(), draft.id())
                .await?
                .version
                .page_type_ids()
                .is_empty()
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn page_type_delete_blocks_known_references_and_published_triggers_remain_strong()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let repository = CrawlerRepository::new(&database);
        let crawler = Crawler::new("Page Type safety");
        repository.create(&crawler).await?;
        let draft = repository
            .create_draft(crawler.id(), "operator", "2026-08-25T02:00:00Z")
            .await?;
        let page = repository
            .create_page_type(
                crawler.id(),
                draft.id(),
                "Seeded",
                0,
                "operator",
                "2026-08-25T02:01:00Z",
            )
            .await?;
        let matcher = repository
            .create_url_matcher(
                crawler.id(),
                draft.id(),
                page.id,
                &UrlMatcher::path_prefix(None, "/seeded"),
                "operator",
                "2026-08-25T02:02:00Z",
            )
            .await?;
        let mut version = repository.version(crawler.id(), draft.id()).await?.version;
        let mut seed = Seed::new(
            "https://example.test/seeded".parse()?,
            "https://example.test/seeded".parse()?,
        );
        seed.entry_page_type_hint = Some(page.id);
        version.add_seed(seed)?;
        repository
            .save_draft(&version, "operator", "2026-08-25T02:02:30Z")
            .await?;
        assert!(matches!(
            repository
                .delete_page_type(
                    crawler.id(),
                    draft.id(),
                    page.id,
                    "operator",
                    "2026-08-25T02:03:00Z",
                )
                .await,
            Err(CrawlerRepositoryError::PageTypeInUse)
        ));

        let published = repository
            .publish(crawler.id(), draft.id(), "operator", "2026-08-25T02:04:00Z")
            .await?
            .version;
        let page_id = page.id.to_string();
        let published_id = published.id().to_string();
        let matcher_id = matcher.id.as_str().to_owned();
        let matcher_json = serde_json::to_string(&matcher.matcher)?;
        let (insert_page, update_page, delete_page, insert_matcher, update_matcher, delete_matcher) =
            execute_sql(&database, move |connection| {
                Ok((
                    connection
                        .execute(
                            "INSERT INTO page_types (id, crawler_version_id, name, priority, configuration_json) VALUES (?1, ?2, 'blocked', 0, '{}')",
                            (new_opaque_id(), published_id),
                        )
                        .is_err(),
                    connection
                        .execute(
                            "UPDATE page_types SET name = 'blocked' WHERE id = ?1",
                            [page_id.as_str()],
                        )
                        .is_err(),
                    connection
                        .execute("DELETE FROM page_types WHERE id = ?1", [page_id.as_str()])
                        .is_err(),
                    connection
                        .execute(
                            "INSERT INTO url_matchers (id, page_type_id, ordinal, matcher_json) VALUES (?1, ?2, 1, ?3)",
                            (new_opaque_id(), page_id.as_str(), matcher_json.as_str()),
                        )
                        .is_err(),
                    connection
                        .execute(
                            "UPDATE url_matchers SET matcher_json = ?1 WHERE id = ?2",
                            (matcher_json.as_str(), matcher_id.as_str()),
                        )
                        .is_err(),
                    connection
                        .execute("DELETE FROM url_matchers WHERE id = ?1", [matcher_id.as_str()])
                        .is_err(),
                ))
            })
            .await?;
        assert!(
            insert_page
                && update_page
                && delete_page
                && insert_matcher
                && update_matcher
                && delete_matcher
        );
        Ok(())
    }

    #[tokio::test]
    async fn semantically_invalid_persisted_matchers_are_corrupt_state()
    -> Result<(), Box<dyn std::error::Error>> {
        for matcher_json in [
            r#"{"Regex":{"pattern":"["}}"#,
            r#"{"PathGlob":{"host":null,"pattern":""}}"#,
            r#"{"PathPrefix":{"host":null,"prefix":"products"}}"#,
            r#"{"ExactHostPathTemplate":{"host":"bad host","path_template":"/products/{id}","query":{}}}"#,
            r#"{"ExactHostPathTemplate":{"host":"example.test","path_template":"/products/{id","query":{}}}"#,
        ] {
            let database = database().await?;
            let repository = CrawlerRepository::new(&database);
            let crawler = Crawler::new("Malformed matcher");
            repository.create(&crawler).await?;
            let draft = repository
                .create_draft(crawler.id(), "operator", "2026-08-25T03:00:00Z")
                .await?;
            let page = repository
                .create_page_type(
                    crawler.id(),
                    draft.id(),
                    "Broken",
                    0,
                    "operator",
                    "2026-08-25T03:01:00Z",
                )
                .await?;
            let page_id = page.id.to_string();
            let matcher_json = matcher_json.to_owned();
            execute_sql(&database, move |connection| {
                connection.execute(
                    "INSERT INTO url_matchers (id, page_type_id, ordinal, matcher_json) VALUES (?1, ?2, 0, ?3)",
                    (new_opaque_id(), page_id, matcher_json),
                )
            })
            .await?;
            assert!(matches!(
                repository.list_page_types(crawler.id(), draft.id()).await,
                Err(CrawlerRepositoryError::CorruptState)
            ));
            assert!(matches!(
                repository
                    .configuration_hash(crawler.id(), draft.id())
                    .await,
                Err(CrawlerRepositoryError::CorruptState)
            ));
        }
        Ok(())
    }

    #[tokio::test]
    async fn repository_write_rejects_an_invalid_legacy_matcher_value()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let repository = CrawlerRepository::new(&database);
        let crawler = Crawler::new("Invalid direct matcher");
        repository.create(&crawler).await?;
        let draft = repository
            .create_draft(crawler.id(), "operator", "2026-08-25T03:10:00Z")
            .await?;
        let page = repository
            .create_page_type(
                crawler.id(),
                draft.id(),
                "Products",
                0,
                "operator",
                "2026-08-25T03:11:00Z",
            )
            .await?;
        let invalid = UrlMatcher::path_prefix(Some("bad host".into()), "products");
        assert!(matches!(
            repository
                .create_url_matcher(
                    crawler.id(),
                    draft.id(),
                    page.id,
                    &invalid,
                    "operator",
                    "2026-08-25T03:12:00Z",
                )
                .await,
            Err(CrawlerRepositoryError::InvalidUrlMatcherDefinition)
        ));
        assert!(
            repository
                .list_url_matchers(crawler.id(), draft.id(), page.id)
                .await?
                .is_empty()
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn matcher_ordinal_is_presentation_only_but_definition_and_multiplicity_hash()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let repository = CrawlerRepository::new(&database);
        let crawler = Crawler::new("Matcher ordinal semantics");
        repository.create(&crawler).await?;
        let draft = repository
            .create_draft(crawler.id(), "operator", "2026-08-25T03:20:00Z")
            .await?;
        let page = repository
            .create_page_type(
                crawler.id(),
                draft.id(),
                "Products",
                0,
                "operator",
                "2026-08-25T03:21:00Z",
            )
            .await?;
        let prefix = repository
            .create_url_matcher(
                crawler.id(),
                draft.id(),
                page.id,
                &UrlMatcher::path_prefix(Some("example.test".into()), "/products"),
                "operator",
                "2026-08-25T03:22:00Z",
            )
            .await?;
        let exact = repository
            .create_url_matcher(
                crawler.id(),
                draft.id(),
                page.id,
                &UrlMatcher::exact_url("https://example.test/products/42".parse()?),
                "operator",
                "2026-08-25T03:23:00Z",
            )
            .await?;
        let target: url::Url = "https://example.test/products/42".parse()?;
        let before_decision = resolve_page_type(
            &target,
            &repository
                .list_page_types(crawler.id(), draft.id())
                .await?
                .iter()
                .map(PageTypeRecord::domain_page_type)
                .collect::<Vec<_>>(),
        );
        let before_hash = repository
            .configuration_hash(crawler.id(), draft.id())
            .await?;

        let prefix_id = prefix.id.as_str().to_owned();
        let exact_id = exact.id.as_str().to_owned();
        execute_sql(&database, move |connection| {
            connection.execute(
                "UPDATE url_matchers SET ordinal = CASE id WHEN ?1 THEN 10 WHEN ?2 THEN 0 END WHERE id IN (?1, ?2)",
                (prefix_id, exact_id),
            )
        })
        .await?;
        let after_hash = repository
            .configuration_hash(crawler.id(), draft.id())
            .await?;
        assert_eq!(before_hash, after_hash);
        let ordered = repository
            .list_url_matchers(crawler.id(), draft.id(), page.id)
            .await?;
        assert_eq!(
            ordered
                .iter()
                .map(|matcher| matcher.id.as_str())
                .collect::<Vec<_>>(),
            vec![exact.id.as_str(), prefix.id.as_str()]
        );
        let after_decision = resolve_page_type(
            &target,
            &repository
                .list_page_types(crawler.id(), draft.id())
                .await?
                .iter()
                .map(PageTypeRecord::domain_page_type)
                .collect::<Vec<_>>(),
        );
        assert_eq!(before_decision, after_decision);

        let changed_matcher_json = serde_json::to_string(&UrlMatcher::exact_url(
            "https://example.test/products/99".parse()?,
        ))?;
        let exact_id = exact.id.as_str().to_owned();
        execute_sql(&database, move |connection| {
            connection.execute(
                "UPDATE url_matchers SET matcher_json = ?1 WHERE id = ?2",
                (changed_matcher_json, exact_id),
            )
        })
        .await?;
        let definition_hash = repository
            .configuration_hash(crawler.id(), draft.id())
            .await?;
        assert_ne!(after_hash, definition_hash);

        let exact_id = exact.id.as_str().to_owned();
        execute_sql(&database, move |connection| {
            connection.execute("DELETE FROM url_matchers WHERE id = ?1", [exact_id])
        })
        .await?;
        assert_ne!(
            definition_hash,
            repository
                .configuration_hash(crawler.id(), draft.id())
                .await?
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines, clippy::expect_used)]
    async fn discovery_policy_persistence_is_immutable_and_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let repository = CrawlerRepository::new(&database);
        let crawler = Crawler::new("Discovery policy integrity");
        repository.create(&crawler).await?;
        let mut draft = repository
            .create_draft(crawler.id(), "operator", "now")
            .await?;
        draft.add_seed(Seed::new(
            "https://example.test/".parse()?,
            "https://example.test/".parse()?,
        ))?;
        repository.save_draft(&draft, "operator", "now").await?;
        let page = repository
            .create_page_type(crawler.id(), draft.id(), "Cycle", 1, "operator", "now")
            .await?;
        let transition_id = DiscoveryTransitionId::new();
        repository
            .create_discovery_transition(
                crawler.id(),
                draft.id(),
                &persisted_transition(transition_id, page.id, page.id, "cycle"),
                "operator",
                "now",
            )
            .await?;
        let _published = repository
            .publish(crawler.id(), draft.id(), "operator", "now")
            .await?;
        let immutable_transition_json = serde_json::to_string(&persisted_transition(
            transition_id,
            page.id,
            page.id,
            "changed",
        ))?;
        let transition_id_string = transition_id.to_string();
        assert!(
            execute_sql(&database, move |connection| Ok(connection
                .execute(
                    "UPDATE discovery_transitions SET configuration_json = ?1 WHERE id = ?2",
                    (immutable_transition_json, transition_id_string),
                )
                .is_err()))
            .await?
        );

        let corrupted_draft = repository
            .create_draft(crawler.id(), "operator", "now")
            .await?;
        let corrupt_page = repository
            .create_page_type(
                crawler.id(),
                corrupted_draft.id(),
                "Corrupt transition source",
                1,
                "operator",
                "now",
            )
            .await?;
        let corrupt_transition_id = DiscoveryTransitionId::new();
        let corrupt_transition = persisted_transition(
            corrupt_transition_id,
            corrupt_page.id,
            corrupt_page.id,
            "corruptible",
        );
        repository
            .create_discovery_transition(
                crawler.id(),
                corrupted_draft.id(),
                &corrupt_transition,
                "operator",
                "now",
            )
            .await?;
        let corrupt_transition_id_string = corrupt_transition_id.to_string();
        execute_sql(&database, move |connection| {
            connection.execute(
                "UPDATE discovery_transitions SET configuration_json = '{}' WHERE id = ?1",
                [corrupt_transition_id_string],
            )
        })
        .await?;
        assert!(matches!(
            repository
                .list_discovery_transitions(crawler.id(), corrupted_draft.id())
                .await,
            Err(CrawlerRepositoryError::CorruptState)
        ));
        let corrupt_transition_json = serde_json::to_string(&corrupt_transition)?;
        let corrupt_transition_id_string = corrupt_transition_id.to_string();
        execute_sql(&database, move |connection| {
            connection.execute(
                "UPDATE discovery_transitions SET configuration_json = ?1 WHERE id = ?2",
                (corrupt_transition_json, corrupt_transition_id_string),
            )
        })
        .await?;
        let mut corrupted_configuration = serde_json::to_value(
            &repository
                .version(crawler.id(), corrupted_draft.id())
                .await?
                .version,
        )?;
        corrupted_configuration["guardrails"]["version"] = serde_json::json!(999);
        let corrupted_configuration_json = corrupted_configuration.to_string();
        let corrupted_draft_id = corrupted_draft.id().to_string();
        execute_sql(&database, move |connection| {
            connection.execute(
                "UPDATE crawler_versions SET semantic_configuration_json = ?1 WHERE id = ?2",
                (corrupted_configuration_json, corrupted_draft_id),
            )
        })
        .await?;
        assert!(matches!(
            repository
                .crawler_version_guardrails(crawler.id(), corrupted_draft.id())
                .await,
            Err(CrawlerRepositoryError::CorruptState)
        ));

        Ok(())
    }
}
