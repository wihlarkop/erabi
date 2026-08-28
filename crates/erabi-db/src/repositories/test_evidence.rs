use erabi_domain::{
    CrawlerVersionId, DiscoveryTransition, PageTypeId, TestEvidence, TestEvidenceId,
};
use turso::{Connection, Row, transaction::TransactionBehavior};
use uuid::Uuid;

use crate::{DbError, ErabiDatabase};

use super::crawler::{
    CrawlerRepositoryError, current_draft_semantic_hash_in_transaction,
    semantic_hash_for_version_in_connection,
};

/// A durable evidence row plus whether its historical hash still matches the
/// requested version's current semantic configuration.
#[derive(Clone, Debug)]
pub struct TestEvidenceRecord {
    pub evidence: TestEvidence,
    pub matches_current_configuration: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum TestEvidenceRepositoryError {
    #[error("Crawler repository operation failed")]
    Crawler(#[source] CrawlerRepositoryError),
    #[error("TestEvidence was not found")]
    TestEvidenceNotFound,
    #[error("TestEvidence does not belong to the requested CrawlerVersion")]
    TestEvidenceNotOwnedByVersion,
    #[error("Artifact was not found")]
    ArtifactNotFound,
    #[error("Draft configuration changed during Test Lab execution")]
    ConfigurationChanged,
    #[error("durable TestEvidence state is invalid")]
    CorruptState,
    #[error("database operation failed")]
    Database(#[source] DbError),
}

impl From<CrawlerRepositoryError> for TestEvidenceRepositoryError {
    fn from(error: CrawlerRepositoryError) -> Self {
        Self::Crawler(error)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TestEvidenceRepository<'database> {
    database: &'database ErabiDatabase,
}

impl<'database> TestEvidenceRepository<'database> {
    #[must_use]
    pub const fn new(database: &'database ErabiDatabase) -> Self {
        Self { database }
    }

    /// Persists one server-generated historical evidence snapshot. The hash
    /// check, reference validation, evidence insert, and transition metadata
    /// attachment use the same immediate transaction.
    ///
    /// # Errors
    /// Returns a typed conflict when the Draft changed, or a corruption,
    /// ownership, reference, or database error when durable state is invalid.
    pub async fn persist_if_configuration_matches(
        &self,
        crawler_id: erabi_domain::CrawlerId,
        evidence: &TestEvidence,
    ) -> Result<(), TestEvidenceRepositoryError> {
        evidence
            .validate()
            .map_err(|_| TestEvidenceRepositoryError::CorruptState)?;
        let mut connection = self
            .database
            .connection()
            .await
            .map_err(TestEvidenceRepositoryError::Database)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(|error| TestEvidenceRepositoryError::Database(error.into()))?;
        let result = persist_in_transaction(&transaction, crawler_id, evidence).await;
        match result {
            Ok(()) => transaction
                .commit()
                .await
                .map_err(|error| TestEvidenceRepositoryError::Database(error.into())),
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    /// Reads one evidence row and validates its duplicated projections.
    ///
    /// # Errors
    /// Returns a typed not-found, ownership, corruption, or database error.
    pub async fn read(
        &self,
        crawler_id: erabi_domain::CrawlerId,
        version_id: CrawlerVersionId,
        evidence_id: TestEvidenceId,
    ) -> Result<TestEvidenceRecord, TestEvidenceRepositoryError> {
        let connection = self
            .database
            .connection()
            .await
            .map_err(TestEvidenceRepositoryError::Database)?;
        let mut rows = connection
            .query(
                "SELECT id, crawler_version_id, evidence_json, executed_at FROM test_evidence WHERE id = ?1",
                [evidence_id.to_string()],
            )
            .await
            .map_err(Self::database)?;
        let Some(row) = rows.next().await.map_err(Self::database)? else {
            return Err(TestEvidenceRepositoryError::TestEvidenceNotFound);
        };
        let stored = read_row(&row)?;
        if stored.evidence.crawler_version_id != version_id {
            return Err(TestEvidenceRepositoryError::TestEvidenceNotOwnedByVersion);
        }
        let current_hash =
            semantic_hash_for_version_in_connection(&connection, crawler_id, version_id)
                .await
                .map_err(TestEvidenceRepositoryError::from)?;
        Ok(TestEvidenceRecord {
            matches_current_configuration: stored.evidence.config_hash == current_hash,
            evidence: stored.evidence,
        })
    }

    /// Lists evidence in deterministic execution-time/UUID order.
    ///
    /// # Errors
    /// Returns a typed corruption, ownership, or database error.
    pub async fn list(
        &self,
        crawler_id: erabi_domain::CrawlerId,
        version_id: CrawlerVersionId,
    ) -> Result<Vec<TestEvidenceRecord>, TestEvidenceRepositoryError> {
        let connection = self
            .database
            .connection()
            .await
            .map_err(TestEvidenceRepositoryError::Database)?;
        let current_hash =
            semantic_hash_for_version_in_connection(&connection, crawler_id, version_id)
                .await
                .map_err(TestEvidenceRepositoryError::from)?;
        let mut rows = connection
            .query(
                "SELECT id, crawler_version_id, evidence_json, executed_at FROM test_evidence WHERE crawler_version_id = ?1 ORDER BY executed_at COLLATE BINARY, id COLLATE BINARY",
                [version_id.to_string()],
            )
            .await
            .map_err(Self::database)?;
        let mut records = Vec::new();
        while let Some(row) = rows.next().await.map_err(Self::database)? {
            let stored = read_row(&row)?;
            if stored.evidence.crawler_version_id != version_id {
                return Err(TestEvidenceRepositoryError::CorruptState);
            }
            records.push(TestEvidenceRecord {
                matches_current_configuration: stored.evidence.config_hash == current_hash,
                evidence: stored.evidence,
            });
        }
        Ok(records)
    }

    fn database(error: turso::Error) -> TestEvidenceRepositoryError {
        TestEvidenceRepositoryError::Database(error.into())
    }
}

struct StoredEvidence {
    evidence: TestEvidence,
}

fn read_row(row: &Row) -> Result<StoredEvidence, TestEvidenceRepositoryError> {
    let row_id = parse_evidence_id(
        &row.get::<String>(0)
            .map_err(|error| TestEvidenceRepositoryError::Database(error.into()))?,
    )?;
    let row_version_id = parse_version_id(
        &row.get::<String>(1)
            .map_err(|error| TestEvidenceRepositoryError::Database(error.into()))?,
    )?;
    let payload = row
        .get::<String>(2)
        .map_err(|error| TestEvidenceRepositoryError::Database(error.into()))?;
    let executed_at = row
        .get::<String>(3)
        .map_err(|error| TestEvidenceRepositoryError::Database(error.into()))?;
    let evidence: TestEvidence =
        serde_json::from_str(&payload).map_err(|_| TestEvidenceRepositoryError::CorruptState)?;
    if evidence.id != row_id
        || evidence.crawler_version_id != row_version_id
        || evidence.executed_at != executed_at
    {
        return Err(TestEvidenceRepositoryError::CorruptState);
    }
    Ok(StoredEvidence { evidence })
}

async fn persist_in_transaction(
    connection: &Connection,
    crawler_id: erabi_domain::CrawlerId,
    evidence: &TestEvidence,
) -> Result<(), TestEvidenceRepositoryError> {
    let current_hash = current_draft_semantic_hash_in_transaction(
        connection,
        crawler_id,
        evidence.crawler_version_id,
    )
    .await
    .map_err(TestEvidenceRepositoryError::from)?;
    if current_hash != evidence.config_hash {
        return Err(TestEvidenceRepositoryError::ConfigurationChanged);
    }
    validate_references(connection, evidence).await?;
    connection
        .execute(
            "INSERT INTO test_evidence (id, crawler_version_id, evidence_json, executed_at) VALUES (?1, ?2, ?3, ?4)",
            (
                evidence.id.to_string(),
                evidence.crawler_version_id.to_string(),
                serde_json::to_string(evidence)
                    .map_err(|error| TestEvidenceRepositoryError::Database(DbError::Serialization(error.to_string())))?,
                evidence.executed_at.as_str(),
            ),
        )
        .await
        .map_err(|error| TestEvidenceRepositoryError::Database(error.into()))?;
    if is_valid_discovery_transition_evidence(evidence) {
        let transition_id = evidence
            .tested_transition_id
            .ok_or(TestEvidenceRepositoryError::CorruptState)?;
        attach_transition_evidence(
            connection,
            evidence.crawler_version_id,
            transition_id,
            evidence.id,
        )
        .await?;
    }
    Ok(())
}

fn is_valid_discovery_transition_evidence(evidence: &TestEvidence) -> bool {
    evidence.test_kind == erabi_domain::TestKind::DiscoveryTransition
        && evidence.tested_transition_id.is_some()
        && evidence
            .discovery
            .as_ref()
            .is_some_and(|discovery| discovery.transition_id == evidence.tested_transition_id)
}

async fn validate_references(
    connection: &Connection,
    evidence: &TestEvidence,
) -> Result<(), TestEvidenceRepositoryError> {
    let page_type_ids = evidence_page_type_ids(evidence);
    for page_type_id in page_type_ids {
        ensure_page_type(connection, evidence.crawler_version_id, page_type_id).await?;
    }
    if let Some(transition_id) = evidence.tested_transition_id {
        ensure_transition(connection, evidence.crawler_version_id, transition_id).await?;
    }
    for artifact_id in &evidence.artifact_ids {
        let mut rows = connection
            .query(
                "SELECT 1 FROM artifacts WHERE id = ?1",
                [artifact_id.to_string()],
            )
            .await
            .map_err(|error| TestEvidenceRepositoryError::Database(error.into()))?;
        if rows
            .next()
            .await
            .map_err(|error| TestEvidenceRepositoryError::Database(error.into()))?
            .is_none()
        {
            return Err(TestEvidenceRepositoryError::ArtifactNotFound);
        }
    }
    Ok(())
}

fn evidence_page_type_ids(evidence: &TestEvidence) -> Vec<PageTypeId> {
    let mut ids = Vec::new();
    if let Some(id) = evidence.evaluated_page_type_id {
        ids.push(id);
    }
    for match_evidence in &evidence.page_type_match {
        ids.extend(
            match_evidence
                .candidates
                .iter()
                .map(|candidate| candidate.page_type_id),
        );
    }
    if let Some(discovery) = &evidence.discovery {
        if let Some(source_page_type_id) = discovery.source_page_type_id {
            ids.push(source_page_type_id);
        }
        if let Some(target_page_type_id) = discovery.target_page_type_id {
            ids.push(target_page_type_id);
        }
        for discovered in &discovery.discovered_urls {
            if let Some(page_match) = &discovered.page_type_match {
                ids.extend(
                    page_match
                        .candidates
                        .iter()
                        .map(|candidate| candidate.page_type_id),
                );
            }
        }
    }
    ids.sort_unstable_by_key(ToString::to_string);
    ids.dedup();
    ids
}

async fn ensure_page_type(
    connection: &Connection,
    version_id: CrawlerVersionId,
    page_type_id: PageTypeId,
) -> Result<(), TestEvidenceRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT crawler_version_id FROM page_types WHERE id = ?1",
            [page_type_id.to_string()],
        )
        .await
        .map_err(|error| TestEvidenceRepositoryError::Database(error.into()))?;
    let Some(row) = rows
        .next()
        .await
        .map_err(|error| TestEvidenceRepositoryError::Database(error.into()))?
    else {
        return Err(TestEvidenceRepositoryError::Crawler(
            CrawlerRepositoryError::PageTypeNotFound,
        ));
    };
    let owner = parse_version_id(
        &row.get::<String>(0)
            .map_err(|error| TestEvidenceRepositoryError::Database(error.into()))?,
    )?;
    if owner != version_id {
        return Err(TestEvidenceRepositoryError::Crawler(
            CrawlerRepositoryError::PageTypeNotOwnedByVersion,
        ));
    }
    Ok(())
}

async fn ensure_transition(
    connection: &Connection,
    version_id: CrawlerVersionId,
    transition_id: erabi_domain::DiscoveryTransitionId,
) -> Result<(), TestEvidenceRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT crawler_version_id FROM discovery_transitions WHERE id = ?1",
            [transition_id.to_string()],
        )
        .await
        .map_err(|error| TestEvidenceRepositoryError::Database(error.into()))?;
    let Some(row) = rows
        .next()
        .await
        .map_err(|error| TestEvidenceRepositoryError::Database(error.into()))?
    else {
        return Err(TestEvidenceRepositoryError::Crawler(
            CrawlerRepositoryError::DiscoveryTransitionNotFound,
        ));
    };
    let owner = parse_version_id(
        &row.get::<String>(0)
            .map_err(|error| TestEvidenceRepositoryError::Database(error.into()))?,
    )?;
    if owner != version_id {
        return Err(TestEvidenceRepositoryError::Crawler(
            CrawlerRepositoryError::TransitionNotOwnedByVersion,
        ));
    }
    Ok(())
}

async fn attach_transition_evidence(
    connection: &Connection,
    version_id: CrawlerVersionId,
    transition_id: erabi_domain::DiscoveryTransitionId,
    evidence_id: TestEvidenceId,
) -> Result<(), TestEvidenceRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT configuration_json FROM discovery_transitions WHERE id = ?1 AND crawler_version_id = ?2",
            (transition_id.to_string(), version_id.to_string()),
        )
        .await
        .map_err(|error| TestEvidenceRepositoryError::Database(error.into()))?;
    let Some(row) = rows
        .next()
        .await
        .map_err(|error| TestEvidenceRepositoryError::Database(error.into()))?
    else {
        return Err(TestEvidenceRepositoryError::Crawler(
            CrawlerRepositoryError::TransitionNotOwnedByVersion,
        ));
    };
    let configuration = row
        .get::<String>(0)
        .map_err(|error| TestEvidenceRepositoryError::Database(error.into()))?;
    let mut transition: DiscoveryTransition = serde_json::from_str(&configuration)
        .map_err(|_| TestEvidenceRepositoryError::CorruptState)?;
    if transition.id != transition_id {
        return Err(TestEvidenceRepositoryError::CorruptState);
    }
    transition.latest_test_evidence_id = Some(evidence_id);
    let configuration = serde_json::to_string(&transition).map_err(|error| {
        TestEvidenceRepositoryError::Database(DbError::Serialization(error.to_string()))
    })?;
    let updated = connection
        .execute(
            "UPDATE discovery_transitions SET configuration_json = ?1 WHERE id = ?2 AND crawler_version_id = ?3",
            (configuration, transition_id.to_string(), version_id.to_string()),
        )
        .await
        .map_err(|error| TestEvidenceRepositoryError::Database(error.into()))?;
    if updated != 1 {
        return Err(TestEvidenceRepositoryError::Crawler(
            CrawlerRepositoryError::ConcurrentVersionTransition,
        ));
    }
    Ok(())
}

fn parse_evidence_id(value: &str) -> Result<TestEvidenceId, TestEvidenceRepositoryError> {
    let uuid = Uuid::parse_str(value).map_err(|_| TestEvidenceRepositoryError::CorruptState)?;
    TestEvidenceId::from_uuid(uuid).ok_or(TestEvidenceRepositoryError::CorruptState)
}

fn parse_version_id(value: &str) -> Result<CrawlerVersionId, TestEvidenceRepositoryError> {
    let uuid = Uuid::parse_str(value).map_err(|_| TestEvidenceRepositoryError::CorruptState)?;
    CrawlerVersionId::from_uuid(uuid).ok_or(TestEvidenceRepositoryError::CorruptState)
}
