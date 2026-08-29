macro_rules! typed_uuid_id {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
        #[serde(transparent)]
        pub struct $name(uuid::Uuid);

        impl $name {
            #[must_use]
            pub fn new() -> Self {
                Self(uuid::Uuid::now_v7())
            }

            /// Rehydrates an application-generated `UUIDv7` identity.
            ///
            /// Persisted/API identifiers are accepted only when they retain
            /// Erabi's `UUIDv7` identity contract.
            #[must_use]
            pub fn from_uuid(value: uuid::Uuid) -> Option<Self> {
                (value.get_version_num() == 7).then_some(Self(value))
            }
            #[must_use]
            pub const fn as_uuid(&self) -> &uuid::Uuid {
                &self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

typed_uuid_id!(CrawlerId);
typed_uuid_id!(CrawlerVersionId);
typed_uuid_id!(SeedId);
typed_uuid_id!(PageTypeId);
typed_uuid_id!(DiscoveryTransitionId);
typed_uuid_id!(SourceId);
typed_uuid_id!(CollectionId);
typed_uuid_id!(RunProfileId);
typed_uuid_id!(TestEvidenceId);
typed_uuid_id!(CrawlRunId);
typed_uuid_id!(CrawlExecutionId);
typed_uuid_id!(ArtifactId);
typed_uuid_id!(CanonicalizationPolicyId);
typed_uuid_id!(DomainScopeId);
