//! Application services crate for the identity workspace.
//!
//! This crate defines application-local helper objects and port contracts.

pub mod command;
pub mod consumer;
pub mod errors;
pub mod mapper;
pub mod outbound_material;
pub mod ports;
pub mod query;
pub mod support;

pub use crate::command::{
    IdentityApplicationFacade, IdentityCommandService, IdentityCommandServiceDeps,
};
pub use crate::consumer::{IdentityConsumerService, IdentityConsumerServiceDeps};
pub use crate::errors::{ApplicationError, ApplicationErrorKind};
pub use crate::mapper::{
    DefaultIdentityAcceptedAuditTrailMarkerMapper, DefaultIdentityDispatchTargetCatalog,
    DefaultIdentityMaintenanceIssueMapper, DefaultIdentityMarkerSubjectMapper,
    DefaultIdentityQueryMaterialDegradationMapper, DefaultIdentityTruthChangeSubjectMapper,
};
pub use crate::ports::{
    IdentityAcceptedAuditTrailMarkerMapper, IdentityClockPort,
    IdentityCommandEffectSummaryRepository, IdentityCursorAssignerPort,
    IdentityDispatchTargetCatalogPort, IdentityIdGeneratorPort, IdentityIdempotencyRepository,
    IdentityJobReportRepository, IdentityMaintenanceIssueMapper, IdentityMarkerSubjectMapper,
    IdentityOperationContextFactoryPort, IdentityOutboxRepository,
    IdentityQueryMaterialDegradationMapper, IdentityStoredResultRepository,
    IdentityTruthChangeSubjectMapper, IdentityUnitOfWork, IdentityUnitOfWorkManagerPort,
};
pub use crate::query::{
    IdentityMemberSummaryPreflight, IdentityQueryService, IdentityQueryServiceDeps,
};
pub use crate::support::{
    ExternalReferenceRefSet, GlobalMemberRefSet, IdempotencyReserveOutcome,
    IdentityAcceptedAuditTrailMarkers, IdentityAcceptedEffectKind, IdentityAcceptedSubjectRefs,
    IdentityCommandAcceptedResultEnvelope, IdentityCommandEffectSummary,
    IdentityCommandEffectSummaryRef, IdentityCommandRejectedResultEnvelope,
    IdentityCommandTypedResult, IdentityConsumerReceiptEnvelope, IdentityDispatchTargetRef,
    IdentityEntrySurfaceKind, IdentityIdempotencyKey, IdentityIdempotencyRecord,
    IdentityIdempotencyRecordRef, IdentityIdempotencyStateKind, IdentityJobRunReport,
    IdentityOperationContext, IdentityOperationContextRef, IdentityOperationName,
    IdentityProjectionRefSet, IdentityQueryMaterialDegradationSummary, IdentityReadDispositionKind,
    IdentityRepositoryCursor, IdentityRepositoryPage, IdentityRequestDigest,
    IdentityRequestMetadataRef, IdentityStoredResultKind, IdentityStoredSurfaceMarkerRef,
    IdentityTransactionRef, IdentityTruthRef, IdentityVersion, IdentityVersionedRef,
    IdentityVisibilityDecision, Page, StoredIdentityOperationResult, Versioned,
};
pub use identity_contracts::refs::IdentityReadSubjectRef;
