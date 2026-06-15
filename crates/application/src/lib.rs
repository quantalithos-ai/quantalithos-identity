//! Application services crate for the identity workspace.
//!
//! This crate defines application-local helper objects and port contracts.

pub mod errors;
pub mod mapper;
pub mod ports;
pub mod support;

pub use crate::errors::{ApplicationError, ApplicationErrorKind};
pub use crate::mapper::{
    DefaultIdentityDispatchTargetCatalog, DefaultIdentityMaintenanceIssueMapper,
    DefaultIdentityMarkerSubjectMapper, DefaultIdentityTruthChangeSubjectMapper,
};
pub use crate::ports::{
    IdentityClockPort, IdentityCommandEffectSummaryRepository, IdentityCursorAssignerPort,
    IdentityDispatchTargetCatalogPort, IdentityIdGeneratorPort, IdentityIdempotencyRepository,
    IdentityJobReportRepository, IdentityMaintenanceIssueMapper, IdentityMarkerSubjectMapper,
    IdentityOperationContextFactoryPort, IdentityOutboxRepository, IdentityStoredResultRepository,
    IdentityTruthChangeSubjectMapper, IdentityUnitOfWork, IdentityUnitOfWorkManagerPort,
};
pub use crate::support::{
    ExternalReferenceRefSet, GlobalMemberRefSet, IdempotencyReserveOutcome,
    IdentityAcceptedEffectKind, IdentityAcceptedSubjectRefs, IdentityCommandEffectSummary,
    IdentityCommandEffectSummaryRef, IdentityConsumerReceiptEnvelope, IdentityDispatchTargetRef,
    IdentityEntrySurfaceKind, IdentityIdempotencyKey, IdentityIdempotencyRecord,
    IdentityIdempotencyRecordRef, IdentityIdempotencyStateKind, IdentityJobRunReport,
    IdentityOperationContext, IdentityOperationContextRef, IdentityOperationName,
    IdentityProjectionRefSet, IdentityReadDispositionKind, IdentityReadSubjectRef,
    IdentityRepositoryCursor, IdentityRepositoryPage, IdentityRequestDigest,
    IdentityRequestMetadataRef, IdentityStoredResultKind, IdentityStoredSurfaceMarkerRef,
    IdentityTransactionRef, IdentityTruthRef, IdentityVersion, IdentityVersionedRef,
    IdentityVisibilityDecision, Page, StoredIdentityOperationResult, Versioned,
};
