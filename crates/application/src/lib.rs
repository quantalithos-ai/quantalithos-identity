//! Application services crate for the identity workspace.
//!
//! This crate defines application-local helper objects and port contracts.

pub mod errors;
pub mod ports;
pub mod support;

pub use crate::errors::{ApplicationError, ApplicationErrorKind};
pub use crate::ports::{
    IdentityClockPort, IdentityCursorAssignerPort, IdentityIdGeneratorPort,
    IdentityOperationContextFactoryPort, IdentityUnitOfWork, IdentityUnitOfWorkManagerPort,
};
pub use crate::support::{
    ExternalReferenceRefSet, GlobalMemberRefSet, IdentityAcceptedEffectKind,
    IdentityCommandEffectSummary, IdentityCommandEffectSummaryRef, IdentityDispatchTargetRef,
    IdentityEntrySurfaceKind, IdentityIdempotencyKey, IdentityIdempotencyRecord,
    IdentityIdempotencyRecordRef, IdentityIdempotencyStateKind, IdentityJobRunReport,
    IdentityOperationContext, IdentityOperationContextRef, IdentityOperationName,
    IdentityProjectionRefSet, IdentityReadDispositionKind, IdentityReadSubjectRef,
    IdentityRepositoryCursor, IdentityRepositoryPage, IdentityRequestDigest,
    IdentityRequestMetadataRef, IdentityStoredResultKind, IdentityStoredSurfaceMarkerRef,
    IdentityTransactionRef, IdentityTruthRef, IdentityVersion, IdentityVersionedRef,
    IdentityVisibilityDecision, Page, StoredIdentityOperationResult, Versioned,
};
