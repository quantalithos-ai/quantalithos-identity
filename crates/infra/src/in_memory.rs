use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use identity_application::errors::{ApplicationError, ApplicationErrorKind};
use identity_application::ports::{
    ExternalReferenceTypedSidecarRefs, HandoffDeliveryOutcome, HandoffReceiptResolution,
    HandoffTargetResolution, IdentityAdapterAvailabilityPort,
    IdentityCommandEffectSummaryRepository, IdentityCursorAssignerPort,
    IdentityHandoffDeliveryPort, IdentityHandoffTargetPort, IdentityIdempotencyRepository,
    IdentityJobReportRepository, IdentityOutboxRepository, IdentityProjectionRepository,
    IdentityReadVisibilityRepository, IdentityReferenceStateRepository,
    IdentityStoredResultRepository, IdentityUnitOfWork, IdentityUnitOfWorkManagerPort,
    TraceHandoffIntentRepository,
};
use identity_application::support::{
    IdempotencyReserveOutcome, IdentityAdapterAvailability, IdentityAdapterModeRef,
    IdentityAdapterRef, IdentityCommandEffectSummary, IdentityCommandEffectSummaryRef,
    IdentityConsumerReceiptEnvelope, IdentityIdempotencyKey, IdentityIdempotencyRecord,
    IdentityIdempotencyRecordRef, IdentityJobRunReport, IdentityOperationContext,
    IdentityOperationContextRef, IdentityOperationName, IdentityProjectionRefSet,
    IdentityRepositoryCursor, IdentityRepositoryPage, IdentityStoredResultKind,
    IdentityTransactionRef, IdentityTruthRef, IdentityVersion, IdentityVersionedRef, Page,
    StoredIdentityOperationResult, Versioned,
};
use identity_contracts::jobs::IdentityJobResultKind;
use identity_contracts::protocol::IdentityJobName;
use identity_contracts::receipts::TraceHandoffIntentRef;
use identity_contracts::refs::{
    AuditScopeRef, AuditTrailRef, ConsumerRef, ExternalReferenceKind, ExternalReferenceRef,
    ExternalSourceRef, GlobalMemberRef, HandoffIssueRef, HandoffReceiptRef, HandoffScopeRef,
    HandoffTargetRef, IdentityAuditSubjectRef, IdentityOutboxRecordRef, IdentityOutboxSubjectRef,
    IdentityProjectionCursorRef, IdentityProjectionRef, IdentityReferenceOwnerRef,
    IdentitySourceOwner, IdentitySourceRef, IdentityTraceRecordRef, IdentityTraceSubjectRef,
    IdentityTruthCursor, MemberSummaryViewRef, ProjectionStateRef, ReferenceResolutionStateRef,
    TopicKeyRef, TraceHandoffSafeMaterialRef, VisibilityContextRef, VisibilityResultRef,
    VisibilityScopeRef,
};
use identity_contracts::views::{IdentityVisibilityAccessSummary, MemberSummaryView};
use identity_domain::handoff::{HandoffStateKind, TraceHandoffIntent};
use identity_domain::outbox::{IdentityOutboxRecord, OutboxStateKind};
use identity_domain::projection_state::{ProjectionState, ProjectionStateKind};
use identity_domain::reference_state::{ReferenceResolutionState, ReferenceResolutionStateKind};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FaultCase {
    RollbackFails,
    CommitUnknown,
    SaveOutboxRecordFails,
    SaveStoredResultFails,
    SaveReceiptEnvelopeFails,
    SaveJobReportFails,
    CompleteIdempotencyFails,
}

#[derive(Clone, Debug, Default)]
pub struct IdentityInMemoryRuntimeBuilder {
    store: RuntimeStore,
}

impl IdentityInMemoryRuntimeBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn seed_member_summary_view(
        mut self,
        view: MemberSummaryView,
        version: IdentityVersion,
    ) -> Self {
        self.store.member_scope_index.insert(
            member_scope_key(&view.member_ref, &view.visibility_scope_ref),
            view.view_ref.as_str().to_owned(),
        );
        self.store.member_summary_views.insert(
            view.view_ref.as_str().to_owned(),
            StoredMemberSummaryView { view, version },
        );
        self
    }

    pub fn seed_projection_state(
        mut self,
        state: ProjectionState,
        version: IdentityVersion,
    ) -> Self {
        self.store.projection_states.insert(
            projection_key(&state.projection_ref),
            StoredProjectionState { state, version },
        );
        self
    }

    pub fn seed_reference_state(
        mut self,
        state: ReferenceResolutionState,
        sidecars: ExternalReferenceTypedSidecarRefs,
        version: IdentityVersion,
    ) -> Self {
        let key = external_reference_key(&state.external_reference_ref);
        self.store.reference_states.insert(
            key.clone(),
            StoredReferenceState {
                state,
                sidecars,
                version,
            },
        );
        self
    }

    pub fn seed_handoff_intent(
        mut self,
        intent: TraceHandoffIntent,
        version: IdentityVersion,
    ) -> Self {
        self.store.handoff_intents.insert(
            intent.handoff_intent_ref.as_str().to_owned(),
            StoredHandoffIntent { intent, version },
        );
        self
    }

    pub fn seed_outbox_record(
        mut self,
        record: IdentityOutboxRecord,
        version: IdentityVersion,
    ) -> Self {
        let key = record.outbox_record_ref.as_str().to_owned();
        self.store.outbox_subject_index.insert(
            outbox_subject_key(&record.subject_ref, &record.outbox_record_ref),
            key.clone(),
        );
        self.store.outbox_trace_index.insert(
            outbox_trace_key(&record.trace_record_ref, &record.outbox_record_ref),
            key.clone(),
        );
        self.store
            .outbox_records
            .insert(key, StoredOutboxRecord { record, version });
        self
    }

    pub fn seed_idempotency_record(
        mut self,
        record: IdentityIdempotencyRecord,
        version: IdentityVersion,
    ) -> Self {
        let key = record.record_ref.as_str().to_owned();
        self.store.idempotency_key_index.insert(
            idempotency_key_key(
                &record.operation_name,
                record.channel,
                &record.idempotency_key,
            ),
            key.clone(),
        );
        self.store
            .idempotency_records
            .insert(key, StoredIdempotencyRecord { record, version });
        self
    }

    pub fn seed_stored_result(mut self, result: StoredIdentityOperationResult) -> Self {
        let key = result.stored_result_ref.as_str().to_owned();
        self.store.stored_result_by_context.insert(
            result.operation_context_ref.as_str().to_owned(),
            key.clone(),
        );
        self.store.stored_results.insert(key, result);
        self
    }

    pub fn seed_consumer_receipt(mut self, envelope: IdentityConsumerReceiptEnvelope) -> Self {
        self.store
            .consumer_receipts
            .insert(envelope.stored_result_ref.as_str().to_owned(), envelope);
        self
    }

    pub fn seed_handoff_callback_receipt(
        mut self,
        envelope: IdentityConsumerReceiptEnvelope,
    ) -> Self {
        self.store
            .handoff_callback_receipts
            .insert(envelope.stored_result_ref.as_str().to_owned(), envelope);
        self
    }

    pub fn seed_effect_summary(mut self, summary: IdentityCommandEffectSummary) -> Self {
        let summary_ref = summary.effect_summary_ref.as_str().to_owned();
        self.store.effects_by_context.insert(
            effect_context_key(&summary.operation_context_ref, &summary.effect_summary_ref),
            summary_ref.clone(),
        );
        self.store.effects_by_truth_ref.insert(
            effect_truth_key(&summary.primary_truth_ref, &summary.effect_summary_ref),
            summary_ref.clone(),
        );
        self.store.effects_after_cursor.insert(
            effect_cursor_key(&summary.accepted_cursor_ref, &summary.effect_summary_ref),
            summary_ref.clone(),
        );
        self.store
            .command_effect_summaries
            .insert(summary_ref, summary);
        self
    }

    pub fn seed_job_report(
        mut self,
        report: IdentityJobRunReport,
        version: IdentityVersion,
    ) -> Self {
        let report_ref = report.report_ref.as_str().to_owned();
        self.store
            .job_report_by_run
            .insert(report.job_run_ref.as_str().to_owned(), report_ref.clone());
        self.store.job_report_by_name.insert(
            job_report_name_key(&report.job_name, &report.report_ref),
            report_ref.clone(),
        );
        self.store.job_report_by_result.insert(
            job_report_result_key(report.result_kind, &report.report_ref),
            report_ref.clone(),
        );
        self.store
            .job_reports
            .insert(report_ref, StoredJobReport { report, version });
        self
    }

    pub fn seed_adapter_availability(mut self, availability: IdentityAdapterAvailability) -> Self {
        self.store
            .adapter_availability
            .insert(availability.adapter_ref.as_str().to_owned(), availability);
        self
    }

    pub fn seed_member_summary_access(
        mut self,
        member_ref: GlobalMemberRef,
        access_summary: IdentityVisibilityAccessSummary,
    ) -> Self {
        self.store
            .member_summary_access
            .insert(member_key(&member_ref), access_summary);
        self
    }

    pub fn inject_fault(mut self, fault: FaultCase) -> Self {
        self.store.faults.insert(fault);
        self
    }

    pub fn build(self) -> IdentityInMemoryRuntime {
        IdentityInMemoryRuntime {
            shared: Arc::new(SharedRuntime {
                store: Mutex::new(self.store),
                next_transaction_id: AtomicU64::new(1),
                next_truth_cursor_id: AtomicU64::new(1),
                next_reference_cursor_id: AtomicU64::new(1),
                staged_by_tx: Mutex::new(HashMap::new()),
            }),
        }
    }
}

#[derive(Clone)]
pub struct IdentityInMemoryRuntime {
    shared: Arc<SharedRuntime>,
}

impl IdentityInMemoryRuntime {
    pub fn builder() -> IdentityInMemoryRuntimeBuilder {
        IdentityInMemoryRuntimeBuilder::new()
    }

    pub fn uow_manager(&self) -> &Self {
        self
    }

    pub fn projection_repository(&self) -> &Self {
        self
    }

    pub fn reference_state_repository(&self) -> &Self {
        self
    }

    pub fn handoff_intent_repository(&self) -> &Self {
        self
    }

    pub fn adapter_availability_port(&self) -> &Self {
        self
    }

    pub fn handoff_target_port(&self) -> &Self {
        self
    }

    pub fn handoff_delivery_port(&self) -> &Self {
        self
    }

    pub fn read_visibility_repository(&self) -> &Self {
        self
    }

    fn stage(
        &self,
        transaction_ref: &IdentityTransactionRef,
        op: StagedOp,
    ) -> Result<(), ApplicationError> {
        let mut staged = self.shared.staged_by_tx.lock().map_err(|_| {
            ApplicationError::consistency_defect("staged transaction map lock poisoned")
        })?;
        staged.entry(tx_key(transaction_ref)).or_default().push(op);
        Ok(())
    }

    fn predicted_view_version(
        &self,
        view_ref: &MemberSummaryViewRef,
    ) -> Result<IdentityVersion, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(IdentityVersion::new(
            store
                .member_summary_views
                .get(view_ref.as_str())
                .map(|stored| stored.version.get() + 1)
                .unwrap_or(1),
        ))
    }

    fn predicted_projection_version(
        &self,
        projection_ref: &IdentityProjectionRef,
    ) -> Result<IdentityVersion, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(IdentityVersion::new(
            store
                .projection_states
                .get(&projection_key(projection_ref))
                .map(|stored| stored.version.get() + 1)
                .unwrap_or(1),
        ))
    }

    fn predicted_reference_version(
        &self,
        reference_ref: &ExternalReferenceRef,
    ) -> Result<IdentityVersion, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(IdentityVersion::new(
            store
                .reference_states
                .get(&external_reference_key(reference_ref))
                .map(|stored| stored.version.get() + 1)
                .unwrap_or(1),
        ))
    }

    fn predicted_handoff_version(
        &self,
        intent_ref: &TraceHandoffIntentRef,
    ) -> Result<IdentityVersion, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(IdentityVersion::new(
            store
                .handoff_intents
                .get(intent_ref.as_str())
                .map(|stored| stored.version.get() + 1)
                .unwrap_or(1),
        ))
    }

    fn predicted_outbox_version(
        &self,
        outbox_ref: &IdentityOutboxRecordRef,
    ) -> Result<IdentityVersion, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(IdentityVersion::new(
            store
                .outbox_records
                .get(outbox_ref.as_str())
                .map(|stored| stored.version.get() + 1)
                .unwrap_or(1),
        ))
    }

    fn predicted_job_report_version(
        &self,
        report_ref: &identity_contracts::refs::IdentityJobReportRef,
    ) -> Result<IdentityVersion, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(IdentityVersion::new(
            store
                .job_reports
                .get(report_ref.as_str())
                .map(|stored| stored.version.get() + 1)
                .unwrap_or(1),
        ))
    }
}

struct SharedRuntime {
    store: Mutex<RuntimeStore>,
    next_transaction_id: AtomicU64,
    next_truth_cursor_id: AtomicU64,
    next_reference_cursor_id: AtomicU64,
    staged_by_tx: Mutex<HashMap<String, Vec<StagedOp>>>,
}

#[derive(Clone, Debug, Default)]
struct RuntimeStore {
    member_summary_views: HashMap<String, StoredMemberSummaryView>,
    member_scope_index: HashMap<String, String>,
    projection_states: HashMap<String, StoredProjectionState>,
    reference_states: HashMap<String, StoredReferenceState>,
    handoff_intents: HashMap<String, StoredHandoffIntent>,
    outbox_records: HashMap<String, StoredOutboxRecord>,
    outbox_subject_index: HashMap<String, String>,
    outbox_trace_index: HashMap<String, String>,
    idempotency_records: HashMap<String, StoredIdempotencyRecord>,
    idempotency_key_index: HashMap<String, String>,
    stored_results: HashMap<String, StoredIdentityOperationResult>,
    stored_result_by_context: HashMap<String, String>,
    consumer_receipts: HashMap<String, IdentityConsumerReceiptEnvelope>,
    handoff_callback_receipts: HashMap<String, IdentityConsumerReceiptEnvelope>,
    command_effect_summaries: HashMap<String, IdentityCommandEffectSummary>,
    effects_by_context: HashMap<String, String>,
    effects_by_truth_ref: HashMap<String, String>,
    effects_after_cursor: HashMap<String, String>,
    job_reports: HashMap<String, StoredJobReport>,
    job_report_by_run: HashMap<String, String>,
    job_report_by_name: HashMap<String, String>,
    job_report_by_result: HashMap<String, String>,
    adapter_availability: HashMap<String, IdentityAdapterAvailability>,
    member_summary_access: HashMap<String, IdentityVisibilityAccessSummary>,
    faults: HashSet<FaultCase>,
}

#[derive(Clone, Debug)]
struct StoredMemberSummaryView {
    view: MemberSummaryView,
    version: IdentityVersion,
}

#[derive(Clone, Debug)]
struct StoredProjectionState {
    state: ProjectionState,
    version: IdentityVersion,
}

#[derive(Clone, Debug)]
struct StoredReferenceState {
    state: ReferenceResolutionState,
    sidecars: ExternalReferenceTypedSidecarRefs,
    version: IdentityVersion,
}

#[derive(Clone, Debug)]
struct StoredHandoffIntent {
    intent: TraceHandoffIntent,
    version: IdentityVersion,
}

#[derive(Clone, Debug)]
struct StoredOutboxRecord {
    record: IdentityOutboxRecord,
    version: IdentityVersion,
}

#[derive(Clone, Debug)]
struct StoredIdempotencyRecord {
    record: IdentityIdempotencyRecord,
    version: IdentityVersion,
}

#[derive(Clone, Debug)]
struct StoredJobReport {
    report: IdentityJobRunReport,
    version: IdentityVersion,
}

#[derive(Clone, Debug)]
enum StagedOp {
    SaveMemberSummaryView {
        view: MemberSummaryView,
        expected_version: Option<IdentityVersion>,
    },
    SaveProjectionState {
        state: ProjectionState,
        expected_version: Option<IdentityVersion>,
    },
    SaveReferenceState {
        state: ReferenceResolutionState,
        expected_version: Option<IdentityVersion>,
    },
    SaveTypedSidecars {
        reference_ref: ExternalReferenceRef,
        sidecars: ExternalReferenceTypedSidecarRefs,
        expected_version: IdentityVersion,
    },
    SaveHandoffIntent {
        intent: TraceHandoffIntent,
        expected_version: Option<IdentityVersion>,
    },
    SaveOutboxRecord {
        record: IdentityOutboxRecord,
        expected_version: Option<IdentityVersion>,
    },
    SaveIdempotencyReservation {
        record: IdentityIdempotencyRecord,
    },
    SaveIdempotencyTerminal {
        record: IdentityIdempotencyRecord,
        expected_version: IdentityVersion,
    },
    SaveStoredResult {
        result: StoredIdentityOperationResult,
    },
    SaveConsumerReceiptEnvelope {
        envelope: IdentityConsumerReceiptEnvelope,
    },
    SaveHandoffCallbackReceiptEnvelope {
        envelope: IdentityConsumerReceiptEnvelope,
    },
    SaveEffectSummary {
        summary: IdentityCommandEffectSummary,
    },
    SaveJobReport {
        report: IdentityJobRunReport,
        expected_version: Option<IdentityVersion>,
    },
}

struct InMemoryUnitOfWork {
    transaction_ref: IdentityTransactionRef,
    shared: Arc<SharedRuntime>,
    truth_cursor: Mutex<Option<IdentityTruthCursor>>,
    reference_cursor: Mutex<Option<IdentityTruthCursor>>,
}

impl IdentityUnitOfWork for InMemoryUnitOfWork {
    fn transaction_ref(&self) -> IdentityTransactionRef {
        self.transaction_ref.clone()
    }

    fn assign_truth_change_cursor(&self) -> Result<IdentityTruthCursor, ApplicationError> {
        let mut truth_cursor = self
            .truth_cursor
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("truth cursor lock poisoned"))?;
        if let Some(existing) = truth_cursor.as_ref() {
            return Ok(existing.clone());
        }

        let next = self
            .shared
            .next_truth_cursor_id
            .fetch_add(1, Ordering::SeqCst);
        let assigned = IdentityTruthCursor::new(format!("truth-cursor-{next}"));
        *truth_cursor = Some(assigned.clone());
        Ok(assigned)
    }

    fn assign_reference_marker_cursor(&self) -> Result<IdentityTruthCursor, ApplicationError> {
        let mut reference_cursor = self
            .reference_cursor
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("reference cursor lock poisoned"))?;
        if let Some(existing) = reference_cursor.as_ref() {
            return Ok(existing.clone());
        }

        let next = self
            .shared
            .next_reference_cursor_id
            .fetch_add(1, Ordering::SeqCst);
        let assigned = IdentityTruthCursor::new(format!("reference-cursor-{next}"));
        *reference_cursor = Some(assigned.clone());
        Ok(assigned)
    }
}

impl IdentityUnitOfWorkManagerPort for IdentityInMemoryRuntime {
    fn begin(&self) -> Result<Box<dyn IdentityUnitOfWork>, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(InMemoryUnitOfWork {
            transaction_ref: IdentityTransactionRef::new(format!("tx-{next}")),
            shared: self.shared.clone(),
            truth_cursor: Mutex::new(None),
            reference_cursor: Mutex::new(None),
        }))
    }

    fn commit(&self, uow: Box<dyn IdentityUnitOfWork>) -> Result<(), ApplicationError> {
        let transaction_ref = uow.transaction_ref();
        let tx_key = tx_key(&transaction_ref);
        let staged_ops = {
            let mut staged = self.shared.staged_by_tx.lock().map_err(|_| {
                ApplicationError::consistency_defect("staged transaction map lock poisoned")
            })?;
            staged.remove(&tx_key).unwrap_or_default()
        };

        let mut store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let current = store.clone();

        for op in staged_ops {
            if let Err(error) = apply_op(&mut store, &current, op) {
                *store = current;
                return Err(error);
            }
        }

        if store.faults.contains(&FaultCase::CommitUnknown) {
            return Err(ApplicationError::new(
                ApplicationErrorKind::CommitStatusUnknown,
                "commit status unknown; check stored replay surface before retry",
            ));
        }

        Ok(())
    }

    fn rollback(&self, uow: Box<dyn IdentityUnitOfWork>) -> Result<(), ApplicationError> {
        let transaction_ref = uow.transaction_ref();
        let tx_key = tx_key(&transaction_ref);

        {
            let store =
                self.shared.store.lock().map_err(|_| {
                    ApplicationError::consistency_defect("runtime store lock poisoned")
                })?;
            if store.faults.contains(&FaultCase::RollbackFails) {
                return Err(ApplicationError::new(
                    ApplicationErrorKind::ConsistencyDefect,
                    "rollback failed; manual intervention required",
                ));
            }
        }

        let mut staged = self.shared.staged_by_tx.lock().map_err(|_| {
            ApplicationError::consistency_defect("staged transaction map lock poisoned")
        })?;
        staged.remove(&tx_key);
        Ok(())
    }
}

impl IdentityCursorAssignerPort for IdentityInMemoryRuntime {
    fn assign_truth_change_cursor(
        &self,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityTruthCursor, ApplicationError> {
        uow.assign_truth_change_cursor()
    }

    fn assign_reference_marker_cursor(
        &self,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityTruthCursor, ApplicationError> {
        uow.assign_reference_marker_cursor()
    }
}

impl IdentityProjectionRepository for IdentityInMemoryRuntime {
    fn find_member_summary_view_ref(
        &self,
        member_ref: GlobalMemberRef,
        visibility_scope_ref: VisibilityScopeRef,
    ) -> Result<Option<MemberSummaryViewRef>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .member_scope_index
            .get(&member_scope_key(&member_ref, &visibility_scope_ref))
            .cloned()
            .map(MemberSummaryViewRef::new))
    }

    fn get_member_summary_view(
        &self,
        view_ref: MemberSummaryViewRef,
    ) -> Result<Option<MemberSummaryView>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .member_summary_views
            .get(view_ref.as_str())
            .map(|stored| stored.view.clone()))
    }

    fn get_projection_state_with_version(
        &self,
        projection_ref: IdentityProjectionRef,
    ) -> Result<Option<Versioned<ProjectionState>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .projection_states
            .get(&projection_key(&projection_ref))
            .map(|stored| Versioned {
                value: stored.state.clone(),
                version: stored.version,
            }))
    }

    fn find_projection_state_ref(
        &self,
        projection_ref: IdentityProjectionRef,
    ) -> Result<Option<ProjectionStateRef>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .projection_states
            .get(&projection_key(&projection_ref))
            .map(|stored| stored.state.projection_state_ref.clone()))
    }

    fn list_projection_states(
        &self,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<ProjectionStateRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_projection_page(
            store.projection_states.values().collect(),
            page,
            |_| true,
        ))
    }

    fn list_stale_projection_states(
        &self,
        maintenance_scope_ref: identity_contracts::refs::MaintenanceScopeRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<ProjectionStateRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_projection_page(
            store.projection_states.values().collect(),
            page,
            |stored| {
                stored.state.state_kind == ProjectionStateKind::Stale
                    && stored.state.maintenance_scope_ref.as_ref() == Some(&maintenance_scope_ref)
            },
        ))
    }

    fn get_projection_source_cursor(
        &self,
        projection_ref: IdentityProjectionRef,
    ) -> Result<Option<IdentityProjectionCursorRef>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .projection_states
            .get(&projection_key(&projection_ref))
            .and_then(|stored| stored.state.source_cursor_ref.clone()))
    }

    fn expand_affected_projection_refs(
        &self,
        _subject_refs: identity_application::support::IdentityAcceptedSubjectRefs,
    ) -> Result<IdentityProjectionRefSet, ApplicationError> {
        Ok(IdentityProjectionRefSet {
            projection_refs: Vec::new(),
        })
    }

    fn save_member_summary_view(
        &self,
        view: MemberSummaryView,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<MemberSummaryViewRef>, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveMemberSummaryView {
                view: view.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: view.view_ref.clone(),
            version: self.predicted_view_version(&view.view_ref)?,
        })
    }

    fn save_projection_state(
        &self,
        state: ProjectionState,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<ProjectionStateRef>, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveProjectionState {
                state: state.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: state.projection_state_ref.clone(),
            version: self.predicted_projection_version(&state.projection_ref)?,
        })
    }

    fn mark_projection_stale(
        &self,
        projection_ref: IdentityProjectionRef,
        state: ProjectionState,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<ProjectionStateRef>, ApplicationError> {
        if state.projection_ref != projection_ref {
            return Err(ApplicationError::invalid_request(
                "projection ref does not match projection state",
            ));
        }
        self.save_projection_state(state, Some(expected_version), uow)
    }
}

impl IdentityReferenceStateRepository for IdentityInMemoryRuntime {
    fn get_reference_state_with_version(
        &self,
        reference_ref: ExternalReferenceRef,
    ) -> Result<Option<Versioned<ReferenceResolutionState>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .reference_states
            .get(&external_reference_key(&reference_ref))
            .map(|stored| Versioned {
                value: stored.state.clone(),
                version: stored.version,
            }))
    }

    fn find_reference_state_ref(
        &self,
        reference_ref: ExternalReferenceRef,
    ) -> Result<Option<ReferenceResolutionStateRef>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .reference_states
            .get(&external_reference_key(&reference_ref))
            .map(|stored| stored.state.resolution_state_ref.clone()))
    }

    fn list_reference_states_by_owner(
        &self,
        owner_ref: IdentityReferenceOwnerRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<ReferenceResolutionStateRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_reference_page(
            store.reference_states.values().collect(),
            page,
            |stored| stored.state.reference_owner_ref == owner_ref,
        ))
    }

    fn list_reference_states_by_kind(
        &self,
        reference_kind: ExternalReferenceKind,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<ReferenceResolutionStateRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_reference_page(
            store.reference_states.values().collect(),
            page,
            |stored| stored.state.external_reference_ref.reference_kind == reference_kind,
        ))
    }

    fn list_stale_reference_states(
        &self,
        _maintenance_scope_ref: identity_contracts::refs::MaintenanceScopeRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<ReferenceResolutionStateRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_reference_page(
            store.reference_states.values().collect(),
            page,
            |stored| {
                matches!(
                    stored.state.state_kind,
                    ReferenceResolutionStateKind::Stale
                        | ReferenceResolutionStateKind::Unavailable
                        | ReferenceResolutionStateKind::Unrecognized
                        | ReferenceResolutionStateKind::PendingReconciliation
                        | ReferenceResolutionStateKind::RefreshFailed
                )
            },
        ))
    }

    fn get_typed_sidecar_refs(
        &self,
        reference_ref: ExternalReferenceRef,
    ) -> Result<ExternalReferenceTypedSidecarRefs, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .reference_states
            .get(&external_reference_key(&reference_ref))
            .map(|stored| stored.sidecars.clone())
            .unwrap_or_else(empty_sidecars))
    }

    fn save_reference_state(
        &self,
        state: ReferenceResolutionState,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<ReferenceResolutionStateRef>, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveReferenceState {
                state: state.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: state.resolution_state_ref.clone(),
            version: self.predicted_reference_version(&state.external_reference_ref)?,
        })
    }

    fn save_typed_sidecar_refs(
        &self,
        reference_ref: ExternalReferenceRef,
        sidecar_refs: ExternalReferenceTypedSidecarRefs,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<ReferenceResolutionStateRef>, ApplicationError> {
        let state = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let stored = state
            .reference_states
            .get(&external_reference_key(&reference_ref))
            .ok_or_else(|| ApplicationError::not_found("reference bundle not found"))?;
        if stored.version != expected_version {
            return Err(ApplicationError::optimistic_version_conflict(
                "reference bundle version mismatch",
            ));
        }
        let value_ref = stored.state.resolution_state_ref.clone();
        drop(state);

        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveTypedSidecars {
                reference_ref,
                sidecars: sidecar_refs,
                expected_version,
            },
        )?;

        Ok(IdentityVersionedRef {
            value_ref,
            version: IdentityVersion::new(expected_version.get() + 1),
        })
    }
}

impl TraceHandoffIntentRepository for IdentityInMemoryRuntime {
    fn get_handoff_intent_with_version(
        &self,
        intent_ref: TraceHandoffIntentRef,
    ) -> Result<Option<Versioned<TraceHandoffIntent>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .handoff_intents
            .get(intent_ref.as_str())
            .map(|stored| Versioned {
                value: stored.intent.clone(),
                version: stored.version,
            }))
    }

    fn list_handoff_intents_by_member(
        &self,
        member_ref: GlobalMemberRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<TraceHandoffIntentRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_handoff_page(
            store.handoff_intents.values().collect(),
            page,
            |stored| stored.intent.member_ref == member_ref,
        ))
    }

    fn list_handoff_intents_by_trace(
        &self,
        trace_record_ref: IdentityTraceRecordRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<TraceHandoffIntentRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_handoff_page(
            store.handoff_intents.values().collect(),
            page,
            |stored| stored.intent.trace_record_refs.contains(&trace_record_ref),
        ))
    }

    fn list_handoff_intents_by_audit_trail(
        &self,
        audit_trail_ref: AuditTrailRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<TraceHandoffIntentRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_handoff_page(
            store.handoff_intents.values().collect(),
            page,
            |stored| stored.intent.audit_trail_ref.as_ref() == Some(&audit_trail_ref),
        ))
    }

    fn list_handoff_intents_by_target(
        &self,
        target_ref: HandoffTargetRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<TraceHandoffIntentRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_handoff_page(
            store.handoff_intents.values().collect(),
            page,
            |stored| stored.intent.handoff_target_ref == target_ref,
        ))
    }

    fn list_retryable_handoff_intents(
        &self,
        target_ref: Option<HandoffTargetRef>,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<TraceHandoffIntentRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_handoff_page(
            store.handoff_intents.values().collect(),
            page,
            |stored| {
                stored.intent.is_retryable()
                    && target_ref
                        .as_ref()
                        .map(|target| &stored.intent.handoff_target_ref == target)
                        .unwrap_or(true)
            },
        ))
    }

    fn save_handoff_intent(
        &self,
        intent: TraceHandoffIntent,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<TraceHandoffIntentRef>, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveHandoffIntent {
                intent: intent.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: intent.handoff_intent_ref.clone(),
            version: self.predicted_handoff_version(&intent.handoff_intent_ref)?,
        })
    }
}

impl IdentityAdapterAvailabilityPort for IdentityInMemoryRuntime {
    fn get_adapter_availability(
        &self,
        adapter_ref: IdentityAdapterRef,
    ) -> Result<IdentityAdapterAvailability, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        store
            .adapter_availability
            .get(adapter_ref.as_str())
            .cloned()
            .ok_or_else(|| ApplicationError::not_found("adapter availability not found"))
    }

    fn list_adapter_availability(
        &self,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityAdapterAvailability>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let entries: Vec<_> = store.adapter_availability.values().cloned().collect();
        let (items, next_cursor) = paged(entries, page, "adapter");
        Ok(Page { items, next_cursor })
    }

    fn assert_adapter_attempt_allowed(
        &self,
        adapter_ref: IdentityAdapterRef,
        required_mode: Option<IdentityAdapterModeRef>,
    ) -> Result<IdentityAdapterAvailability, ApplicationError> {
        let availability = self.get_adapter_availability(adapter_ref)?;
        if let Some(required_mode) = required_mode {
            if availability.adapter_mode_ref != required_mode {
                return Err(ApplicationError::dependency_unavailable(
                    "adapter mode mismatch",
                ));
            }
        }
        Ok(availability)
    }
}

impl IdentityHandoffTargetPort for IdentityInMemoryRuntime {
    fn resolve_handoff_target(
        &self,
        target_ref: HandoffTargetRef,
        scope_ref: HandoffScopeRef,
        _safe_material_ref: TraceHandoffSafeMaterialRef,
    ) -> Result<HandoffTargetResolution, ApplicationError> {
        let availability = {
            let store =
                self.shared.store.lock().map_err(|_| {
                    ApplicationError::consistency_defect("runtime store lock poisoned")
                })?;
            store
                .adapter_availability
                .values()
                .next()
                .cloned()
                .ok_or_else(|| {
                    ApplicationError::dependency_unavailable("no adapter availability seeded")
                })?
        };

        Ok(HandoffTargetResolution {
            target_ref,
            scope_ref,
            adapter_ref: availability.adapter_ref,
            adapter_mode_ref: availability.adapter_mode_ref,
        })
    }
}

impl IdentityHandoffDeliveryPort for IdentityInMemoryRuntime {
    fn deliver_handoff(
        &self,
        intent_ref: TraceHandoffIntentRef,
        target_resolution: HandoffTargetResolution,
        safe_material_ref: TraceHandoffSafeMaterialRef,
    ) -> Result<HandoffDeliveryOutcome, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let stored = store
            .handoff_intents
            .get(intent_ref.as_str())
            .ok_or_else(|| ApplicationError::not_found("handoff intent not found"))?;
        if stored.intent.handoff_target_ref != target_resolution.target_ref {
            return Err(ApplicationError::invalid_request("handoff target mismatch"));
        }
        if stored.intent.safe_material_ref != safe_material_ref {
            return Err(ApplicationError::invalid_request(
                "handoff material mismatch",
            ));
        }
        Ok(HandoffDeliveryOutcome::UnsupportedTarget {
            issue_ref: HandoffIssueRef::new(identity_source_ref(
                IdentitySourceOwner::Identity,
                "handoff-unsupported",
            )),
        })
    }

    fn resolve_handoff_receipt(
        &self,
        receipt_ref: HandoffReceiptRef,
    ) -> Result<HandoffReceiptResolution, ApplicationError> {
        Ok(HandoffReceiptResolution {
            receipt_ref,
            receipt_state: ReferenceResolutionStateKind::Resolved,
            issue_ref: None,
        })
    }
}

impl IdentityReadVisibilityRepository for IdentityInMemoryRuntime {
    fn resolve_member_summary_read(
        &self,
        member_ref: GlobalMemberRef,
        _view_ref: Option<MemberSummaryViewRef>,
        _consumer_ref: ConsumerRef,
        _visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .member_summary_access
            .get(&member_key(&member_ref))
            .cloned())
    }

    fn resolve_trace_read(
        &self,
        _subject_ref: IdentityTraceSubjectRef,
        _consumer_ref: ConsumerRef,
        _visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError> {
        Ok(None)
    }

    fn resolve_audit_read(
        &self,
        _audit_subject_ref: IdentityAuditSubjectRef,
        _audit_scope_ref: AuditScopeRef,
        _consumer_ref: ConsumerRef,
        _visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError> {
        Ok(None)
    }

    fn resolve_report_read(
        &self,
        _report_ref: identity_contracts::refs::ReconciliationReportRef,
        _consumer_ref: ConsumerRef,
        _visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError> {
        Ok(None)
    }

    fn resolve_reconciliation_scope_read(
        &self,
        _maintenance_scope_ref: identity_contracts::refs::MaintenanceScopeRef,
        _consumer_ref: ConsumerRef,
        _visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError> {
        Ok(None)
    }

    fn resolve_projection_state_read(
        &self,
        _projection_ref: IdentityProjectionRef,
        _projection_state_ref: Option<ProjectionStateRef>,
        _consumer_ref: ConsumerRef,
        _visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError> {
        Ok(None)
    }

    fn resolve_reference_state_read(
        &self,
        _external_reference_ref: ExternalReferenceRef,
        _owner_ref: Option<IdentityReferenceOwnerRef>,
        _consumer_ref: ConsumerRef,
        _visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError> {
        Ok(None)
    }

    fn resolve_outbox_record_read(
        &self,
        _outbox_ref: Option<IdentityOutboxRecordRef>,
        _subject_ref: Option<IdentityOutboxSubjectRef>,
        _topic_key_ref: Option<TopicKeyRef>,
        _consumer_ref: ConsumerRef,
        _visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError> {
        Ok(None)
    }

    fn resolve_handoff_intent_read(
        &self,
        _intent_ref: TraceHandoffIntentRef,
        _consumer_ref: ConsumerRef,
        _visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError> {
        Ok(None)
    }

    fn get_visibility_decision(
        &self,
        _visibility_result_ref: VisibilityResultRef,
    ) -> Result<Option<identity_application::support::IdentityVisibilityDecision>, ApplicationError>
    {
        Ok(None)
    }

    fn save_visibility_decision(
        &self,
        _decision: identity_application::support::IdentityVisibilityDecision,
        _expected_version: Option<IdentityVersion>,
        _uow: &dyn IdentityUnitOfWork,
    ) -> Result<
        IdentityVersionedRef<identity_contracts::refs::IdentityVisibilityDecisionRef>,
        ApplicationError,
    > {
        Err(ApplicationError::consistency_defect(
            "visibility decision persistence is not part of commit-03-b fake runtime skeleton",
        ))
    }
}

impl IdentityOutboxRepository for IdentityInMemoryRuntime {
    fn get_outbox_record_with_version(
        &self,
        outbox_ref: IdentityOutboxRecordRef,
    ) -> Result<Option<Versioned<IdentityOutboxRecord>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .outbox_records
            .get(outbox_ref.as_str())
            .map(|stored| Versioned {
                value: stored.record.clone(),
                version: stored.version,
            }))
    }

    fn list_pending_outbox_records(
        &self,
        topic_key_ref: Option<TopicKeyRef>,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityOutboxRecordRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_outbox_page(
            store.outbox_records.values().collect(),
            page,
            |stored| {
                stored.record.outbox_state.state_kind == OutboxStateKind::PendingPublish
                    && topic_key_ref
                        .as_ref()
                        .map(|topic| &stored.record.topic_key_ref == topic)
                        .unwrap_or(true)
            },
        ))
    }

    fn list_retryable_outbox_records(
        &self,
        topic_key_ref: Option<TopicKeyRef>,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityOutboxRecordRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_outbox_page(
            store.outbox_records.values().collect(),
            page,
            |stored| {
                stored.record.is_retryable()
                    && topic_key_ref
                        .as_ref()
                        .map(|topic| &stored.record.topic_key_ref == topic)
                        .unwrap_or(true)
            },
        ))
    }

    fn list_outbox_records_by_subject(
        &self,
        subject_ref: IdentityOutboxSubjectRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityOutboxRecordRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_outbox_page(
            store.outbox_records.values().collect(),
            page,
            |stored| stored.record.matches_subject(&subject_ref),
        ))
    }

    fn find_outbox_records_by_trace(
        &self,
        trace_record_ref: IdentityTraceRecordRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityOutboxRecordRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_outbox_page(
            store.outbox_records.values().collect(),
            page,
            |stored| stored.record.trace_record_ref == trace_record_ref,
        ))
    }

    fn save_outbox_record(
        &self,
        record: IdentityOutboxRecord,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<IdentityOutboxRecordRef>, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveOutboxRecord {
                record: record.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: record.outbox_record_ref.clone(),
            version: self.predicted_outbox_version(&record.outbox_record_ref)?,
        })
    }

    fn update_outbox_state(
        &self,
        record: IdentityOutboxRecord,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<IdentityOutboxRecordRef>, ApplicationError> {
        self.save_outbox_record(record, Some(expected_version), uow)
    }
}

impl IdentityIdempotencyRepository for IdentityInMemoryRuntime {
    fn get_by_key(
        &self,
        operation_name: IdentityOperationName,
        channel: identity_contracts::refs::IdentityOperationChannel,
        idempotency_key: IdentityIdempotencyKey,
    ) -> Result<Option<Versioned<IdentityIdempotencyRecord>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let Some(record_ref) = store.idempotency_key_index.get(&idempotency_key_key(
            &operation_name,
            channel,
            &idempotency_key,
        )) else {
            return Ok(None);
        };
        Ok(store
            .idempotency_records
            .get(record_ref)
            .map(|stored| Versioned {
                value: stored.record.clone(),
                version: stored.version,
            }))
    }

    fn reserve(
        &self,
        context: IdentityOperationContext,
        record_ref: IdentityIdempotencyRecordRef,
        reserved_at: identity_contracts::refs::IdentityTimestamp,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdempotencyReserveOutcome, ApplicationError> {
        let idempotency_key = context
            .idempotency_key
            .clone()
            .ok_or_else(|| ApplicationError::invalid_request("idempotency key is required"))?;
        if let Some(existing) = self.get_by_key(
            context.operation_name.clone(),
            context.channel,
            idempotency_key,
        )? {
            let record = existing.value.clone();
            if record.can_replay(&context.request_digest) {
                let stored_result_ref = record.stored_result_ref.clone().ok_or_else(|| {
                    ApplicationError::new(
                        ApplicationErrorKind::DuplicateReplayConsistencyDefect,
                        "completed idempotency record missing stored result",
                    )
                })?;
                return Ok(IdempotencyReserveOutcome::ReplayAvailable {
                    record: existing,
                    stored_result_ref,
                });
            }
            if record
                .request_digest
                .conflicts_with(&context.request_digest)
            {
                return Ok(IdempotencyReserveOutcome::Conflict(existing));
            }
            return Ok(IdempotencyReserveOutcome::InFlight(existing));
        }

        let record = IdentityIdempotencyRecord::reserve(record_ref, &context, reserved_at)
            .ok_or_else(|| ApplicationError::invalid_request("idempotency key is required"))?;
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveIdempotencyReservation {
                record: record.clone(),
            },
        )?;
        Ok(IdempotencyReserveOutcome::Reserved(Versioned {
            value: record,
            version: IdentityVersion::new(1),
        }))
    }

    fn complete_with_stored_result(
        &self,
        record: IdentityIdempotencyRecord,
        stored_result_ref: identity_contracts::refs::IdentityStoredResultRef,
        completed_at: identity_contracts::refs::IdentityTimestamp,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<IdentityIdempotencyRecordRef>, ApplicationError> {
        let next = record.complete(stored_result_ref, completed_at);
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveIdempotencyTerminal {
                record: next.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: next.record_ref,
            version: IdentityVersion::new(expected_version.get() + 1),
        })
    }

    fn complete_rejected_with_stored_result(
        &self,
        record: IdentityIdempotencyRecord,
        stored_result_ref: identity_contracts::refs::IdentityStoredResultRef,
        completed_at: identity_contracts::refs::IdentityTimestamp,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<IdentityIdempotencyRecordRef>, ApplicationError> {
        let next = record.complete_rejected(stored_result_ref, completed_at);
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveIdempotencyTerminal {
                record: next.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: next.record_ref,
            version: IdentityVersion::new(expected_version.get() + 1),
        })
    }

    fn mark_conflict(
        &self,
        record: IdentityIdempotencyRecord,
        _conflicted_at: identity_contracts::refs::IdentityTimestamp,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<IdentityIdempotencyRecordRef>, ApplicationError> {
        let next = record.mark_conflict();
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveIdempotencyTerminal {
                record: next.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: next.record_ref,
            version: IdentityVersion::new(expected_version.get() + 1),
        })
    }
}

impl IdentityStoredResultRepository for IdentityInMemoryRuntime {
    fn get_stored_result(
        &self,
        stored_result_ref: identity_contracts::refs::IdentityStoredResultRef,
    ) -> Result<Option<StoredIdentityOperationResult>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .stored_results
            .get(stored_result_ref.as_str())
            .cloned())
    }

    fn find_by_operation_context(
        &self,
        context_ref: IdentityOperationContextRef,
    ) -> Result<Option<StoredIdentityOperationResult>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let Some(result_ref) = store.stored_result_by_context.get(context_ref.as_str()) else {
            return Ok(None);
        };
        Ok(store.stored_results.get(result_ref).cloned())
    }

    fn save_command_accepted_result(
        &self,
        result: StoredIdentityOperationResult,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<identity_contracts::refs::IdentityStoredResultRef, ApplicationError> {
        validate_stored_result_kind(&result, IdentityStoredResultKind::CommandAccepted)?;
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveStoredResult {
                result: result.clone(),
            },
        )?;
        Ok(result.stored_result_ref)
    }

    fn save_command_rejected_result(
        &self,
        result: StoredIdentityOperationResult,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<identity_contracts::refs::IdentityStoredResultRef, ApplicationError> {
        validate_stored_result_kind(&result, IdentityStoredResultKind::CommandRejected)?;
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveStoredResult {
                result: result.clone(),
            },
        )?;
        Ok(result.stored_result_ref)
    }

    fn save_consumer_receipt_result(
        &self,
        result: StoredIdentityOperationResult,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<identity_contracts::refs::IdentityStoredResultRef, ApplicationError> {
        validate_stored_result_kind(&result, IdentityStoredResultKind::ConsumerReceipt)?;
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveStoredResult {
                result: result.clone(),
            },
        )?;
        Ok(result.stored_result_ref)
    }

    fn get_consumer_receipt(
        &self,
        stored_result_ref: identity_contracts::refs::IdentityStoredResultRef,
    ) -> Result<Option<IdentityConsumerReceiptEnvelope>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .consumer_receipts
            .get(stored_result_ref.as_str())
            .cloned())
    }

    fn save_consumer_receipt(
        &self,
        envelope: IdentityConsumerReceiptEnvelope,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<identity_contracts::refs::IdentityStoredResultRef, ApplicationError> {
        validate_receipt_envelope_kind(&envelope, IdentityStoredResultKind::ConsumerReceipt)?;
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveConsumerReceiptEnvelope {
                envelope: envelope.clone(),
            },
        )?;
        Ok(envelope.stored_result_ref)
    }

    fn save_job_report_result(
        &self,
        result: StoredIdentityOperationResult,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<identity_contracts::refs::IdentityStoredResultRef, ApplicationError> {
        validate_stored_result_kind(&result, IdentityStoredResultKind::JobReport)?;
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveStoredResult {
                result: result.clone(),
            },
        )?;
        Ok(result.stored_result_ref)
    }

    fn save_handoff_callback_receipt_result(
        &self,
        result: StoredIdentityOperationResult,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<identity_contracts::refs::IdentityStoredResultRef, ApplicationError> {
        validate_stored_result_kind(&result, IdentityStoredResultKind::HandoffCallbackReceipt)?;
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveStoredResult {
                result: result.clone(),
            },
        )?;
        Ok(result.stored_result_ref)
    }

    fn get_handoff_callback_receipt(
        &self,
        stored_result_ref: identity_contracts::refs::IdentityStoredResultRef,
    ) -> Result<Option<IdentityConsumerReceiptEnvelope>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .handoff_callback_receipts
            .get(stored_result_ref.as_str())
            .cloned())
    }

    fn save_handoff_callback_receipt(
        &self,
        envelope: IdentityConsumerReceiptEnvelope,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<identity_contracts::refs::IdentityStoredResultRef, ApplicationError> {
        validate_receipt_envelope_kind(
            &envelope,
            IdentityStoredResultKind::HandoffCallbackReceipt,
        )?;
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveHandoffCallbackReceiptEnvelope {
                envelope: envelope.clone(),
            },
        )?;
        Ok(envelope.stored_result_ref)
    }
}

impl IdentityCommandEffectSummaryRepository for IdentityInMemoryRuntime {
    fn get_effect_summary(
        &self,
        effect_summary_ref: IdentityCommandEffectSummaryRef,
    ) -> Result<Option<IdentityCommandEffectSummary>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .command_effect_summaries
            .get(effect_summary_ref.as_str())
            .cloned())
    }

    fn list_effects_by_operation_context(
        &self,
        context_ref: IdentityOperationContextRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityCommandEffectSummaryRef>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let mut items: Vec<_> = store
            .command_effect_summaries
            .values()
            .filter(|summary| summary.operation_context_ref == context_ref)
            .map(|summary| summary.effect_summary_ref.clone())
            .collect();
        items.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        let (items, next_cursor) = paged(items, page, "effect-context");
        Ok(Page { items, next_cursor })
    }

    fn list_effects_by_truth_ref(
        &self,
        truth_ref: IdentityTruthRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityCommandEffectSummaryRef>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let mut items: Vec<_> = store
            .command_effect_summaries
            .values()
            .filter(|summary| summary.primary_truth_ref == truth_ref)
            .map(|summary| summary.effect_summary_ref.clone())
            .collect();
        items.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        let (items, next_cursor) = paged(items, page, "effect-truth");
        Ok(Page { items, next_cursor })
    }

    fn list_effects_after_cursor(
        &self,
        after_cursor: Option<IdentityTruthCursor>,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityCommandEffectSummaryRef>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let mut items: Vec<_> = store
            .command_effect_summaries
            .values()
            .filter(|summary| {
                after_cursor
                    .as_ref()
                    .map(|cursor| summary.accepted_cursor_ref.as_str() > cursor.as_str())
                    .unwrap_or(true)
            })
            .map(|summary| summary.effect_summary_ref.clone())
            .collect();
        items.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        let (items, next_cursor) = paged(items, page, "effect-cursor");
        Ok(Page { items, next_cursor })
    }

    fn save_effect_summary(
        &self,
        summary: IdentityCommandEffectSummary,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityCommandEffectSummaryRef, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveEffectSummary {
                summary: summary.clone(),
            },
        )?;
        Ok(summary.effect_summary_ref)
    }
}

impl IdentityJobReportRepository for IdentityInMemoryRuntime {
    fn get_job_report_with_version(
        &self,
        report_ref: identity_contracts::refs::IdentityJobReportRef,
    ) -> Result<Option<Versioned<IdentityJobRunReport>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .job_reports
            .get(report_ref.as_str())
            .map(|stored| Versioned {
                value: stored.report.clone(),
                version: stored.version,
            }))
    }

    fn find_job_report_by_run(
        &self,
        job_run_ref: identity_contracts::refs::IdentityJobRunRef,
    ) -> Result<Option<Versioned<IdentityJobRunReport>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let Some(report_ref) = store.job_report_by_run.get(job_run_ref.as_str()) else {
            return Ok(None);
        };
        Ok(store.job_reports.get(report_ref).map(|stored| Versioned {
            value: stored.report.clone(),
            version: stored.version,
        }))
    }

    fn list_job_reports_by_name(
        &self,
        job_name: IdentityJobName,
        page: IdentityRepositoryPage,
    ) -> Result<
        Page<IdentityVersionedRef<identity_contracts::refs::IdentityJobReportRef>>,
        ApplicationError,
    > {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_job_report_page(
            store.job_reports.values().collect(),
            page,
            |stored| stored.report.job_name == job_name,
        ))
    }

    fn list_job_reports_by_result(
        &self,
        result_kind: IdentityJobResultKind,
        page: IdentityRepositoryPage,
    ) -> Result<
        Page<IdentityVersionedRef<identity_contracts::refs::IdentityJobReportRef>>,
        ApplicationError,
    > {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_job_report_page(
            store.job_reports.values().collect(),
            page,
            |stored| stored.report.result_kind == result_kind,
        ))
    }

    fn save_job_report(
        &self,
        report: IdentityJobRunReport,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<
        IdentityVersionedRef<identity_contracts::refs::IdentityJobReportRef>,
        ApplicationError,
    > {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveJobReport {
                report: report.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: report.report_ref.clone(),
            version: self.predicted_job_report_version(&report.report_ref)?,
        })
    }
}

fn apply_op(
    store: &mut RuntimeStore,
    baseline: &RuntimeStore,
    op: StagedOp,
) -> Result<(), ApplicationError> {
    match op {
        StagedOp::SaveMemberSummaryView {
            view,
            expected_version,
        } => apply_save_member_summary_view(store, view, expected_version),
        StagedOp::SaveProjectionState {
            state,
            expected_version,
        } => apply_save_projection_state(store, baseline, state, expected_version),
        StagedOp::SaveReferenceState {
            state,
            expected_version,
        } => apply_save_reference_state(store, state, expected_version),
        StagedOp::SaveTypedSidecars {
            reference_ref,
            sidecars,
            expected_version,
        } => apply_save_typed_sidecars(store, reference_ref, sidecars, expected_version),
        StagedOp::SaveHandoffIntent {
            intent,
            expected_version,
        } => apply_save_handoff_intent(store, intent, expected_version),
        StagedOp::SaveOutboxRecord {
            record,
            expected_version,
        } => apply_save_outbox_record(store, record, expected_version),
        StagedOp::SaveIdempotencyReservation { record } => {
            apply_save_idempotency_reservation(store, record)
        }
        StagedOp::SaveIdempotencyTerminal {
            record,
            expected_version,
        } => apply_save_idempotency_terminal(store, record, expected_version),
        StagedOp::SaveStoredResult { result } => apply_save_stored_result(store, result),
        StagedOp::SaveConsumerReceiptEnvelope { envelope } => {
            apply_save_consumer_receipt(store, envelope)
        }
        StagedOp::SaveHandoffCallbackReceiptEnvelope { envelope } => {
            apply_save_handoff_callback_receipt(store, envelope)
        }
        StagedOp::SaveEffectSummary { summary } => apply_save_effect_summary(store, summary),
        StagedOp::SaveJobReport {
            report,
            expected_version,
        } => apply_save_job_report(store, report, expected_version),
    }
}

fn apply_save_member_summary_view(
    store: &mut RuntimeStore,
    view: MemberSummaryView,
    expected_version: Option<IdentityVersion>,
) -> Result<(), ApplicationError> {
    match (
        store.member_summary_views.get(view.view_ref.as_str()),
        expected_version,
    ) {
        (None, None) => {
            store.member_scope_index.insert(
                member_scope_key(&view.member_ref, &view.visibility_scope_ref),
                view.view_ref.as_str().to_owned(),
            );
            store.member_summary_views.insert(
                view.view_ref.as_str().to_owned(),
                StoredMemberSummaryView {
                    view,
                    version: IdentityVersion::new(1),
                },
            );
            Ok(())
        }
        (Some(existing), Some(expected)) if existing.version == expected => {
            store.member_scope_index.insert(
                member_scope_key(&view.member_ref, &view.visibility_scope_ref),
                view.view_ref.as_str().to_owned(),
            );
            store.member_summary_views.insert(
                view.view_ref.as_str().to_owned(),
                StoredMemberSummaryView {
                    view,
                    version: IdentityVersion::new(expected.get() + 1),
                },
            );
            Ok(())
        }
        (None, Some(_)) => Err(ApplicationError::not_found(
            "member summary view not found for update",
        )),
        _ => Err(ApplicationError::optimistic_version_conflict(
            "member summary view version mismatch",
        )),
    }
}

fn apply_save_projection_state(
    store: &mut RuntimeStore,
    _baseline: &RuntimeStore,
    state: ProjectionState,
    expected_version: Option<IdentityVersion>,
) -> Result<(), ApplicationError> {
    let key = projection_key(&state.projection_ref);
    match (store.projection_states.get(&key), expected_version) {
        (None, None) => {
            store.projection_states.insert(
                key,
                StoredProjectionState {
                    state,
                    version: IdentityVersion::new(1),
                },
            );
            Ok(())
        }
        (Some(existing), Some(expected)) if existing.version == expected => {
            let existing_cursor = existing
                .state
                .source_cursor_ref
                .as_ref()
                .map(|cursor| cursor.source_cursor_ref.external_ref.as_str());
            let next_cursor = state
                .source_cursor_ref
                .as_ref()
                .map(|cursor| cursor.source_cursor_ref.external_ref.as_str());
            if existing_cursor.is_some() && next_cursor.is_some() && next_cursor < existing_cursor {
                return Err(ApplicationError::optimistic_version_conflict(
                    "newer projection state already exists",
                ));
            }
            store.projection_states.insert(
                key,
                StoredProjectionState {
                    state,
                    version: IdentityVersion::new(expected.get() + 1),
                },
            );
            Ok(())
        }
        (None, Some(_)) => Err(ApplicationError::not_found(
            "projection state not found for update",
        )),
        _ => Err(ApplicationError::optimistic_version_conflict(
            "projection state version mismatch",
        )),
    }
}

fn apply_save_reference_state(
    store: &mut RuntimeStore,
    state: ReferenceResolutionState,
    expected_version: Option<IdentityVersion>,
) -> Result<(), ApplicationError> {
    let key = external_reference_key(&state.external_reference_ref);
    match (store.reference_states.get(&key), expected_version) {
        (None, None) => {
            store.reference_states.insert(
                key,
                StoredReferenceState {
                    state,
                    sidecars: empty_sidecars(),
                    version: IdentityVersion::new(1),
                },
            );
            Ok(())
        }
        (Some(existing), Some(expected)) if existing.version == expected => {
            let mut next_state = state;
            if matches!(
                next_state.state_kind,
                ReferenceResolutionStateKind::Unavailable
            ) && next_state.safe_summary_ref.is_none()
            {
                next_state.safe_summary_ref = existing.state.safe_summary_ref.clone();
                next_state.source_version_ref = existing.state.source_version_ref.clone();
            }
            store.reference_states.insert(
                key,
                StoredReferenceState {
                    state: next_state,
                    sidecars: existing.sidecars.clone(),
                    version: IdentityVersion::new(expected.get() + 1),
                },
            );
            Ok(())
        }
        (None, Some(_)) => Err(ApplicationError::not_found(
            "reference state not found for update",
        )),
        _ => Err(ApplicationError::optimistic_version_conflict(
            "reference state version mismatch",
        )),
    }
}

fn apply_save_typed_sidecars(
    store: &mut RuntimeStore,
    reference_ref: ExternalReferenceRef,
    sidecars: ExternalReferenceTypedSidecarRefs,
    expected_version: IdentityVersion,
) -> Result<(), ApplicationError> {
    let key = external_reference_key(&reference_ref);
    let existing = store
        .reference_states
        .get(&key)
        .ok_or_else(|| ApplicationError::not_found("reference state not found for sidecar save"))?;
    if existing.version != expected_version {
        return Err(ApplicationError::optimistic_version_conflict(
            "reference bundle version mismatch",
        ));
    }
    let state = existing.state.clone();
    store.reference_states.insert(
        key,
        StoredReferenceState {
            state,
            sidecars,
            version: IdentityVersion::new(expected_version.get() + 1),
        },
    );
    Ok(())
}

fn apply_save_handoff_intent(
    store: &mut RuntimeStore,
    intent: TraceHandoffIntent,
    expected_version: Option<IdentityVersion>,
) -> Result<(), ApplicationError> {
    match (
        store
            .handoff_intents
            .get(intent.handoff_intent_ref.as_str()),
        expected_version,
    ) {
        (None, None) => {
            store.handoff_intents.insert(
                intent.handoff_intent_ref.as_str().to_owned(),
                StoredHandoffIntent {
                    intent,
                    version: IdentityVersion::new(1),
                },
            );
            Ok(())
        }
        (Some(existing), Some(expected)) if existing.version == expected => {
            if intent.handoff_state.state_kind == HandoffStateKind::Delivered
                && intent.handoff_state.receipt_ref.is_none()
            {
                return Err(ApplicationError::domain_rejected(
                    "delivered handoff requires formal receipt marker",
                ));
            }
            store.handoff_intents.insert(
                intent.handoff_intent_ref.as_str().to_owned(),
                StoredHandoffIntent {
                    intent,
                    version: IdentityVersion::new(expected.get() + 1),
                },
            );
            Ok(())
        }
        (None, Some(_)) => Err(ApplicationError::not_found(
            "handoff intent not found for update",
        )),
        _ => Err(ApplicationError::optimistic_version_conflict(
            "handoff intent version mismatch",
        )),
    }
}

fn apply_save_outbox_record(
    store: &mut RuntimeStore,
    record: IdentityOutboxRecord,
    expected_version: Option<IdentityVersion>,
) -> Result<(), ApplicationError> {
    if store.faults.contains(&FaultCase::SaveOutboxRecordFails) {
        return Err(ApplicationError::dependency_unavailable(
            "outbox record persistence unavailable",
        ));
    }

    match (
        store.outbox_records.get(record.outbox_record_ref.as_str()),
        expected_version,
    ) {
        (None, None) => {
            if store
                .outbox_records
                .contains_key(record.outbox_record_ref.as_str())
            {
                return Err(ApplicationError::new(
                    ApplicationErrorKind::FormalUniqueConflict,
                    "outbox record already exists",
                ));
            }
            let key = record.outbox_record_ref.as_str().to_owned();
            store.outbox_subject_index.insert(
                outbox_subject_key(&record.subject_ref, &record.outbox_record_ref),
                key.clone(),
            );
            store.outbox_trace_index.insert(
                outbox_trace_key(&record.trace_record_ref, &record.outbox_record_ref),
                key.clone(),
            );
            store.outbox_records.insert(
                key,
                StoredOutboxRecord {
                    record,
                    version: IdentityVersion::new(1),
                },
            );
            Ok(())
        }
        (Some(existing), Some(expected)) if existing.version == expected => {
            let key = record.outbox_record_ref.as_str().to_owned();
            store.outbox_subject_index.retain(|_, value| value != &key);
            store.outbox_trace_index.retain(|_, value| value != &key);
            store.outbox_subject_index.insert(
                outbox_subject_key(&record.subject_ref, &record.outbox_record_ref),
                key.clone(),
            );
            store.outbox_trace_index.insert(
                outbox_trace_key(&record.trace_record_ref, &record.outbox_record_ref),
                key.clone(),
            );
            store.outbox_records.insert(
                key,
                StoredOutboxRecord {
                    record,
                    version: IdentityVersion::new(expected.get() + 1),
                },
            );
            Ok(())
        }
        (None, Some(_)) => Err(ApplicationError::not_found(
            "outbox record not found for update",
        )),
        _ => Err(ApplicationError::optimistic_version_conflict(
            "outbox record version mismatch",
        )),
    }
}

fn apply_save_idempotency_reservation(
    store: &mut RuntimeStore,
    record: IdentityIdempotencyRecord,
) -> Result<(), ApplicationError> {
    let namespace_key = idempotency_key_key(
        &record.operation_name,
        record.channel,
        &record.idempotency_key,
    );
    if store.idempotency_key_index.contains_key(&namespace_key) {
        return Err(ApplicationError::new(
            ApplicationErrorKind::FormalUniqueConflict,
            "idempotency namespace already reserved",
        ));
    }

    let key = record.record_ref.as_str().to_owned();
    store
        .idempotency_key_index
        .insert(namespace_key, key.clone());
    store.idempotency_records.insert(
        key,
        StoredIdempotencyRecord {
            record,
            version: IdentityVersion::new(1),
        },
    );
    Ok(())
}

fn apply_save_idempotency_terminal(
    store: &mut RuntimeStore,
    record: IdentityIdempotencyRecord,
    expected_version: IdentityVersion,
) -> Result<(), ApplicationError> {
    if store.faults.contains(&FaultCase::CompleteIdempotencyFails) {
        return Err(ApplicationError::dependency_unavailable(
            "idempotency completion unavailable",
        ));
    }

    let key = record.record_ref.as_str().to_owned();
    let Some(existing) = store.idempotency_records.get(&key) else {
        return Err(ApplicationError::not_found(
            "idempotency record not found for update",
        ));
    };
    if existing.version != expected_version {
        return Err(ApplicationError::optimistic_version_conflict(
            "idempotency record version mismatch",
        ));
    }
    store.idempotency_records.insert(
        key,
        StoredIdempotencyRecord {
            record,
            version: IdentityVersion::new(expected_version.get() + 1),
        },
    );
    Ok(())
}

fn apply_save_stored_result(
    store: &mut RuntimeStore,
    result: StoredIdentityOperationResult,
) -> Result<(), ApplicationError> {
    if store.faults.contains(&FaultCase::SaveStoredResultFails) {
        return Err(ApplicationError::dependency_unavailable(
            "stored result persistence unavailable",
        ));
    }
    let stored_result_ref = result.stored_result_ref.as_str().to_owned();
    if store.stored_results.contains_key(&stored_result_ref) {
        return Err(ApplicationError::new(
            ApplicationErrorKind::FormalUniqueConflict,
            "stored result already exists",
        ));
    }
    if let Some(existing_ref) = store
        .stored_result_by_context
        .get(result.operation_context_ref.as_str())
    {
        if existing_ref != &stored_result_ref {
            return Err(ApplicationError::new(
                ApplicationErrorKind::FormalUniqueConflict,
                "operation context already has a stored result",
            ));
        }
    }
    store.stored_result_by_context.insert(
        result.operation_context_ref.as_str().to_owned(),
        stored_result_ref.clone(),
    );
    store.stored_results.insert(stored_result_ref, result);
    Ok(())
}

fn apply_save_consumer_receipt(
    store: &mut RuntimeStore,
    envelope: IdentityConsumerReceiptEnvelope,
) -> Result<(), ApplicationError> {
    if store.faults.contains(&FaultCase::SaveReceiptEnvelopeFails) {
        return Err(ApplicationError::dependency_unavailable(
            "consumer receipt persistence unavailable",
        ));
    }
    let stored_result_ref = envelope.stored_result_ref.as_str().to_owned();
    if store.consumer_receipts.contains_key(&stored_result_ref) {
        return Err(ApplicationError::new(
            ApplicationErrorKind::FormalUniqueConflict,
            "consumer receipt envelope already exists",
        ));
    }
    store.consumer_receipts.insert(stored_result_ref, envelope);
    Ok(())
}

fn apply_save_handoff_callback_receipt(
    store: &mut RuntimeStore,
    envelope: IdentityConsumerReceiptEnvelope,
) -> Result<(), ApplicationError> {
    if store.faults.contains(&FaultCase::SaveReceiptEnvelopeFails) {
        return Err(ApplicationError::dependency_unavailable(
            "handoff callback receipt persistence unavailable",
        ));
    }
    let stored_result_ref = envelope.stored_result_ref.as_str().to_owned();
    if store
        .handoff_callback_receipts
        .contains_key(&stored_result_ref)
    {
        return Err(ApplicationError::new(
            ApplicationErrorKind::FormalUniqueConflict,
            "handoff callback receipt envelope already exists",
        ));
    }
    store
        .handoff_callback_receipts
        .insert(stored_result_ref, envelope);
    Ok(())
}

fn apply_save_effect_summary(
    store: &mut RuntimeStore,
    summary: IdentityCommandEffectSummary,
) -> Result<(), ApplicationError> {
    let key = summary.effect_summary_ref.as_str().to_owned();
    if store.command_effect_summaries.contains_key(&key) {
        return Err(ApplicationError::new(
            ApplicationErrorKind::FormalUniqueConflict,
            "command effect summary already exists",
        ));
    }
    store.effects_by_context.insert(
        effect_context_key(&summary.operation_context_ref, &summary.effect_summary_ref),
        key.clone(),
    );
    store.effects_by_truth_ref.insert(
        effect_truth_key(&summary.primary_truth_ref, &summary.effect_summary_ref),
        key.clone(),
    );
    store.effects_after_cursor.insert(
        effect_cursor_key(&summary.accepted_cursor_ref, &summary.effect_summary_ref),
        key.clone(),
    );
    store.command_effect_summaries.insert(key, summary);
    Ok(())
}

fn apply_save_job_report(
    store: &mut RuntimeStore,
    report: IdentityJobRunReport,
    expected_version: Option<IdentityVersion>,
) -> Result<(), ApplicationError> {
    if store.faults.contains(&FaultCase::SaveJobReportFails) {
        return Err(ApplicationError::dependency_unavailable(
            "job report persistence unavailable",
        ));
    }

    match (
        store.job_reports.get(report.report_ref.as_str()),
        expected_version,
    ) {
        (None, None) => {
            if let Some(existing) = store.job_report_by_run.get(report.job_run_ref.as_str()) {
                if existing != report.report_ref.as_str() {
                    return Err(ApplicationError::new(
                        ApplicationErrorKind::FormalUniqueConflict,
                        "job run already has a report",
                    ));
                }
            }
            let key = report.report_ref.as_str().to_owned();
            store
                .job_report_by_run
                .insert(report.job_run_ref.as_str().to_owned(), key.clone());
            store.job_report_by_name.insert(
                job_report_name_key(&report.job_name, &report.report_ref),
                key.clone(),
            );
            store.job_report_by_result.insert(
                job_report_result_key(report.result_kind, &report.report_ref),
                key.clone(),
            );
            store.job_reports.insert(
                key,
                StoredJobReport {
                    report,
                    version: IdentityVersion::new(1),
                },
            );
            Ok(())
        }
        (Some(existing), Some(expected)) if existing.version == expected => {
            let key = report.report_ref.as_str().to_owned();
            store.job_report_by_name.retain(|_, value| value != &key);
            store.job_report_by_result.retain(|_, value| value != &key);
            store
                .job_report_by_run
                .insert(report.job_run_ref.as_str().to_owned(), key.clone());
            store.job_report_by_name.insert(
                job_report_name_key(&report.job_name, &report.report_ref),
                key.clone(),
            );
            store.job_report_by_result.insert(
                job_report_result_key(report.result_kind, &report.report_ref),
                key.clone(),
            );
            store.job_reports.insert(
                key,
                StoredJobReport {
                    report,
                    version: IdentityVersion::new(expected.get() + 1),
                },
            );
            Ok(())
        }
        (None, Some(_)) => Err(ApplicationError::not_found(
            "job report not found for update",
        )),
        _ => Err(ApplicationError::optimistic_version_conflict(
            "job report version mismatch",
        )),
    }
}

fn project_projection_page<F>(
    entries: Vec<&StoredProjectionState>,
    page: IdentityRepositoryPage,
    predicate: F,
) -> Page<IdentityVersionedRef<ProjectionStateRef>>
where
    F: Fn(&StoredProjectionState) -> bool,
{
    let filtered: Vec<_> = entries
        .into_iter()
        .filter(|entry| predicate(entry))
        .collect();
    let start = parse_page_cursor(page.cursor.as_ref(), "projection");
    let items: Vec<_> = filtered
        .iter()
        .skip(start)
        .take(page.limit as usize)
        .map(|entry| IdentityVersionedRef {
            value_ref: entry.state.projection_state_ref.clone(),
            version: entry.version,
        })
        .collect();
    let next_cursor = if start + items.len() < filtered.len() {
        Some(IdentityRepositoryCursor::new(format!(
            "projection:{}",
            start + items.len()
        )))
    } else {
        None
    };
    Page { items, next_cursor }
}

fn project_outbox_page<F>(
    entries: Vec<&StoredOutboxRecord>,
    page: IdentityRepositoryPage,
    predicate: F,
) -> Page<IdentityVersionedRef<IdentityOutboxRecordRef>>
where
    F: Fn(&StoredOutboxRecord) -> bool,
{
    let filtered: Vec<_> = entries
        .into_iter()
        .filter(|entry| predicate(entry))
        .collect();
    let start = parse_page_cursor(page.cursor.as_ref(), "outbox");
    let items: Vec<_> = filtered
        .iter()
        .skip(start)
        .take(page.limit as usize)
        .map(|entry| IdentityVersionedRef {
            value_ref: entry.record.outbox_record_ref.clone(),
            version: entry.version,
        })
        .collect();
    let next_cursor = if start + items.len() < filtered.len() {
        Some(IdentityRepositoryCursor::new(format!(
            "outbox:{}",
            start + items.len()
        )))
    } else {
        None
    };
    Page { items, next_cursor }
}

fn project_job_report_page<F>(
    entries: Vec<&StoredJobReport>,
    page: IdentityRepositoryPage,
    predicate: F,
) -> Page<IdentityVersionedRef<identity_contracts::refs::IdentityJobReportRef>>
where
    F: Fn(&StoredJobReport) -> bool,
{
    let filtered: Vec<_> = entries
        .into_iter()
        .filter(|entry| predicate(entry))
        .collect();
    let start = parse_page_cursor(page.cursor.as_ref(), "job-report");
    let items: Vec<_> = filtered
        .iter()
        .skip(start)
        .take(page.limit as usize)
        .map(|entry| IdentityVersionedRef {
            value_ref: entry.report.report_ref.clone(),
            version: entry.version,
        })
        .collect();
    let next_cursor = if start + items.len() < filtered.len() {
        Some(IdentityRepositoryCursor::new(format!(
            "job-report:{}",
            start + items.len()
        )))
    } else {
        None
    };
    Page { items, next_cursor }
}

fn project_reference_page<F>(
    entries: Vec<&StoredReferenceState>,
    page: IdentityRepositoryPage,
    predicate: F,
) -> Page<IdentityVersionedRef<ReferenceResolutionStateRef>>
where
    F: Fn(&StoredReferenceState) -> bool,
{
    let filtered: Vec<_> = entries
        .into_iter()
        .filter(|entry| predicate(entry))
        .collect();
    let start = parse_page_cursor(page.cursor.as_ref(), "reference");
    let items: Vec<_> = filtered
        .iter()
        .skip(start)
        .take(page.limit as usize)
        .map(|entry| IdentityVersionedRef {
            value_ref: entry.state.resolution_state_ref.clone(),
            version: entry.version,
        })
        .collect();
    let next_cursor = if start + items.len() < filtered.len() {
        Some(IdentityRepositoryCursor::new(format!(
            "reference:{}",
            start + items.len()
        )))
    } else {
        None
    };
    Page { items, next_cursor }
}

fn project_handoff_page<F>(
    entries: Vec<&StoredHandoffIntent>,
    page: IdentityRepositoryPage,
    predicate: F,
) -> Page<IdentityVersionedRef<TraceHandoffIntentRef>>
where
    F: Fn(&StoredHandoffIntent) -> bool,
{
    let filtered: Vec<_> = entries
        .into_iter()
        .filter(|entry| predicate(entry))
        .collect();
    let start = parse_page_cursor(page.cursor.as_ref(), "handoff");
    let items: Vec<_> = filtered
        .iter()
        .skip(start)
        .take(page.limit as usize)
        .map(|entry| IdentityVersionedRef {
            value_ref: entry.intent.handoff_intent_ref.clone(),
            version: entry.version,
        })
        .collect();
    let next_cursor = if start + items.len() < filtered.len() {
        Some(IdentityRepositoryCursor::new(format!(
            "handoff:{}",
            start + items.len()
        )))
    } else {
        None
    };
    Page { items, next_cursor }
}

fn paged<T>(
    items: Vec<T>,
    page: IdentityRepositoryPage,
    prefix: &str,
) -> (Vec<T>, Option<IdentityRepositoryCursor>)
where
    T: Clone,
{
    let start = parse_page_cursor(page.cursor.as_ref(), prefix);
    let page_items: Vec<_> = items
        .iter()
        .skip(start)
        .take(page.limit as usize)
        .cloned()
        .collect();
    let next_cursor = if start + page_items.len() < items.len() {
        Some(IdentityRepositoryCursor::new(format!(
            "{prefix}:{}",
            start + page_items.len()
        )))
    } else {
        None
    };
    (page_items, next_cursor)
}

fn parse_page_cursor(cursor: Option<&IdentityRepositoryCursor>, prefix: &str) -> usize {
    cursor
        .and_then(|cursor| cursor.as_str().strip_prefix(&format!("{prefix}:")))
        .and_then(|index| index.parse::<usize>().ok())
        .unwrap_or(0)
}

fn tx_key(transaction_ref: &IdentityTransactionRef) -> String {
    transaction_ref.as_str().to_owned()
}

fn member_key(member_ref: &GlobalMemberRef) -> String {
    member_ref.id().as_str().to_owned()
}

fn member_scope_key(member_ref: &GlobalMemberRef, scope_ref: &VisibilityScopeRef) -> String {
    format!("{}::{}", member_ref.id().as_str(), scope_ref.as_str())
}

fn outbox_subject_key(
    subject_ref: &IdentityOutboxSubjectRef,
    outbox_ref: &IdentityOutboxRecordRef,
) -> String {
    format!("{}::{}", subject_ref.as_str(), outbox_ref.as_str())
}

fn outbox_trace_key(
    trace_ref: &IdentityTraceRecordRef,
    outbox_ref: &IdentityOutboxRecordRef,
) -> String {
    format!("{}::{}", trace_ref.as_str(), outbox_ref.as_str())
}

fn projection_key(projection_ref: &IdentityProjectionRef) -> String {
    projection_ref.as_str().to_owned()
}

fn external_reference_key(reference_ref: &ExternalReferenceRef) -> String {
    format!(
        "{:?}::{}::{}",
        reference_ref.reference_kind,
        reference_ref.source_ref.owner() as u8,
        reference_ref.source_ref.external_ref.as_str()
    )
}

fn idempotency_key_key(
    operation_name: &IdentityOperationName,
    channel: identity_contracts::refs::IdentityOperationChannel,
    idempotency_key: &IdentityIdempotencyKey,
) -> String {
    format!(
        "{}::{channel:?}::{}",
        operation_name.as_str(),
        idempotency_key.as_public().as_str()
    )
}

fn effect_context_key(
    context_ref: &IdentityOperationContextRef,
    effect_ref: &IdentityCommandEffectSummaryRef,
) -> String {
    format!("{}::{}", context_ref.as_str(), effect_ref.as_str())
}

fn effect_truth_key(
    truth_ref: &IdentityTruthRef,
    effect_ref: &IdentityCommandEffectSummaryRef,
) -> String {
    format!("{}::{}", truth_ref_key(truth_ref), effect_ref.as_str())
}

fn effect_cursor_key(
    cursor_ref: &IdentityTruthCursor,
    effect_ref: &IdentityCommandEffectSummaryRef,
) -> String {
    format!("{}::{}", cursor_ref.as_str(), effect_ref.as_str())
}

fn truth_ref_key(truth_ref: &IdentityTruthRef) -> String {
    match truth_ref {
        IdentityTruthRef::GlobalMember(value) => format!("global_member:{}", value.id().as_str()),
        IdentityTruthRef::RoleCapabilitySummary(value) => {
            format!("role_capability_summary:{}", value.summary_id.as_str())
        }
        IdentityTruthRef::RoleCapabilitySourceSnapshot(value) => {
            format!(
                "role_capability_source_snapshot:{}",
                value.snapshot_id.as_str()
            )
        }
        IdentityTruthRef::CareerRecord(value) => {
            format!("career_record:{}", value.record_id.as_str())
        }
        IdentityTruthRef::MemoryReference(value) => {
            format!("memory_reference:{}", value.reference_id.as_str())
        }
        IdentityTruthRef::TraceHandoffIntent(value) => {
            format!("trace_handoff_intent:{}", value.as_str())
        }
    }
}

fn job_report_name_key(
    job_name: &IdentityJobName,
    report_ref: &identity_contracts::refs::IdentityJobReportRef,
) -> String {
    format!("{}::{}", job_name.as_str(), report_ref.as_str())
}

fn job_report_result_key(
    result_kind: IdentityJobResultKind,
    report_ref: &identity_contracts::refs::IdentityJobReportRef,
) -> String {
    format!("{result_kind:?}::{}", report_ref.as_str())
}

fn validate_stored_result_kind(
    result: &StoredIdentityOperationResult,
    expected: IdentityStoredResultKind,
) -> Result<(), ApplicationError> {
    if result.result_kind != expected {
        return Err(ApplicationError::invalid_request(format!(
            "stored result kind mismatch: expected {expected:?}"
        )));
    }
    Ok(())
}

fn validate_receipt_envelope_kind(
    envelope: &IdentityConsumerReceiptEnvelope,
    expected: IdentityStoredResultKind,
) -> Result<(), ApplicationError> {
    if envelope.result_kind != expected {
        return Err(ApplicationError::invalid_request(format!(
            "receipt envelope kind mismatch: expected {expected:?}"
        )));
    }
    Ok(())
}

fn empty_sidecars() -> ExternalReferenceTypedSidecarRefs {
    ExternalReferenceTypedSidecarRefs {
        role_capability_safe_summary_ref: None,
        career_safe_summary_ref: None,
        memory_safe_summary_ref: None,
        governance_basis_summary_ref: None,
        evidence_summary_ref: None,
        source_version_ref: None,
    }
}

fn identity_source_ref(owner: IdentitySourceOwner, token: &str) -> IdentitySourceRef {
    IdentitySourceRef::new(
        owner,
        ExternalSourceRef::new(token.to_owned()).expect("valid external source ref"),
    )
    .expect("valid source ref")
}

#[cfg(test)]
mod tests {
    use core_contracts::actor::{ActorKind, ActorRef};
    use identity_contracts::events::{IdentityConsumerOutcome, IdentityConsumerReceipt};
    use identity_contracts::protocol::{
        IdentityDigestAlgorithmMarkerRef, IdentityInboundConsumerName, IdentityJobName,
        IdentityProtocolSchemaVersionRef,
    };
    use identity_contracts::receipts::MaintenanceIssueRef;
    use identity_contracts::refs::{
        ExternalReferenceSafeSummaryRef, ExternalSourceVersionRef, HandoffAttemptRef,
        IdentityCanonicalRequestMarkerRef, IdentityChangeKind, IdentityConsumerReceiptRef,
        IdentityJobReportRef, IdentityJobRunRef, IdentityJobScopeMarkerRef,
        IdentityOperationChannel, IdentityOutboxPayloadMarkerRef, IdentityRequestDigestValue,
        IdentityStoredResultRef, IdentityTimestamp, TopicKeyRef,
    };
    use identity_contracts::views::{
        IdentityReadMaterialKind, IdentityReadMaterialMarker, MemberSummarySliceKind,
        MemberSummarySliceRef,
    };
    use identity_domain::handoff::HandoffState;
    use identity_domain::outbox::{IdentityOutboxRecord, OutboxState};

    use super::*;

    fn timestamp(value: i64) -> IdentityTimestamp {
        IdentityTimestamp::from_clock(value).expect("valid timestamp")
    }

    fn member_ref(token: &str) -> GlobalMemberRef {
        identity_contracts::refs::GlobalMemberRef::from_id(
            identity_contracts::refs::GlobalMemberId::new(token.to_owned())
                .expect("valid member id"),
        )
    }

    fn scope_ref(token: &str) -> VisibilityScopeRef {
        VisibilityScopeRef::new(token)
    }

    fn visibility_result(token: &str) -> VisibilityResultRef {
        VisibilityResultRef::new(token)
    }

    fn projection_ref(token: &str) -> IdentityProjectionRef {
        IdentityProjectionRef::new(token)
    }

    fn projection_cursor(token: &str) -> IdentityProjectionCursorRef {
        IdentityProjectionCursorRef::new(identity_source_ref(IdentitySourceOwner::Identity, token))
    }

    fn maintenance_scope(token: &str) -> identity_contracts::refs::MaintenanceScopeRef {
        identity_contracts::refs::MaintenanceScopeRef::new(identity_source_ref(
            IdentitySourceOwner::Identity,
            token,
        ))
    }

    fn summary_view(scope: &str) -> MemberSummaryView {
        let member = member_ref("member-1");
        let source = identity_source_ref(IdentitySourceOwner::Identity, "summary-source-1");
        MemberSummaryView::from_projection(
            MemberSummaryViewRef::new(format!("view-{scope}")),
            member.clone(),
            scope_ref(scope),
            MemberSummarySliceRef::new(
                MemberSummarySliceKind::Anchor,
                member.clone(),
                source.clone(),
            ),
            MemberSummarySliceRef::new(MemberSummarySliceKind::Lifecycle, member.clone(), source),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            visibility_result(&format!("visibility-{scope}")),
            Some(IdentityTruthCursor::new("truth-cursor-1")),
            IdentityReadMaterialMarker::new(IdentityReadMaterialKind::SafeSummaryRefs, None),
        )
        .expect("summary view")
    }

    fn projection_state(cursor: &str) -> ProjectionState {
        ProjectionState::stale(
            ProjectionStateRef::from_id(
                identity_contracts::refs::ProjectionStateId::new(format!(
                    "projection-state-{cursor}"
                ))
                .expect("projection state id"),
            ),
            projection_ref("projection-1"),
            Some(member_ref("member-1")),
            projection_cursor(cursor),
            maintenance_scope("scope-1"),
            timestamp(1),
        )
    }

    fn external_reference() -> ExternalReferenceRef {
        ExternalReferenceRef::new(
            ExternalReferenceKind::MethodSource,
            identity_source_ref(IdentitySourceOwner::MethodLibrary, "method-source-1"),
        )
    }

    fn reference_owner() -> IdentityReferenceOwnerRef {
        IdentityReferenceOwnerRef::new(
            identity_contracts::refs::IdentityReferenceOwnerKind::Maintenance,
            identity_source_ref(IdentitySourceOwner::Identity, "owner-1"),
        )
    }

    fn reference_state_resolved() -> ReferenceResolutionState {
        let reference_ref = external_reference();
        ReferenceResolutionState::resolved(
            ReferenceResolutionStateRef::from_id(
                identity_contracts::refs::ReferenceResolutionStateId::new(
                    "reference-state-1".to_owned(),
                )
                .expect("reference state id"),
            ),
            reference_ref.clone(),
            reference_owner(),
            ExternalSourceVersionRef::new(identity_source_ref(
                IdentitySourceOwner::MethodLibrary,
                "source-version-1",
            )),
            ExternalReferenceSafeSummaryRef::new(
                reference_ref,
                identity_source_ref(IdentitySourceOwner::MethodLibrary, "safe-summary-1"),
            ),
            timestamp(1),
        )
    }

    fn handoff_intent() -> TraceHandoffIntent {
        TraceHandoffIntent {
            handoff_intent_ref: TraceHandoffIntentRef::new("handoff-1"),
            member_ref: member_ref("member-1"),
            trace_record_refs: vec![IdentityTraceRecordRef::new("trace-1")],
            audit_trail_ref: Some(AuditTrailRef::new("audit-1")),
            handoff_target_ref: HandoffTargetRef::new("target-1"),
            handoff_scope_ref: HandoffScopeRef::new("scope-1"),
            safe_material_ref: TraceHandoffSafeMaterialRef::new("material-1"),
            handoff_state: HandoffState::pending(timestamp(1)),
            created_at: timestamp(1),
            updated_at: timestamp(1),
        }
    }

    fn adapter_availability() -> IdentityAdapterAvailability {
        IdentityAdapterAvailability::available(
            IdentityAdapterRef::new("adapter-1"),
            IdentityAdapterModeRef::new("fake"),
            timestamp(1),
        )
    }

    fn request_digest(token: &str) -> identity_application::support::IdentityRequestDigest {
        identity_application::support::IdentityRequestDigest::from_canonical_marker(
            IdentityCanonicalRequestMarkerRef::new(format!("canonical-{token}")),
            IdentityRequestDigestValue::new(format!("digest-{token}")),
            IdentityProtocolSchemaVersionRef::new("identity.command.v1"),
            IdentityDigestAlgorithmMarkerRef::new("sha256-v1"),
        )
    }

    fn command_context(
        operation_name: &str,
        idempotency_key: &str,
        digest_token: &str,
    ) -> IdentityOperationContext {
        IdentityOperationContext::from_command(
            IdentityOperationContextRef::new(format!("context-{operation_name}-{idempotency_key}")),
            IdentityOperationName::new(operation_name),
            ActorRef::new("actor-1", ActorKind::Human),
            identity_application::support::IdentityRequestMetadataRef::new(format!(
                "metadata-{operation_name}"
            )),
            Some(IdentityIdempotencyKey::new(idempotency_key.to_owned())),
            request_digest(digest_token),
            None,
            timestamp(1),
        )
    }

    fn job_context(
        operation_name: &str,
        idempotency_key: &str,
        digest_token: &str,
        job_run_ref: &str,
    ) -> IdentityOperationContext {
        IdentityOperationContext::from_job(
            IdentityOperationContextRef::new(format!("context-{operation_name}-{idempotency_key}")),
            IdentityOperationName::new(operation_name),
            ActorRef::system("job-system"),
            identity_application::support::IdentityRequestMetadataRef::new(format!(
                "metadata-{operation_name}"
            )),
            IdentityIdempotencyKey::new(idempotency_key.to_owned()),
            request_digest(digest_token),
            None,
            IdentityJobRunRef::new(job_run_ref),
            timestamp(1),
        )
    }

    fn stored_result_ref(token: &str) -> IdentityStoredResultRef {
        IdentityStoredResultRef::new(format!("stored-result-{token}"))
    }

    fn command_stored_result(
        token: &str,
        context_ref: &IdentityOperationContextRef,
    ) -> StoredIdentityOperationResult {
        StoredIdentityOperationResult::command_accepted(
            stored_result_ref(token),
            context_ref.clone(),
            identity_application::support::IdentityStoredSurfaceMarkerRef::new(format!(
                "surface-{token}"
            )),
            timestamp(2),
        )
    }

    fn consumer_receipt_envelope(
        token: &str,
        context_ref: &IdentityOperationContextRef,
    ) -> IdentityConsumerReceiptEnvelope {
        IdentityConsumerReceiptEnvelope::consumer_receipt(
            context_ref.clone(),
            identity_application::support::IdentityStoredSurfaceMarkerRef::new(format!(
                "surface-{token}"
            )),
            IdentityConsumerReceipt {
                receipt_ref: IdentityConsumerReceiptRef::new(format!("receipt-{token}")),
                consumer_name: IdentityInboundConsumerName::new(
                    "HandleRoleCapabilitySourceChanged",
                ),
                outcome: IdentityConsumerOutcome::Accepted,
                stored_result_ref: stored_result_ref(token),
                trace_refs: vec![IdentityTraceRecordRef::new(format!("trace-{token}"))],
                outbox_refs: vec![IdentityOutboxRecordRef::new(format!("outbox-{token}"))],
                issue_refs: Vec::new(),
            },
            timestamp(2),
        )
    }

    fn job_report(
        token: &str,
        stored_result_ref: Option<IdentityStoredResultRef>,
    ) -> IdentityJobRunReport {
        IdentityJobRunReport::start(
            IdentityJobReportRef::new(format!("job-report-{token}")),
            IdentityJobRunRef::new(format!("job-run-{token}")),
            IdentityJobName::new("RunIdentityReconciliation"),
            IdentityJobScopeMarkerRef::new(format!("job-scope-{token}")),
            Some(identity_contracts::refs::IdentityJobCursorRef::new(
                format!("job-input-cursor-{token}"),
            )),
            timestamp(1),
        )
        .partial(
            vec![MaintenanceIssueRef::new(format!("issue-{token}"))],
            Some(identity_contracts::refs::IdentityJobCursorRef::new(
                format!("job-output-cursor-{token}"),
            )),
            stored_result_ref,
            timestamp(2),
        )
    }

    fn outbox_record(token: &str, state: OutboxState) -> IdentityOutboxRecord {
        IdentityOutboxRecord {
            outbox_record_ref: IdentityOutboxRecordRef::new(format!("outbox-{token}")),
            member_ref: member_ref("member-1"),
            subject_ref: IdentityOutboxSubjectRef::new(format!("subject-{token}")),
            change_kind_ref: identity_contracts::refs::IdentityChangeKindRef::new(
                IdentityChangeKind::DerivedMarkerChanged,
                None,
            ),
            payload_marker_ref: IdentityOutboxPayloadMarkerRef::new(format!("payload-{token}")),
            topic_key_ref: TopicKeyRef::new(format!("topic-{token}")),
            trace_record_ref: IdentityTraceRecordRef::new(format!("trace-{token}")),
            outbox_state: state,
            created_at: timestamp(1),
            updated_at: timestamp(1),
        }
    }

    #[test]
    fn projection_rebuild_race_preserves_newer_state() {
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_projection_state(projection_state("cursor-new"), IdentityVersion::new(3))
            .build();

        let uow = runtime.begin().expect("uow");
        let older_state = projection_state("cursor-old");
        runtime
            .save_projection_state(older_state, Some(IdentityVersion::new(2)), uow.as_ref())
            .expect("stage");
        let error = runtime
            .commit(uow)
            .expect_err("older rebuild snapshot must be rejected");
        assert_eq!(error.kind, ApplicationErrorKind::OptimisticVersionConflict);

        let persisted = runtime
            .get_projection_state_with_version(projection_ref("projection-1"))
            .expect("load")
            .expect("state");
        assert_eq!(persisted.version, IdentityVersion::new(3));
        assert_eq!(
            persisted.value.source_cursor_ref,
            Some(projection_cursor("cursor-new"))
        );
    }

    #[test]
    fn reference_refresh_preserves_last_good_snapshot() {
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_reference_state(
                reference_state_resolved(),
                ExternalReferenceTypedSidecarRefs {
                    role_capability_safe_summary_ref: Some(ExternalReferenceSafeSummaryRef::new(
                        external_reference(),
                        identity_source_ref(IdentitySourceOwner::MethodLibrary, "safe-summary-1"),
                    )),
                    career_safe_summary_ref: None,
                    memory_safe_summary_ref: None,
                    governance_basis_summary_ref: None,
                    evidence_summary_ref: None,
                    source_version_ref: Some(ExternalSourceVersionRef::new(identity_source_ref(
                        IdentitySourceOwner::MethodLibrary,
                        "source-version-1",
                    ))),
                },
                IdentityVersion::new(4),
            )
            .build();

        let unavailable = ReferenceResolutionState::unavailable(
            ReferenceResolutionStateRef::from_id(
                identity_contracts::refs::ReferenceResolutionStateId::new(
                    "reference-state-1".to_owned(),
                )
                .expect("reference state id"),
            ),
            external_reference(),
            reference_owner(),
            MaintenanceIssueRef::new("reference-unavailable"),
            timestamp(2),
        );

        let uow = runtime.begin().expect("uow");
        runtime
            .save_reference_state(unavailable, Some(IdentityVersion::new(4)), uow.as_ref())
            .expect("stage");
        runtime.commit(uow).expect("commit");

        let persisted = runtime
            .get_reference_state_with_version(external_reference())
            .expect("load")
            .expect("state");
        assert_eq!(persisted.version, IdentityVersion::new(5));
        assert_eq!(
            persisted.value.state_kind,
            ReferenceResolutionStateKind::Unavailable
        );
        assert!(persisted.value.safe_summary_ref.is_some());
        assert!(persisted.value.source_version_ref.is_some());
        assert!(
            runtime
                .get_typed_sidecar_refs(external_reference())
                .expect("sidecars")
                .role_capability_safe_summary_ref
                .is_some()
        );
    }

    #[test]
    fn handoff_delivered_requires_formal_receipt() {
        let mut intent = handoff_intent();
        intent.handoff_state = HandoffState {
            state_kind: HandoffStateKind::Delivered,
            attempt_ref: Some(HandoffAttemptRef::new(identity_source_ref(
                IdentitySourceOwner::Identity,
                "attempt-1",
            ))),
            receipt_ref: None,
            issue_ref: None,
            changed_at: timestamp(2),
        };

        let runtime = IdentityInMemoryRuntime::builder()
            .seed_handoff_intent(handoff_intent(), IdentityVersion::new(2))
            .seed_adapter_availability(adapter_availability())
            .build();

        let uow = runtime.begin().expect("uow");
        runtime
            .save_handoff_intent(intent, Some(IdentityVersion::new(2)), uow.as_ref())
            .expect("stage");
        let error = runtime
            .commit(uow)
            .expect_err("missing receipt must reject delivered");
        assert_eq!(error.kind, ApplicationErrorKind::DomainRejected);

        let persisted = runtime
            .get_handoff_intent_with_version(TraceHandoffIntentRef::new("handoff-1"))
            .expect("load")
            .expect("intent");
        assert_eq!(persisted.version, IdentityVersion::new(2));
        assert_eq!(
            persisted.value.handoff_state.state_kind,
            HandoffStateKind::PendingHandoff
        );
    }

    #[test]
    fn rollback_failure_surfaces_manual_intervention_without_hidden_writes() {
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_projection_state(projection_state("cursor-1"), IdentityVersion::new(1))
            .inject_fault(FaultCase::RollbackFails)
            .build();

        let mut degraded = projection_state("cursor-1");
        degraded
            .mark_degraded(MaintenanceIssueRef::new("projection-issue"), timestamp(2))
            .expect("mark degraded");

        let uow = runtime.begin().expect("uow");
        runtime
            .save_projection_state(degraded, Some(IdentityVersion::new(1)), uow.as_ref())
            .expect("stage");
        let error = runtime.rollback(uow).expect_err("rollback must fail");
        assert_eq!(error.kind, ApplicationErrorKind::ConsistencyDefect);
        assert!(error.message.contains("manual intervention"));

        let persisted = runtime
            .get_projection_state_with_version(projection_ref("projection-1"))
            .expect("load")
            .expect("state");
        assert_eq!(persisted.version, IdentityVersion::new(1));
        assert_eq!(persisted.value.state_kind, ProjectionStateKind::Stale);
    }

    #[test]
    fn idempotency_namespace_isolated_by_operation_and_channel() {
        let command_context = command_context("establish_member", "idem-shared", "same");
        let consumer_context = IdentityOperationContext::from_inbound_event(
            IdentityOperationContextRef::new("context-consumer"),
            IdentityOperationName::new("handle_role_source_changed"),
            ActorRef::system("worker"),
            identity_application::support::IdentityRequestMetadataRef::new("metadata-consumer"),
            IdentityIdempotencyKey::new("idem-shared"),
            request_digest("same"),
            None,
            identity_contracts::refs::IdentitySourceEventRef::new("source-event-1"),
            timestamp(1),
        );
        let job_context = job_context("refresh_reference", "idem-shared", "same", "job-run-1");

        let runtime = IdentityInMemoryRuntime::builder().build();

        let command_uow = runtime.begin().expect("command uow");
        let command_result = runtime
            .reserve(
                command_context.clone(),
                IdentityIdempotencyRecordRef::new("idem-record-command"),
                timestamp(1),
                command_uow.as_ref(),
            )
            .expect("reserve command");
        runtime.commit(command_uow).expect("commit command");
        assert!(matches!(
            command_result,
            IdempotencyReserveOutcome::Reserved(_)
        ));

        let consumer_uow = runtime.begin().expect("consumer uow");
        let consumer_result = runtime
            .reserve(
                consumer_context.clone(),
                IdentityIdempotencyRecordRef::new("idem-record-consumer"),
                timestamp(1),
                consumer_uow.as_ref(),
            )
            .expect("reserve consumer");
        runtime.commit(consumer_uow).expect("commit consumer");
        assert!(matches!(
            consumer_result,
            IdempotencyReserveOutcome::Reserved(_)
        ));

        let job_uow = runtime.begin().expect("job uow");
        let job_result = runtime
            .reserve(
                job_context.clone(),
                IdentityIdempotencyRecordRef::new("idem-record-job"),
                timestamp(1),
                job_uow.as_ref(),
            )
            .expect("reserve job");
        runtime.commit(job_uow).expect("commit job");
        assert!(matches!(job_result, IdempotencyReserveOutcome::Reserved(_)));

        assert!(
            runtime
                .get_by_key(
                    command_context.operation_name,
                    IdentityOperationChannel::Command,
                    IdentityIdempotencyKey::new("idem-shared"),
                )
                .expect("load command")
                .is_some()
        );
        assert!(
            runtime
                .get_by_key(
                    consumer_context.operation_name,
                    IdentityOperationChannel::Consumer,
                    IdentityIdempotencyKey::new("idem-shared"),
                )
                .expect("load consumer")
                .is_some()
        );
        assert!(
            runtime
                .get_by_key(
                    job_context.operation_name,
                    IdentityOperationChannel::Job,
                    IdentityIdempotencyKey::new("idem-shared"),
                )
                .expect("load job")
                .is_some()
        );
    }

    #[test]
    fn duplicate_missing_stored_result_does_not_recompute() {
        let context = command_context("establish_member", "idem-1", "same");
        let record = IdentityIdempotencyRecord {
            record_ref: IdentityIdempotencyRecordRef::new("idem-record-1"),
            operation_name: context.operation_name.clone(),
            channel: IdentityOperationChannel::Command,
            idempotency_key: IdentityIdempotencyKey::new("idem-1"),
            request_digest: context.request_digest.clone(),
            state: identity_application::support::IdentityIdempotencyStateKind::Completed,
            stored_result_ref: Some(stored_result_ref("missing")),
            reserved_at: timestamp(1),
            completed_at: Some(timestamp(2)),
        };
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_idempotency_record(record, IdentityVersion::new(2))
            .build();

        let uow = runtime.begin().expect("uow");
        let outcome = runtime
            .reserve(
                context,
                IdentityIdempotencyRecordRef::new("idem-record-new"),
                timestamp(3),
                uow.as_ref(),
            )
            .expect("reserve");
        match outcome {
            IdempotencyReserveOutcome::ReplayAvailable {
                record,
                stored_result_ref,
            } => {
                assert_eq!(record.version, IdentityVersion::new(2));
                assert_eq!(
                    stored_result_ref,
                    IdentityStoredResultRef::new("stored-result-missing")
                );
            }
            other => panic!("unexpected reserve outcome: {other:?}"),
        }
        assert!(
            runtime
                .get_stored_result(IdentityStoredResultRef::new("stored-result-missing"))
                .expect("lookup")
                .is_none()
        );
        runtime.rollback(uow).expect("rollback");
    }

    #[test]
    fn consumer_duplicate_replays_typed_receipt() {
        let context = IdentityOperationContext::from_inbound_event(
            IdentityOperationContextRef::new("context-consumer-1"),
            IdentityOperationName::new("handle_role_source_changed"),
            ActorRef::system("worker"),
            identity_application::support::IdentityRequestMetadataRef::new("metadata-consumer"),
            IdentityIdempotencyKey::new("idem-consumer-1"),
            request_digest("consumer"),
            None,
            identity_contracts::refs::IdentitySourceEventRef::new("source-event-1"),
            timestamp(1),
        );
        let record = IdentityIdempotencyRecord {
            record_ref: IdentityIdempotencyRecordRef::new("idem-record-consumer"),
            operation_name: context.operation_name.clone(),
            channel: IdentityOperationChannel::Consumer,
            idempotency_key: IdentityIdempotencyKey::new("idem-consumer-1"),
            request_digest: context.request_digest.clone(),
            state: identity_application::support::IdentityIdempotencyStateKind::Completed,
            stored_result_ref: Some(stored_result_ref("consumer-1")),
            reserved_at: timestamp(1),
            completed_at: Some(timestamp(2)),
        };
        let stored = StoredIdentityOperationResult::consumer_receipt(
            stored_result_ref("consumer-1"),
            context.context_ref.clone(),
            identity_application::support::IdentityStoredSurfaceMarkerRef::new(
                "surface-consumer-1",
            ),
            timestamp(2),
        );
        let envelope = consumer_receipt_envelope("consumer-1", &context.context_ref);

        let runtime = IdentityInMemoryRuntime::builder()
            .seed_idempotency_record(record, IdentityVersion::new(2))
            .seed_stored_result(stored)
            .seed_consumer_receipt(envelope.clone())
            .build();

        let uow = runtime.begin().expect("uow");
        let outcome = runtime
            .reserve(
                context,
                IdentityIdempotencyRecordRef::new("idem-record-consumer-new"),
                timestamp(3),
                uow.as_ref(),
            )
            .expect("reserve");
        let replay_ref = match outcome {
            IdempotencyReserveOutcome::ReplayAvailable {
                stored_result_ref, ..
            } => stored_result_ref,
            other => panic!("unexpected outcome: {other:?}"),
        };
        let replay = runtime
            .get_consumer_receipt(replay_ref)
            .expect("load receipt")
            .expect("receipt");
        assert_eq!(replay, envelope);
        runtime.rollback(uow).expect("rollback");
    }

    #[test]
    fn job_duplicate_replays_stored_report() {
        let context = job_context("run_reconciliation", "idem-job-1", "job", "job-run-job-1");
        let stored = StoredIdentityOperationResult::job_report(
            stored_result_ref("job-1"),
            context.context_ref.clone(),
            identity_application::support::IdentityStoredSurfaceMarkerRef::new("surface-job-1"),
            timestamp(2),
        );
        let report = job_report("job-1", Some(stored_result_ref("job-1")));
        let record = IdentityIdempotencyRecord {
            record_ref: IdentityIdempotencyRecordRef::new("idem-record-job-1"),
            operation_name: context.operation_name.clone(),
            channel: IdentityOperationChannel::Job,
            idempotency_key: IdentityIdempotencyKey::new("idem-job-1"),
            request_digest: context.request_digest.clone(),
            state: identity_application::support::IdentityIdempotencyStateKind::Completed,
            stored_result_ref: Some(stored_result_ref("job-1")),
            reserved_at: timestamp(1),
            completed_at: Some(timestamp(2)),
        };
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_idempotency_record(record, IdentityVersion::new(2))
            .seed_stored_result(stored)
            .seed_job_report(report.clone(), IdentityVersion::new(1))
            .build();

        let outcome = runtime
            .reserve(
                context,
                IdentityIdempotencyRecordRef::new("idem-record-job-1-new"),
                timestamp(3),
                runtime.begin().expect("uow").as_ref(),
            )
            .expect("reserve");
        let replay_ref = match outcome {
            IdempotencyReserveOutcome::ReplayAvailable {
                stored_result_ref, ..
            } => stored_result_ref,
            other => panic!("unexpected outcome: {other:?}"),
        };
        let stored = runtime
            .get_stored_result(replay_ref)
            .expect("load stored")
            .expect("stored");
        assert_eq!(stored.result_kind, IdentityStoredResultKind::JobReport);
        let persisted = runtime
            .find_job_report_by_run(IdentityJobRunRef::new("job-run-job-1"))
            .expect("lookup")
            .expect("report");
        assert_eq!(persisted.value, report);
    }

    #[test]
    fn same_key_different_digest_conflicts() {
        let context = command_context("establish_member", "idem-conflict", "same");
        let existing = IdentityIdempotencyRecord {
            record_ref: IdentityIdempotencyRecordRef::new("idem-record-conflict"),
            operation_name: context.operation_name.clone(),
            channel: IdentityOperationChannel::Command,
            idempotency_key: IdentityIdempotencyKey::new("idem-conflict"),
            request_digest: request_digest("original"),
            state: identity_application::support::IdentityIdempotencyStateKind::Reserved,
            stored_result_ref: None,
            reserved_at: timestamp(1),
            completed_at: None,
        };
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_idempotency_record(existing, IdentityVersion::new(1))
            .build();

        let uow = runtime.begin().expect("uow");
        let outcome = runtime
            .reserve(
                context,
                IdentityIdempotencyRecordRef::new("idem-record-conflict-new"),
                timestamp(2),
                uow.as_ref(),
            )
            .expect("reserve");
        assert!(matches!(outcome, IdempotencyReserveOutcome::Conflict(_)));
        runtime.rollback(uow).expect("rollback");
    }

    #[test]
    fn same_key_same_digest_in_flight_visible() {
        let context = command_context("establish_member", "idem-flight", "flight");
        let existing = IdentityIdempotencyRecord {
            record_ref: IdentityIdempotencyRecordRef::new("idem-record-flight"),
            operation_name: context.operation_name.clone(),
            channel: IdentityOperationChannel::Command,
            idempotency_key: IdentityIdempotencyKey::new("idem-flight"),
            request_digest: context.request_digest.clone(),
            state: identity_application::support::IdentityIdempotencyStateKind::Reserved,
            stored_result_ref: None,
            reserved_at: timestamp(1),
            completed_at: None,
        };
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_idempotency_record(existing, IdentityVersion::new(1))
            .build();

        let uow = runtime.begin().expect("uow");
        let outcome = runtime
            .reserve(
                context,
                IdentityIdempotencyRecordRef::new("idem-record-flight-new"),
                timestamp(2),
                uow.as_ref(),
            )
            .expect("reserve");
        assert!(matches!(outcome, IdempotencyReserveOutcome::InFlight(_)));
        runtime.rollback(uow).expect("rollback");
    }

    #[test]
    fn stored_result_saved_before_idempotency_complete() {
        let context = command_context("establish_member", "idem-complete", "complete");
        let runtime = IdentityInMemoryRuntime::builder()
            .inject_fault(FaultCase::CompleteIdempotencyFails)
            .build();

        let reserve_uow = runtime.begin().expect("reserve uow");
        let reserved = runtime
            .reserve(
                context.clone(),
                IdentityIdempotencyRecordRef::new("idem-record-complete"),
                timestamp(1),
                reserve_uow.as_ref(),
            )
            .expect("reserve");
        runtime.commit(reserve_uow).expect("commit reserve");

        let reserved_record = match reserved {
            IdempotencyReserveOutcome::Reserved(record) => record,
            other => panic!("unexpected reserve outcome: {other:?}"),
        };

        let complete_uow = runtime.begin().expect("complete uow");
        let stored = command_stored_result("complete", &context.context_ref);
        runtime
            .save_command_accepted_result(stored.clone(), complete_uow.as_ref())
            .expect("save stored");
        runtime
            .complete_with_stored_result(
                reserved_record.value,
                stored.stored_result_ref.clone(),
                timestamp(2),
                reserved_record.version,
                complete_uow.as_ref(),
            )
            .expect("stage complete");
        let error = runtime
            .commit(complete_uow)
            .expect_err("complete failure must abort");
        assert_eq!(error.kind, ApplicationErrorKind::DependencyUnavailable);

        let persisted_record = runtime
            .get_by_key(
                context.operation_name.clone(),
                IdentityOperationChannel::Command,
                IdentityIdempotencyKey::new("idem-complete"),
            )
            .expect("load")
            .expect("record");
        assert_eq!(
            persisted_record.value.state,
            identity_application::support::IdentityIdempotencyStateKind::Reserved
        );
        assert!(
            runtime
                .find_by_operation_context(context.context_ref)
                .expect("lookup stored")
                .is_none()
        );
    }

    #[test]
    fn outbox_state_lists_and_updates_are_formal() {
        let pending = outbox_record("pending", OutboxState::pending(timestamp(1)));
        let retryable = outbox_record(
            "retryable",
            OutboxState::retryable_failed(
                identity_contracts::refs::OutboxDeliveryIssueRef::new(identity_source_ref(
                    IdentitySourceOwner::Identity,
                    "outbox-issue-1",
                )),
                timestamp(2),
            ),
        );
        let published = outbox_record(
            "published",
            OutboxState::published(
                identity_contracts::refs::OutboxDeliveryAttemptRef::new(identity_source_ref(
                    IdentitySourceOwner::Identity,
                    "outbox-attempt-1",
                )),
                timestamp(3),
            ),
        );

        let runtime = IdentityInMemoryRuntime::builder()
            .seed_outbox_record(pending.clone(), IdentityVersion::new(1))
            .seed_outbox_record(retryable.clone(), IdentityVersion::new(1))
            .seed_outbox_record(published.clone(), IdentityVersion::new(1))
            .build();

        let pending_page = runtime
            .list_pending_outbox_records(None, IdentityRepositoryPage::new(None, 10))
            .expect("list pending");
        assert_eq!(pending_page.items.len(), 1);
        assert_eq!(
            pending_page.items[0].value_ref,
            IdentityOutboxRecordRef::new("outbox-pending")
        );

        let retryable_page = runtime
            .list_retryable_outbox_records(None, IdentityRepositoryPage::new(None, 10))
            .expect("list retryable");
        assert_eq!(retryable_page.items.len(), 1);
        assert_eq!(
            retryable_page.items[0].value_ref,
            IdentityOutboxRecordRef::new("outbox-retryable")
        );

        let by_trace = runtime
            .find_outbox_records_by_trace(
                IdentityTraceRecordRef::new("trace-pending"),
                IdentityRepositoryPage::new(None, 10),
            )
            .expect("list by trace");
        assert_eq!(by_trace.items.len(), 1);

        let mut updated = pending.clone();
        updated
            .mark_published(OutboxState::published(
                identity_contracts::refs::OutboxDeliveryAttemptRef::new(identity_source_ref(
                    IdentitySourceOwner::Identity,
                    "outbox-attempt-2",
                )),
                timestamp(4),
            ))
            .expect("mark published");
        let uow = runtime.begin().expect("uow");
        runtime
            .update_outbox_state(updated, IdentityVersion::new(1), uow.as_ref())
            .expect("stage update");
        runtime.commit(uow).expect("commit");

        let persisted = runtime
            .get_outbox_record_with_version(IdentityOutboxRecordRef::new("outbox-pending"))
            .expect("load")
            .expect("record");
        assert_eq!(persisted.version, IdentityVersion::new(2));
        assert_eq!(
            persisted.value.outbox_state.state_kind,
            OutboxStateKind::Published
        );
    }

    #[test]
    fn scoped_lookup_uses_persisted_member_scope_index() {
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member_summary_view(summary_view("scope-a"), IdentityVersion::new(1))
            .seed_member_summary_view(summary_view("scope-b"), IdentityVersion::new(1))
            .build();

        let found = runtime
            .find_member_summary_view_ref(member_ref("member-1"), scope_ref("scope-b"))
            .expect("lookup");
        assert_eq!(found, Some(MemberSummaryViewRef::new("view-scope-b")));
    }

    #[test]
    fn summary_view_scope_guard_matches_design() {
        let view = summary_view("scope-a");
        assert!(view.belongs_to(&member_ref("member-1")));
        assert!(view.matches_visibility_scope(&scope_ref("scope-a")));
        assert!(!view.matches_visibility_scope(&scope_ref("scope-b")));
        assert!(view.assert_body_free().is_ok());
    }

    #[test]
    fn read_visibility_repository_returns_formal_scope() {
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member_summary_access(
                member_ref("member-1"),
                IdentityVisibilityAccessSummary {
                    consumer_ref: ConsumerRef::new("consumer-1"),
                    actor_ref: Some(ActorRef::new("actor-1", ActorKind::Human)),
                    visibility_context_ref: VisibilityContextRef::new("context-1"),
                    scope_ref: scope_ref("scope-a"),
                    access_state: identity_contracts::views::IdentityVisibilityAccessState::Visible,
                    redaction_profile_ref: None,
                    visibility_result_ref: visibility_result("visibility-a"),
                },
            )
            .build();

        let access = runtime
            .resolve_member_summary_read(
                member_ref("member-1"),
                None,
                ConsumerRef::new("consumer-1"),
                VisibilityContextRef::new("context-1"),
            )
            .expect("resolve")
            .expect("summary access");
        assert_eq!(access.scope_ref, scope_ref("scope-a"));
    }
}
