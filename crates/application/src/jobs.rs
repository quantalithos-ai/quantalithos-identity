//! Shared operations-job scaffold and stored-report replay helpers.

use identity_contracts::jobs::{
    IdentityExternalReferenceRefreshScopeDto, IdentityJobItemCounts, IdentityJobReportSurface,
    IdentityJobRequest, IdentityJobResponse, IdentityJobRunDisposition,
    IdentityProjectionRebuildScopeDto, IdentityReconciliationTargetScopeDto,
    RebuildIdentityProjectionJobInput, RebuildIdentityProjectionJobOutput,
    RefreshExternalReferenceStateJobInput, RefreshExternalReferenceStateJobOutput,
    RunIdentityReconciliationJobInput, RunIdentityReconciliationJobOutput,
};
use identity_contracts::queries::IdentityPublicPageRequest;
use identity_contracts::receipts::{MaintenanceIssueKind, MaintenanceIssueRef};
use identity_contracts::refs::{
    GlobalMemberRef, IdentityOperationChannel, IdentityProjectionKind, IdentityStoredResultRef,
    IdentityTimestamp, ReconciliationFindingMaterialKind, ReconciliationReportRef,
};
use identity_domain::projection_state::ProjectionStateKind;
use identity_domain::reconciliation::{ReconciliationPolicy, ReconciliationReportStateKind};
use identity_domain::reference_state::ReferenceResolutionStateKind;

use crate::errors::{ApplicationError, ApplicationErrorKind};
use crate::ports::{
    IdentityClockPort, IdentityIdGeneratorPort, IdentityIdempotencyRepository,
    IdentityJobReportRepository, IdentityMaintenanceIssueMapper, IdentityMaintenanceLoadedTarget,
    IdentityMaintenanceRepository, IdentityProjectionRepository,
    IdentityReconciliationReportRepository, IdentityReferenceStateRepository,
    IdentityStoredResultRepository, IdentityUnitOfWork, IdentityUnitOfWorkManagerPort,
};
use crate::support::{
    IdempotencyReserveOutcome, IdentityIdempotencyRecord, IdentityJobRunReport,
    IdentityOperationContext, IdentityRepositoryCursor, IdentityRepositoryPage,
    IdentityStoredResultKind, IdentityStoredSurfaceMarkerRef, Page, StoredIdentityOperationResult,
    Versioned,
};

/// Shared dependencies for operations-job scaffolding and duplicate replay.
pub struct IdentityJobServiceDeps<'a> {
    /// Job write transaction manager.
    pub unit_of_work_manager: &'a dyn IdentityUnitOfWorkManagerPort,
    /// Trusted clock used by report and replay persistence decisions.
    pub clock: &'a dyn IdentityClockPort,
    /// Stable id and marker generator.
    pub id_generator: &'a dyn IdentityIdGeneratorPort,
    /// Duplicate replay reserve and completion repository.
    pub idempotency_repository: &'a dyn IdentityIdempotencyRepository,
    /// Stored replay shell repository.
    pub stored_result_repository: &'a dyn IdentityStoredResultRepository,
    /// Replayable job report repository.
    pub job_report_repository: &'a dyn IdentityJobReportRepository,
    /// Projection repository used by maintenance rebuild jobs.
    pub projection_repository: &'a dyn IdentityProjectionRepository,
    /// Maintenance target repository used by rebuild/refresh/reconciliation jobs.
    pub maintenance_repository: &'a dyn IdentityMaintenanceRepository,
    /// External reference bundle repository used by refresh jobs.
    pub reference_state_repository: &'a dyn IdentityReferenceStateRepository,
    /// External reference resolver used by refresh jobs.
    pub external_reference_resolver: &'a dyn crate::ports::IdentityExternalReferenceResolverPort,
    /// Reconciliation report repository used by report-only maintenance.
    pub reconciliation_report_repository: &'a dyn IdentityReconciliationReportRepository,
    /// Safe maintenance issue mapper.
    pub maintenance_issue_mapper: &'a dyn IdentityMaintenanceIssueMapper,
}

/// Finalized first-run job outcome ready for report persistence and response assembly.
pub struct IdentityJobExecution<T> {
    /// Typed body-free public output assembled from the first run.
    pub output: T,
    /// Replayable application-local report assembly object.
    pub report: IdentityJobRunReport,
}

impl<T> IdentityJobExecution<T> {
    /// Creates a finalized job execution bundle from typed output and report material.
    pub fn new(output: T, report: IdentityJobRunReport) -> Self {
        Self { output, report }
    }
}

/// Shared job service scaffold for operations-job vertical slices.
pub struct IdentityJobService<'a> {
    deps: IdentityJobServiceDeps<'a>,
}

impl<'a> IdentityJobService<'a> {
    /// Creates a job service from formal shared job dependencies.
    pub fn new(deps: IdentityJobServiceDeps<'a>) -> Self {
        Self { deps }
    }

    /// Returns the shared job dependencies for later vertical slices.
    pub fn deps(&self) -> &IdentityJobServiceDeps<'a> {
        &self.deps
    }

    /// Rebuilds projection material for the requested maintenance scope or explicit targets.
    pub fn rebuild_identity_projection(
        &self,
        request: IdentityJobRequest<RebuildIdentityProjectionJobInput>,
        context: IdentityOperationContext,
    ) -> Result<IdentityJobResponse<RebuildIdentityProjectionJobOutput>, ApplicationError> {
        let actor_ref = context.actor_ref.clone();
        let operation_channel = context.channel;
        self.dispatch_job_scaffold(
            context,
            request,
            |report| {
                Ok(rebuild_output(
                    IdentityJobRunDisposition::DuplicateReplayed,
                    report,
                ))
            },
            |request, _, now, mut report, uow| {
                let page = repository_page(&request.input.page);
                let selected = match &request.input.rebuild_scope {
                    IdentityProjectionRebuildScopeDto::ExplicitProjectionRefs(projection_refs) => {
                        if projection_refs.is_empty() {
                            return Err(ApplicationError::invalid_request(
                                "rebuild projection scope must include at least one projection ref",
                            ));
                        }
                        Page {
                            items: projection_refs.clone(),
                            next_cursor: None,
                        }
                    }
                    IdentityProjectionRebuildScopeDto::StaleInMaintenanceScope => self
                        .deps
                        .maintenance_repository
                        .list_projection_targets_for_rebuild(
                            request.input.maintenance_scope_ref.clone(),
                            page,
                        )?,
                };

                let mut affected_member_refs = Vec::new();
                for projection_ref in &selected.items {
                    report.affected_projection_refs.push(projection_ref.clone());

                    let Some(loaded) = self
                        .deps
                        .projection_repository
                        .get_projection_state_with_version(projection_ref.clone())?
                    else {
                        let issue_ref = self
                            .deps
                            .maintenance_issue_mapper
                            .projection_missing_state_issue(projection_ref.clone());
                        report.failed_projection_refs.push(projection_ref.clone());
                        report.issue_refs.push(issue_ref);
                        continue;
                    };

                    if let Some(member_ref) = loaded.value.member_ref.clone() {
                        push_unique_member_ref(&mut affected_member_refs, member_ref);
                    }

                    let policy = ReconciliationPolicy::for_projection_rebuild(
                        request.input.maintenance_scope_ref.clone(),
                        projection_ref.clone(),
                        Some(actor_ref.clone()),
                        operation_channel,
                    );
                    policy.assert_not_truth_write()?;
                    policy.assert_not_cross_repo_repair()?;

                    let mut pending_state = loaded.value.clone();
                    pending_state.mark_rebuild_pending(
                        request.input.maintenance_scope_ref.clone(),
                        now,
                        operation_channel,
                    )?;
                    let pending_version = self
                        .deps
                        .projection_repository
                        .save_projection_state(pending_state.clone(), Some(loaded.version), uow)?
                        .version;

                    if projection_ref.projection_kind != IdentityProjectionKind::MemberSummary {
                        let issue_ref = self
                            .deps
                            .maintenance_issue_mapper
                            .projection_unsupported_writer_issue(projection_ref.clone());
                        pending_state.mark_rebuild_failed(issue_ref.clone(), now)?;
                        self.deps.projection_repository.save_projection_state(
                            pending_state,
                            Some(pending_version),
                            uow,
                        )?;
                        report.failed_projection_refs.push(projection_ref.clone());
                        report.issue_refs.push(issue_ref);
                        continue;
                    }

                    let Some(rebuild_plan) = self
                        .deps
                        .projection_repository
                        .get_member_summary_rebuild_plan(projection_ref.clone())?
                    else {
                        let issue_ref = self
                            .deps
                            .maintenance_issue_mapper
                            .projection_missing_rebuild_input_issue(projection_ref.clone());
                        pending_state.mark_rebuild_failed(issue_ref.clone(), now)?;
                        self.deps.projection_repository.save_projection_state(
                            pending_state,
                            Some(pending_version),
                            uow,
                        )?;
                        report.failed_projection_refs.push(projection_ref.clone());
                        report.issue_refs.push(issue_ref);
                        continue;
                    };

                    let invalid_rebuild_input = rebuild_plan.projection_ref != *projection_ref
                        || rebuild_plan.view_inputs.is_empty()
                        || rebuild_plan
                            .view_inputs
                            .iter()
                            .any(|input| input.member_ref != rebuild_plan.member_ref);
                    if invalid_rebuild_input {
                        let issue_ref = self
                            .deps
                            .maintenance_issue_mapper
                            .projection_missing_rebuild_input_issue(projection_ref.clone());
                        pending_state.mark_rebuild_failed(issue_ref.clone(), now)?;
                        self.deps.projection_repository.save_projection_state(
                            pending_state,
                            Some(pending_version),
                            uow,
                        )?;
                        report.failed_projection_refs.push(projection_ref.clone());
                        report.issue_refs.push(issue_ref);
                        continue;
                    }

                    let Some(source_cursor_ref) = self
                        .deps
                        .projection_repository
                        .get_projection_source_cursor(projection_ref.clone())?
                    else {
                        let issue_ref = self
                            .deps
                            .maintenance_issue_mapper
                            .projection_missing_cursor_issue(projection_ref.clone());
                        pending_state.mark_rebuild_failed(issue_ref.clone(), now)?;
                        self.deps.projection_repository.save_projection_state(
                            pending_state,
                            Some(pending_version),
                            uow,
                        )?;
                        report.failed_projection_refs.push(projection_ref.clone());
                        report.issue_refs.push(issue_ref);
                        continue;
                    };

                    for view_input in rebuild_plan.view_inputs {
                        let view = identity_contracts::views::MemberSummaryView::from_projection(
                            view_input.view_ref,
                            view_input.member_ref,
                            view_input.visibility_scope_ref,
                            view_input.anchor_slice_ref,
                            view_input.lifecycle_slice_ref,
                            view_input.role_capability_slice_refs,
                            view_input.career_slice_refs,
                            view_input.memory_slice_refs,
                            view_input.visibility_result_ref,
                            view_input.read_surface_kind,
                            view_input.source_cursor_ref,
                            view_input.projection_freshness_ref,
                            view_input.read_material_marker,
                        )?;
                        self.deps
                            .projection_repository
                            .save_member_summary_view(view, None, uow)?;
                    }

                    pending_state.mark_rebuilt(source_cursor_ref, now)?;
                    self.deps.projection_repository.save_projection_state(
                        pending_state,
                        Some(pending_version),
                        uow,
                    )?;
                    report.rebuilt_projection_refs.push(projection_ref.clone());
                }

                report.affected_member_refs = affected_member_refs;
                finish_rebuild_execution(now, selected.next_cursor, report)
            },
        )
    }

    /// Refreshes external reference bundle state for the requested targets.
    pub fn refresh_external_reference_state(
        &self,
        request: IdentityJobRequest<RefreshExternalReferenceStateJobInput>,
        context: IdentityOperationContext,
    ) -> Result<IdentityJobResponse<RefreshExternalReferenceStateJobOutput>, ApplicationError> {
        let actor_ref = context.actor_ref.clone();
        let operation_channel = context.channel;
        self.dispatch_job_scaffold(
            context,
            request,
            |report| Ok(refresh_output(IdentityJobRunDisposition::DuplicateReplayed, report)),
            |request, _, now, mut report, uow| {
                let page = repository_page(&request.input.page);
                let selected = match &request.input.refresh_scope {
                    IdentityExternalReferenceRefreshScopeDto::ExplicitReferenceRefs(
                        reference_refs,
                    ) => {
                        if reference_refs.is_empty() {
                            return Err(ApplicationError::invalid_request(
                                "reference refresh scope must include at least one external reference ref",
                            ));
                        }
                        Page {
                            items: reference_refs.clone(),
                            next_cursor: None,
                        }
                    }
                    IdentityExternalReferenceRefreshScopeDto::StaleInMaintenanceScope => self
                        .deps
                        .reference_state_repository
                        .list_stale_reference_states(
                            request.input.maintenance_scope_ref.clone(),
                            page,
                        )?,
                    IdentityExternalReferenceRefreshScopeDto::ByOwner(owner_ref) => self
                        .deps
                        .reference_state_repository
                        .list_reference_states_by_owner(owner_ref.clone(), page)?,
                    IdentityExternalReferenceRefreshScopeDto::ByKind(reference_kind) => self
                        .deps
                        .reference_state_repository
                        .list_reference_states_by_kind(*reference_kind, page)?,
                };

                for reference_ref in &selected.items {
                    let Some(loaded) = self
                        .deps
                        .reference_state_repository
                        .get_reference_state_with_version(reference_ref.clone())?
                    else {
                        let issue_ref = self
                            .deps
                            .maintenance_issue_mapper
                            .reference_missing_state_issue(reference_ref.clone());
                        report.failed_reference_refs.push(reference_ref.clone());
                        report.issue_refs.push(issue_ref);
                        continue;
                    };

                    let policy = ReconciliationPolicy::for_reference_refresh(
                        request.input.maintenance_scope_ref.clone(),
                        reference_ref.clone(),
                        Some(actor_ref.clone()),
                        operation_channel,
                    );
                    policy.assert_not_cross_repo_repair()?;
                    policy.assert_body_free()?;

                    let outcome = match self.deps.external_reference_resolver.resolve_external_reference(
                        reference_ref.clone(),
                        loaded.value.reference_owner_ref.clone(),
                    ) {
                        Ok(outcome) => outcome,
                        Err(_) => {
                            let issue_ref = self
                                .deps
                                .maintenance_issue_mapper
                                .reference_refresh_failed_issue(reference_ref.clone());
                            report.failed_reference_refs.push(reference_ref.clone());
                            report.issue_refs.push(issue_ref);
                            continue;
                        }
                    };

                    if outcome.state.external_reference_ref != *reference_ref {
                        return Err(ApplicationError::consistency_defect(
                            "reference resolver returned a different bundle key",
                        ));
                    }
                    if outcome.state.reference_owner_ref != loaded.value.reference_owner_ref {
                        return Err(ApplicationError::consistency_defect(
                            "reference resolver returned a different bundle owner",
                        ));
                    }

                    self.deps.reference_state_repository.save_reference_state(
                        outcome.state.clone(),
                        Some(loaded.version),
                        uow,
                    )?;
                    if let Some(sidecar_refs) = outcome.typed_sidecar_refs {
                        self.deps.reference_state_repository.save_typed_sidecar_refs(
                            reference_ref.clone(),
                            sidecar_refs,
                            loaded.version,
                            uow,
                        )?;
                    }

                    if outcome.state.is_usable_for_truth_update() {
                        report.refreshed_reference_refs.push(reference_ref.clone());
                    } else {
                        let Some(issue_ref) = outcome.state.issue_ref.clone() else {
                            return Err(ApplicationError::consistency_defect(
                                "non-usable refreshed reference state must carry a maintenance issue ref",
                            ));
                        };
                        report.failed_reference_refs.push(reference_ref.clone());
                        report.issue_refs.push(issue_ref);
                    }
                }

                finish_refresh_execution(now, selected.next_cursor, report)
            },
        )
    }

    /// Runs report-only reconciliation for the requested maintenance targets.
    pub fn run_identity_reconciliation(
        &self,
        request: IdentityJobRequest<RunIdentityReconciliationJobInput>,
        context: IdentityOperationContext,
    ) -> Result<IdentityJobResponse<RunIdentityReconciliationJobOutput>, ApplicationError> {
        let actor_ref = context.actor_ref.clone();
        let operation_channel = context.channel;
        self.dispatch_job_scaffold(
            context,
            request,
            |report| {
                Ok(reconciliation_output(
                    IdentityJobRunDisposition::DuplicateReplayed,
                    report,
                ))
            },
            |request, _, now, mut report, uow| {
                if matches!(
                    request.input.finding_material.material_kind,
                    ReconciliationFindingMaterialKind::ForbiddenExternalBody
                        | ReconciliationFindingMaterialKind::ForbiddenDiagnosticBody
                        | ReconciliationFindingMaterialKind::ForbiddenSecret
                ) {
                    return Err(ApplicationError::domain_rejected(
                        "reconciliation finding material must remain body-free",
                    ));
                }

                let page = repository_page(&request.input.page);
                let selected = match &request.input.target_scope {
                    IdentityReconciliationTargetScopeDto::ExplicitTargets(target_refs) => {
                        if target_refs.is_empty() {
                            return Err(ApplicationError::invalid_request(
                                "reconciliation scope must include at least one maintenance target",
                            ));
                        }
                        Page {
                            items: target_refs.clone(),
                            next_cursor: None,
                        }
                    }
                    IdentityReconciliationTargetScopeDto::ByMaintenanceScope => self
                        .deps
                        .maintenance_repository
                        .list_report_targets(request.input.maintenance_scope_ref.clone(), page)?,
                };

                let mut finding_refs = Vec::new();
                let mut report_issue_refs = Vec::new();
                for target_ref in &selected.items {
                    let policy = ReconciliationPolicy::for_reconciliation(
                        request.input.maintenance_scope_ref.clone(),
                        target_ref.clone(),
                        request.input.finding_intent_ref.clone(),
                        request.input.finding_material.clone(),
                        Some(actor_ref.clone()),
                        operation_channel,
                    );
                    policy.assert_report_only()?;

                    let Some(inspection) = self
                        .deps
                        .maintenance_repository
                        .load_maintenance_target_inspection_context(target_ref.clone())?
                    else {
                        let issue_ref = self
                            .deps
                            .maintenance_issue_mapper
                            .maintenance_target_missing_issue(target_ref.clone());
                        report.inspected_target_refs.push(target_ref.clone());
                        report.issue_refs.push(issue_ref.clone());
                        report_issue_refs.push(issue_ref);
                        continue;
                    };

                    report
                        .inspected_target_refs
                        .push(inspection.target_ref.clone());
                    extend_issue_refs(
                        &mut report.issue_refs,
                        issues_from_loaded_target(&inspection.loaded_target),
                    );
                    extend_issue_refs(
                        &mut report_issue_refs,
                        issues_from_loaded_target(&inspection.loaded_target),
                    );
                    if finding_required(&inspection.loaded_target) {
                        finding_refs.push(self.deps.id_generator.new_reconciliation_finding_ref()?);
                    }
                }

                let reconciliation_report_ref = ReconciliationReportRef::from_id(
                    self.deps.id_generator.new_reconciliation_report_id()?,
                );
                let reconciliation_report = if selected.items.is_empty() {
                    identity_domain::reconciliation::ReconciliationReport::no_finding(
                        reconciliation_report_ref,
                        request.input.maintenance_scope_ref.clone(),
                        Vec::new(),
                        Some(actor_ref),
                        now,
                    )?
                } else if !finding_refs.is_empty() || !report_issue_refs.is_empty() {
                    identity_domain::reconciliation::ReconciliationReport::generated(
                        reconciliation_report_ref,
                        request.input.maintenance_scope_ref.clone(),
                        report.inspected_target_refs.clone(),
                        finding_refs,
                        report_issue_refs,
                        Some(actor_ref),
                        now,
                    )?
                } else {
                    identity_domain::reconciliation::ReconciliationReport::no_finding(
                        reconciliation_report_ref,
                        request.input.maintenance_scope_ref.clone(),
                        report.inspected_target_refs.clone(),
                        Some(actor_ref),
                        now,
                    )?
                };
                let saved_report = self.deps.reconciliation_report_repository.save_report(
                    reconciliation_report.clone(),
                    None,
                    uow,
                )?;
                report.report_refs.push(saved_report.value_ref.clone());

                finish_reconciliation_execution(
                    now,
                    selected.next_cursor,
                    report,
                    reconciliation_report.report_state,
                )
            },
        )
    }

    /// Shared precheck that keeps the public job envelope aligned with the operation context.
    pub fn assert_job_context<T>(
        request: &IdentityJobRequest<T>,
        context: &IdentityOperationContext,
    ) -> Result<(), ApplicationError> {
        if context.channel != IdentityOperationChannel::Job {
            return Err(ApplicationError::invalid_request(
                "job context must use the job channel",
            ));
        }

        if context.operation_name.as_str() != request.job_name.as_str() {
            return Err(ApplicationError::invalid_request(format!(
                "operation name {} does not match job {}",
                context.operation_name.as_str(),
                request.job_name.as_str(),
            )));
        }

        if context.actor_ref != request.system_actor_ref.clone() {
            return Err(ApplicationError::invalid_request(
                "job context actor does not match the public system actor",
            ));
        }

        let Some(idempotency_key) = context.idempotency_key.as_ref() else {
            return Err(ApplicationError::invalid_request(
                "job context must carry an idempotency key",
            ));
        };

        if idempotency_key.as_public() != &request.idempotency_key {
            return Err(ApplicationError::invalid_request(
                "job context idempotency key does not match the public job request",
            ));
        }

        let Some(job_run_ref) = context.job_run_ref.as_ref() else {
            return Err(ApplicationError::invalid_request(
                "job context must carry a job run ref",
            ));
        };

        if job_run_ref != &request.job_run_ref {
            return Err(ApplicationError::invalid_request(
                "job context run ref does not match the public job request",
            ));
        }

        Ok(())
    }

    /// Shared helper that reserves job idempotency inside the active write transaction.
    pub fn reserve_idempotency(
        &self,
        context: &IdentityOperationContext,
        reserved_at: IdentityTimestamp,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdempotencyReserveOutcome, ApplicationError> {
        let record_ref = self
            .deps
            .id_generator
            .new_identity_idempotency_record_ref()?;
        self.deps
            .idempotency_repository
            .reserve(context.clone(), record_ref, reserved_at, uow)
    }

    /// Creates the initial replayable report assembly object for one first-run job.
    pub fn start_report<T>(
        &self,
        request: &IdentityJobRequest<T>,
        started_at: IdentityTimestamp,
    ) -> Result<IdentityJobRunReport, ApplicationError> {
        Ok(IdentityJobRunReport::start(
            self.deps.id_generator.new_identity_job_report_ref()?,
            request.job_run_ref.clone(),
            request.job_name.clone(),
            request.scope_marker_ref.clone(),
            request.input_cursor_ref.clone(),
            started_at,
        ))
    }

    /// Converts one replayable report assembly object into the public report shell.
    pub fn public_report(report: &IdentityJobRunReport) -> IdentityJobReportSurface {
        report.to_surface()
    }

    /// Assembles the public job response from a persisted report and typed output.
    pub fn response<T>(
        job_name: identity_contracts::protocol::IdentityJobName,
        stored_result_ref: IdentityStoredResultRef,
        output: T,
        report: &IdentityJobRunReport,
    ) -> IdentityJobResponse<T> {
        IdentityJobResponse {
            job_name,
            report_ref: report.report_ref.clone(),
            stored_result_ref,
            output,
            report: Self::public_report(report),
        }
    }

    /// Shared scaffold that enforces reserve/replay ordering without executing any specific job body twice.
    pub fn dispatch_job_scaffold<TRequest, TOutput, FReplay, FHandler>(
        &self,
        context: IdentityOperationContext,
        request: IdentityJobRequest<TRequest>,
        replay_output: FReplay,
        handler: FHandler,
    ) -> Result<IdentityJobResponse<TOutput>, ApplicationError>
    where
        FReplay: FnOnce(&IdentityJobRunReport) -> Result<TOutput, ApplicationError>,
        FHandler: FnOnce(
            &IdentityJobRequest<TRequest>,
            Versioned<IdentityIdempotencyRecord>,
            IdentityTimestamp,
            IdentityJobRunReport,
            &dyn IdentityUnitOfWork,
        ) -> Result<IdentityJobExecution<TOutput>, ApplicationError>,
    {
        Self::assert_job_context(&request, &context)?;

        let now = self.deps.clock.now()?;
        let uow = self.deps.unit_of_work_manager.begin()?;
        match self.reserve_idempotency(&context, now, uow.as_ref())? {
            IdempotencyReserveOutcome::ReplayAvailable {
                stored_result_ref, ..
            } => {
                let replay = self.replay_response(&request, stored_result_ref, replay_output);
                self.rollback_quietly(uow);
                replay
            }
            IdempotencyReserveOutcome::Conflict(_) => {
                self.rollback_quietly(uow);
                Err(ApplicationError::new(
                    ApplicationErrorKind::IdempotencyConflict,
                    "same job idempotency key is already bound to different canonical material",
                ))
            }
            IdempotencyReserveOutcome::InFlight(_) => {
                self.rollback_quietly(uow);
                Err(ApplicationError::new(
                    ApplicationErrorKind::IdempotencyInFlight,
                    "same job idempotency key and digest is still in flight",
                ))
            }
            IdempotencyReserveOutcome::Reserved(record) => {
                let initial_report = self.start_report(&request, context.started_at)?;
                match handler(&request, record.clone(), now, initial_report, uow.as_ref()) {
                    Ok(execution) => match self.persist_execution(
                        &context,
                        &request,
                        record,
                        now,
                        execution,
                        uow.as_ref(),
                    ) {
                        Ok(response) => match self.deps.unit_of_work_manager.commit(uow) {
                            Ok(()) => Ok(response),
                            Err(err) => Err(err),
                        },
                        Err(err) => {
                            self.rollback_quietly(uow);
                            Err(err)
                        }
                    },
                    Err(err) => {
                        self.rollback_quietly(uow);
                        Err(err)
                    }
                }
            }
        }
    }

    fn replay_response<TRequest, TOutput, FReplay>(
        &self,
        request: &IdentityJobRequest<TRequest>,
        stored_result_ref: IdentityStoredResultRef,
        replay_output: FReplay,
    ) -> Result<IdentityJobResponse<TOutput>, ApplicationError>
    where
        FReplay: FnOnce(&IdentityJobRunReport) -> Result<TOutput, ApplicationError>,
    {
        let stored = self
            .deps
            .stored_result_repository
            .get_stored_result(stored_result_ref.clone())?
            .ok_or_else(|| {
                Self::duplicate_replay_consistency_error(format!(
                    "stored job result {} is missing",
                    stored_result_ref.as_str()
                ))
            })?;

        if stored.result_kind != IdentityStoredResultKind::JobReport {
            return Err(Self::duplicate_replay_consistency_error(format!(
                "stored result kind {:?} cannot replay a job response",
                stored.result_kind
            )));
        }

        let versioned = self
            .deps
            .job_report_repository
            .find_job_report_by_run(request.job_run_ref.clone())?
            .ok_or_else(|| {
                Self::duplicate_replay_consistency_error(format!(
                    "stored job report for run {} is missing",
                    request.job_run_ref.as_str()
                ))
            })?;
        let report = versioned.value;
        self.validate_report_for_replay(request, &report, &stored_result_ref)?;
        let output = replay_output(&report)?;
        Ok(Self::response(
            request.job_name.clone(),
            stored_result_ref,
            output,
            &report,
        ))
    }

    fn persist_execution<TRequest, TOutput>(
        &self,
        context: &IdentityOperationContext,
        request: &IdentityJobRequest<TRequest>,
        reserved: Versioned<IdentityIdempotencyRecord>,
        completed_at: IdentityTimestamp,
        execution: IdentityJobExecution<TOutput>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityJobResponse<TOutput>, ApplicationError> {
        let stored_result_ref = self.deps.id_generator.new_identity_stored_result_ref()?;
        let surface_marker_ref: IdentityStoredSurfaceMarkerRef = self
            .deps
            .id_generator
            .new_identity_stored_surface_marker_ref()?;
        let stored = StoredIdentityOperationResult::job_report(
            stored_result_ref.clone(),
            context.context_ref.clone(),
            surface_marker_ref,
            completed_at,
        );

        let report = execution
            .report
            .with_stored_result_ref(stored_result_ref.clone());
        self.validate_final_report(request, &report)?;
        self.deps
            .job_report_repository
            .save_job_report(report.clone(), None, uow)?;
        self.deps
            .stored_result_repository
            .save_job_report_result(stored, uow)?;
        self.deps
            .idempotency_repository
            .complete_with_stored_result(
                reserved.value,
                stored_result_ref.clone(),
                completed_at,
                reserved.version,
                uow,
            )?;

        Ok(Self::response(
            request.job_name.clone(),
            stored_result_ref,
            execution.output,
            &report,
        ))
    }

    fn validate_final_report<T>(
        &self,
        request: &IdentityJobRequest<T>,
        report: &IdentityJobRunReport,
    ) -> Result<(), ApplicationError> {
        if report.job_name.as_str() != request.job_name.as_str() {
            return Err(ApplicationError::consistency_defect(
                "job report name does not match the public request",
            ));
        }
        if report.job_run_ref.as_str() != request.job_run_ref.as_str() {
            return Err(ApplicationError::consistency_defect(
                "job report run ref does not match the public request",
            ));
        }
        if report.job_scope_ref.as_str() != request.scope_marker_ref.as_str() {
            return Err(ApplicationError::consistency_defect(
                "job report scope marker does not match the public request",
            ));
        }
        if report.finished_at.is_none() {
            return Err(ApplicationError::consistency_defect(
                "job report must be finished before persistence",
            ));
        }
        report.validate_result_issue_invariant()
    }

    fn validate_report_for_replay<T>(
        &self,
        request: &IdentityJobRequest<T>,
        report: &IdentityJobRunReport,
        stored_result_ref: &IdentityStoredResultRef,
    ) -> Result<(), ApplicationError> {
        self.validate_final_report(request, report)?;
        match report.stored_result_ref.as_ref() {
            Some(report_ref) if report_ref == stored_result_ref => Ok(()),
            Some(_) => Err(Self::duplicate_replay_consistency_error(
                "stored job report points at a different stored result ref",
            )),
            None => Err(Self::duplicate_replay_consistency_error(
                "stored job report is missing its stored result ref",
            )),
        }
    }

    fn duplicate_replay_consistency_error(message: impl Into<String>) -> ApplicationError {
        ApplicationError::new(
            ApplicationErrorKind::DuplicateReplayConsistencyDefect,
            message.into(),
        )
    }

    fn rollback_quietly(&self, uow: Box<dyn IdentityUnitOfWork>) {
        let _ = self.deps.unit_of_work_manager.rollback(uow);
    }
}

fn repository_page(page: &IdentityPublicPageRequest) -> IdentityRepositoryPage {
    IdentityRepositoryPage::new(
        page.cursor
            .as_ref()
            .map(|cursor| IdentityRepositoryCursor::new(cursor.as_str())),
        page.limit,
    )
}

fn job_cursor(
    cursor: Option<IdentityRepositoryCursor>,
) -> Option<identity_contracts::refs::IdentityJobCursorRef> {
    cursor.map(|cursor: IdentityRepositoryCursor| {
        identity_contracts::refs::IdentityJobCursorRef::new(cursor.as_str())
    })
}

fn disposition_from_result_kind(
    result_kind: identity_contracts::jobs::IdentityJobResultKind,
) -> IdentityJobRunDisposition {
    match result_kind {
        identity_contracts::jobs::IdentityJobResultKind::Succeeded => {
            IdentityJobRunDisposition::Completed
        }
        identity_contracts::jobs::IdentityJobResultKind::Partial => {
            IdentityJobRunDisposition::Partial
        }
        identity_contracts::jobs::IdentityJobResultKind::Failed => {
            IdentityJobRunDisposition::Failed
        }
        identity_contracts::jobs::IdentityJobResultKind::Noop => IdentityJobRunDisposition::Noop,
        identity_contracts::jobs::IdentityJobResultKind::RetryableFailed => {
            IdentityJobRunDisposition::RetryableFailed
        }
    }
}

fn is_retryable_issue(issue_ref: &MaintenanceIssueRef) -> bool {
    issue_ref.issue_kind == MaintenanceIssueKind::Unavailable
}

fn rebuild_counts(report: &IdentityJobRunReport) -> IdentityJobItemCounts {
    IdentityJobItemCounts {
        scanned_count: report.affected_projection_refs.len() as u32,
        changed_count: report.rebuilt_projection_refs.len() as u32,
        failed_count: report.failed_projection_refs.len() as u32,
        skipped_count: 0,
    }
}

fn rebuild_output(
    disposition: IdentityJobRunDisposition,
    report: &IdentityJobRunReport,
) -> RebuildIdentityProjectionJobOutput {
    RebuildIdentityProjectionJobOutput {
        disposition,
        counts: rebuild_counts(report),
        rebuilt_projection_refs: report.rebuilt_projection_refs.clone(),
        failed_projection_refs: report.failed_projection_refs.clone(),
        report_refs: report.report_refs.clone(),
        issue_refs: report.issue_refs.clone(),
    }
}

fn refresh_counts(report: &IdentityJobRunReport) -> IdentityJobItemCounts {
    IdentityJobItemCounts {
        scanned_count: (report.refreshed_reference_refs.len() + report.failed_reference_refs.len())
            as u32,
        changed_count: report.refreshed_reference_refs.len() as u32,
        failed_count: report.failed_reference_refs.len() as u32,
        skipped_count: 0,
    }
}

fn refresh_output(
    disposition: IdentityJobRunDisposition,
    report: &IdentityJobRunReport,
) -> RefreshExternalReferenceStateJobOutput {
    RefreshExternalReferenceStateJobOutput {
        disposition,
        counts: refresh_counts(report),
        refreshed_reference_refs: report.refreshed_reference_refs.clone(),
        failed_reference_refs: report.failed_reference_refs.clone(),
        issue_refs: report.issue_refs.clone(),
    }
}

fn reconciliation_counts(report: &IdentityJobRunReport) -> IdentityJobItemCounts {
    IdentityJobItemCounts {
        scanned_count: report.inspected_target_refs.len() as u32,
        changed_count: if report.report_refs.is_empty() { 0 } else { 1 },
        failed_count: report.issue_refs.len() as u32,
        skipped_count: 0,
    }
}

fn reconciliation_output(
    disposition: IdentityJobRunDisposition,
    report: &IdentityJobRunReport,
) -> RunIdentityReconciliationJobOutput {
    RunIdentityReconciliationJobOutput {
        disposition,
        counts: reconciliation_counts(report),
        report_refs: report.report_refs.clone(),
        inspected_target_refs: report.inspected_target_refs.clone(),
        issue_refs: report.issue_refs.clone(),
    }
}

fn finish_rebuild_execution(
    now: IdentityTimestamp,
    next_cursor: Option<IdentityRepositoryCursor>,
    mut report: IdentityJobRunReport,
) -> Result<IdentityJobExecution<RebuildIdentityProjectionJobOutput>, ApplicationError> {
    let output_cursor_ref = job_cursor(next_cursor);
    let issue_refs = report.issue_refs.clone();
    report = if report.affected_projection_refs.is_empty() {
        report.noop(output_cursor_ref, None, now)
    } else if issue_refs.is_empty() {
        report.succeed(output_cursor_ref, None, now)
    } else if report.rebuilt_projection_refs.is_empty() && issue_refs.iter().all(is_retryable_issue)
    {
        report.retryable_fail(issue_refs, now)?
    } else if report.rebuilt_projection_refs.is_empty() {
        report.fail(issue_refs, now)?
    } else {
        report.partial(issue_refs, output_cursor_ref, None, now)?
    };

    let output = rebuild_output(disposition_from_result_kind(report.result_kind), &report);
    Ok(IdentityJobExecution::new(output, report))
}

fn finish_refresh_execution(
    now: IdentityTimestamp,
    next_cursor: Option<IdentityRepositoryCursor>,
    mut report: IdentityJobRunReport,
) -> Result<IdentityJobExecution<RefreshExternalReferenceStateJobOutput>, ApplicationError> {
    let output_cursor_ref = job_cursor(next_cursor);
    let issue_refs = report.issue_refs.clone();
    report =
        if report.refreshed_reference_refs.is_empty() && report.failed_reference_refs.is_empty() {
            report.noop(output_cursor_ref, None, now)
        } else if issue_refs.is_empty() {
            report.succeed(output_cursor_ref, None, now)
        } else if report.refreshed_reference_refs.is_empty()
            && issue_refs.iter().all(is_retryable_issue)
        {
            report.retryable_fail(issue_refs, now)?
        } else if report.refreshed_reference_refs.is_empty() {
            report.fail(issue_refs, now)?
        } else {
            report.partial(issue_refs, output_cursor_ref, None, now)?
        };

    let output = refresh_output(disposition_from_result_kind(report.result_kind), &report);
    Ok(IdentityJobExecution::new(output, report))
}

fn finish_reconciliation_execution(
    now: IdentityTimestamp,
    next_cursor: Option<IdentityRepositoryCursor>,
    mut report: IdentityJobRunReport,
    report_state: ReconciliationReportStateKind,
) -> Result<IdentityJobExecution<RunIdentityReconciliationJobOutput>, ApplicationError> {
    let output_cursor_ref = job_cursor(next_cursor);
    let issue_refs = report.issue_refs.clone();
    report = if report.inspected_target_refs.is_empty() && issue_refs.is_empty() {
        report.noop(output_cursor_ref, None, now)
    } else if report_state == ReconciliationReportStateKind::Failed {
        report.fail(issue_refs, now)?
    } else if issue_refs.is_empty() {
        report.succeed(output_cursor_ref, None, now)
    } else {
        report.partial(issue_refs, output_cursor_ref, None, now)?
    };

    let output = reconciliation_output(disposition_from_result_kind(report.result_kind), &report);
    Ok(IdentityJobExecution::new(output, report))
}

fn push_unique_member_ref(member_refs: &mut Vec<GlobalMemberRef>, member_ref: GlobalMemberRef) {
    if !member_refs.contains(&member_ref) {
        member_refs.push(member_ref);
    }
}

fn extend_issue_refs(
    issue_refs: &mut Vec<MaintenanceIssueRef>,
    additional: Vec<MaintenanceIssueRef>,
) {
    for issue_ref in additional {
        issue_refs.push(issue_ref);
    }
}

fn issues_from_loaded_target(
    loaded_target: &IdentityMaintenanceLoadedTarget,
) -> Vec<MaintenanceIssueRef> {
    match loaded_target {
        IdentityMaintenanceLoadedTarget::Projection { issue_ref, .. }
        | IdentityMaintenanceLoadedTarget::ReferenceResolution { issue_ref, .. } => {
            issue_ref.iter().cloned().collect()
        }
        IdentityMaintenanceLoadedTarget::ReconciliationReport { issue_refs, .. } => {
            issue_refs.clone()
        }
    }
}

fn finding_required(loaded_target: &IdentityMaintenanceLoadedTarget) -> bool {
    match loaded_target {
        IdentityMaintenanceLoadedTarget::Projection {
            state_kind,
            issue_ref,
            source_cursor_ref,
            ..
        } => {
            issue_ref.is_some()
                || source_cursor_ref.is_none()
                || !matches!(
                    state_kind,
                    ProjectionStateKind::Fresh | ProjectionStateKind::Rebuilt
                )
        }
        IdentityMaintenanceLoadedTarget::ReferenceResolution {
            state_kind,
            issue_ref,
            ..
        } => issue_ref.is_some() || *state_kind != ReferenceResolutionStateKind::Resolved,
        IdentityMaintenanceLoadedTarget::ReconciliationReport {
            report_state,
            finding_refs,
            issue_refs,
            ..
        } => {
            !finding_refs.is_empty()
                || !issue_refs.is_empty()
                || matches!(
                    report_state,
                    ReconciliationReportStateKind::FindingDetected
                        | ReconciliationReportStateKind::Partial
                        | ReconciliationReportStateKind::Failed
                )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::IdentityJobService;
    use crate::support::IdentityJobRunReport;
    use crate::support::{
        IdentityIdempotencyKey, IdentityOperationContext, IdentityOperationContextRef,
        IdentityOperationName, IdentityRequestDigest, IdentityRequestMetadataRef,
    };
    use core_contracts::actor::ActorRef;
    use identity_contracts::jobs::IdentityJobRequest;
    use identity_contracts::protocol::{
        IdentityDigestAlgorithmMarkerRef, IdentityJobName, IdentityProtocolSchemaVersionRef,
    };
    use identity_contracts::refs::{
        IdentityCanonicalRequestMarkerRef, IdentityJobCursorRef, IdentityJobRunMetadataRef,
        IdentityJobRunRef, IdentityJobScopeMarkerRef, IdentityRequestDigestValue,
        IdentityTimestamp,
    };

    fn request_digest(token: &str) -> IdentityRequestDigest {
        IdentityRequestDigest::from_canonical_marker(
            IdentityCanonicalRequestMarkerRef::new(format!("canonical-{token}")),
            IdentityRequestDigestValue::new(format!("digest-{token}")),
            IdentityProtocolSchemaVersionRef::new("identity.job.v1"),
            IdentityDigestAlgorithmMarkerRef::new("sha256-v1"),
        )
    }

    fn job_request(token: &str) -> IdentityJobRequest<String> {
        IdentityJobRequest {
            job_name: IdentityJobName::new("RunIdentityReconciliation"),
            job_run_ref: IdentityJobRunRef::new(format!("job-run-{token}")),
            run_metadata_ref: IdentityJobRunMetadataRef::new(format!("job-metadata-{token}")),
            scope_marker_ref: IdentityJobScopeMarkerRef::new(format!("job-scope-{token}")),
            idempotency_key: format!("idem-{token}").into(),
            input_cursor_ref: Some(IdentityJobCursorRef::new(format!("job-cursor-{token}"))),
            schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.job.v1"),
            system_actor_ref: ActorRef::system("identity-job"),
            input: format!("job-input-{token}"),
        }
    }

    fn job_context(token: &str) -> IdentityOperationContext {
        IdentityOperationContext::from_job(
            IdentityOperationContextRef::new(format!("context-{token}")),
            IdentityOperationName::new("RunIdentityReconciliation"),
            ActorRef::system("identity-job"),
            IdentityRequestMetadataRef::new(format!("request-metadata-{token}")),
            IdentityIdempotencyKey::new(format!("idem-{token}")),
            request_digest(token),
            None,
            IdentityJobRunRef::new(format!("job-run-{token}")),
            IdentityTimestamp::from_clock(1).expect("timestamp"),
        )
    }

    #[test]
    fn job_context_requires_matching_job_run_ref() {
        let request = job_request("mismatch");
        let mut context = job_context("mismatch");
        context.job_run_ref = Some(IdentityJobRunRef::new("job-run-other"));

        let err = IdentityJobService::assert_job_context(&request, &context).expect_err("error");
        assert_eq!(
            err.kind,
            crate::errors::ApplicationErrorKind::InvalidRequest
        );
    }

    #[test]
    fn partial_result_requires_issue_refs() {
        let report = IdentityJobRunReport::start(
            identity_contracts::refs::IdentityJobReportRef::new("job-report-1"),
            IdentityJobRunRef::new("job-run-1"),
            IdentityJobName::new("RunIdentityReconciliation"),
            IdentityJobScopeMarkerRef::new("job-scope-1"),
            None,
            IdentityTimestamp::from_clock(1).expect("timestamp"),
        );

        let err = report
            .partial(
                Vec::new(),
                None,
                None,
                IdentityTimestamp::from_clock(2).expect("timestamp"),
            )
            .expect_err("error");
        assert_eq!(
            err.kind,
            crate::errors::ApplicationErrorKind::ConsistencyDefect
        );
    }
}
