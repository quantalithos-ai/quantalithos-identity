//! Shared operations-job scaffold and stored-report replay helpers.

use identity_contracts::jobs::{IdentityJobReportSurface, IdentityJobRequest, IdentityJobResponse};
use identity_contracts::refs::{
    IdentityOperationChannel, IdentityStoredResultRef, IdentityTimestamp,
};

use crate::errors::{ApplicationError, ApplicationErrorKind};
use crate::ports::{
    IdentityClockPort, IdentityIdGeneratorPort, IdentityIdempotencyRepository,
    IdentityJobReportRepository, IdentityStoredResultRepository, IdentityUnitOfWork,
    IdentityUnitOfWorkManagerPort,
};
use crate::support::{
    IdempotencyReserveOutcome, IdentityIdempotencyRecord, IdentityJobRunReport,
    IdentityOperationContext, IdentityStoredResultKind, IdentityStoredSurfaceMarkerRef,
    StoredIdentityOperationResult, Versioned,
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
