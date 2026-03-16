using DataBridgeAPI.Lambda.Models;

namespace DataBridgeAPI.Lambda.Services;

public interface IStepFunctionsService
{
    /// <summary>
    /// Starts a new Step Function execution. Routes to the correct state machine based on
    /// <see cref="StartExecutionRequest.TransactionType"/>:
    /// <list type="bullet">
    ///   <item><description><b>Extraction</b> → EXTRACTION_STATE_MACHINE_ARN</description></item>
    ///   <item><description><b>Ingestion</b>  → INGESTION_STATE_MACHINE_ARN</description></item>
    /// </list>
    /// Returns 202 data including the full <c>executionArn</c> which the caller
    /// should pass back to <see cref="GetExecutionStatusAsync"/> for polling.
    /// </summary>
    Task<StartExecutionResponse> StartExecutionAsync(
        StartExecutionRequest request,
        string requestId,
        string baseUrl,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the status, input, output, failure details, and full event log of an execution.
    /// Pass the <c>executionArn</c> returned by <see cref="StartExecutionAsync"/> directly.
    /// </summary>
    Task<ExecutionStatusResponse> GetExecutionStatusAsync(
        string executionArn,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists executions from both Extraction and Ingestion state machines combined,
    /// ordered by start date descending.
    /// <para>
    /// For executions that are FAILED, TIMED_OUT, or ABORTED the <c>failure</c> field
    /// is populated (error code + cause) via a parallel <c>DescribeExecution</c> call.
    /// Call <see cref="GetExecutionStatusAsync"/> to retrieve the full event log / <c>FailedAt</c> state.
    /// </para>
    /// </summary>
    /// <param name="transactionType">Optional filter — omit to return both types.</param>
    /// <param name="statusFilter">Optional AWS Step Functions status filter (RUNNING, SUCCEEDED, FAILED, TIMED_OUT, ABORTED).</param>
    /// <param name="maxResults">Max executions to return per state machine (1–1000). Defaults to 50.</param>
    /// <param name="tenantId">Optional — filters executions by matching <c>tenant_id</c> in SFN input.</param>
    /// <param name="tenantResourceId">Optional — used to derive S3 bucket name (<c>rs-cdkdev-{id}-s3</c>) for progress.json. Does not affect SFN filtering.</param>
    /// <param name="baseUrl">Base URL of the API — used to construct per-item <c>detailUrl</c>.</param>
    /// <param name="cancellationToken"></param>
    Task<ListExecutionsResponse> ListExecutionsAsync(
        TransactionType? transactionType,
        string? statusFilter,
        int maxResults,
        string? tenantId,
        string? tenantResourceId,
        string baseUrl,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns execution counts grouped by transaction type and status, for UI dashboard display.
    /// Makes parallel calls per (state machine × status) so each counter is accurate.
    /// <para>
    /// A <see cref="ExecutionCountsResponse.IsTruncated"/> flag is set when any single
    /// (state machine × status) bucket exceeds the internal pagination cap.
    /// </para>
    /// </summary>
    /// <param name="transactionType">Optional — omit to count both types.</param>
    /// <param name="tenantId">Optional — when supplied, counts only executions for the given tenant ID.</param>
    /// <param name="cancellationToken"></param>
    Task<ExecutionCountsResponse> GetExecutionCountsAsync(
        TransactionType? transactionType,
        string? tenantId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Stops (cancels) a running Step Function execution.
    /// Has no effect if the execution is already in a terminal state.
    /// </summary>
    /// <param name="executionArn">Full ARN of the execution to cancel.</param>
    /// <param name="cancellationToken"></param>
    Task CancelExecutionAsync(
        string executionArn,
        CancellationToken cancellationToken = default);
}
