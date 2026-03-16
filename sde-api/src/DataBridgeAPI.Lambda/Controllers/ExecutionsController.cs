using DataBridgeAPI.Lambda.Models;
using DataBridgeAPI.Lambda.Services;
using Microsoft.AspNetCore.Mvc;

namespace DataBridgeAPI.Lambda.Controllers;

/// <summary>
/// Manages Step Function executions.
///
/// GET    /api/executions                  → list all executions (both types, filterable)
/// GET    /api/executions/counts           → dashboard counters grouped by type and status
/// POST   /api/executions                  → start a new execution (async, 202 Accepted)
/// GET    /api/executions/{id}             → poll status + full event history
/// DELETE /api/executions/{id}             → cancel a running execution
/// </summary>
[ApiController]
[Route("api/executions")]
public class ExecutionsController : ControllerBase
{
    private readonly IStepFunctionsService _stepFunctions;
    private readonly ILogger<ExecutionsController> _logger;

    public ExecutionsController(IStepFunctionsService stepFunctions, ILogger<ExecutionsController> logger)
    {
        _stepFunctions = stepFunctions;
        _logger = logger;
    }

    // ── DELETE /api/executions/{executionId} ─────────────────────────────────

    /// <summary>Cancel a running execution.</summary>
    /// <remarks>
    /// Sends a StopExecution request to Step Functions.
    /// Returns 204 No Content on success.
    /// Has no effect if the execution is already in a terminal state (SUCCEEDED, FAILED, etc.).
    /// </remarks>
    /// <param name="executionId">Execution name (last segment of the ARN).</param>
    /// <param name="executionArn">Full execution ARN — required.</param>
    /// <param name="cancellationToken"></param>
    [HttpDelete("{executionId}")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status500InternalServerError)]
    public async Task<IActionResult> CancelExecutionAsync(
        [FromRoute] string executionId,
        [FromQuery] string executionArn,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(executionArn))
            return BadRequest(new ProblemDetails
            {
                Title  = "Missing executionArn",
                Detail = "The 'executionArn' query parameter is required.",
                Status = StatusCodes.Status400BadRequest
            });

        _logger.LogInformation("DELETE /api/executions/{Id} - Arn: {Arn}", executionId, executionArn);

        try
        {
            await _stepFunctions.CancelExecutionAsync(executionArn, cancellationToken);
            return NoContent();
        }
        catch (Amazon.StepFunctions.Model.ExecutionDoesNotExistException)
        {
            return NotFound(new ProblemDetails
            {
                Title  = "Execution not found",
                Detail = $"No execution found with ARN: {executionArn}",
                Status = StatusCodes.Status404NotFound
            });
        }
        catch (Exception ex)
        {
            var errorId = Guid.NewGuid().ToString();
            _logger.LogError(ex, "DELETE /api/executions/{Id} failed - ErrorId: {ErrorId}", executionId, errorId);
            return Problem(
                detail: $"An unexpected error occurred. ErrorId: {errorId}",
                statusCode: StatusCodes.Status500InternalServerError);
        }
    }

    // ── POST /api/executions ─────────────────────────────────────────────────

    /// <summary>Start a new Step Function execution.</summary>
    /// <remarks>
    /// Returns a 202 Accepted with the execution ARN and a status URL.
    /// Poll GET /api/executions/{executionId} to track progress.
    /// </remarks>
    /// <param name="request">Optional name, payload and metadata.</param>
    /// <param name="cancellationToken"></param>
    [HttpPost]
    [ProducesResponseType(typeof(StartExecutionResponse), StatusCodes.Status202Accepted)]
    [ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status500InternalServerError)]
    public async Task<IActionResult> StartExecutionAsync(
        [FromBody] StartExecutionRequest request,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("POST /api/executions - ExecutionName: {Name}", request.ExecutionName);

        // Extraction: require exposureSetIds (non-empty) and s3OutputPath
        if (request.TransactionType == TransactionType.Extraction)
        {
            if (request.ExposureSetIds is not { Count: > 0 })
                return BadRequest(new ProblemDetails
                {
                    Title  = "Missing exposureSetIds",
                    Detail = "The 'exposureSetIds' field is required and must contain at least one ID when transactionType is 'Extraction'.",
                    Status = StatusCodes.Status400BadRequest
                });

            if (string.IsNullOrWhiteSpace(request.S3OutputPath))
                return BadRequest(new ProblemDetails
                {
                    Title  = "Missing s3OutputPath",
                    Detail = "The 's3OutputPath' field is required when transactionType is 'Extraction'. Example: s3://my-bucket/output/",
                    Status = StatusCodes.Status400BadRequest
                });
        }

        // Ingestion: require fileName
        if (request.TransactionType == TransactionType.Ingestion
            && string.IsNullOrWhiteSpace(request.FileName))
        {
            return BadRequest(new ProblemDetails
            {
                Title  = "Missing fileName",
                Detail = "The 'fileName' field is required when transactionType is 'Ingestion'.",
                Status = StatusCodes.Status400BadRequest
            });
        }

        try
        {
            var baseUrl = $"{Request.Scheme}://{Request.Host}";
            var requestId = HttpContext.TraceIdentifier;
            var response = await _stepFunctions.StartExecutionAsync(request, requestId, baseUrl, cancellationToken);

            _logger.LogInformation("POST /api/executions - Started: {Arn}", response.ExecutionArn);
            return Accepted(response.StatusUrl, response);
        }
        catch (Exception ex)
        {
            var errorId = Guid.NewGuid().ToString();
            _logger.LogError(ex, "POST /api/executions failed - ErrorId: {ErrorId}", errorId);
            return Problem(
                detail: $"An unexpected error occurred. ErrorId: {errorId}",
                statusCode: StatusCodes.Status500InternalServerError);
        }
    }

    // ── GET /api/executions/{executionId} ────────────────────────────────────

    /// <summary>Get the status and full details of an execution.</summary>
    /// <remarks>
    /// Poll until <c>isComplete</c> is <c>true</c>.
    ///
    /// Response includes:
    /// - <c>status</c> — RUNNING | SUCCEEDED | FAILED | TIMED_OUT | ABORTED
    /// - <c>input</c>  — the payload passed into the state machine
    /// - <c>output</c> — final output when SUCCEEDED (parsed JSON)
    /// - <c>failure</c> — error code, cause, and state name when FAILED / TIMED_OUT / ABORTED
    /// - <c>durationMs</c> — wall-clock time once complete
    /// - <c>events</c> — full event log in chronological order (every state transition)
    ///
    /// The <c>executionArn</c> parameter is **required** — it is always returned in the
    /// <c>executionArn</c> field of the POST /api/executions response, and embedded in the
    /// pre-built <c>statusUrl</c>.
    /// </remarks>
    /// <param name="executionId">Short execution name (last segment of the ARN — for routing only).</param>
    /// <param name="executionArn">Full execution ARN (required). Returned by POST /api/executions.</param>
    /// <param name="cancellationToken"></param>
    [HttpGet("{executionId}")]
    [ProducesResponseType(typeof(ExecutionStatusResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status500InternalServerError)]
    public async Task<IActionResult> GetExecutionStatusAsync(
        [FromRoute] string executionId,
        [FromQuery] string? executionArn,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("GET /api/executions/{ExecutionId}", executionId);

        if (string.IsNullOrWhiteSpace(executionArn))
        {
            return BadRequest(new ProblemDetails
            {
                Title  = "Missing executionArn",
                Detail = "The 'executionArn' query parameter is required. " +
                         "It is returned in the 'executionArn' field of POST /api/executions " +
                         "and is embedded in the pre-built 'statusUrl'.",
                Status = StatusCodes.Status400BadRequest
            });
        }

        try
        {
            var response = await _stepFunctions.GetExecutionStatusAsync(executionArn, cancellationToken);
            return Ok(response);
        }
        catch (Amazon.StepFunctions.Model.ExecutionDoesNotExistException)
        {
            _logger.LogWarning("GET /api/executions/{ExecutionId} - Not found", executionId);
            return NotFound(new ProblemDetails
            {
                Title = "Execution Not Found",
                Detail = $"No execution found with ID '{executionId}'.",
                Status = StatusCodes.Status404NotFound
            });
        }
        catch (Exception ex)
        {
            var errorId = Guid.NewGuid().ToString();
            _logger.LogError(ex, "GET /api/executions/{ExecutionId} failed - ErrorId: {ErrorId}", executionId, errorId);
            return Problem(
                detail: $"An unexpected error occurred. ErrorId: {errorId}",
                statusCode: StatusCodes.Status500InternalServerError);
        }
    }

    // ── GET /api/executions/counts ───────────────────────────────────────

    /// <summary>Get execution counts grouped by transaction type and status (for UI dashboard).</summary>
    /// <remarks>
    /// Returns accurate counts for every (state machine × status) bucket by paginating through
    /// Step Functions <c>ListExecutions</c> in parallel. Caps at 5 000 per bucket; if any bucket
    /// exceeds that, <c>isTruncated</c> is set to <c>true</c>.
    ///
    /// When <c>tenantId</c> is supplied, counts are filtered by the <c>tenant_id</c> field in each
    /// execution's input (uses describe-based filtering; caps at 1 000 executions per state machine).
    ///
    /// Response shape:
    /// <code>
    /// {
    ///   "totalCount": 42,
    ///   "extractionCount": 20,
    ///   "ingestionCount": 22,
    ///   "runningCount": 3,
    ///   "succeededCount": 35,
    ///   "failedCount": 4,
    ///   "timedOutCount": 0,
    ///   "abortedCount": 0,
    ///   "extractionBreakdown": { "running": 1, "succeeded": 17, "failed": 2, ... },
    ///   "ingestionBreakdown":  { "running": 2, "succeeded": 18, "failed": 2, ... },
    ///   "isTruncated": false
    /// }
    /// </code>
    /// </remarks>
    /// <param name="transactionType">Optional — Extraction or Ingestion. Omit for both.</param>
    /// <param name="tenantId">Optional — filters counts by tenant ID.</param>
    /// <param name="cancellationToken"></param>
    [HttpGet("counts")]
    [ProducesResponseType(typeof(ExecutionCountsResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status500InternalServerError)]
    public async Task<IActionResult> GetExecutionCountsAsync(
        [FromQuery] TransactionType? transactionType = null,
        [FromQuery] string? tenantId = null,
        [FromQuery(Name = "tenant_id")] string? tenantIdSnake = null,
        CancellationToken cancellationToken = default)
    {
        tenantId ??= tenantIdSnake;
        _logger.LogInformation("GET /api/executions/counts - type={Type} tenantId={TenantId}",
            transactionType?.ToString() ?? "All", tenantId ?? "Any");
        try
        {
            var response = await _stepFunctions.GetExecutionCountsAsync(transactionType, tenantId, cancellationToken);
            return Ok(response);
        }
        catch (Exception ex)
        {
            var errorId = Guid.NewGuid().ToString();
            _logger.LogError(ex, "GET /api/executions/counts failed - ErrorId: {ErrorId}", errorId);
            return Problem(
                detail: $"An unexpected error occurred. ErrorId: {errorId}",
                statusCode: StatusCodes.Status500InternalServerError);
        }
    }

    // ── GET /api/executions ──────────────────────────────────────────────

    /// <summary>List all executions from both Extraction and Ingestion state machines.</summary>
    /// <remarks>
    /// Returns a combined, date-descending list from both Step Function state machines.
    ///
    /// Optional filters:
    /// - <c>transactionType</c> — <c>Extraction</c> or <c>Ingestion</c> (omit for both)
    /// - <c>status</c>          — RUNNING | SUCCEEDED | FAILED | TIMED_OUT | ABORTED (omit for all)
    /// - <c>tenantResourceId</c> — filters by tenant resource ID
    /// - <c>maxResults</c>      — max items per state machine, 1–100 (default 50)
    ///
    /// For executions that are FAILED / TIMED_OUT / ABORTED the <c>failure</c> object
    /// is populated with the error code and cause. Call GET /api/executions/{executionId}
    /// for the full per-step event log.
    /// </remarks>
    /// <param name="transactionType">Optional — Extraction or Ingestion.</param>
    /// <param name="status">Optional AWS Step Functions status filter.</param>
    /// <param name="tenantResourceId">Optional — filters by tenant resource ID.</param>
    /// <param name="maxResults">Max executions per state machine (1–100). Default 50.</param>
    /// <param name="cancellationToken"></param>
    [HttpGet]
    [ProducesResponseType(typeof(ListExecutionsResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status500InternalServerError)]
    public async Task<IActionResult> ListExecutionsAsync(
        [FromQuery] TransactionType? transactionType = null,
        [FromQuery] string? status = null,
        [FromQuery] string? tenantResourceId = null,
        [FromQuery(Name = "tenant_resource_id")] string? tenantResourceIdSnake = null,
        [FromQuery] string? tenantId = null,
        [FromQuery(Name = "tenant_id")] string? tenantIdSnake = null,
        [FromQuery] int maxResults = 50,
        CancellationToken cancellationToken = default)
    {
        tenantResourceId ??= tenantResourceIdSnake;
        tenantId ??= tenantIdSnake;
        _logger.LogInformation(
            "GET /api/executions - type={Type} status={Status} tenantId={TenantId} tenantResourceId={TenantResourceId} max={Max}",
            transactionType?.ToString() ?? "All", status ?? "All", tenantId ?? "Any", tenantResourceId ?? "Any", maxResults);

        if (maxResults is < 1 or > 100)
        {
            return BadRequest(new ProblemDetails
            {
                Title  = "Invalid maxResults",
                Detail = "maxResults must be between 1 and 100.",
                Status = StatusCodes.Status400BadRequest
            });
        }

        try
        {
            var baseUrl = $"{Request.Scheme}://{Request.Host}";
            var response = await _stepFunctions.ListExecutionsAsync(
                transactionType, status, maxResults, tenantId, tenantResourceId, baseUrl, cancellationToken);

            return Ok(response);
        }
        catch (Exception ex)
        {
            var errorId = Guid.NewGuid().ToString();
            _logger.LogError(ex, "GET /api/executions list failed - ErrorId: {ErrorId}", errorId);
            return Problem(
                detail: $"An unexpected error occurred. ErrorId: {errorId}",
                statusCode: StatusCodes.Status500InternalServerError);
        }
    }
}
