using System.Text.Json.Serialization;

namespace DataBridgeAPI.Lambda.Models;

/// <summary>
/// Full execution status response — returned by GET /api/executions/{executionId}.
/// Poll until <see cref="IsComplete"/> is <c>true</c>.
/// </summary>
public class ExecutionStatusResponse
{
    // ── Identity ──────────────────────────────────────────────────────────────

    /// <summary>Full ARN of the execution.</summary>
    [JsonPropertyName("executionArn")]
    public required string ExecutionArn { get; set; }

    /// <summary>Short execution name (last segment of the ARN).</summary>
    [JsonPropertyName("executionId")]
    public required string ExecutionId { get; set; }

    /// <summary>Human-readable execution name set at start time.</summary>
    [JsonPropertyName("name")]
    public string? Name { get; set; }

    /// <summary>Which pipeline was triggered: Extraction | Ingestion.</summary>
    [JsonPropertyName("transactionType")]
    public string? TransactionType { get; set; }

    /// <summary>Tenant ID extracted from the step function input (<c>tenant_id</c>).</summary>
    [JsonPropertyName("tenantId")]
    public string? TenantId { get; set; }

    // ── Status ────────────────────────────────────────────────────────────────

    /// <summary>RUNNING | SUCCEEDED | FAILED | TIMED_OUT | ABORTED | PENDING_REDRIVE</summary>
    [JsonPropertyName("status")]
    public required string Status { get; set; }

    /// <summary><c>true</c> once the execution has reached a terminal state.</summary>
    [JsonPropertyName("isComplete")]
    public bool IsComplete => Status is "SUCCEEDED" or "FAILED" or "TIMED_OUT" or "ABORTED";

    // ── Timing ────────────────────────────────────────────────────────────────

    [JsonPropertyName("startDate")]
    public DateTime? StartDate { get; set; }

    /// <summary>Set once the execution reaches a terminal state.</summary>
    [JsonPropertyName("stopDate")]
    public DateTime? StopDate { get; set; }

    /// <summary>
    /// Wall-clock duration in milliseconds.
    /// Available as soon as <see cref="StopDate"/> is set.
    /// </summary>
    [JsonPropertyName("durationMs")]
    public long? DurationMs { get; set; }

    // ── Input / Output ────────────────────────────────────────────────────────

    /// <summary>The JSON payload that was passed into the state machine.</summary>
    [JsonPropertyName("input")]
    public object? Input { get; set; }

    /// <summary>The final JSON output of the execution. Only present when Status is SUCCEEDED.</summary>
    [JsonPropertyName("output")]
    public object? Output { get; set; }

    // ── Failure ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Populated when Status is FAILED, TIMED_OUT, or ABORTED.
    /// Contains the error code, human-readable cause, and the step where the failure occurred.
    /// </summary>
    [JsonPropertyName("failure")]
    public ExecutionFailure? Failure { get; set; }

    // ── Progress ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Ordered list of execution history events (chronological).
    /// Each entry represents a state transition — use this to track per-step progress
    /// and to diagnose failures.
    /// </summary>
    [JsonPropertyName("events")]
    public List<ExecutionEvent> Events { get; set; } = [];
}

/// <summary>Failure details — present only when the execution did not succeed.</summary>
public class ExecutionFailure
{
    /// <summary>The AWS Step Functions error code (e.g. States.TaskFailed, States.Timeout).</summary>
    [JsonPropertyName("error")]
    public string? Error { get; set; }

    /// <summary>Human-readable description of what went wrong.</summary>
    [JsonPropertyName("cause")]
    public string? Cause { get; set; }

    /// <summary>Name of the state where the failure was first detected, if available.</summary>
    [JsonPropertyName("failedAt")]
    public string? FailedAt { get; set; }
}

/// <summary>A single execution history event.</summary>
public class ExecutionEvent
{
    [JsonPropertyName("id")]
    public long Id { get; set; }

    [JsonPropertyName("previousEventId")]
    public long PreviousEventId { get; set; }

    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; set; }

    /// <summary>
    /// AWS history event type. Common values:
    /// ExecutionStarted, ExecutionSucceeded, ExecutionFailed, ExecutionTimedOut, ExecutionAborted,
    /// PassStateEntered, PassStateExited, TaskStateEntered, TaskStateExited,
    /// TaskScheduled, TaskStarted, TaskSucceeded, TaskFailed,
    /// LambdaFunctionScheduled, LambdaFunctionStarted, LambdaFunctionSucceeded, LambdaFunctionFailed.
    /// </summary>
    [JsonPropertyName("type")]
    public required string Type { get; set; }

    /// <summary>Name of the state this event belongs to (when applicable).</summary>
    [JsonPropertyName("stateName")]
    public string? StateName { get; set; }

    /// <summary>Flat key/value detail bag — content varies by event type.</summary>
    [JsonPropertyName("details")]
    public Dictionary<string, string?> Details { get; set; } = [];
}
