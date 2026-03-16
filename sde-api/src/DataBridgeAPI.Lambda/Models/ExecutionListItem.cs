using System.Text.Json.Serialization;

namespace DataBridgeAPI.Lambda.Models;

/// <summary>
/// Summary of a single execution — returned as part of GET /api/executions list.
/// For the full event log call GET /api/executions/{executionId}?executionArn=...
/// </summary>
public class ExecutionListItem
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
    public required string TransactionType { get; set; }

    /// <summary>Tenant resource ID extracted from the step function input. Populated when available; null otherwise.</summary>
    [JsonPropertyName("tenantResourceId")]
    public string? TenantResourceId { get; set; }

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

    /// <summary>Wall-clock duration in milliseconds. Available once <see cref="StopDate"/> is set.</summary>
    [JsonPropertyName("durationMs")]
    public long? DurationMs { get; set; }

    // ── Failure ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Populated when Status is FAILED, TIMED_OUT, or ABORTED.
    /// Contains the AWS error code and human-readable cause.
    /// Call GET /api/executions/{executionId}?executionArn=... for the complete event log
    /// including <c>FailedAt</c> state name.
    /// </summary>
    [JsonPropertyName("failure")]
    public ExecutionFailure? Failure { get; set; }

    // ── Navigation ────────────────────────────────────────────────────────────

    /// <summary>
    /// Fully-qualified URL to GET the complete execution details and event history for this item.
    /// </summary>
    [JsonPropertyName("detailUrl")]
    public string? DetailUrl { get; set; }

    // ── Progress ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Live progress report fetched from S3 (databridge/runs/{run_id}/progress.json).
    /// Populated for RUNNING executions; <c>null</c> when the file does not exist or the execution is terminal.
    /// </summary>
    [JsonPropertyName("progress")]
    public object? Progress { get; set; }

    /// <summary>
    /// Number of records ingested when available (derived from execution output or progress payload).
    /// </summary>
    [JsonPropertyName("recordsIngested")]
    public int? RecordsIngested { get; set; }

    /// <summary>
    /// Number of records extracted when available (derived from execution output or progress payload).
    /// </summary>
    [JsonPropertyName("recordsExtracted")]
    public int? RecordsExtracted { get; set; }
}
