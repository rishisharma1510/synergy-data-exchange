using System.Text.Json.Serialization;

namespace DataBridgeAPI.Lambda.Models;

/// <summary>
/// Execution counts response — returned by GET /api/executions/counts.
/// Provides summary counters for the UI dashboard.
/// </summary>
public class ExecutionCountsResponse
{
    // ── Totals by transaction type ─────────────────────────────────────────

    /// <summary>Total executions across all types and statuses.</summary>
    [JsonPropertyName("totalCount")]
    public int TotalCount { get; set; }

    /// <summary>Total Extraction executions (all statuses).</summary>
    [JsonPropertyName("extractionCount")]
    public int ExtractionCount { get; set; }

    /// <summary>Total Ingestion executions (all statuses).</summary>
    [JsonPropertyName("ingestionCount")]
    public int IngestionCount { get; set; }

    // ── Totals by status ──────────────────────────────────────────────────

    /// <summary>Executions currently in RUNNING state.</summary>
    [JsonPropertyName("runningCount")]
    public int RunningCount { get; set; }

    /// <summary>Executions that completed successfully.</summary>
    [JsonPropertyName("succeededCount")]
    public int SucceededCount { get; set; }

    /// <summary>Executions that ended in FAILED state.</summary>
    [JsonPropertyName("failedCount")]
    public int FailedCount { get; set; }

    /// <summary>Executions that were TIMED_OUT.</summary>
    [JsonPropertyName("timedOutCount")]
    public int TimedOutCount { get; set; }

    /// <summary>Executions that were ABORTED.</summary>
    [JsonPropertyName("abortedCount")]
    public int AbortedCount { get; set; }

    // ── Per-type status breakdown ─────────────────────────────────────────

    /// <summary>Status breakdown for Extraction executions only.</summary>
    [JsonPropertyName("extractionBreakdown")]
    public StatusBreakdown ExtractionBreakdown { get; set; } = new();

    /// <summary>Status breakdown for Ingestion executions only.</summary>
    [JsonPropertyName("ingestionBreakdown")]
    public StatusBreakdown IngestionBreakdown { get; set; } = new();

    // ── Meta ──────────────────────────────────────────────────────────────

    /// <summary>
    /// <c>true</c> if any individual count may be an undercount because
    /// the number of executions for a given (state machine × status) combination
    /// exceeded the internal pagination cap. Counts are still accurate up to that cap.
    /// </summary>
    [JsonPropertyName("isTruncated")]
    public bool IsTruncated { get; set; }

    /// <summary>Filters applied to this result.</summary>
    [JsonPropertyName("appliedFilters")]
    public CountFilters? AppliedFilters { get; set; }
}

/// <summary>Status-level breakdown for a single transaction type.</summary>
public class StatusBreakdown
{
    [JsonPropertyName("running")]
    public int Running { get; set; }

    [JsonPropertyName("succeeded")]
    public int Succeeded { get; set; }

    [JsonPropertyName("failed")]
    public int Failed { get; set; }

    [JsonPropertyName("timedOut")]
    public int TimedOut { get; set; }

    [JsonPropertyName("aborted")]
    public int Aborted { get; set; }

    [JsonPropertyName("total")]
    public int Total => Running + Succeeded + Failed + TimedOut + Aborted;
}

/// <summary>Filters applied to a counts request.</summary>
public class CountFilters
{
    /// <summary>Transaction type filter, if supplied. Null means both types included.</summary>
    [JsonPropertyName("transactionType")]
    public string? TransactionType { get; set; }

    /// <summary>Tenant ID filter, if supplied. Null means all tenants included.</summary>
    [JsonPropertyName("tenantId")]
    public string? TenantId { get; set; }
}
