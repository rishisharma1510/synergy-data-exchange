using System.Text.Json.Serialization;

namespace DataBridgeAPI.Lambda.Models;

/// <summary>
/// Response from GET /api/executions — combined list from both state machines.
/// </summary>
public class ListExecutionsResponse
{
    /// <summary>Combined list of executions from Extraction and Ingestion state machines,
    /// ordered by start date descending.</summary>
    [JsonPropertyName("items")]
    public List<ExecutionListItem> Items { get; set; } = [];

    /// <summary>Total number of items returned (respects <c>maxResults</c>).</summary>
    [JsonPropertyName("totalCount")]
    public int TotalCount { get; set; }

    /// <summary>Number of Extraction executions in this result set.</summary>
    [JsonPropertyName("extractionCount")]
    public int ExtractionCount { get; set; }

    /// <summary>Number of Ingestion executions in this result set.</summary>
    [JsonPropertyName("ingestionCount")]
    public int IngestionCount { get; set; }

    /// <summary>
    /// Filters applied to this result.
    /// </summary>
    [JsonPropertyName("appliedFilters")]
    public ListExecutionsFilters? AppliedFilters { get; set; }
}

/// <summary>Echoes back the query parameters that were applied.</summary>
public class ListExecutionsFilters
{
    /// <summary>Type filter, if supplied (Extraction | Ingestion). Null means both types are included.</summary>
    [JsonPropertyName("transactionType")]
    public string? TransactionType { get; set; }

    /// <summary>Status filter, if supplied (e.g. RUNNING, FAILED). Null means all statuses.</summary>
    [JsonPropertyName("status")]
    public string? Status { get; set; }

    /// <summary>Tenant ID filter used to match executions (matched against <c>tenant_id</c> in SFN input).</summary>
    [JsonPropertyName("tenantId")]
    public string? TenantId { get; set; }

    /// <summary>Tenant resource ID used for S3 bucket derivation (<c>rs-cdkdev-{tenantResourceId}-s3</c>).</summary>
    [JsonPropertyName("tenantResourceId")]
    public string? TenantResourceId { get; set; }

    /// <summary>Maximum items requested per state machine.</summary>
    [JsonPropertyName("maxResults")]
    public int MaxResults { get; set; }
}
