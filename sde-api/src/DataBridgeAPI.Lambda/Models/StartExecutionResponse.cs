using System.Text.Json.Serialization;

namespace DataBridgeAPI.Lambda.Models;

/// <summary>Returned on 202 Accepted after successfully starting an execution.</summary>
public class StartExecutionResponse
{
    /// <summary>The pipeline that was triggered.</summary>
    [JsonPropertyName("transactionType")]
    public required string TransactionType { get; set; }

    /// <summary>Full execution ARN — pass as <c>?executionArn=</c> when polling status.</summary>
    [JsonPropertyName("executionArn")]
    public required string ExecutionArn { get; set; }

    /// <summary>Short execution ID (last segment of the ARN) — used in the status URL path.</summary>
    [JsonPropertyName("executionId")]
    public required string ExecutionId { get; set; }

    [JsonPropertyName("startDate")]
    public DateTime StartDate { get; set; }

    /// <summary>The tenant ID embedded in the step function input. Null if not supplied.</summary>
    [JsonPropertyName("tenantId")]
    public string? TenantId { get; set; }

    /// <summary>
    /// Poll this URL to track progress. Append <c>&amp;executionArn={executionArn}</c>
    /// to avoid any ARN reconstruction on the server.
    /// </summary>
    [JsonPropertyName("statusUrl")]
    public required string StatusUrl { get; set; }
}
