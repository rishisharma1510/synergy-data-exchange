using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;

namespace DataBridgeAPI.Lambda.Models;

/// <summary>Request body for starting a new Step Function execution.</summary>
public class StartExecutionRequest
{
    /// <summary>
    /// Determines which pipeline to trigger.
    /// <list type="bullet">
    ///   <item><description><b>Extraction</b> — triggers the Extraction state machine.</description></item>
    ///   <item><description><b>Ingestion</b> — triggers the Ingestion state machine.</description></item>
    /// </list>
    /// </summary>
    [JsonPropertyName("transactionType")]
    [Required]
    public TransactionType TransactionType { get; set; }

    /// <summary>
    /// Optional human-readable name for this execution. Must be unique within the state machine (max 80 chars).
    /// Auto-generated if omitted.
    /// </summary>
    [JsonPropertyName("executionName")]
    [StringLength(80)]
    public string? ExecutionName { get; set; }

    // ── Extraction fields ──────────────────────────────────────────────────────

    /// <summary>
    /// List of Exposure Set IDs to extract.
    /// <b>Required when <see cref="TransactionType"/> is <c>Extraction</c>.</b>
    /// </summary>
    [JsonPropertyName("exposureSetIds")]
    public List<int>? ExposureSetIds { get; set; }

    /// <summary>
    /// Artifact type passed to the Extraction state machine input as <c>artifact_type</c>.
    /// Accepts <c>artifactType</c> or <c>exportType</c>. Defaults to <c>"mdf"</c> if omitted.
    /// </summary>
    [JsonPropertyName("artifactType")]
    public string? ArtifactType { get; set; }

    /// <summary>Alias for <see cref="ArtifactType"/> — accepted when UI sends <c>exportType</c>.</summary>
    [JsonPropertyName("exportType")]
    public string? ExportType { set => ArtifactType ??= value; get => ArtifactType; }

    /// <summary>
    /// S3 path where Extraction results will be written.
    /// <b>Required when <see cref="TransactionType"/> is <c>Extraction</c>.</b>
    /// Example: <c>s3://my-bucket/output/2026-03-04/</c>
    /// </summary>
    [JsonPropertyName("s3OutputPath")]
    public string? S3OutputPath { get; set; }

    // ── Ingestion fields ──────────────────────────────────────────────────────

    /// <summary>
    /// Name of the file to ingest.
    /// <b>Required when <see cref="TransactionType"/> is <c>Ingestion</c>.</b>
    /// </summary>
    [JsonPropertyName("fileName")]
    public string? FileName { get; set; }

    // ── Shared ────────────────────────────────────────────────────────────────

    /// <summary>Tenant identifier passed to the step function as <c>tenant_id</c>.</summary>
    [JsonPropertyName("tenantId")]
    public string? TenantId { get => _tenantId ?? Metadata?.TenantId; set => _tenantId = value; }
    private string? _tenantId;

    /// <summary>Alias for <see cref="TenantId"/> — accepted when UI sends <c>tenant_id</c>.</summary>
    [JsonPropertyName("tenant_id")]
    public string? TenantIdSnakeCase { set => _tenantId ??= value; get => TenantId; }

    /// <summary>Tenant resource id (e.g. <c>bridgete01</c>) used to derive the tenant S3 bucket.</summary>
    [JsonPropertyName("tenantResourceId")]
    public string? TenantResourceId { get => _tenantResourceId ?? Metadata?.TenantResourceId; set => _tenantResourceId = value; }
    private string? _tenantResourceId;

    /// <summary>
    /// Optional metadata wrapper — accepted when the UI nests tenant fields inside a <c>metadata</c> object.
    /// </summary>
    [JsonPropertyName("metadata")]
    public RequestMetadata? Metadata { get; set; }
}

/// <summary>Nested metadata accepted when the UI wraps tenant fields inside a <c>metadata</c> object.</summary>
public class RequestMetadata
{
    [JsonPropertyName("tenant_id")]
    public string? TenantId { get; set; }

    [JsonPropertyName("tenantId")]
    public string? TenantIdCamel { set => TenantId ??= value; get => TenantId; }

    [JsonPropertyName("tenantResourceId")]
    public string? TenantResourceId { get; set; }
}
