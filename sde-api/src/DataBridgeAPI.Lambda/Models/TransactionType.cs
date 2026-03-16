using System.Text.Json.Serialization;

namespace DataBridgeAPI.Lambda.Models;

/// <summary>
/// Determines which DataBridge Step Function pipeline is triggered.
/// </summary>
[JsonConverter(typeof(JsonStringEnumConverter))]
public enum TransactionType
{
    /// <summary>Triggers the Extraction state machine.</summary>
    Extraction,

    /// <summary>Triggers the Ingestion state machine.</summary>
    Ingestion
}
