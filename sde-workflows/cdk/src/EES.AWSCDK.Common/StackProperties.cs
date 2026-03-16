using Amazon.CDK;
using Microsoft.Extensions.Configuration;
using Verisk.EES.CDK.Common.Props.Stack;
using Verisk.EES.CDK.Common.Stacks;

namespace EES.AWSCDK.Common;

/// <summary>
/// Stack properties with strongly-typed configuration.
/// Extends StackBaseProps from Verisk.EES.CDK.Common for consistent tagging and CDK patterns.
/// </summary>
/// <typeparam name="TStackConfiguration">Configuration type that implements IStackConfiguration</typeparam>
public class StackProperties<TStackConfiguration> : StackBaseProps where TStackConfiguration : IStackConfiguration
{
    /// <summary>
    /// Strongly-typed stack configuration settings
    /// </summary>
    public TStackConfiguration StackConfigurationSettings { get; set; } = default!;

    /// <summary>
    /// Default region from CDK context
    /// </summary>
    public string CDK_DEFAULT_REGION { get; set; } = string.Empty;

    /// <summary>
    /// Default account from CDK context
    /// </summary>
    public string CDK_DEFAULT_ACCOUNT { get; set; } = string.Empty;

    /// <summary>
    /// Configuration root for accessing additional settings
    /// </summary>
    public IConfiguration? ConfigurationRoot { get; set; }

    /// <summary>
    /// Application variables for token replacement
    /// </summary>
    public IReadOnlyDictionary<string, string> ApplicationVariables { get; set; } = new Dictionary<string, string>();

    /// <summary>
    /// Dictionary of tokens to replace in configuration values
    /// </summary>
    public Dictionary<string, string> TokenReplacementDictionary { get; set; } = new Dictionary<string, string>();

    /// <summary>
    /// Stack name template using prefix and stage (computed from base class properties)
    /// </summary>
    public string StackTemplate => $"{StackTemplatePrefix}{{0}}-{InstanceStage}";

    /// <summary>
    /// VPC ID for deployments requiring VPC configuration
    /// </summary>
    public string? VpcId { get; set; }

    /// <summary>
    /// Custom synthesizer for using pre-created bootstrap roles (intentionally hides base class property)
    /// </summary>
    public new IStackSynthesizer? Synthesizer { get; set; }
}
