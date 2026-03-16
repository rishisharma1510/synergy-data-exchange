using Verisk.EES.CDK.Common.Stacks;

namespace EES.AWSCDK.Common;

/// <summary>
/// Factory for creating stack properties with configuration
/// </summary>
public interface IStackPropertiesFactory
{
    /// <summary>
    /// Get stack properties with the specified configuration type
    /// </summary>
    /// <typeparam name="T">Configuration type implementing IStackConfiguration</typeparam>
    /// <param name="tokens">Optional token replacement dictionary</param>
    /// <returns>Stack properties with configuration</returns>
    StackProperties<T> GetStackProperties<T>(Dictionary<string, string>? tokens = null) where T : IStackConfiguration, new();

    /// <summary>
    /// Get stack properties with a custom configuration section name
    /// </summary>
    /// <typeparam name="T">Configuration type implementing IStackConfiguration</typeparam>
    /// <param name="configurationSettingsName">Name of the configuration section</param>
    /// <param name="tokens">Optional token replacement dictionary</param>
    /// <returns>Stack properties with configuration</returns>
    StackProperties<T> GetStackProperties<T>(string configurationSettingsName, Dictionary<string, string>? tokens = null) where T : IStackConfiguration, new();
}
