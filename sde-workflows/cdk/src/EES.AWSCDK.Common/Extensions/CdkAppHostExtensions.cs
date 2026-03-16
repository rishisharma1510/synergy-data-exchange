using Amazon.CDK;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Reflection;

namespace EES.AWSCDK.Common.Extensions;

/// <summary>
/// Extension methods for CDK host configuration
/// Matches SynergyDrive pattern
/// </summary>
public static class CdkAppHostExtensions
{
    /// <summary>
    /// Initialize the root CDK construct (App) and extract environment/region from context
    /// </summary>
    public static IHostBuilder InitializeRootConstruct(this IHostBuilder hostBuilder, out string? targetEnvironment, out string? region)
    {
        var app = new App();
        targetEnvironment = app.Node.TryGetContext("TargetEnvironment")?.ToString();
        region = app.Node.TryGetContext("awsregion")?.ToString();
        hostBuilder.ConfigureServices(services => services.AddSingleton<App>(app));
        return hostBuilder;
    }

    /// <summary>
    /// Load application settings from appsettings.json files
    /// </summary>
    public static IHostBuilder LoadApplicationSettings(
        this IHostBuilder hostBuilder, 
        string targetEnvironment, 
        string[] commandLineArgs, 
        string environmentVariablePrefix)
    {
        var builder = new ConfigurationBuilder()
            .SetBasePath(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location)!)
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
            .AddJsonFile($"appsettings.{targetEnvironment}.json", optional: true, reloadOnChange: true)
            .AddJsonFile($"appsettings.variables.{targetEnvironment}.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables(environmentVariablePrefix)
            .AddCommandLine(commandLineArgs);

        hostBuilder.ConfigureServices(services => services.AddSingleton<IConfigurationRoot>(builder.Build()));

        return hostBuilder;
    }
}
