using System.Runtime.CompilerServices;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using EES.AWSCDK.Common;
using EES.AWSCDK.Common.Extensions;
using EES.AP.SDE.CDK.Services;

[assembly: InternalsVisibleToAttribute("EES.AP.SDE.CDK.Tests")]
namespace EES.AP.SDE.CDK;

public class Program
{
    public virtual void ConfigureServices(IServiceCollection services)
    {
        services.AddScoped<IStackPropertiesFactory, StackPropertiesFactory>()
            .AddScoped<StackGenerator>();
    }

    public void SynthesizeToCloudAssembly(string[] commandLineArgs)
    {
        _ = Host.CreateDefaultBuilder()
            .InitializeRootConstruct(out string? targetEnvironment, out string? region)
            .LoadApplicationSettings(targetEnvironment!, commandLineArgs, "SDE_CDK_")
            .ConfigureServices(services => ConfigureServices(services))
            .Build()
            .Services
            .GetService<StackGenerator>()!
            .GenerateStacks()
            .RootConstructApp
            .Synth();
    }

    [ExcludeFromCodeCoverage(Justification = "console app entry point")]
    public static void Main(string[] args)
    {
        (new Program()).SynthesizeToCloudAssembly(args);
    }
}
