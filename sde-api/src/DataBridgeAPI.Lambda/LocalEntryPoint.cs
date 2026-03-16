using System.Diagnostics.CodeAnalysis;

namespace DataBridgeAPI.Lambda;

/// <summary>
/// Entry point when running locally via Kestrel (dotnet run).
/// Not deployed to Lambda.
/// </summary>
[ExcludeFromCodeCoverage]
public class LocalEntryPoint
{
    public static void Main(string[] args) =>
        CreateHostBuilder(args).Build().Run();

    public static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureWebHostDefaults(web => web.UseStartup<Startup>());
}
