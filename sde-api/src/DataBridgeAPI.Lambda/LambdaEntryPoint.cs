using System.Diagnostics.CodeAnalysis;

namespace DataBridgeAPI.Lambda;

/// <summary>
/// Lambda entry point for API Gateway REST API (v1) proxy integration.
///
/// Handler string (set this in CDK / aws-lambda-tools-defaults.json):
///   DataBridgeAPI.Lambda::DataBridgeAPI.Lambda.LambdaEntryPoint::FunctionHandlerAsync
/// </summary>
[ExcludeFromCodeCoverage]
public class LambdaEntryPoint : Amazon.Lambda.AspNetCoreServer.APIGatewayProxyFunction
{
    /// <summary>
    /// Configure the ASP.NET Core host.  Only Startup wiring belongs here.
    /// </summary>
    protected override void Init(IWebHostBuilder builder) =>
        builder.UseStartup<Startup>();

    /// <summary>
    /// Optional: configure the generic IHostBuilder (e.g. extra logging providers).
    /// </summary>
    protected override void Init(IHostBuilder builder) { }
}
