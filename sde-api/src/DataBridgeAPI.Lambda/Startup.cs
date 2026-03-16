using System.Diagnostics.CodeAnalysis;
using Amazon.CloudWatchLogs;
using Amazon.S3;
using Amazon.StepFunctions;
using DataBridgeAPI.Lambda.Services;
using Microsoft.OpenApi.Models;

namespace DataBridgeAPI.Lambda;

[ExcludeFromCodeCoverage]
public class Startup
{
    public Startup(IConfiguration configuration) =>
        Configuration = configuration;

    public IConfiguration Configuration { get; }

    // ── Service registration ────────────────────────────────────────────────
    public void ConfigureServices(IServiceCollection services)
    {
        services.AddControllers();

        // CORS — allow all origins/methods/headers to match the API Gateway CORS configuration
        services.AddCors(options =>
        {
            options.AddDefaultPolicy(policy =>
                policy.AllowAnyOrigin()
                      .AllowAnyMethod()
                      .AllowAnyHeader());
        });

        // Swagger / OpenAPI
        services.AddSwaggerGen(c =>
        {
            c.SwaggerDoc("v1", new OpenApiInfo
            {
                Title = "DataBridge API",
                Version = "v1",
                Description = "Triggers and monitors AWS Step Functions executions (Extraction + Ingestion pipelines)."
            });

            // Include XML doc comments so Swagger shows <summary> / <remarks> from controllers & models
            var xmlFile = $"{System.Reflection.Assembly.GetExecutingAssembly().GetName().Name}.xml";
            var xmlPath = Path.Combine(AppContext.BaseDirectory, xmlFile);
            if (File.Exists(xmlPath))
                c.IncludeXmlComments(xmlPath, includeControllerXmlComments: true);
        });

        // AWS Step Functions client — region resolved from environment / IAM role
        services.AddSingleton<IAmazonStepFunctions, AmazonStepFunctionsClient>();

        // AWS S3 client — used to fetch per-run progress.json from the databridge bucket
        services.AddSingleton<IAmazonS3, AmazonS3Client>();

        // AWS CloudWatch Logs client — used as fallback source for record counters
        services.AddSingleton<IAmazonCloudWatchLogs, AmazonCloudWatchLogsClient>();

        // Step Functions service (reads STATE_MACHINE_ARN from env at runtime)
        services.AddScoped<IStepFunctionsService, StepFunctionsService>();
    }

    // ── Middleware pipeline ────────────────────────────────────────────────
    public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
    {
        if (env.IsDevelopment())
        {
            app.UseDeveloperExceptionPage();
        }

        app.UseSwagger();
        app.UseSwaggerUI(c =>
        {
            // Use a relative path so SwaggerUI works behind the API Gateway stage prefix.
            // Absolute "/swagger/v1/swagger.json" would lose the stage segment in the URL.
            c.SwaggerEndpoint("./v1/swagger.json", "DataBridge API v1");
            c.RoutePrefix = "swagger";
        });

        app.UseRouting();
        app.UseCors();          // must come after UseRouting and before UseAuthorization
        app.UseAuthorization();
        app.UseEndpoints(endpoints => endpoints.MapControllers());
    }
}
