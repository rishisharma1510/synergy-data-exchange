using System.IO;
using Amazon.CDK;
using Amazon.CDK.AWS.APIGateway;
using Amazon.CDK.AWS.IAM;
using Amazon.CDK.AWS.Lambda;
using Amazon.CDK.AWS.Logs;
using Amazon.CDK.AWS.StepFunctions;
using Constructs;

namespace DataBridgeAPI.Cdk;

/// <summary>
/// DataBridge API Stack
///
/// Resources created (no IAM role creation  all roles are pre-existing):
///   - Lambda function (.NET 8 / ASP.NET Core) using ss-cdkdev-databridge-size-estimator-role
///   - LambdaRestApi (API Gateway v1 + Lambda proxy)
///   - CloudWatch Log Group for API GW access logs
///   - CloudWatch Log Group for Lambda (manual  avoids LogRetention auto-Lambda)
///
/// Pre-requisites (one-time setup outside CDK):
///   - Step Functions and X-Ray permissions already added to the Lambda role
///
/// Deploy workflow:
///   1. dotnet publish src/DataBridgeAPI.Lambda -c Release -r linux-x64 --self-contained false -o src/DataBridgeAPI.Lambda/publish
///   2. cdk deploy  (from the cdk/ directory)
/// </summary>
public class DataBridgeApiStack : Stack
{
    private const string ExtractionArn =
        "arn:aws:states:us-east-1:451952076009:stateMachine:ss-cdkdev-sde-extraction";

    private const string IngestionArn =
        "arn:aws:states:us-east-1:451952076009:stateMachine:ss-cdkdev-sde-ingestion";

    // Pre-existing Lambda execution role  has AWSLambdaBasicExecutionRole + DataBridgeApiPermissions
    private const string LambdaRoleName = "ss-cdkdev-sde-size-estimator-role";

    public DataBridgeApiStack(Construct scope, string id, IStackProps? props = null)
        : base(scope, id, props)
    {
        // 1. Import existing Step Function state machines (already exist in account)
        _ = StateMachine.FromStateMachineArn(this, "ExtractionStateMachine", ExtractionArn);
        _ = StateMachine.FromStateMachineArn(this, "IngestionStateMachine", IngestionArn);

        // 2. Import the pre-existing Lambda execution role (role creation is blocked by VA-PB-Standard)
        var lambdaRole = Role.FromRoleName(this, "LambdaExecutionRole", LambdaRoleName);

        // NOTE: iam:PutRolePolicy is blocked by VA-PB-Standard so this policy cannot be updated via CDK/CloudFormation.
        // The log group ARNs below are intentionally kept at their original values to prevent CloudFormation from
        // attempting an update. To grant access to the new log groups (ss-cdkdev-sde-*), update the role policy manually.
        lambdaRole.AddToPrincipalPolicy(new PolicyStatement(new PolicyStatementProps
        {
            Actions = new[]
            {
                "logs:FilterLogEvents",
                "logs:GetLogEvents",
                "logs:DescribeLogGroups",
                "logs:DescribeLogStreams"
            },
            Resources = new[]
            {
                "arn:aws:logs:us-east-1:451952076009:log-group:/aws/vendedlogs/states/ss-cdkdev-databridge-extraction",
                "arn:aws:logs:us-east-1:451952076009:log-group:/aws/vendedlogs/states/ss-cdkdev-databridge-extraction:*",
                "arn:aws:logs:us-east-1:451952076009:log-group:/aws/vendedlogs/states/ss-cdkdev-databridge-ingestion",
                "arn:aws:logs:us-east-1:451952076009:log-group:/aws/vendedlogs/states/ss-cdkdev-databridge-ingestion:*"
            }
        }));

        // 3. Lambda Function (.NET 8 / ASP.NET Core)
        // CWD when CDK runs (from cdk.json app command) is the cdk/ directory.
        // Pre-publish the Lambda before deploying:
        //   dotnet publish src/DataBridgeAPI.Lambda -c Release -r linux-x64 --self-contained false -o src/DataBridgeAPI.Lambda/publish
        var lambdaPublishPath = Path.GetFullPath(
            Path.Combine(Directory.GetCurrentDirectory(), "..", "src", "DataBridgeAPI.Lambda", "publish"));

        if (!Directory.Exists(lambdaPublishPath))
            throw new InvalidOperationException(
                $"Lambda publish output not found at '{lambdaPublishPath}'. " +
                "Run: dotnet publish src/DataBridgeAPI.Lambda -c Release -r linux-x64 --self-contained false -o src/DataBridgeAPI.Lambda/publish");

        var lambdaFunction = new Function(this, "DataBridgeApiLambda", new FunctionProps
        {
            FunctionName = "DataBridgeApiFunction",
            Description = "ASP.NET Core Lambda - MVC controllers route all API Gateway requests",
            Runtime = Runtime.DOTNET_8,
            Handler = "DataBridgeAPI.Lambda::DataBridgeAPI.Lambda.LambdaEntryPoint::FunctionHandlerAsync",
            Role = lambdaRole,
            MemorySize = 512,
            Timeout = Duration.Seconds(30),
            Tracing = Tracing.ACTIVE,
            // LogRetention is intentionally omitted  it would auto-create a Lambda + IAM role.
            // The Lambda log group /aws/lambda/DataBridgeApiFunction is auto-created by Lambda on first invocation.
            Environment = new Dictionary<string, string>
            {
                ["EXTRACTION_STATE_MACHINE_ARN"] = ExtractionArn,
                ["INGESTION_STATE_MACHINE_ARN"]  = IngestionArn,
                ["EXTRACTION_STEPFUNCTIONS_LOG_GROUP"] = "/aws/vendedlogs/states/ss-cdkdev-sde-extraction",
                ["INGESTION_STEPFUNCTIONS_LOG_GROUP"]  = "/aws/vendedlogs/states/ss-cdkdev-sde-ingestion",
                ["ASPNETCORE_ENVIRONMENT"] = "Production",
                ["DOTNET_SYSTEM_GLOBALIZATION_INVARIANT"] = "1"
            },
            Code = Code.FromAsset(lambdaPublishPath)
        });

        // 4. API Gateway access log group
        var apiLogGroup = new LogGroup(this, "ApiGatewayAccessLogGroup", new LogGroupProps
        {
            LogGroupName = "/aws/apigateway/DataBridgeAPI",
            RemovalPolicy = RemovalPolicy.DESTROY,
            Retention = RetentionDays.ONE_WEEK
        });

        // 5. LambdaRestApi - catch-all proxy, all routing handled inside Lambda by ASP.NET Core MVC
        //    CloudWatchRole = false  - avoids CDK auto-creating an API GW CloudWatch execution role
        //    (the @aws-cdk/aws-apigateway:disableCloudWatchRole feature flag in cdk.json also disables it globally)
        var api = new LambdaRestApi(this, "DataBridgeRestApi", new LambdaRestApiProps
        {
            RestApiName = "DataBridgeAPI",
            Description = "DataBridge REST API - routes handled by ASP.NET Core MVC controllers inside Lambda",
            Handler = lambdaFunction,
            Proxy = true,
            CloudWatchRole = false,
            DeployOptions = new StageOptions
            {
                StageName = "api",
                LoggingLevel = MethodLoggingLevel.OFF,   // OFF because no CloudWatch role
                MetricsEnabled = true,
                TracingEnabled = true,
                AccessLogDestination = new LogGroupLogDestination(apiLogGroup),
                AccessLogFormat = AccessLogFormat.JsonWithStandardFields(
                    new JsonWithStandardFieldProps
                    {
                        Caller = false,
                        HttpMethod = true,
                        Ip = true,
                        Protocol = true,
                        RequestTime = true,
                        ResourcePath = true,
                        ResponseLength = true,
                        Status = true,
                        User = true
                    })
            },
            DefaultCorsPreflightOptions = new CorsOptions
            {
                AllowOrigins = Cors.ALL_ORIGINS,
                AllowMethods = Cors.ALL_METHODS,
                AllowHeaders = new[]
                {
                    "Content-Type",
                    "X-Amz-Date",
                    "Authorization",
                    "X-Api-Key",
                    "X-Amz-Security-Token"
                }
            }
        });

        // 6. Stack Outputs
        _ = new CfnOutput(this, "ApiGatewayUrl", new CfnOutputProps
        {
            Value = api.Url,
            Description = "Base URL of the DataBridge REST API",
            ExportName = "DataBridgeApiUrl"
        });

        _ = new CfnOutput(this, "StartExecutionEndpoint", new CfnOutputProps
        {
            Value = $"{api.Url}api/executions",
            Description = "POST - start a Step Function execution (body: { transactionType, executionName?, input?, metadata? })",
            ExportName = "DataBridgeStartExecutionUrl"
        });

        _ = new CfnOutput(this, "GetExecutionStatusEndpoint", new CfnOutputProps
        {
            Value = $"{api.Url}api/executions/{{executionId}}?executionArn={{executionArn}}",
            Description = "GET - poll execution status, output, failure details, and full event log",
            ExportName = "DataBridgeGetExecutionUrl"
        });

        _ = new CfnOutput(this, "LambdaFunctionArn", new CfnOutputProps
        {
            Value = lambdaFunction.FunctionArn,
            Description = "Lambda Function ARN",
            ExportName = "DataBridgeLambdaArn"
        });
    }
}
