using Amazon.CDK;
using Amazon.CDK.AWS.EC2;
using Amazon.CDK.AWS.ECR;
using Amazon.CDK.AWS.ECS;
using Amazon.CDK.AWS.IAM;
using Amazon.CDK.AWS.Logs;
using SecretsManager = Amazon.CDK.AWS.SecretsManager;
using Constructs;
using EES.AP.SDE.CDK.Models;
using EES.AWSCDK.Common;
using EES.AWSCDK.Common.Extensions;
using Verisk.EES.CDK.Common.Helpers;
using EES.AWSCDK.Observability;

namespace EES.AP.SDE.CDK.Stacks;

/// <summary>
/// Fargate Stack - Creates ECS Fargate infrastructure for PATH 1 (Express) and PATH 2 (Standard)
/// </summary>
public class SDEFargateStack : Stack
{
    public const string STACK_NAME = "SDEFargateStack";

    public ICluster Cluster { get; }
    public FargateTaskDefinition ExpressTaskDefinition { get; }
    public FargateTaskDefinition StandardTaskDefinition { get; }

    public SDEFargateStack(Construct scope, string id, StackProperties<SDEConfiguration> props)
        : base(scope, id, new StackProps
        {
            Env = new Amazon.CDK.Environment
            {
                Account = props.AccountId,
                Region = props.Region
            },
            Description = "Synergy Data Exchange Pipeline - Fargate infrastructure for Express and Standard paths"
        })
    {
        var fargateSettings = props.StackConfigurationSettings?.Fargate ?? new FargateConfiguration();
        var ecrSettings = props.StackConfigurationSettings?.Ecr ?? new EcrConfiguration();
        var vpcSettings = props.StackConfigurationSettings?.Vpc ?? new VpcConfiguration();
        var apiSettings = props.StackConfigurationSettings?.Api ?? new ApiConfiguration();
        var sfnSettings = props.StackConfigurationSettings?.StepFunctions ?? new StepFunctionsConfiguration();
        var stage = props.InstanceStage ?? "dev";

        // SQL edition for PATH 2 (Standard) — "Standard" (SPLA) or "Developer" (free, non-prod only).
        // Controlled by SQL_EDITION GitHub environment variable via
        // SDE_CDK_Settings__Fargate__SqlEdition. PATH 1 is always Express (free).
        var standardSqlEdition = fargateSettings.SqlEdition;
        if (!string.Equals(standardSqlEdition, "Standard", StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(standardSqlEdition, "Developer", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine($"WARNING: Unsupported SqlEdition '{standardSqlEdition}'. Falling back to 'Standard'.");
            standardSqlEdition = "Standard";
        }
        Console.WriteLine($"SDEFargateStack: PATH 2 SQL edition = {standardSqlEdition} ({(string.Equals(standardSqlEdition, "Developer", StringComparison.OrdinalIgnoreCase) ? "FREE — no SPLA" : "SPLA-liable")})");

        // MAX_WORKERS: explicit per-path defaults, overridable via GitHub env vars
        int expressMaxWorkers  = fargateSettings.ExpressMaxParallelTables;
        int standardMaxWorkers = fargateSettings.StandardMaxParallelTables;

        // Look up or create VPC
        IVpc vpc;
        if (!string.IsNullOrEmpty(vpcSettings.VpcId))
        {
            vpc = Vpc.FromLookup(this, "Vpc", new VpcLookupOptions
            {
                VpcId = vpcSettings.VpcId
            });
        }
        else
        {
            vpc = Vpc.FromLookup(this, "Vpc", new VpcLookupOptions
            {
                IsDefault = true
            });
        }

        // Create ECS Cluster with environment-prefixed name
        #pragma warning disable CS0618 // ContainerInsights is obsolete but ContainerInsightsV2 not available in current CDK version
        Cluster = new Cluster(this, "Synergy Data ExchangeCluster", new ClusterProps
        {
            ClusterName = $"ss-cdk{stage}-{fargateSettings.ClusterName}",
            Vpc = vpc,
            ContainerInsights = true
        });
        #pragma warning restore CS0618

        // Get security groups from configuration or create new
        ISecurityGroup[] taskSecurityGroups;
        if (!string.IsNullOrEmpty(vpcSettings.SecurityGroupIds))
        {
            taskSecurityGroups = this.GetSecurityGroups(vpcSettings.SecurityGroupIds, "FargateSg");
        }
        else
        {
            // Create security group for Fargate tasks
            var taskSecurityGroup = new SecurityGroup(this, "TaskSecurityGroup", new SecurityGroupProps
            {
                Vpc = vpc,
                Description = "Security group for Synergy Data Exchange Fargate tasks",
                AllowAllOutbound = true
            });
            taskSecurityGroups = new ISecurityGroup[] { taskSecurityGroup };
        }

        // Get subnets from configuration or use VPC defaults
        ISubnet[]? taskSubnets = null;
        if (!string.IsNullOrEmpty(vpcSettings.SubnetIds))
        {
            taskSubnets = this.GetSubnets(vpcSettings.SubnetIds, "FargateSubnet");
        }

        // Reference existing ECR repositories with environment prefix
        var expressRepo = Repository.FromRepositoryName(this, "ExpressRepo", ecrSettings.GetExpressRepositoryName(stage));
        var standardRepo = Repository.FromRepositoryName(this, "StandardRepo", ecrSettings.GetStandardRepositoryName(stage));

        // Create or import SA password secret for SQL Server authentication
        var saPasswordSecret = new SecretsManager.Secret(this, "SaPasswordSecret", new SecretsManager.SecretProps
        {
            SecretName = $"ss-cdk{stage}-sde-sa-password",
            Description = "SQL Server SA password for Synergy Data Exchange containers",
            GenerateSecretString = new SecretsManager.SecretStringGenerator
            {
                PasswordLength = 32,
                ExcludeCharacters = "\"@/\\",
                IncludeSpace = false,
                RequireEachIncludedType = true
            }
        });

        // Create IAM role for task execution using configuration
        var executionRole = Utility.CreateRole(this, "fargate-exec", fargateSettings.ExecutionRoleConfig, props.TokenReplacementDictionary);

        // Create IAM role for task using configuration
        var taskRole = Utility.CreateRole(this, "fargate-task", fargateSettings.TaskRoleConfig, props.TokenReplacementDictionary);

        // Create log groups
        var expressLogGroup = new LogGroup(this, "ExpressLogGroup", new LogGroupProps
        {
            LogGroupName = $"/ecs/ss-cdk{stage}-sde/express",
            Retention = RetentionDays.ONE_MONTH,
            RemovalPolicy = RemovalPolicy.DESTROY
        });

        var standardLogGroup = new LogGroup(this, "StandardLogGroup", new LogGroupProps
        {
            LogGroupName = $"/ecs/ss-cdk{stage}-sde/standard",
            Retention = RetentionDays.ONE_MONTH,
            RemovalPolicy = RemovalPolicy.DESTROY
        });

        // PATH 1: Express Task Definition (SQL Server Express - ≤ThresholdSmallGb, FREE)
        ExpressTaskDefinition = new FargateTaskDefinition(this, "ExpressTaskDefinition", new FargateTaskDefinitionProps
        {
            Family = $"ss-cdk{stage}-sde-express",
            Cpu = fargateSettings.ExpressCpu,
            MemoryLimitMiB = fargateSettings.ExpressMemory,
            ExecutionRole = executionRole,
            TaskRole = taskRole,
            EphemeralStorageGiB = fargateSettings.EphemeralStorageGiB,
            RuntimePlatform = new RuntimePlatform
            {
                CpuArchitecture = CpuArchitecture.X86_64,
                OperatingSystemFamily = OperatingSystemFamily.LINUX
            }
        });

        ExpressTaskDefinition.AddContainer("ExpressContainer", new ContainerDefinitionOptions
        {
            ContainerName = $"ss-cdk{stage}-sde-express",
            Image = ContainerImage.FromEcrRepository(expressRepo, ecrSettings.ImageTag),
            Essential = true,
            Logging = LogDrivers.AwsLogs(new AwsLogDriverProps
            {
                StreamPrefix = "express",
                LogGroup = expressLogGroup
            }),
            Environment = new Dictionary<string, string>
            {
                ["PATH_TYPE"] = "EXPRESS",
                ["SQL_EDITION"] = "express",
                ["MSSQL_PID"] = "Express",
                ["MAX_WORKERS"] = expressMaxWorkers.ToString(),
                ["MAX_DATABASE_SIZE_GB"] = sfnSettings.ThresholdSmallGb.ToString(),
                ["TENANT_CONTEXT_LAMBDA"] = apiSettings.TenantContextLambda,
                ["APP_TOKEN_SECRET"] = apiSettings.AppTokenSecret,
                ["IDENTITY_PROVIDER_TOKEN_ENDPOINT"] = apiSettings.IdentityProviderTokenEndpoint,
                ["EXPOSURE_API_URL"] = apiSettings.ExposureApiUrl,
                ["SID_API_URL"] = apiSettings.SidApiUrl,
                ["AWS_REGION"] = props.Region ?? "us-east-1",
                ["SQLSERVER_USERNAME"] = "sa",
                ["AWS_RESPONSE_CHECKSUM_VALIDATION"] = "when_required",
                ["OTEL_SERVICE_NAME"] = $"sde-express-{stage}"
            },
            Secrets = new Dictionary<string, Secret>
            {
                ["SA_PASSWORD"] = Secret.FromSecretsManager(saPasswordSecret),
                ["SQLSERVER_PASSWORD"] = Secret.FromSecretsManager(saPasswordSecret)
            }
        });

        ExpressTaskDefinition.AddAdotSidecar(ExpressTaskDefinition, stage, executionRole);

        // PATH 2: Standard Task Definition (SQL Server Standard - 9-80GB)
        StandardTaskDefinition = new FargateTaskDefinition(this, "StandardTaskDefinition", new FargateTaskDefinitionProps
        {
            Family = $"ss-cdk{stage}-sde-standard",
            Cpu = fargateSettings.StandardCpu,
            MemoryLimitMiB = fargateSettings.StandardMemory,
            ExecutionRole = executionRole,
            TaskRole = taskRole,
            EphemeralStorageGiB = fargateSettings.EphemeralStorageGiB,
            RuntimePlatform = new RuntimePlatform
            {
                CpuArchitecture = CpuArchitecture.X86_64,
                OperatingSystemFamily = OperatingSystemFamily.LINUX
            }
        });

        StandardTaskDefinition.AddContainer("StandardContainer", new ContainerDefinitionOptions
        {
            ContainerName = $"ss-cdk{stage}-sde-standard",
            Image = ContainerImage.FromEcrRepository(standardRepo, ecrSettings.ImageTag),
            Essential = true,
            Logging = LogDrivers.AwsLogs(new AwsLogDriverProps
            {
                StreamPrefix = "standard",
                LogGroup = standardLogGroup
            }),
            Environment = new Dictionary<string, string>
            {
                ["PATH_TYPE"] = "STANDARD",
                ["SQL_EDITION"] = standardSqlEdition.ToLowerInvariant(),
                ["MSSQL_PID"] = standardSqlEdition,       // Overrides the Dockerfile ENV at container start
                ["MAX_WORKERS"] = standardMaxWorkers.ToString(),
                ["MAX_DATABASE_SIZE_GB"] = sfnSettings.ThresholdLargeGb.ToString(),
                ["TENANT_CONTEXT_LAMBDA"] = apiSettings.TenantContextLambda,
                ["APP_TOKEN_SECRET"] = apiSettings.AppTokenSecret,
                ["IDENTITY_PROVIDER_TOKEN_ENDPOINT"] = apiSettings.IdentityProviderTokenEndpoint,
                ["EXPOSURE_API_URL"] = apiSettings.ExposureApiUrl,
                ["SID_API_URL"] = apiSettings.SidApiUrl,
                ["AWS_REGION"] = props.Region ?? "us-east-1",
                ["SQLSERVER_USERNAME"] = "sa",
                ["AWS_RESPONSE_CHECKSUM_VALIDATION"] = "when_required",
                ["OTEL_SERVICE_NAME"] = $"sde-standard-{stage}"
            },
            Secrets = new Dictionary<string, Secret>
            {
                ["SA_PASSWORD"] = Secret.FromSecretsManager(saPasswordSecret),
                ["SQLSERVER_PASSWORD"] = Secret.FromSecretsManager(saPasswordSecret)
            }
        });

        StandardTaskDefinition.AddAdotSidecar(StandardTaskDefinition, stage, executionRole);

        // Outputs
        _ = new CfnOutput(this, "ClusterArn", new CfnOutputProps
        {
            Value = Cluster.ClusterArn,
            Description = "ARN of ECS Cluster",
            ExportName = $"{id}-ClusterArn"
        });

        // Task definition ARNs are kept as exports for visibility, but StepFunctions stack
        // no longer imports them via Fn::ImportValue (it constructs the ARN from the stable
        // task family name to avoid "cannot update export in use" CloudFormation errors).
        _ = new CfnOutput(this, "ExpressTaskDefinitionArn", new CfnOutputProps
        {
            Value = ExpressTaskDefinition.TaskDefinitionArn,
            Description = "ARN of Express Task Definition (PATH 1)",
            ExportName = $"{id}-ExpressTaskDefinitionArn"
        });

        _ = new CfnOutput(this, "StandardTaskDefinitionArn", new CfnOutputProps
        {
            Value = StandardTaskDefinition.TaskDefinitionArn,
            Description = "ARN of Standard Task Definition (PATH 2)",
            ExportName = $"{id}-StandardTaskDefinitionArn"
        });

        _ = new CfnOutput(this, "SecurityGroupId", new CfnOutputProps
        {
            Value = string.Join(",", taskSecurityGroups.Select(sg => sg.SecurityGroupId)),
            Description = "Security Group ID(s) for Fargate tasks",
            ExportName = $"{id}-SecurityGroupId"
        });
    }
}
