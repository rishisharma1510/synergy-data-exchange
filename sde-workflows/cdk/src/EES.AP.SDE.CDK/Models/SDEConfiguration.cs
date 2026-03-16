using Verisk.EES.CDK.Common.Helpers;
using Verisk.EES.CDK.Common.Models;
using Verisk.EES.CDK.Common.Stacks;

namespace EES.AP.SDE.CDK.Models;

/// <summary>
/// Configuration for Synergy Data Exchange CDK stacks
/// </summary>
public class SDEConfiguration : IStackConfiguration
{
    /// <summary>
    /// ECR repository configuration
    /// </summary>
    public EcrConfiguration Ecr { get; set; } = new();

    /// <summary>
    /// Fargate configuration for PATH 1 and PATH 2
    /// </summary>
    public FargateConfiguration Fargate { get; set; } = new();

    /// <summary>
    /// AWS Batch configuration for PATH 3
    /// </summary>
    public BatchConfiguration Batch { get; set; } = new();

    /// <summary>
    /// Step Functions configuration
    /// </summary>
    public StepFunctionsConfiguration StepFunctions { get; set; } = new();

    /// <summary>
    /// VPC configuration
    /// </summary>
    public VpcConfiguration Vpc { get; set; } = new();

    /// <summary>
    /// API configuration for SID and Exposure services
    /// </summary>
    public ApiConfiguration Api { get; set; } = new();

    /// <summary>
    /// SPLA usage tracking and cost monitoring configuration
    /// </summary>
    public MonitoringConfiguration Monitoring { get; set; } = new();

    /// <summary>
    /// Replace tokens and variables in configuration values
    /// </summary>
    public void ReplaceTokensAndVariable(Dictionary<string, string> tokens)
    {
        Ecr?.ReplaceTokensAndVariable(tokens);
        Fargate?.ReplaceTokensAndVariable(tokens);
        Batch?.ReplaceTokensAndVariable(tokens);
        StepFunctions?.ReplaceTokensAndVariable(tokens);
        Vpc?.ReplaceTokensAndVariable(tokens);
        Api?.ReplaceTokensAndVariable(tokens);
        Monitoring?.ReplaceTokensAndVariable(tokens);
    }
}

public class EcrConfiguration
{
    /// <summary>
    /// ECR repository name for Express image (PATH 1)
    /// Will be prefixed with ss-cdk{stage}- at deployment
    /// </summary>
    public string ExpressRepositoryName { get; set; } = "sde-express";

    /// <summary>
    /// ECR repository name for Standard image (PATH 2)
    /// Will be prefixed with ss-cdk{stage}- at deployment
    /// </summary>
    public string StandardRepositoryName { get; set; } = "sde-standard";

    /// <summary>
    /// ECR repository name for Batch image (PATH 3)
    /// Will be prefixed with ss-cdk{stage}- at deployment
    /// </summary>
    public string BatchRepositoryName { get; set; } = "sde-batch";

    /// <summary>
    /// Image tag to deploy
    /// </summary>
    public string ImageTag { get; set; } = "latest";

    /// <summary>
    /// Gets the full repository name with environment prefix
    /// </summary>
    public string GetExpressRepositoryName(string stage) => $"ss-cdk{stage}-{ExpressRepositoryName}";
    
    /// <summary>
    /// Gets the full repository name with environment prefix
    /// </summary>
    public string GetStandardRepositoryName(string stage) => $"ss-cdk{stage}-{StandardRepositoryName}";
    
    /// <summary>
    /// Gets the full repository name with environment prefix
    /// </summary>
    public string GetBatchRepositoryName(string stage) => $"ss-cdk{stage}-{BatchRepositoryName}";

    public void ReplaceTokensAndVariable(Dictionary<string, string> tokens)
    {
        ExpressRepositoryName = Utility.ReplaceToken(ExpressRepositoryName, tokens);
        StandardRepositoryName = Utility.ReplaceToken(StandardRepositoryName, tokens);
        BatchRepositoryName = Utility.ReplaceToken(BatchRepositoryName, tokens);
        ImageTag = Utility.ReplaceToken(ImageTag, tokens);
    }
}

public class FargateConfiguration
{
    /// <summary>
    /// ECS Cluster name
    /// </summary>
    public string ClusterName { get; set; } = "sde-cluster";

    /// <summary>
    /// PATH 1: Express task CPU (1024 = 1 vCPU)
    /// </summary>
    public int ExpressCpu { get; set; } = 4096; // 4 vCPU

    /// <summary>
    /// PATH 1: Express task memory in MB
    /// </summary>
    public int ExpressMemory { get; set; } = 30720; // 30 GiB

    /// <summary>
    /// PATH 2: Standard task CPU (1024 = 1 vCPU)
    /// </summary>
    public int StandardCpu { get; set; } = 8192; // 8 vCPU

    /// <summary>
    /// PATH 2: Standard task memory in MB
    /// </summary>
    public int StandardMemory { get; set; } = 61440; // 60 GiB

    /// <summary>
    /// Parallel table extraction workers for PATH 1 (Express) Fargate tasks.
    /// Set via EXPRESS_MAX_PARALLEL_TABLES GitHub env var.
    /// </summary>
    public int ExpressMaxParallelTables { get; set; } = 4;

    /// <summary>
    /// Parallel table extraction workers for PATH 2 (Standard) Fargate tasks.
    /// Set via STANDARD_MAX_PARALLEL_TABLES GitHub env var.
    /// </summary>
    public int StandardMaxParallelTables { get; set; } = 8;

    /// <summary>
    /// SQL Server edition for PATH 2 (Standard Fargate) and PATH 3 (Batch) containers.
    /// Sets the MSSQL_PID environment variable on the container at deploy time.
    /// Allowed values: Standard | Developer
    ///   Standard  — SQL Server Standard edition (SPLA-liable). Use in PRODUCTION only.
    ///   Developer — SQL Server Developer edition (free, full-featured, dev/test licensed).
    ///               Set SQL_EDITION=Developer in non-prod GitHub environments to eliminate
    ///               SPLA cost across 20-30 non-prod deployments.
    /// PATH 1 (Express) is unaffected — always runs SQL Server Express (free).
    /// Default: Standard (production-safe default — must be explicitly overridden for non-prod).
    /// </summary>
    public string SqlEdition { get; set; } = "Standard";

    /// <summary>
    /// Ephemeral storage in GB (max 200 for Fargate)
    /// </summary>
    public int EphemeralStorageGiB { get; set; } = 200;

    /// <summary>
    /// Task timeout in minutes
    /// </summary>
    public int TaskTimeoutMinutes { get; set; } = 60;

    /// <summary>
    /// Task execution role configuration
    /// </summary>
    public RoleConfiguration ExecutionRoleConfig { get; set; } = new();

    /// <summary>
    /// Task role configuration
    /// </summary>
    public RoleConfiguration TaskRoleConfig { get; set; } = new();

    public void ReplaceTokensAndVariable(Dictionary<string, string> tokens)
    {
        ClusterName = Utility.ReplaceToken(ClusterName, tokens);
        SqlEdition = Utility.ReplaceToken(SqlEdition, tokens);
        ExecutionRoleConfig?.ReplaceTokensAndVariable(tokens);
        TaskRoleConfig?.ReplaceTokensAndVariable(tokens);
    }
}

public class BatchConfiguration
{
    /// <summary>
    /// Compute environment name
    /// </summary>
    public string ComputeEnvironmentName { get; set; } = "sde-batch-ce";

    /// <summary>
    /// Job queue name
    /// </summary>
    public string JobQueueName { get; set; } = "sde-batch-queue";

    /// <summary>
    /// Job definition name
    /// </summary>
    public string JobDefinitionName { get; set; } = "sde-batch-job";

    /// <summary>
    /// EC2 instance types for batch
    /// </summary>
    public string[] InstanceTypes { get; set; } = new[] { "r5.4xlarge", "r5.8xlarge", "r5.16xlarge" };

    /// <summary>
    /// Maximum vCPUs for compute environment
    /// </summary>
    public int MaxVcpus { get; set; } = 1024;

    /// <summary>
    /// Minimum vCPUs for compute environment
    /// </summary>
    public int MinVcpus { get; set; } = 0;

    /// <summary>
    /// vCPUs per job
    /// </summary>
    public int JobVcpus { get; set; } = 16;

    /// <summary>
    /// Memory per job in MB
    /// </summary>
    public int JobMemory { get; set; } = 65536; // 64 GB

    /// <summary>
    /// Parallel table extraction workers per Batch job.
    /// Matches JobVcpus by default (1 worker per vCPU). Set via BATCH_MAX_PARALLEL_TABLES GitHub env var.
    /// </summary>
    public int JobMaxParallelTables { get; set; } = 16;

    /// <summary>
    /// SQL Server edition for PATH 3 (Batch) containers.
    /// Must match FargateConfiguration.SqlEdition — both are controlled by the single
    /// SQL_EDITION GitHub environment variable in common.deploy.yml.
    /// Allowed values: Standard | Developer (see FargateConfiguration.SqlEdition for details).
    /// </summary>
    public string SqlEdition { get; set; } = "Standard";

    /// <summary>
    /// EBS volume size in GB
    /// </summary>
    public int EbsVolumeSizeGiB { get; set; } = 500;

    /// <summary>
    /// EC2 instance role configuration
    /// </summary>
    public RoleConfiguration InstanceRoleConfig { get; set; } = new();

    /// <summary>
    /// Batch job role configuration
    /// </summary>
    public RoleConfiguration JobRoleConfig { get; set; } = new();

    /// <summary>
    /// ECS Task execution role configuration
    /// </summary>
    public RoleConfiguration ExecutionRoleConfig { get; set; } = new();

    public void ReplaceTokensAndVariable(Dictionary<string, string> tokens)
    {
        ComputeEnvironmentName = Utility.ReplaceToken(ComputeEnvironmentName, tokens);
        JobQueueName = Utility.ReplaceToken(JobQueueName, tokens);
        JobDefinitionName = Utility.ReplaceToken(JobDefinitionName, tokens);
        SqlEdition = Utility.ReplaceToken(SqlEdition, tokens);
        InstanceRoleConfig?.ReplaceTokensAndVariable(tokens);
        JobRoleConfig?.ReplaceTokensAndVariable(tokens);
        ExecutionRoleConfig?.ReplaceTokensAndVariable(tokens);
    }
}

public class StepFunctionsConfiguration
{
    /// <summary>
    /// Extraction state machine name
    /// </summary>
    public string ExtractionStateMachineName { get; set; } = "sde-extraction";

    /// <summary>
    /// Ingestion state machine name
    /// </summary>
    public string IngestionStateMachineName { get; set; } = "sde-ingestion";

    /// <summary>
    /// Threshold for switching from PATH 1 to PATH 2 (GB)
    /// </summary>
    public int ThresholdSmallGb { get; set; } = 9;

    /// <summary>
    /// Threshold for switching from PATH 2 to PATH 3 (GB)
    /// </summary>
    public int ThresholdLargeGb { get; set; } = 80;

    /// <summary>
    /// State machine execution role configuration
    /// </summary>
    public RoleConfiguration StateMachineRoleConfig { get; set; } = new();

    /// <summary>
    /// Size estimator Lambda role configuration
    /// </summary>
    public RoleConfiguration LambdaRoleConfig { get; set; } = new();

    /// <summary>
    /// Lambda layer ARNs to attach (Dynatrace OneAgent Python layer).
    /// Leave empty / omit to skip layer attachment.
    /// </summary>
    public List<string> LayerVersionArns { get; set; } = new();

    /// <summary>
    /// Observability environment variables merged into the Lambda function at deploy time.
    /// Typically contains DT_* Dynatrace variables and AWS_LAMBDA_EXEC_WRAPPER.
    /// </summary>
    public Dictionary<string, string> LambdaEnvironmentVariables { get; set; } = new();

    public void ReplaceTokensAndVariable(Dictionary<string, string> tokens)
    {
        ExtractionStateMachineName = Utility.ReplaceToken(ExtractionStateMachineName, tokens);
        IngestionStateMachineName = Utility.ReplaceToken(IngestionStateMachineName, tokens);
        StateMachineRoleConfig?.ReplaceTokensAndVariable(tokens);
        LambdaRoleConfig?.ReplaceTokensAndVariable(tokens);
        LayerVersionArns = Utility.ReplaceToken(LayerVersionArns, tokens);
        LambdaEnvironmentVariables = Utility.ReplaceToken(LambdaEnvironmentVariables, tokens);
    }
}

public class VpcConfiguration
{
    /// <summary>
    /// VPC ID to use (if blank, creates new VPC)
    /// </summary>
    public string VpcId { get; set; } = "";

    /// <summary>
    /// Comma-separated subnet IDs for tasks (if blank, uses VPC defaults)
    /// </summary>
    public string SubnetIds { get; set; } = "";

    /// <summary>
    /// Comma-separated security group IDs (if blank, creates new)
    /// </summary>
    public string SecurityGroupIds { get; set; } = "";

    public void ReplaceTokensAndVariable(Dictionary<string, string> tokens)
    {
        VpcId = Utility.ReplaceToken(VpcId, tokens);
        SubnetIds = Utility.ReplaceToken(SubnetIds, tokens);
        SecurityGroupIds = Utility.ReplaceToken(SecurityGroupIds, tokens);
    }
}

/// <summary>
/// API configuration for Tenant Context access
/// Note: SID/Exposure/Iceberg/Glue info comes from TenantContext at runtime
/// </summary>
public class ApiConfiguration
{
    /// <summary>
    /// Tenant Context Lambda function name for cross-account credentials
    /// </summary>
    public string TenantContextLambda { get; set; } = "";

    /// <summary>
    /// App token secret name in Secrets Manager for Tenant Context Lambda authentication
    /// </summary>
    public string AppTokenSecret { get; set; } = "";

    /// <summary>
    /// Full Okta token endpoint URL for OAuth2 client-credentials grant.
    /// Constructed in the deploy pipeline as Issuer + "/v1/token" — update there if the path changes.
    /// </summary>
    public string IdentityProviderTokenEndpoint { get; set; } = "";

    /// <summary>
    /// Full URL for Exposure API (e.g. https://dev-na.synergystudio.verisk.com/api/exposure/v1).
    /// Constructed in the deploy pipeline as BASE_URL + path — update the path version there.
    /// </summary>
    public string ExposureApiUrl { get; set; } = "";

    /// <summary>
    /// Full URL for SID Control API (e.g. https://dev-na.synergystudio.verisk.com/api/sid/v1).
    /// Constructed in the deploy pipeline as BASE_URL + path — update the path version there.
    /// </summary>
    public string SidApiUrl { get; set; } = "";

    public void ReplaceTokensAndVariable(Dictionary<string, string> tokens)
    {
        TenantContextLambda = Utility.ReplaceToken(TenantContextLambda, tokens);
        AppTokenSecret = Utility.ReplaceToken(AppTokenSecret, tokens);
        IdentityProviderTokenEndpoint = Utility.ReplaceToken(IdentityProviderTokenEndpoint, tokens);
        ExposureApiUrl = Utility.ReplaceToken(ExposureApiUrl, tokens);
        SidApiUrl = Utility.ReplaceToken(SidApiUrl, tokens);
    }
}

/// <summary>
/// Configuration for SPLA usage tracking and cost monitoring.
/// Controls the DynamoDB table, S3 report destination, alerting thresholds, and notification targets.
/// </summary>
public class MonitoringConfiguration
{
    /// <summary>
    /// S3 bucket that holds the monthly SPLA usage reports.
    /// This bucket must already exist; Synergy Data Exchange does not create it.
    /// Format: ss-cdk{stage}-metrics-shared  (the {stage} token is replaced at deploy time)
    /// </summary>
    public string ReportBucket { get; set; } = "ss-cdk{stage}-metrics-shared";

    /// <summary>
    /// S3 key prefix within ReportBucket for SPLA reports.
    /// The monthly reporter appends {YYYY-MM}.json to this prefix.
    /// Format: {stage}/usagebyservice/SQL_SPLA
    /// </summary>
    public string ReportPrefix { get; set; } = "{stage}/usagebyservice/SQL_SPLA";

    /// <summary>
    /// CloudWatch alarm threshold — warning level (cores).
    /// Default 80 cores = 5 simultaneous PATH 2/3 jobs.
    /// </summary>
    public int ConcurrentCoresWarningThreshold { get; set; } = 80;

    /// <summary>
    /// CloudWatch alarm threshold — critical level (cores).
    /// Default 160 cores = 10 simultaneous PATH 2/3 jobs — escalate immediately.
    /// </summary>
    public int ConcurrentCoresCriticalThreshold { get; set; } = 160;

    /// <summary>
    /// Optional e-mail address to pre-subscribe to the SPLA alerts SNS topic.
    /// Leave empty to skip — you can subscribe manually via the AWS Console.
    /// </summary>
    public string AlertEmailAddress { get; set; } = "";

    /// <summary>IAM role for the SPLA tracker Lambda.</summary>
    public RoleConfiguration TrackerRoleConfig { get; set; } = new();

    /// <summary>IAM role for the monthly SPLA reporter Lambda.</summary>
    public RoleConfiguration ReporterRoleConfig { get; set; } = new();

    public void ReplaceTokensAndVariable(Dictionary<string, string> tokens)
    {
        ReportBucket       = Utility.ReplaceToken(ReportBucket, tokens);
        ReportPrefix       = Utility.ReplaceToken(ReportPrefix, tokens);
        AlertEmailAddress  = Utility.ReplaceToken(AlertEmailAddress, tokens);
        TrackerRoleConfig?.ReplaceTokensAndVariable(tokens);
        ReporterRoleConfig?.ReplaceTokensAndVariable(tokens);
    }
}
