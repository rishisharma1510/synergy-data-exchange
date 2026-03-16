using Amazon.CDK;
using Amazon.CDK.AWS.EC2;
using Amazon.CDK.AWS.IAM;
using Amazon.CDK.AWS.Lambda;
using Amazon.CDK.AWS.Logs;
using Amazon.CDK.AWS.StepFunctions;
using Constructs;
using EES.AP.SDE.CDK.Models;
using EES.AWSCDK.Common;
using Verisk.EES.CDK.Common.Helpers;

namespace EES.AP.SDE.CDK.Stacks;

/// <summary>
/// Step Functions Stack - Creates state machines for orchestrating Synergy Data Exchange and extraction
/// Uses three-path architecture based on database size:
/// - PATH 1: ≤7GB → SQL Server Express (Fargate) - FREE
/// - PATH 2: 7-60GB → SQL Server Standard (Fargate) - SPLA Licensed
/// - PATH 3: >60GB → SQL Server Standard (Batch EC2) - SPLA Licensed
/// </summary>
public class SDEStepFunctionsStack : Stack
{
    public const string STACK_NAME = "SDEStepFunctionsStack";

    public StateMachine ExtractionStateMachine { get; }
    public StateMachine IngestionStateMachine { get; }

    public SDEStepFunctionsStack(Construct scope, string id, StackProperties<SDEConfiguration> props)
        : base(scope, id, new StackProps
        {
            Env = new Amazon.CDK.Environment
            {
                Account = props.AccountId,
                Region = props.Region
            },
            Description = "Synergy Data Exchange Pipeline - Step Functions state machines for orchestration"
        })
    {
        var sfnSettings = props.StackConfigurationSettings?.StepFunctions ?? new StepFunctionsConfiguration();
        var vpcSettings = props.StackConfigurationSettings?.Vpc ?? new VpcConfiguration();
        var stage = props.InstanceStage ?? "dev";
        var fargateStackName = $"ss-cdk-sde-fargate-{stage}";
        var batchStackName = $"ss-cdk-sde-batch-{stage}";

        // Import stable Fargate resources from CloudFormation exports
        var clusterArn = Fn.ImportValue($"{fargateStackName}-ClusterArn");
        var securityGroupId = Fn.ImportValue($"{fargateStackName}-SecurityGroupId");

        // Construct task definition ARNs from deterministic family names rather than importing
        // the per-revision ARN from CloudFormation exports. Task def ARNs include a revision suffix
        // (:N) that changes every deploy; importing changing exports blocks CloudFormation with
        // "cannot update export in use". Using a family-only ARN (no :N suffix) is stable and
        // tells ECS to run the latest active revision — which is always what we want.
        var expressTaskDefArn = $"arn:aws:ecs:{Region}:{Account}:task-definition/ss-cdk{stage}-sde-express";
        var standardTaskDefArn = $"arn:aws:ecs:{Region}:{Account}:task-definition/ss-cdk{stage}-sde-standard";

        // Import Batch resources from CloudFormation exports
        var jobQueueArn = Fn.ImportValue($"{batchStackName}-JobQueueArn");

        // Construct the job definition name from config rather than importing the per-revision ARN.
        // Batch job definition ARNs include a revision suffix (:N) that changes every deploy;
        // importing that changing export blocks CloudFormation with "cannot update export in use".
        // Passing just the name (no :N) tells Batch to run the latest active revision.
        var batchSettings = props.StackConfigurationSettings?.Batch ?? new BatchConfiguration();
        var jobDefinitionArn = $"ss-cdk{stage}-{batchSettings.JobDefinitionName}";

        // Container names must match the Fargate task definition container names
        var expressContainerName = $"ss-cdk{stage}-sde-express";
        var standardContainerName = $"ss-cdk{stage}-sde-standard";

        // Get subnet IDs as array
        var subnetIds = vpcSettings.SubnetIds?.Split(',').Select(s => s.Trim()).ToArray() ?? Array.Empty<string>();

        // Create log group for state machine execution logs with environment prefix
        var extractionLogGroup = new LogGroup(this, "ExtractionStateMachineLogGroup", new LogGroupProps
        {
            LogGroupName = $"/aws/stepfunctions/ss-cdk{stage}-{sfnSettings.ExtractionStateMachineName}",
            Retention = RetentionDays.ONE_MONTH,
            RemovalPolicy = RemovalPolicy.DESTROY
        });

        var ingestionLogGroup = new LogGroup(this, "IngestionStateMachineLogGroup", new LogGroupProps
        {
            LogGroupName = $"/aws/stepfunctions/ss-cdk{stage}-{sfnSettings.IngestionStateMachineName}",
            Retention = RetentionDays.ONE_MONTH,
            RemovalPolicy = RemovalPolicy.DESTROY
        });

        // Create IAM role for Step Functions using configuration
        var stateMachineRole = Utility.CreateRole(this, "sfn-role", sfnSettings.StateMachineRoleConfig, props.TokenReplacementDictionary);

        // Grant the state machine role permission to read ingestion summary JSON from any tenant S3 bucket.
        // Required for the ReadIngestionSummary aws-sdk:s3:getObject integration state.
        stateMachineRole.AddToPrincipalPolicy(new PolicyStatement(new PolicyStatementProps
        {
            Actions = new[] { "s3:GetObject" },
            Resources = new[] { "arn:aws:s3:::*/sde/runs/*/ingestion_summary.json" }
        }));

        // Build ECS network configuration for raw ASL
        var networkConfig = new EcsNetworkConfig(clusterArn, subnetIds, securityGroupId);

        // Create Lambda function for size estimation
        var sizeEstimatorLambda = CreateSizeEstimatorLambda(stage, sfnSettings, props.TokenReplacementDictionary);
        var sizeEstimatorLambdaArn = sizeEstimatorLambda.FunctionArn;

        // Create Extraction State Machine (Iceberg → MDF)
        var extractionDefinition = CreateExtractionDefinition(
            sfnSettings, networkConfig, expressTaskDefArn, standardTaskDefArn, jobQueueArn, jobDefinitionArn, expressContainerName, standardContainerName, sizeEstimatorLambdaArn);
        ExtractionStateMachine = new StateMachine(this, "ExtractionStateMachine", new StateMachineProps
        {
            StateMachineName = $"ss-cdk{stage}-{sfnSettings.ExtractionStateMachineName}",
            DefinitionBody = DefinitionBody.FromChainable(extractionDefinition),
            Role = stateMachineRole,
            TracingEnabled = true,
            Logs = new LogOptions
            {
                Destination = extractionLogGroup,
                Level = LogLevel.ALL,
                IncludeExecutionData = true
            }
        });

        // Create Ingestion State Machine (MDF → Iceberg)
        var ingestionDefinition = CreateIngestionDefinition(
            sfnSettings, networkConfig, expressTaskDefArn, standardTaskDefArn, jobQueueArn, jobDefinitionArn, expressContainerName, standardContainerName, sizeEstimatorLambdaArn);
        IngestionStateMachine = new StateMachine(this, "IngestionStateMachine", new StateMachineProps
        {
            StateMachineName = $"ss-cdk{stage}-{sfnSettings.IngestionStateMachineName}",
            DefinitionBody = DefinitionBody.FromChainable(ingestionDefinition),
            Role = stateMachineRole,
            TracingEnabled = true,
            Logs = new LogOptions
            {
                Destination = ingestionLogGroup,
                Level = LogLevel.ALL,
                IncludeExecutionData = true
            }
        });

        // Outputs
        _ = new CfnOutput(this, "ExtractionStateMachineArn", new CfnOutputProps
        {
            Value = ExtractionStateMachine.StateMachineArn,
            Description = "ARN of Extraction State Machine",
            ExportName = $"{id}-ExtractionStateMachineArn"
        });

        _ = new CfnOutput(this, "IngestionStateMachineArn", new CfnOutputProps
        {
            Value = IngestionStateMachine.StateMachineArn,
            Description = "ARN of Ingestion State Machine",
            ExportName = $"{id}-IngestionStateMachineArn"
        });
    }

    private record EcsNetworkConfig(string ClusterArn, string[] SubnetIds, string SecurityGroupId);

    private IChainable CreateExtractionDefinition(
        StepFunctionsConfiguration settings,
        EcsNetworkConfig networkConfig,
        string expressTaskDefArn,
        string standardTaskDefArn,
        string jobQueueArn,
        string jobDefinitionArn,
        string expressContainerName,
        string standardContainerName,
        string sizeEstimatorLambdaArn)
    {
        // Validate original input schema: tenant_id, run_id, activity_id, callback_url, exposure_ids, artifact_type
        // tenant_bucket is NOT a caller input — it is resolved by the Lambda from tenant context
        // and returned via the EstimateExtractionSize ResultSelector.
        var validateInput = new Pass(this, "ValidateExtractionInput", new PassProps
        {
            Comment = "Validate input and add mode for size estimation",
            Parameters = new Dictionary<string, object>
            {
                ["mode"] = "extraction",
                ["tenant_id.$"] = "$.tenant_id",
                ["run_id.$"] = "$.run_id",
                ["activity_id.$"] = "$.activity_id",
                ["callback_url.$"] = "$.callback_url",
                ["exposure_ids.$"] = "$.exposure_ids",
                ["artifact_type.$"] = "$.artifact_type",
                ["s3_output_path.$"] = "$.S3outputpath"
            },
            ResultPath = "$"
        });

        // Invoke Lambda to estimate extraction size based on exposure_ids
        // Lambda queries Iceberg tables via Athena to count records across table hierarchy
        // Lambda also fetches tenant context and returns sde_context with tenant_role, glue_database
        var estimateSize = new CustomState(this, "EstimateExtractionSize", new CustomStateProps
        {
            StateJson = new Dictionary<string, object>
            {
                ["Type"] = "Task",
                ["Comment"] = "Query Iceberg tables via Athena to count records and estimate MDF size. Also fetches tenant context.",
                ["Resource"] = "arn:aws:states:::lambda:invoke",
                ["Parameters"] = new Dictionary<string, object>
                {
                    ["FunctionName"] = sizeEstimatorLambdaArn,
                    ["Payload.$"] = "$"
                },
                ["ResultSelector"] = new Dictionary<string, object>
                {
                    // Original input fields
                    ["tenant_id.$"] = "$.Payload.tenant_id",
                    ["run_id.$"] = "$.Payload.run_id",
                    ["activity_id.$"] = "$.Payload.activity_id",
                    ["callback_url.$"] = "$.Payload.callback_url",
                    ["exposure_ids.$"] = "$.Payload.exposure_ids",
                    ["artifact_type.$"] = "$.Payload.artifact_type",
                    ["s3_output_path.$"] = "$.Payload.s3_output_path",
                    // Size estimation results
                    ["estimated_size_bytes.$"] = "$.Payload.estimated_size_bytes",
                    ["total_record_count.$"] = "$.Payload.total_record_count",
                    ["selected_path.$"] = "$.Payload.selected_path",
                    // Tenant context for downstream tasks (CRITICAL for cross-account access)
                    // bucket_name is available as $.sde_context.bucket_name — no separate tenant_bucket field
                    ["sde_context.$"] = "$.Payload.sde_context"
                },
                ["ResultPath"] = "$"
            }
        });

        var determinePath = new Choice(this, "DetermineExtractionPath", new ChoiceProps
        {
            Comment = "Route based on selected_path from size estimator"
        });

        var path1Express = new Pass(this, "Path1ExpressExtraction", new PassProps
        {
            Comment = $"PATH 1: Database ≤{settings.ThresholdSmallGb}GB - Use SQL Server Express (Fargate) - FREE"
        });

        var path2Standard = new Pass(this, "Path2StandardExtraction", new PassProps
        {
            Comment = $"PATH 2: Database {settings.ThresholdSmallGb}-{settings.ThresholdLargeGb}GB - Use SQL Server Standard (Fargate) - SPLA"
        });

        var path3Batch = new Pass(this, "Path3BatchExtraction", new PassProps
        {
            Comment = $"PATH 3: Database >{settings.ThresholdLargeGb}GB - Use SQL Server Standard (Batch EC2) - SPLA"
        });

        // ECS RunTask using CustomState with raw ASL for Express (PATH 1)
        var runExpressTask = CreateExtractionEcsRunTaskState(
            "RunExpressExtractionTask",
            "Run Fargate Express task for extraction (PATH 1)",
            networkConfig,
            expressTaskDefArn,
            expressContainerName);

        // ECS RunTask using CustomState for Standard (PATH 2)
        var runStandardTask = CreateExtractionEcsRunTaskState(
            "RunStandardExtractionTask",
            "Run Fargate Standard task for extraction (PATH 2)",
            networkConfig,
            standardTaskDefArn,
            standardContainerName);

        // Batch SubmitJob using CustomState (PATH 3)
        var runBatchJob = CreateExtractionBatchSubmitJobState(
            "RunBatchExtractionJob",
            "Submit Batch job for extraction (PATH 3)",
            jobQueueArn,
            jobDefinitionArn,
            "sde-extraction");

        var success = new Succeed(this, "ExtractionComplete", new SucceedProps
        {
            Comment = "Extraction completed successfully"
        });

        var failure = new Fail(this, "ExtractionFailed", new FailProps
        {
            Cause = "Extraction task failed",
            Error = "EXTRACTION_ERROR"
        });

        // Build the state machine definition - route based on selected_path from Lambda
        determinePath
            .When(Condition.StringEquals("$.selected_path", "EXPRESS"), path1Express)
            .When(Condition.StringEquals("$.selected_path", "STANDARD"), path2Standard)
            .Otherwise(path3Batch);

        path1Express.Next(runExpressTask);
        path2Standard.Next(runStandardTask);
        path3Batch.Next(runBatchJob);

        runExpressTask.Next(success);
        runStandardTask.Next(success);
        runBatchJob.Next(success);

        return validateInput.Next(estimateSize).Next(determinePath);
    }

    private IChainable CreateIngestionDefinition(
        StepFunctionsConfiguration settings,
        EcsNetworkConfig networkConfig,
        string expressTaskDefArn,
        string standardTaskDefArn,
        string jobQueueArn,
        string jobDefinitionArn,
        string expressContainerName,
        string standardContainerName,
        string sizeEstimatorLambdaArn)
    {
        // Validate original input schema for ingestion
        // Accepts source_s3_bucket + source_s3_key and builds s3_input_path internally.
        // callback_url is optional (defaults to empty string).
        var validateInput = new Pass(this, "ValidateIngestionInput", new PassProps
        {
            Comment = "Normalize input: build s3_input_path from source_s3_bucket+source_s3_key",
            Parameters = new Dictionary<string, object>
            {
                ["mode"] = "ingestion",
                ["tenant_id.$"] = "$.tenant_id",
                ["run_id.$"] = "$.run_id",
                ["activity_id.$"] = "$.activity_id",
                ["callback_url"] = "",
                ["s3_input_path.$"] = "States.Format('s3://{}/{}', $.source_s3_bucket, $.source_s3_key)",
                ["artifact_type.$"] = "$.artifact_type"
            },
            ResultPath = "$"
        });

        // Invoke Lambda to get actual file size from S3 using HeadObject
        // Lambda also fetches tenant context and returns sde_context
        var getFileSize = new CustomState(this, "GetIngestionFileSize", new CustomStateProps
        {
            StateJson = new Dictionary<string, object>
            {
                ["Type"] = "Task",
                ["Comment"] = "Get actual MDF file size from S3. Also fetches tenant context.",
                ["Resource"] = "arn:aws:states:::lambda:invoke",
                ["Parameters"] = new Dictionary<string, object>
                {
                    ["FunctionName"] = sizeEstimatorLambdaArn,
                    ["Payload.$"] = "$"
                },
                ["ResultSelector"] = new Dictionary<string, object>
                {
                    // Original input fields
                    ["tenant_id.$"] = "$.Payload.tenant_id",
                    ["run_id.$"] = "$.Payload.run_id",
                    ["activity_id.$"] = "$.Payload.activity_id",
                    ["callback_url.$"] = "$.Payload.callback_url",
                    ["s3_input_path.$"] = "$.Payload.s3_input_path",
                    ["artifact_type.$"] = "$.Payload.artifact_type",
                    // Size estimation results
                    ["estimated_size_bytes.$"] = "$.Payload.estimated_size_bytes",
                    ["selected_path.$"] = "$.Payload.selected_path",
                    // Tenant context for downstream tasks (CRITICAL for cross-account access)
                    ["sde_context.$"] = "$.Payload.sde_context"
                },
                ["ResultPath"] = "$"
            }
        });

        var determinePath = new Choice(this, "DetermineIngestionPath", new ChoiceProps
        {
            Comment = "Route based on selected_path from size estimator"
        });

        var path1Express = new Pass(this, "Path1ExpressIngestion", new PassProps
        {
            Comment = $"PATH 1: MDF ≤{settings.ThresholdSmallGb}GB - Use SQL Server Express (Fargate) - FREE"
        });

        var path2Standard = new Pass(this, "Path2StandardIngestion", new PassProps
        {
            Comment = $"PATH 2: MDF {settings.ThresholdSmallGb}-{settings.ThresholdLargeGb}GB - Use SQL Server Standard (Fargate) - SPLA"
        });

        var path3Batch = new Pass(this, "Path3BatchIngestion", new PassProps
        {
            Comment = $"PATH 3: MDF >{settings.ThresholdLargeGb}GB - Use SQL Server Standard (Batch EC2) - SPLA"
        });

        // ECS RunTask using CustomState with raw ASL for Express (PATH 1)
        var runExpressTask = CreateIngestionEcsRunTaskState(
            "RunExpressIngestionTask",
            "Run Fargate Express task for ingestion (PATH 1)",
            networkConfig,
            expressTaskDefArn,
            expressContainerName);

        // ECS RunTask using CustomState for Standard (PATH 2)
        var runStandardTask = CreateIngestionEcsRunTaskState(
            "RunStandardIngestionTask",
            "Run Fargate Standard task for ingestion (PATH 2)",
            networkConfig,
            standardTaskDefArn,
            standardContainerName);

        // Batch SubmitJob using CustomState (PATH 3)
        var runBatchJob = CreateIngestionBatchSubmitJobState(
            "RunBatchIngestionJob",
            "Submit Batch job for ingestion (PATH 3)",
            jobQueueArn,
            jobDefinitionArn,
            "sde-ingestion");

        var success = new Succeed(this, "IngestionComplete", new SucceedProps
        {
            Comment = "Ingestion completed successfully"
        });

        var failure = new Fail(this, "IngestionFailed", new FailProps
        {
            Cause = "Ingestion task failed",
            Error = "INGESTION_ERROR"
        });

        // Read ingestion summary JSON written by Fargate task to the tenant S3 bucket.
        // This makes the structured ingestion result visible as the Step Functions execution output.
        // On error (e.g. summary not written), fall through to success with original state.
        var readIngestionSummary = new CustomState(this, "ReadIngestionSummary", new CustomStateProps
        {
            StateJson = new Dictionary<string, object>
            {
                ["Type"] = "Task",
                ["Comment"] = "Read ingestion summary JSON from S3 to surface as SF execution output",
                ["Resource"] = "arn:aws:states:::aws-sdk:s3:getObject",
                ["Parameters"] = new Dictionary<string, object>
                {
                    ["Bucket.$"] = "$.sde_context.bucket_name",
                    ["Key.$"] = "States.Format('sde/runs/{}/ingestion_summary.json', $.run_id)"
                },
                ["ResultSelector"] = new Dictionary<string, object>
                {
                    ["summary.$"] = "States.StringToJson($.Body)"
                },
                ["OutputPath"] = "$.summary",
                ["Catch"] = new object[]
                {
                    new Dictionary<string, object>
                    {
                        ["ErrorEquals"] = new[] { "States.ALL" },
                        ["Comment"] = "If summary not found on S3, proceed without it",
                        ["ResultPath"] = null,
                        ["Next"] = "IngestionComplete"
                    }
                },
                ["Next"] = "IngestionComplete"
            }
        });

        // Build the state machine definition - route based on selected_path from Lambda
        determinePath
            .When(Condition.StringEquals("$.selected_path", "EXPRESS"), path1Express)
            .When(Condition.StringEquals("$.selected_path", "STANDARD"), path2Standard)
            .Otherwise(path3Batch);

        path1Express.Next(runExpressTask);
        path2Standard.Next(runStandardTask);
        path3Batch.Next(runBatchJob);

        // All paths funnel through the summary reader before completing
        runExpressTask.Next(readIngestionSummary);
        runStandardTask.Next(readIngestionSummary);
        runBatchJob.Next(readIngestionSummary);
        readIngestionSummary.Next(success);

        return validateInput.Next(getFileSize).Next(determinePath);
    }

    /// <summary>
    /// Create the size estimator Lambda function.
    /// This Lambda estimates extraction size by querying Iceberg tables via Athena,
    /// counting records across the table hierarchy for given exposure_ids.
    /// For ingestion, it gets actual file size from S3 using HeadObject.
    /// </summary>
    private Function CreateSizeEstimatorLambda(string stage, StepFunctionsConfiguration settings, Dictionary<string, string>? tokenReplacementDictionary)
    {
        // Use Utility.CreateRole which handles permissions boundaries correctly
        var lambdaRole = Utility.CreateRole(this, "lambda-role", settings.LambdaRoleConfig, tokenReplacementDictionary);

        // Merge observability env vars (DT_* / AWS_LAMBDA_EXEC_WRAPPER) with function-specific vars.
        // Empty DT_ values in dev mean Dynatrace is disabled; CI/CD injects real values per environment.
        var envVars = new Dictionary<string, string>(settings.LambdaEnvironmentVariables)
        {
            ["GLUE_DATABASE"] = "",  // Set at runtime based on tenant_id
            // Athena results written to tenant bucket: {bucket}/sde/Extraction/{activity_id}/
            ["AVG_BYTES_PER_RECORD"] = "500",  // 500 bytes per record (last-resort fallback)
            ["PARQUET_TO_MDF_EXPANSION_FACTOR"] = "28",  // Calibrated: act-003 actual=6.4GB, est=4.65GB at 20x => 27.5x exact, 28x for safety
            ["THRESHOLD_SMALL_GB"] = settings.ThresholdSmallGb.ToString(),
            ["THRESHOLD_LARGE_GB"] = settings.ThresholdLargeGb.ToString(),
            ["APP_TOKEN_SECRET_NAME"] = $"rs-cdk{stage}-app-token-secret",
            ["TENANT_CONTEXT_LAMBDA_NAME"] = $"rs-cdk{stage}-ap-tenant-context-lambda"
        };

        // Remove empty observability vars so they don't show up as blank env vars when Dynatrace is disabled
        foreach (var key in envVars.Keys.Where(k => envVars[k] == string.Empty &&
            (k.StartsWith("DT_") || k == "AWS_LAMBDA_EXEC_WRAPPER")).ToList())
        {
            envVars.Remove(key);
        }

        var lambdaProps = new FunctionProps
        {
            FunctionName = $"ss-cdk{stage}-sde-size-estimator",
            Runtime = Runtime.PYTHON_3_12,
            Handler = "size_estimator.lambda_handler",
            Code = Code.FromAsset("src/EES.AP.SDE.CDK/lambda"),
            Role = lambdaRole,
            Timeout = Duration.Seconds(900),  // 15 min: safety net — parallel Athena queries finish in max(single query) ≈ 300s
            MemorySize = 2048,
            EphemeralStorageSize = Size.Mebibytes(4096),
            Description = "Estimates extraction size by querying Iceberg tables via Athena, or gets S3 file size for ingestion",
            Environment = envVars
        };

        // Attach Dynatrace OneAgent Python layer if ARN is configured
        var layerArns = settings.LayerVersionArns?.Where(a => !string.IsNullOrWhiteSpace(a)).ToList();
        if (layerArns?.Count > 0)
        {
            lambdaProps.Layers = layerArns
                .Select((arn, i) => (ILayerVersion)LayerVersion.FromLayerVersionArn(this, $"size-estimator-layer-{i}", arn))
                .ToArray();
            Console.WriteLine($"SizeEstimatorLambda: attached {layerArns.Count} layer(s): {string.Join(", ", layerArns)}");
        }

        var lambda = new Function(this, "SizeEstimatorLambda", lambdaProps);

        return lambda;
    }

    /// <summary>
    /// Create ECS RunTask state for EXTRACTION workflow using CustomState with raw ASL JSON.
    /// Extraction reads from Iceberg and writes MDF to S3 (tenant_bucket).
    /// </summary>
    private CustomState CreateExtractionEcsRunTaskState(
        string stateId,
        string comment,
        EcsNetworkConfig networkConfig,
        string taskDefinitionArn,
        string containerName)
    {
        return new CustomState(this, stateId, new CustomStateProps
        {
            StateJson = new Dictionary<string, object>
            {
                ["Type"] = "Task",
                ["Comment"] = comment,
                ["Resource"] = "arn:aws:states:::ecs:runTask.sync",
                ["Parameters"] = new Dictionary<string, object>
                {
                    ["Cluster"] = networkConfig.ClusterArn,
                    ["TaskDefinition"] = taskDefinitionArn,
                    ["LaunchType"] = "FARGATE",
                    ["NetworkConfiguration"] = new Dictionary<string, object>
                    {
                        ["AwsvpcConfiguration"] = new Dictionary<string, object>
                        {
                            ["Subnets"] = networkConfig.SubnetIds,
                            ["SecurityGroups"] = new[] { networkConfig.SecurityGroupId },
                            ["AssignPublicIp"] = "DISABLED"
                        }
                    },
                    ["Overrides"] = new Dictionary<string, object>
                    {
                        // Assign tenant role directly to task - NO STS AssumeRole needed!
                        // Task runs with tenant credentials, avoids role chaining issues
                        ["TaskRoleArn.$"] = "$.sde_context.tenant_role",
                        ["ContainerOverrides"] = new object[]
                        {
                            new Dictionary<string, object>
                            {
                                ["Name"] = containerName,
                                ["Environment"] = new object[]
                                {
                                    new Dictionary<string, object> { ["Name"] = "PIPELINE_MODE", ["Value"] = "extraction" },
                                    new Dictionary<string, object> { ["Name"] = "TENANT_ID", ["Value.$"] = "$.tenant_id" },
                                    new Dictionary<string, object> { ["Name"] = "RUN_ID", ["Value.$"] = "$.run_id" },
                                    new Dictionary<string, object> { ["Name"] = "ACTIVITY_ID", ["Value.$"] = "$.activity_id" },
                                    new Dictionary<string, object> { ["Name"] = "CALLBACK_URL", ["Value.$"] = "$.callback_url" },
                                    new Dictionary<string, object> { ["Name"] = "TENANT_BUCKET", ["Value.$"] = "$.sde_context.bucket_name" },
                                    new Dictionary<string, object> { ["Name"] = "EXPOSURE_IDS", ["Value.$"] = "States.JsonToString($.exposure_ids)" },
                                    new Dictionary<string, object> { ["Name"] = "ARTIFACT_TYPE", ["Value.$"] = "$.artifact_type" },
                                    new Dictionary<string, object> { ["Name"] = "S3_OUTPUT_PATH", ["Value.$"] = "$.s3_output_path" },
                                    // Tenant context (role already assigned via TaskRoleArn above)
                                    new Dictionary<string, object> { ["Name"] = "GLUE_DATABASE", ["Value.$"] = "$.sde_context.glue_database" },
                                    new Dictionary<string, object> { ["Name"] = "TENANT_RESOURCE_ID", ["Value.$"] = "$.sde_context.resource_id" }
                                }
                            }
                        }
                    },
                    // Cost allocation tags for Fargate tasks
                    ["Tags"] = new object[]
                    {
                        new Dictionary<string, object> { ["Key"] = "EES-ActivitySid", ["Value.$"] = "$.activity_id" },
                        new Dictionary<string, object> { ["Key"] = "EES-Tenant", ["Value.$"] = "$.sde_context.resource_id" },
                        new Dictionary<string, object> { ["Key"] = "EES-TenantId", ["Value.$"] = "$.tenant_id" }
                    }
                },
                ["ResultPath"] = "$.taskResult",
                ["Retry"] = new object[]
                {
                    new Dictionary<string, object>
                    {
                        ["ErrorEquals"] = new[] { "States.TaskFailed" },
                        ["IntervalSeconds"] = 30,
                        ["MaxAttempts"] = 2,
                        ["BackoffRate"] = 2.0
                    }
                }
            }
        });
    }

    /// <summary>
    /// Create ECS RunTask state for INGESTION workflow using CustomState with raw ASL JSON.
    /// Ingestion reads MDF from S3 (s3_input_path) and writes to Iceberg.
    /// </summary>
    private CustomState CreateIngestionEcsRunTaskState(
        string stateId,
        string comment,
        EcsNetworkConfig networkConfig,
        string taskDefinitionArn,
        string containerName)
    {
        return new CustomState(this, stateId, new CustomStateProps
        {
            StateJson = new Dictionary<string, object>
            {
                ["Type"] = "Task",
                ["Comment"] = comment,
                ["Resource"] = "arn:aws:states:::ecs:runTask.sync",
                ["Parameters"] = new Dictionary<string, object>
                {
                    ["Cluster"] = networkConfig.ClusterArn,
                    ["TaskDefinition"] = taskDefinitionArn,
                    ["LaunchType"] = "FARGATE",
                    ["NetworkConfiguration"] = new Dictionary<string, object>
                    {
                        ["AwsvpcConfiguration"] = new Dictionary<string, object>
                        {
                            ["Subnets"] = networkConfig.SubnetIds,
                            ["SecurityGroups"] = new[] { networkConfig.SecurityGroupId },
                            ["AssignPublicIp"] = "DISABLED"
                        }
                    },
                    ["Overrides"] = new Dictionary<string, object>
                    {
                        // Assign tenant role directly to task - NO STS AssumeRole needed!
                        // Task runs with tenant credentials, avoids role chaining issues
                        ["TaskRoleArn.$"] = "$.sde_context.tenant_role",
                        ["ContainerOverrides"] = new object[]
                        {
                            new Dictionary<string, object>
                            {
                                ["Name"] = containerName,
                                ["Environment"] = new object[]
                                {
                                    new Dictionary<string, object> { ["Name"] = "PIPELINE_MODE", ["Value"] = "ingestion" },
                                    new Dictionary<string, object> { ["Name"] = "TENANT_ID", ["Value.$"] = "$.tenant_id" },
                                    new Dictionary<string, object> { ["Name"] = "RUN_ID", ["Value.$"] = "$.run_id" },
                                    new Dictionary<string, object> { ["Name"] = "ACTIVITY_ID", ["Value.$"] = "$.activity_id" },
                                    new Dictionary<string, object> { ["Name"] = "CALLBACK_URL", ["Value.$"] = "$.callback_url" },
                                    new Dictionary<string, object> { ["Name"] = "S3_INPUT_PATH", ["Value.$"] = "$.s3_input_path" },
                                    new Dictionary<string, object> { ["Name"] = "ARTIFACT_TYPE", ["Value.$"] = "$.artifact_type" },
                                    // Tenant context (role already assigned via TaskRoleArn above)
                                    new Dictionary<string, object> { ["Name"] = "GLUE_DATABASE", ["Value.$"] = "$.sde_context.glue_database" },
                                    new Dictionary<string, object> { ["Name"] = "TENANT_BUCKET", ["Value.$"] = "$.sde_context.bucket_name" },
                                    new Dictionary<string, object> { ["Name"] = "TENANT_RESOURCE_ID", ["Value.$"] = "$.sde_context.resource_id" },
                                    // SID transformation — always enabled for ingestion; URLs come from static task-def env vars
                                    new Dictionary<string, object> { ["Name"] = "SID_ENABLED", ["Value"] = "true" }
                                }
                            }
                        }
                    },
                    // Cost allocation tags for Fargate tasks
                    ["Tags"] = new object[]
                    {
                        new Dictionary<string, object> { ["Key"] = "EES-ActivitySid", ["Value.$"] = "$.activity_id" },
                        new Dictionary<string, object> { ["Key"] = "EES-Tenant", ["Value.$"] = "$.sde_context.resource_id" },
                        new Dictionary<string, object> { ["Key"] = "EES-TenantId", ["Value.$"] = "$.tenant_id" }
                    }
                },
                ["ResultPath"] = "$.taskResult",
                ["Retry"] = new object[]
                {
                    new Dictionary<string, object>
                    {
                        ["ErrorEquals"] = new[] { "States.TaskFailed" },
                        ["IntervalSeconds"] = 30,
                        ["MaxAttempts"] = 2,
                        ["BackoffRate"] = 2.0
                    }
                }
            }
        });
    }

    /// <summary>
    /// Create Batch SubmitJob state for EXTRACTION workflow using CustomState with raw ASL JSON.
    /// </summary>
    private CustomState CreateExtractionBatchSubmitJobState(
        string stateId,
        string comment,
        string jobQueueArn,
        string jobDefinitionArn,
        string jobName)
    {
        return new CustomState(this, stateId, new CustomStateProps
        {
            StateJson = new Dictionary<string, object>
            {
                ["Type"] = "Task",
                ["Comment"] = comment,
                ["Resource"] = "arn:aws:states:::batch:submitJob.sync",
                ["Parameters"] = new Dictionary<string, object>
                {
                    ["JobName"] = jobName,
                    ["JobQueue"] = jobQueueArn,
                    ["JobDefinition"] = jobDefinitionArn,
                    ["ContainerOverrides"] = new Dictionary<string, object>
                    {
                        ["Environment"] = new object[]
                        {
                            new Dictionary<string, object> { ["Name"] = "PIPELINE_MODE", ["Value"] = "extraction" },
                            new Dictionary<string, object> { ["Name"] = "TENANT_ID", ["Value.$"] = "$.tenant_id" },
                            new Dictionary<string, object> { ["Name"] = "RUN_ID", ["Value.$"] = "$.run_id" },
                            new Dictionary<string, object> { ["Name"] = "ACTIVITY_ID", ["Value.$"] = "$.activity_id" },
                            new Dictionary<string, object> { ["Name"] = "CALLBACK_URL", ["Value.$"] = "$.callback_url" },
                            new Dictionary<string, object> { ["Name"] = "TENANT_BUCKET", ["Value.$"] = "$.sde_context.bucket_name" },
                            new Dictionary<string, object> { ["Name"] = "EXPOSURE_IDS", ["Value.$"] = "States.JsonToString($.exposure_ids)" },
                            new Dictionary<string, object> { ["Name"] = "ARTIFACT_TYPE", ["Value.$"] = "$.artifact_type" },
                            // Tenant context from size estimator Lambda (for cross-account access)
                            new Dictionary<string, object> { ["Name"] = "TENANT_ROLE_ARN", ["Value.$"] = "$.sde_context.tenant_role" },
                            new Dictionary<string, object> { ["Name"] = "GLUE_DATABASE", ["Value.$"] = "$.sde_context.glue_database" },
                            new Dictionary<string, object> { ["Name"] = "TENANT_RESOURCE_ID", ["Value.$"] = "$.sde_context.resource_id" }
                        }
                    },
                    // Cost allocation tags for Batch jobs
                    ["Tags"] = new Dictionary<string, object>
                    {
                        ["EES-ActivitySid.$"] = "$.activity_id",
                        ["EES-Tenant.$"] = "$.sde_context.resource_id",
                        ["EES-TenantId.$"] = "$.tenant_id"
                    }
                },
                ["ResultPath"] = "$.jobResult",
                ["Retry"] = new object[]
                {
                    new Dictionary<string, object>
                    {
                        ["ErrorEquals"] = new[] { "States.TaskFailed" },
                        ["IntervalSeconds"] = 60,
                        ["MaxAttempts"] = 2,
                        ["BackoffRate"] = 2.0
                    }
                }
            }
        });
    }

    /// <summary>
    /// Create Batch SubmitJob state for INGESTION workflow using CustomState with raw ASL JSON.
    /// </summary>
    private CustomState CreateIngestionBatchSubmitJobState(
        string stateId,
        string comment,
        string jobQueueArn,
        string jobDefinitionArn,
        string jobName)
    {
        return new CustomState(this, stateId, new CustomStateProps
        {
            StateJson = new Dictionary<string, object>
            {
                ["Type"] = "Task",
                ["Comment"] = comment,
                ["Resource"] = "arn:aws:states:::batch:submitJob.sync",
                ["Parameters"] = new Dictionary<string, object>
                {
                    ["JobName"] = jobName,
                    ["JobQueue"] = jobQueueArn,
                    ["JobDefinition"] = jobDefinitionArn,
                    ["ContainerOverrides"] = new Dictionary<string, object>
                    {
                        ["Environment"] = new object[]
                        {
                            new Dictionary<string, object> { ["Name"] = "PIPELINE_MODE", ["Value"] = "ingestion" },
                            new Dictionary<string, object> { ["Name"] = "TENANT_ID", ["Value.$"] = "$.tenant_id" },
                            new Dictionary<string, object> { ["Name"] = "RUN_ID", ["Value.$"] = "$.run_id" },
                            new Dictionary<string, object> { ["Name"] = "ACTIVITY_ID", ["Value.$"] = "$.activity_id" },
                            new Dictionary<string, object> { ["Name"] = "CALLBACK_URL", ["Value.$"] = "$.callback_url" },
                            new Dictionary<string, object> { ["Name"] = "S3_INPUT_PATH", ["Value.$"] = "$.s3_input_path" },
                            new Dictionary<string, object> { ["Name"] = "ARTIFACT_TYPE", ["Value.$"] = "$.artifact_type" },
                            // Tenant context from size estimator Lambda (for cross-account access)
                            new Dictionary<string, object> { ["Name"] = "TENANT_ROLE_ARN", ["Value.$"] = "$.sde_context.tenant_role" },
                            new Dictionary<string, object> { ["Name"] = "GLUE_DATABASE", ["Value.$"] = "$.sde_context.glue_database" },
                            new Dictionary<string, object> { ["Name"] = "TENANT_BUCKET", ["Value.$"] = "$.sde_context.bucket_name" },
                            new Dictionary<string, object> { ["Name"] = "TENANT_RESOURCE_ID", ["Value.$"] = "$.sde_context.resource_id" }
                        }
                    },
                    // Cost allocation tags for Batch jobs
                    ["Tags"] = new Dictionary<string, object>
                    {
                        ["EES-ActivitySid.$"] = "$.activity_id",
                        ["EES-Tenant.$"] = "$.sde_context.resource_id",
                        ["EES-TenantId.$"] = "$.tenant_id"
                    }
                },
                ["ResultPath"] = "$.jobResult",
                ["Retry"] = new object[]
                {
                    new Dictionary<string, object>
                    {
                        ["ErrorEquals"] = new[] { "States.TaskFailed" },
                        ["IntervalSeconds"] = 60,
                        ["MaxAttempts"] = 2,
                        ["BackoffRate"] = 2.0
                    }
                }
            }
        });
    }
}
