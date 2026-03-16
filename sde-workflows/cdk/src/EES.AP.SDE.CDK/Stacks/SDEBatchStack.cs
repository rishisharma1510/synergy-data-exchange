using Amazon.CDK;
using Amazon.CDK.AWS.Batch;
using Amazon.CDK.AWS.EC2;
using Amazon.CDK.AWS.ECR;
using Amazon.CDK.AWS.ECS;
using Amazon.CDK.AWS.IAM;
using Amazon.CDK.AWS.Logs;
using Constructs;
using EES.AP.SDE.CDK.Models;
using EES.AWSCDK.Common;
using EES.AWSCDK.Common.Extensions;
using Verisk.EES.CDK.Common.Helpers;

namespace EES.AP.SDE.CDK.Stacks;

/// <summary>
/// Batch Stack - Creates AWS Batch infrastructure for PATH 3 (>60GB databases)
/// Uses EC2 instances with EBS volumes for large database processing
/// </summary>
public class SDEBatchStack : Stack
{
    public const string STACK_NAME = "SDEBatchStack";

    public IManagedComputeEnvironment ComputeEnvironment { get; }
    public IJobQueue JobQueue { get; }
    public EcsJobDefinition JobDefinition { get; }

    public SDEBatchStack(Construct scope, string id, StackProperties<SDEConfiguration> props)
        : base(scope, id, new StackProps
        {
            Env = new Amazon.CDK.Environment
            {
                Account = props.AccountId,
                Region = props.Region
            },
            Description = "Synergy Data Exchange Pipeline - AWS Batch infrastructure for large databases (>60GB)"
        })
    {
        var batchSettings = props.StackConfigurationSettings?.Batch ?? new BatchConfiguration();
        var ecrSettings = props.StackConfigurationSettings?.Ecr ?? new EcrConfiguration();
        var vpcSettings = props.StackConfigurationSettings?.Vpc ?? new VpcConfiguration();
        var apiSettings = props.StackConfigurationSettings?.Api ?? new ApiConfiguration();
        var stage = props.InstanceStage ?? "dev";

        // SQL edition for PATH 3 (Batch) — "Standard" (SPLA) or "Developer" (free, non-prod only).
        // Controlled by SQL_EDITION GitHub environment variable via
        // SDE_CDK_Settings__Batch__SqlEdition. Both Batch and Fargate must use the same value.
        var batchSqlEdition = batchSettings.SqlEdition;
        if (!string.Equals(batchSqlEdition, "Standard", StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(batchSqlEdition, "Developer", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine($"WARNING: Unsupported Batch SqlEdition '{batchSqlEdition}'. Falling back to 'Standard'.");
            batchSqlEdition = "Standard";
        }
        Console.WriteLine($"SDEBatchStack: PATH 3 SQL edition = {batchSqlEdition} ({(string.Equals(batchSqlEdition, "Developer", StringComparison.OrdinalIgnoreCase) ? "FREE — no SPLA" : "SPLA-liable")})");

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

        // Get security groups from configuration or create new
        ISecurityGroup[] batchSecurityGroups;
        if (!string.IsNullOrEmpty(vpcSettings.SecurityGroupIds))
        {
            batchSecurityGroups = this.GetSecurityGroups(vpcSettings.SecurityGroupIds, "BatchSg");
        }
        else
        {
            // Create security group for Batch compute environment
            var batchSecurityGroup = new SecurityGroup(this, "BatchSecurityGroup", new SecurityGroupProps
            {
                Vpc = vpc,
                Description = "Security group for Synergy Data Exchange Batch compute environment",
                AllowAllOutbound = true
            });
            batchSecurityGroups = new ISecurityGroup[] { batchSecurityGroup };
        }

        // Get subnets from configuration or use VPC defaults
        ISubnet[]? batchSubnets = null;
        if (!string.IsNullOrEmpty(vpcSettings.SubnetIds))
        {
            batchSubnets = this.GetSubnets(vpcSettings.SubnetIds, "BatchSubnet");
        }

        // Reference existing ECR repository with environment prefix
        var batchRepo = Repository.FromRepositoryName(this, "BatchRepo", ecrSettings.GetBatchRepositoryName(stage));

        // Create IAM role for EC2 instances using configuration
        var instanceRole = Utility.CreateRole(this, "batch-instance", batchSettings.InstanceRoleConfig, props.TokenReplacementDictionary);

        // Create instance profile
        var instanceProfile = new CfnInstanceProfile(this, "BatchInstanceProfile", new CfnInstanceProfileProps
        {
            Roles = new[] { instanceRole.RoleName }
        });

        // Create Launch Template with EBS volume configuration for large databases
        var launchTemplate = CreateLaunchTemplate(stage, batchSettings, instanceProfile);

        // Create Managed Compute Environment with EC2 and Launch Template for EBS
        ComputeEnvironment = new ManagedEc2EcsComputeEnvironment(this, "BatchComputeEnvironment", new ManagedEc2EcsComputeEnvironmentProps
        {
            ComputeEnvironmentName = $"ss-cdk{stage}-{batchSettings.ComputeEnvironmentName}",
            Vpc = vpc,
            VpcSubnets = batchSubnets != null 
                ? new SubnetSelection { Subnets = batchSubnets }
                : new SubnetSelection { SubnetType = SubnetType.PRIVATE_WITH_EGRESS },
            SecurityGroups = batchSecurityGroups,
            InstanceRole = instanceRole,
            MaxvCpus = batchSettings.MaxVcpus,
            MinvCpus = batchSettings.MinVcpus,
            InstanceTypes = batchSettings.InstanceTypes.Select(t => new InstanceType(t)).ToArray(),
            AllocationStrategy = AllocationStrategy.BEST_FIT_PROGRESSIVE,
            Spot = false, // Use on-demand for reliable large data processing
            UseOptimalInstanceClasses = true,
            LaunchTemplate = launchTemplate
        });

        // Create Job Queue
        JobQueue = new JobQueue(this, "BatchJobQueue", new JobQueueProps
        {
            JobQueueName = $"ss-cdk{stage}-{batchSettings.JobQueueName}",
            Priority = 1,
            ComputeEnvironments = new[]
            {
                new OrderedComputeEnvironment
                {
                    ComputeEnvironment = ComputeEnvironment,
                    Order = 1
                }
            }
        });

        // Create log group
        var batchLogGroup = new LogGroup(this, "BatchLogGroup", new LogGroupProps
        {
            LogGroupName = $"/aws/batch/ss-cdk{stage}-sde-batch",
            Retention = RetentionDays.ONE_MONTH,
            RemovalPolicy = RemovalPolicy.DESTROY
        });

        // Create execution role for ECS containers using configuration
        var executionRole = Utility.CreateRole(this, "batch-exec", batchSettings.ExecutionRoleConfig, props.TokenReplacementDictionary);

        // Create job role using configuration
        var jobRole = Utility.CreateRole(this, "batch-job", batchSettings.JobRoleConfig, props.TokenReplacementDictionary);

        // Create ECS Job Definition for Batch
        JobDefinition = new EcsJobDefinition(this, "BatchJobDefinition", new EcsJobDefinitionProps
        {
            JobDefinitionName = $"ss-cdk{stage}-{batchSettings.JobDefinitionName}",
            Container = new EcsEc2ContainerDefinition(this, "BatchContainer", new EcsEc2ContainerDefinitionProps
            {
                Image = ContainerImage.FromEcrRepository(batchRepo, ecrSettings.ImageTag),
                Cpu = batchSettings.JobVcpus,
                Memory = Size.Mebibytes(batchSettings.JobMemory),
                JobRole = jobRole,
                ExecutionRole = executionRole,
                Logging = new AwsLogDriver(new AwsLogDriverProps
                {
                    StreamPrefix = "batch",
                    LogGroup = batchLogGroup
                }),
                Privileged = true, // Required for mounting EBS volumes
                Environment = new Dictionary<string, string>
                {
                    ["PATH_TYPE"] = "BATCH",
                    ["SQL_EDITION"] = batchSqlEdition.ToLowerInvariant(),
                    ["MSSQL_PID"] = batchSqlEdition,       // Overrides the Dockerfile ENV at container start
                    ["MAX_WORKERS"] = batchSettings.JobMaxParallelTables.ToString(),
                    ["MAX_DATABASE_SIZE_GB"] = "500",
                    ["TENANT_CONTEXT_LAMBDA"] = apiSettings.TenantContextLambda,
                    ["APP_TOKEN_SECRET"] = apiSettings.AppTokenSecret,
                    ["IDENTITY_PROVIDER_TOKEN_ENDPOINT"] = apiSettings.IdentityProviderTokenEndpoint,
                    ["EXPOSURE_API_URL"] = apiSettings.ExposureApiUrl,
                    ["SID_API_URL"] = apiSettings.SidApiUrl,
                    ["AWS_REGION"] = props.Region ?? "us-east-1"
                }
            }),
            Timeout = Duration.Hours(12), // 12 hour timeout for large databases
            RetryAttempts = 1
        });

        // Note: EBS volume permissions are configured via InstanceRoleConfig in appsettings.json

        // Outputs
        _ = new CfnOutput(this, "ComputeEnvironmentArn", new CfnOutputProps
        {
            Value = ComputeEnvironment.ComputeEnvironmentArn,
            Description = "ARN of Batch Compute Environment",
            ExportName = $"{id}-ComputeEnvironmentArn"
        });

        _ = new CfnOutput(this, "JobQueueArn", new CfnOutputProps
        {
            Value = JobQueue.JobQueueArn,
            Description = "ARN of Batch Job Queue",
            ExportName = $"{id}-JobQueueArn"
        });

        _ = new CfnOutput(this, "JobDefinitionArn", new CfnOutputProps
        {
            Value = JobDefinition.JobDefinitionArn,
            Description = "ARN of Batch Job Definition",
            ExportName = $"{id}-JobDefinitionArn"
        });
    }

    /// <summary>
    /// Creates a Launch Template with EBS volume configuration for large database processing.
    /// The EBS volume is mounted at /data for SQL Server data files.
    /// </summary>
    private LaunchTemplate CreateLaunchTemplate(string stage, BatchConfiguration batchSettings, CfnInstanceProfile instanceProfile)
    {
        // User data script to format and mount EBS volume
        var userData = UserData.ForLinux();
        userData.AddCommands(
            "#!/bin/bash",
            "set -e",
            "",
            "# Wait for EBS volume to be attached",
            "echo 'Waiting for EBS volume...'",
            "while [ ! -e /dev/xvdf ]; do sleep 1; done",
            "",
            "# Check if volume needs formatting (new volume)",
            "if ! blkid /dev/xvdf; then",
            "    echo 'Formatting new EBS volume...'",
            "    mkfs -t xfs /dev/xvdf",
            "fi",
            "",
            "# Create mount point and mount volume",
            "mkdir -p /data",
            "mount /dev/xvdf /data",
            "chmod 777 /data",
            "",
            "# Add to fstab for persistence",
            "echo '/dev/xvdf /data xfs defaults,nofail 0 2' >> /etc/fstab",
            "",
            "echo 'EBS volume mounted at /data'"
        );

        // Create Launch Template with EBS configuration
        var launchTemplate = new LaunchTemplate(this, "BatchLaunchTemplate", new LaunchTemplateProps
        {
            LaunchTemplateName = $"ss-cdk{stage}-sde-batch-lt",
            UserData = userData,
            BlockDevices = new[]
            {
                new BlockDevice
                {
                    DeviceName = "/dev/xvdf",
                    Volume = BlockDeviceVolume.Ebs(batchSettings.EbsVolumeSizeGiB, new EbsDeviceOptions
                    {
                        VolumeType = EbsDeviceVolumeType.GP3,
                        Iops = 3000, // gp3 baseline
                        Throughput = 125, // MB/s baseline
                        DeleteOnTermination = true,
                        Encrypted = true
                    })
                }
            }
        });

        return launchTemplate;
    }
}
