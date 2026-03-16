using Amazon.CDK;
using Amazon.CDK.AWS.ECR;
using Constructs;
using EES.AP.SDE.CDK.Models;
using EES.AWSCDK.Common;

namespace EES.AP.SDE.CDK.Stacks;

/// <summary>
/// ECR Stack - Creates ECR repositories for all three pipeline paths
/// </summary>
public class SDEEcrStack : Stack
{
    public const string STACK_NAME = "SDEEcrStack";

    public IRepository ExpressRepository { get; }
    public IRepository StandardRepository { get; }
    public IRepository BatchRepository { get; }

    public SDEEcrStack(Construct scope, string id, StackProperties<SDEConfiguration> props)
        : base(scope, id, new StackProps
        {
            Env = new Amazon.CDK.Environment
            {
                Account = props.AccountId,
                Region = props.Region
            },
            Description = "Synergy Data Exchange Pipeline - ECR Repositories for Express, Standard, and Batch images"
        })
    {
        var settings = props.StackConfigurationSettings?.Ecr ?? new EcrConfiguration();
        var stage = props.InstanceStage ?? "dev";

        // PATH 1: Express repository (SQL Server Express - ≤7GB databases)
        ExpressRepository = new Repository(this, "ExpressRepository", new RepositoryProps
        {
            RepositoryName = settings.GetExpressRepositoryName(stage),
            ImageScanOnPush = true,
            ImageTagMutability = TagMutability.MUTABLE,
            RemovalPolicy = RemovalPolicy.RETAIN,
            LifecycleRules = new[]
            {
                new LifecycleRule
                {
                    Description = "Keep last 10 images",
                    MaxImageCount = 10,
                    RulePriority = 1
                }
            }
        });

        // PATH 2: Standard repository (SQL Server Standard - 7-60GB databases)
        StandardRepository = new Repository(this, "StandardRepository", new RepositoryProps
        {
            RepositoryName = settings.GetStandardRepositoryName(stage),
            ImageScanOnPush = true,
            ImageTagMutability = TagMutability.MUTABLE,
            RemovalPolicy = RemovalPolicy.RETAIN,
            LifecycleRules = new[]
            {
                new LifecycleRule
                {
                    Description = "Keep last 10 images",
                    MaxImageCount = 10,
                    RulePriority = 1
                }
            }
        });

        // PATH 3: Batch repository (SQL Server Standard on EC2 - >60GB databases)
        BatchRepository = new Repository(this, "BatchRepository", new RepositoryProps
        {
            RepositoryName = settings.GetBatchRepositoryName(stage),
            ImageScanOnPush = true,
            ImageTagMutability = TagMutability.MUTABLE,
            RemovalPolicy = RemovalPolicy.RETAIN,
            LifecycleRules = new[]
            {
                new LifecycleRule
                {
                    Description = "Keep last 10 images",
                    MaxImageCount = 10,
                    RulePriority = 1
                }
            }
        });

        // Outputs
        _ = new CfnOutput(this, "ExpressRepositoryArn", new CfnOutputProps
        {
            Value = ExpressRepository.RepositoryArn,
            Description = "ARN of Express ECR repository (PATH 1)",
            ExportName = $"{id}-ExpressRepositoryArn"
        });

        _ = new CfnOutput(this, "ExpressRepositoryUri", new CfnOutputProps
        {
            Value = ExpressRepository.RepositoryUri,
            Description = "URI of Express ECR repository",
            ExportName = $"{id}-ExpressRepositoryUri"
        });

        _ = new CfnOutput(this, "StandardRepositoryArn", new CfnOutputProps
        {
            Value = StandardRepository.RepositoryArn,
            Description = "ARN of Standard ECR repository (PATH 2)",
            ExportName = $"{id}-StandardRepositoryArn"
        });

        _ = new CfnOutput(this, "StandardRepositoryUri", new CfnOutputProps
        {
            Value = StandardRepository.RepositoryUri,
            Description = "URI of Standard ECR repository",
            ExportName = $"{id}-StandardRepositoryUri"
        });

        _ = new CfnOutput(this, "BatchRepositoryArn", new CfnOutputProps
        {
            Value = BatchRepository.RepositoryArn,
            Description = "ARN of Batch ECR repository (PATH 3)",
            ExportName = $"{id}-BatchRepositoryArn"
        });

        _ = new CfnOutput(this, "BatchRepositoryUri", new CfnOutputProps
        {
            Value = BatchRepository.RepositoryUri,
            Description = "URI of Batch ECR repository",
            ExportName = $"{id}-BatchRepositoryUri"
        });
    }
}
