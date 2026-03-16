using Amazon.CDK;
using Amazon.CDK.AWS.IAM;
using Amazon.CDK.AWS.S3;
using Amazon.CDK.AWS.SSM;
using Constructs;
using EES.AP.SDE.CDK.Models;
using EES.AWSCDK.Common;
using System.IO;
using System.Text.Json;
using EcrRepository = Amazon.CDK.AWS.ECR.Repository;
using EcrRepositoryProps = Amazon.CDK.AWS.ECR.RepositoryProps;
using EcrLifecycleRule = Amazon.CDK.AWS.ECR.LifecycleRule;
using IEcrRepository = Amazon.CDK.AWS.ECR.IRepository;

namespace EES.AP.SDE.CDK.Stacks;

/// <summary>
/// CDK Bootstrap Stack - Creates the bootstrap resources required for CDK deployments
/// This replaces the standard 'cdk bootstrap' command by creating the required IAM roles,
/// S3 bucket, and ECR repository with proper permissions boundaries.
/// </summary>
public class SDEBootstrapStack : Stack
{
    public const string STACK_NAME = "CDKToolkit-sde";
    public const string DEFAULT_QUALIFIER = "sde";
    public const int BOOTSTRAP_VERSION = 21;

    public IBucket FileAssetsBucket { get; }
    public IEcrRepository ContainerAssetsRepository { get; }
    public Role DeployRole { get; }
    public Role FilePublishingRole { get; }
    public Role ImagePublishingRole { get; }
    public Role CloudFormationExecutionRole { get; }
    public Role LookupRole { get; }

    public SDEBootstrapStack(Construct scope, string id, StackProperties<SDEConfiguration> props)
        : base(scope, id, new StackProps
        {
            Env = new Amazon.CDK.Environment
            {
                Account = props.AccountId,
                Region = props.Region
            },
            Description = "Synergy Data Exchange CDK Bootstrap Stack - Creates CDK deployment infrastructure",
            StackName = STACK_NAME,
            // Use LegacyStackSynthesizer - embeds assets inline without needing bootstrap bucket
            // This is required for the bootstrap stack since it creates the bucket
            Synthesizer = new LegacyStackSynthesizer()
        })
    {
        var accountId = props.AccountId ?? Aws.ACCOUNT_ID;
        var region = props.Region ?? Aws.REGION;
        var qualifier = DEFAULT_QUALIFIER;
        
        // Get permissions boundary if one exists in the organization
        var permissionsBoundaryArn = GetPermissionsBoundaryArn(accountId);
        var permissionsBoundary = permissionsBoundaryArn != null 
            ? ManagedPolicy.FromManagedPolicyArn(this, "PermissionsBoundary", permissionsBoundaryArn)
            : null;

        // S3 Bucket for file assets
        FileAssetsBucket = new Bucket(this, "StagingBucket", new BucketProps
        {
            BucketName = $"ss-cdk-{qualifier}-assets-{accountId}-{region}",
            Encryption = BucketEncryption.S3_MANAGED,
            BlockPublicAccess = BlockPublicAccess.BLOCK_ALL,
            AutoDeleteObjects = false,
            RemovalPolicy = RemovalPolicy.RETAIN,
            Versioned = true,
            EnforceSSL = true
        });

        // ECR Repository for container image assets
        ContainerAssetsRepository = new EcrRepository(this, "ContainerAssetsRepository", new EcrRepositoryProps
        {
            RepositoryName = $"ss-cdk-{qualifier}-container-assets-{accountId}-{region}",
            ImageScanOnPush = true,
            RemovalPolicy = RemovalPolicy.RETAIN,
            LifecycleRules = new[]
            {
                new EcrLifecycleRule
                {
                    Description = "Keep last 100 images (used during deployments)",
                    MaxImageCount = 100,
                    RulePriority = 1
                }
            }
        });

        // Load execution policies from JSON files
        var execPolicy1 = LoadPolicyDocument("exec-policy-1.json");
        var execPolicy2 = LoadPolicyDocument("exec-policy-2.json");

        // CloudFormation Execution Role - used by CloudFormation to create resources
        CloudFormationExecutionRole = new Role(this, "CloudFormationExecutionRole", new RoleProps
        {
            RoleName = $"ss-cdk-{qualifier}-cfn-exec-role-{accountId}-{region}",
            AssumedBy = new ServicePrincipal("cloudformation.amazonaws.com"),
            Description = "Role used by CloudFormation to create resources during CDK deployment",
            PermissionsBoundary = permissionsBoundary,
            InlinePolicies = new Dictionary<string, PolicyDocument>
            {
                ["CfnExecPolicy1"] = execPolicy1,
                ["CfnExecPolicy2"] = execPolicy2
            }
        });

        // Deploy Role - assumed by CDK CLI and pipelines to deploy
        DeployRole = new Role(this, "DeployRole", new RoleProps
        {
            RoleName = $"ss-cdk-{qualifier}-deploy-role-{accountId}-{region}",
            AssumedBy = new CompositePrincipal(
                new AccountPrincipal(accountId),
                new ServicePrincipal("cloudformation.amazonaws.com")
            ),
            Description = "Role assumed by CDK CLI and pipelines to deploy stacks",
            PermissionsBoundary = permissionsBoundary
        });

        // Deploy role permissions
        DeployRole.AddToPolicy(new PolicyStatement(new PolicyStatementProps
        {
            Sid = "CloudFormationDeploymentPermissions",
            Effect = Effect.ALLOW,
            Actions = new[]
            {
                "cloudformation:CreateChangeSet",
                "cloudformation:DeleteChangeSet",
                "cloudformation:DescribeChangeSet",
                "cloudformation:DescribeStacks",
                "cloudformation:DescribeStackEvents",
                "cloudformation:ExecuteChangeSet",
                "cloudformation:GetTemplate",
                "cloudformation:DeleteStack",
                "cloudformation:UpdateTerminationProtection"
            },
            Resources = new[] { "*" }
        }));

        DeployRole.AddToPolicy(new PolicyStatement(new PolicyStatementProps
        {
            Sid = "ReadOutputsAndLogs",
            Effect = Effect.ALLOW,
            Actions = new[]
            {
                "cloudformation:DescribeStackResources",
                "ssm:GetParameter"
            },
            Resources = new[] { "*" }
        }));

        DeployRole.AddToPolicy(new PolicyStatement(new PolicyStatementProps
        {
            Sid = "PassRoleToCfn",
            Effect = Effect.ALLOW,
            Actions = new[] { "iam:PassRole" },
            Resources = new[] { CloudFormationExecutionRole.RoleArn }
        }));

        DeployRole.AddToPolicy(new PolicyStatement(new PolicyStatementProps
        {
            Sid = "S3AssetAccess",
            Effect = Effect.ALLOW,
            Actions = new[]
            {
                "s3:GetObject*",
                "s3:GetBucket*",
                "s3:List*"
            },
            Resources = new[]
            {
                FileAssetsBucket.BucketArn,
                $"{FileAssetsBucket.BucketArn}/*"
            }
        }));

        // File Publishing Role - publishes file assets to S3
        FilePublishingRole = new Role(this, "FilePublishingRole", new RoleProps
        {
            RoleName = $"ss-cdk-{qualifier}-file-publishing-role-{accountId}-{region}",
            AssumedBy = new AccountPrincipal(accountId),
            Description = "Role used to publish file assets to S3 during CDK deployment",
            PermissionsBoundary = permissionsBoundary
        });

        FilePublishingRole.AddToPolicy(new PolicyStatement(new PolicyStatementProps
        {
            Sid = "S3PublishAccess",
            Effect = Effect.ALLOW,
            Actions = new[]
            {
                "s3:GetObject*",
                "s3:GetBucket*",
                "s3:GetEncryptionConfiguration",
                "s3:List*",
                "s3:DeleteObject*",
                "s3:PutObject*",
                "s3:Abort*"
            },
            Resources = new[]
            {
                FileAssetsBucket.BucketArn,
                $"{FileAssetsBucket.BucketArn}/*"
            }
        }));

        // Image Publishing Role - publishes container images to ECR
        ImagePublishingRole = new Role(this, "ImagePublishingRole", new RoleProps
        {
            RoleName = $"ss-cdk-{qualifier}-image-publishing-role-{accountId}-{region}",
            AssumedBy = new AccountPrincipal(accountId),
            Description = "Role used to publish container images to ECR during CDK deployment",
            PermissionsBoundary = permissionsBoundary
        });

        ImagePublishingRole.AddToPolicy(new PolicyStatement(new PolicyStatementProps
        {
            Sid = "ECRPublishAccess",
            Effect = Effect.ALLOW,
            Actions = new[]
            {
                "ecr:PutImage",
                "ecr:InitiateLayerUpload",
                "ecr:UploadLayerPart",
                "ecr:CompleteLayerUpload",
                "ecr:BatchCheckLayerAvailability",
                "ecr:DescribeRepositories",
                "ecr:DescribeImages",
                "ecr:BatchGetImage",
                "ecr:GetDownloadUrlForLayer"
            },
            Resources = new[] { ContainerAssetsRepository.RepositoryArn }
        }));

        ImagePublishingRole.AddToPolicy(new PolicyStatement(new PolicyStatementProps
        {
            Sid = "ECRAuthToken",
            Effect = Effect.ALLOW,
            Actions = new[] { "ecr:GetAuthorizationToken" },
            Resources = new[] { "*" }
        }));

        // Lookup Role - used for VPC lookups and context provider queries
        LookupRole = new Role(this, "LookupRole", new RoleProps
        {
            RoleName = $"ss-cdk-{qualifier}-lookup-role-{accountId}-{region}",
            AssumedBy = new AccountPrincipal(accountId),
            Description = "Role used for VPC and resource lookups during CDK synth",
            PermissionsBoundary = permissionsBoundary
        });

        LookupRole.AddToPolicy(new PolicyStatement(new PolicyStatementProps
        {
            Sid = "LookupPermissions",
            Effect = Effect.ALLOW,
            Actions = new[]
            {
                "ec2:DescribeVpcs",
                "ec2:DescribeSubnets",
                "ec2:DescribeAvailabilityZones",
                "ec2:DescribeSecurityGroups",
                "ec2:DescribeRouteTables",
                "ec2:DescribeVpnGateways",
                "ssm:GetParameter"
            },
            Resources = new[] { "*" }
        }));

        // SSM Parameter for bootstrap version - used by CDK to verify bootstrap
        _ = new StringParameter(this, "CdkBootstrapVersion", new StringParameterProps
        {
            ParameterName = $"/cdk-bootstrap/{qualifier}/version",
            StringValue = BOOTSTRAP_VERSION.ToString(),
            Description = "CDK Bootstrap Version"
        });

        // Outputs
        _ = new CfnOutput(this, "BucketName", new CfnOutputProps
        {
            Value = FileAssetsBucket.BucketName,
            Description = "S3 bucket for CDK assets",
            ExportName = $"CdkBootstrap-{qualifier}-FileAssetsBucket"
        });

        _ = new CfnOutput(this, "BootstrapVersion", new CfnOutputProps
        {
            Value = BOOTSTRAP_VERSION.ToString(),
            Description = "CDK Bootstrap Version"
        });
    }

    /// <summary>
    /// Loads a policy document from the BootstrapsExecutionPolicies folder
    /// </summary>
    private PolicyDocument LoadPolicyDocument(string fileName)
    {
        var policyPath = Path.Combine(
            AppContext.BaseDirectory,
            "BootstrapsExecutionPolicies",
            fileName
        );

        // If not in bin directory, try relative to project
        if (!File.Exists(policyPath))
        {
            policyPath = Path.Combine(
                Directory.GetCurrentDirectory(),
                "BootstrapsExecutionPolicies",
                fileName
            );
        }

        if (!File.Exists(policyPath))
        {
            // Fallback: create minimal policy if file not found
            return new PolicyDocument(new PolicyDocumentProps
            {
                Statements = new[]
                {
                    new PolicyStatement(new PolicyStatementProps
                    {
                        Effect = Effect.ALLOW,
                        Actions = new[] { "cloudformation:*" },
                        Resources = new[] { "*" }
                    })
                }
            });
        }

        var policyJson = File.ReadAllText(policyPath);
        using var doc = JsonDocument.Parse(policyJson);
        var policyObject = ConvertJsonElement(doc.RootElement);
        return PolicyDocument.FromJson(policyObject);
    }

    /// <summary>
    /// Converts a JsonElement to primitive types that JSII can handle
    /// </summary>
    private static object? ConvertJsonElement(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.Object => element.EnumerateObject()
                .ToDictionary(p => p.Name, p => ConvertJsonElement(p.Value)),
            JsonValueKind.Array => element.EnumerateArray()
                .Select(ConvertJsonElement)
                .ToArray(),
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.TryGetInt64(out var l) ? l : element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            _ => element.GetRawText()
        };
    }

    /// <summary>
    /// Gets the permissions boundary ARN if one is configured for the organization
    /// </summary>
    private static string? GetPermissionsBoundaryArn(string accountId)
    {
        // Check for common Verisk permissions boundary
        // This can be customized based on your organization's policy
        var boundaryName = System.Environment.GetEnvironmentVariable("CDK_PERMISSIONS_BOUNDARY");
        if (!string.IsNullOrEmpty(boundaryName))
        {
            return $"arn:aws:iam::{accountId}:policy/{boundaryName}";
        }

        // Default Verisk permissions boundary (if applicable)
        // Return null to not apply any boundary
        return null;
    }
}
