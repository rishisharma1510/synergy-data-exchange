using Amazon.CDK;
using Amazon.CDK.AWS.EC2;
using Amazon.CDK.AWS.SSM;
using Constructs;
using System.Diagnostics.CodeAnalysis;

namespace EES.AWSCDK.Common.Extensions;

/// <summary>
/// Extension methods for CDK Constructs.
/// Provides utility methods for VPC, subnets, security groups, and SSM parameters.
/// </summary>
[ExcludeFromCodeCoverage]
public static class ConstructsExtensions
{
    /// <summary>
    /// Get subnet objects from comma-separated subnet IDs
    /// </summary>
    /// <param name="scope">The construct scope</param>
    /// <param name="subnetIDs">Comma-separated list of subnet IDs</param>
    /// <param name="constructIdPrefix">Prefix for construct IDs</param>
    /// <returns>Array of ISubnet objects</returns>
    public static ISubnet[] GetSubnets(this Construct scope, string subnetIDs, string constructIdPrefix = "")
    {
        var subnets = new List<ISubnet>();
        var idArray = subnetIDs?.Split(',')
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x.Trim())
            .ToArray() ?? Array.Empty<string>();
            
        foreach (var subnetId in idArray)
        {
            subnets.Add(Subnet.FromSubnetId(scope, constructIdPrefix + "-" + subnetId, subnetId));
        }
        return subnets.ToArray();
    }

    /// <summary>
    /// Get security group objects from comma-separated security group IDs
    /// </summary>
    /// <param name="scope">The construct scope</param>
    /// <param name="sgIDs">Comma-separated list of security group IDs</param>
    /// <param name="constructIdPrefix">Prefix for construct IDs</param>
    /// <returns>Array of ISecurityGroup objects</returns>
    public static ISecurityGroup[] GetSecurityGroups(this Construct scope, string sgIDs, string constructIdPrefix = "")
    {
        var securityGroups = new List<ISecurityGroup>();
        var gpIDs = sgIDs?.Split(',')
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x.Trim())
            .Distinct()
            .ToArray() ?? Array.Empty<string>();
            
        foreach (var gpId in gpIDs)
        {
            securityGroups.Add(SecurityGroup.FromSecurityGroupId(scope, constructIdPrefix + "-" + gpId, gpId));
        }

        return securityGroups.ToArray();
    }

    /// <summary>
    /// Add a stack output as an SSM parameter
    /// </summary>
    /// <param name="scope">The construct scope</param>
    /// <param name="stackname">Name of the stack</param>
    /// <param name="key">Parameter key</param>
    /// <param name="value">Parameter value</param>
    /// <param name="instanceStage">Environment stage (e.g., dev, staging, prod)</param>
    /// <returns>The created StringParameter</returns>
    public static StringParameter AddStackOutputParameter(this Construct scope, string stackname, string key, string value, string instanceStage)
    {
        var parameterName = $"/rs/{instanceStage}/cdk/stack-output/{stackname}/{key}";
        return scope.AddToSSM(key, parameterName, value);
    }

    /// <summary>
    /// Add a value to SSM Parameter Store
    /// </summary>
    /// <param name="scope">The construct scope</param>
    /// <param name="id">Construct ID</param>
    /// <param name="parameterName">Full parameter name/path</param>
    /// <param name="value">Parameter value</param>
    /// <returns>The created StringParameter</returns>
    public static StringParameter AddToSSM(this Construct scope, string id, string parameterName, string value)
    {
        return new StringParameter(scope, id, new StringParameterProps
        {
            DataType = ParameterDataType.TEXT,
            ParameterName = parameterName,
            SimpleName = false,
            StringValue = value
        });
    }

    /// <summary>
    /// Add a CloudFormation stack output
    /// </summary>
    /// <param name="scope">The construct scope</param>
    /// <param name="outputId">Output ID</param>
    /// <param name="stackOutputKey">Export name for cross-stack references</param>
    /// <param name="stackOutputValue">Output value</param>
    public static void AddStackOutput(this Construct scope, string outputId, string stackOutputKey, string stackOutputValue)
    {
        new CfnOutput(scope, outputId, new CfnOutputProps 
        { 
            Value = stackOutputValue, 
            ExportName = stackOutputKey, 
            Key = outputId 
        });
    }

    /// <summary>
    /// Import a VPC by ID
    /// </summary>
    /// <param name="scope">The construct scope</param>
    /// <param name="id">Construct ID</param>
    /// <param name="vpcId">VPC ID to import</param>
    /// <returns>IVpc reference to the existing VPC</returns>
    public static IVpc ImportVpc(this Construct scope, string id, string vpcId)
    {
        return Vpc.FromLookup(scope, id, new VpcLookupOptions
        {
            VpcId = vpcId
        });
    }
}
