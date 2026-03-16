using Amazon.CDK;
using Microsoft.Extensions.Configuration;
using System.Collections.ObjectModel;
using Verisk.EES.CDK.Common.Helpers;
using Verisk.EES.CDK.Common.Stacks;

namespace EES.AWSCDK.Common;

/// <summary>
/// Factory implementation for creating stack properties from configuration
/// Matches SynergyDrive pattern
/// </summary>
public class StackPropertiesFactory : IStackPropertiesFactory
{
    private readonly App _app;
    private readonly IConfigurationRoot _configurationRoot;

    public StackPropertiesFactory(App app, IConfigurationRoot configurationRoot)
    {
        _app = app;
        _configurationRoot = configurationRoot;
    }

    /// <inheritdoc />
    public StackProperties<T> GetStackProperties<T>(Dictionary<string, string>? tokens = null) where T : IStackConfiguration, new()
    {
        return GetStackProperties<T>(typeof(T).Name, tokens);
    }

    /// <inheritdoc />
    public StackProperties<T> GetStackProperties<T>(string configurationSettingsName, Dictionary<string, string>? tokens = null) where T : IStackConfiguration, new()
    {
        var stackProperties = _configurationRoot.GetSection("Settings").Get<StackProperties<T>>()!;
        var regionFromContext = _app.Node.TryGetContext("awsregion")?.ToString();
        stackProperties.Region = !string.IsNullOrWhiteSpace(regionFromContext) 
            ? regionFromContext 
            : stackProperties.CDK_DEFAULT_REGION;
        stackProperties.AccountId = stackProperties.CDK_DEFAULT_ACCOUNT;
        stackProperties.ApplicationVariables = GetApplicationVariables();
        
        tokens ??= [];
        tokens.Add("{stage}", stackProperties.InstanceStage);
        tokens.Add("{region}", stackProperties.CDK_DEFAULT_REGION);
        tokens.Add("{account-id}", stackProperties.CDK_DEFAULT_ACCOUNT);

        stackProperties.TokenReplacementDictionary = BuildTokenReplacementDictionary(stackProperties.ApplicationVariables, tokens);

        stackProperties.StackConfigurationSettings = _configurationRoot.GetSection(configurationSettingsName).Get<T>()!;
        stackProperties.StackConfigurationSettings.ReplaceTokensAndVariable(stackProperties.TokenReplacementDictionary);
        
        if (!string.IsNullOrEmpty(stackProperties.Qualifier))
        {
            stackProperties.Synthesizer = GetStackSynthesizer(stackProperties.Qualifier);
        }
        
        return stackProperties;
    }

    /// <summary>
    /// Returns Application variables as ReadOnlyDictionary.
    /// </summary>
    private ReadOnlyDictionary<string, string> GetApplicationVariables()
    {
        var dictFromSettings = new Dictionary<string, string>();
        _configurationRoot.GetSection("ApplicationVariables").Bind(dictFromSettings);
        
        var dictVariables = new Dictionary<string, string>();
        foreach (var kv in dictFromSettings)
        {
            dictVariables[kv.Key] = Utility.ReplaceToken(kv.Value, dictVariables);
        }

        return new ReadOnlyDictionary<string, string>(dictVariables);
    }

    private static Dictionary<string, string> BuildTokenReplacementDictionary(
        IReadOnlyDictionary<string, string> applicationVariables, 
        Dictionary<string, string> tokens)
    {
        var dict = new Dictionary<string, string>();

        foreach (var kv in tokens)
        {
            dict[kv.Key] = kv.Value;
        }

        foreach (var kv in applicationVariables)
        {
            var tokenKey = "{#" + kv.Key + "}";
            dict[tokenKey] = kv.Value;
        }

        return dict;
    }

    private static DefaultStackSynthesizer GetStackSynthesizer(string qualifier)
    {
        return new DefaultStackSynthesizer(new DefaultStackSynthesizerProps
        {
            Qualifier = qualifier,
            FileAssetsBucketName = "ss-cdk-${Qualifier}-assets-${AWS::AccountId}-${AWS::Region}",
            BucketPrefix = "",
            ImageAssetsRepositoryName = "ss-cdk-${Qualifier}-container-assets-${AWS::AccountId}-${AWS::Region}",
            DeployRoleArn = "arn:${AWS::Partition}:iam::${AWS::AccountId}:role/ss-cdk-${Qualifier}-deploy-role-${AWS::AccountId}-${AWS::Region}",
            DeployRoleExternalId = "",
            FileAssetPublishingRoleArn = "arn:${AWS::Partition}:iam::${AWS::AccountId}:role/ss-cdk-${Qualifier}-file-publishing-role-${AWS::AccountId}-${AWS::Region}",
            FileAssetPublishingExternalId = "",
            ImageAssetPublishingRoleArn = "arn:${AWS::Partition}:iam::${AWS::AccountId}:role/ss-cdk-${Qualifier}-image-publishing-role-${AWS::AccountId}-${AWS::Region}",
            ImageAssetPublishingExternalId = "",
            CloudFormationExecutionRole = "arn:${AWS::Partition}:iam::${AWS::AccountId}:role/ss-cdk-${Qualifier}-cfn-exec-role-${AWS::AccountId}-${AWS::Region}",
            LookupRoleArn = "arn:${AWS::Partition}:iam::${AWS::AccountId}:role/ss-cdk-${Qualifier}-lookup-role-${AWS::AccountId}-${AWS::Region}",
            LookupRoleExternalId = "",
            BootstrapStackVersionSsmParameter = "/cdk-bootstrap/${Qualifier}/version",
            GenerateBootstrapVersionRule = true,
        });
    }
}
