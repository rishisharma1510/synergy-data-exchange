using Amazon.CDK;
using EES.AP.SDE.CDK.Models;
using EES.AP.SDE.CDK.Stacks;
using EES.AWSCDK.Common;
using EES.AWSCDK.Common.Extensions;

namespace EES.AP.SDE.CDK.Services;

/// <summary>
/// Generates CDK stacks based on context flags
/// Matches SynergyDrive pattern
/// </summary>
public class StackGenerator
{
    private readonly App _app;
    private readonly IStackPropertiesFactory _stackPropertiesFactory;

    /// <summary>
    /// The root CDK App for synth
    /// </summary>
    public App RootConstructApp => _app;

    public StackGenerator(App app, IStackPropertiesFactory stackPropertiesFactory)
    {
        _app = app;
        _stackPropertiesFactory = stackPropertiesFactory;
    }

    /// <summary>
    /// Generate stacks based on CDK context
    /// </summary>
    public StackGenerator GenerateStacks()
    {
        // Deploy CDK bootstrap infrastructure (IAM roles, S3, ECR, SSM param)
        // Must be deployed first, before any other stacks
        if (_app.IsContextPresent("DeployBootstrapStack"))
        {
            DeployBootstrapStack();
        }

        // Deploy ECR repositories (required first)
        if (_app.IsContextPresent("DeployEcrStack"))
        {
            DeployEcrStack();
        }

        // Deploy Fargate infrastructure (PATH 1 & PATH 2)
        if (_app.IsContextPresent("DeployFargateStack"))
        {
            DeployFargateStack();
        }

        // Deploy Batch infrastructure (PATH 3)
        if (_app.IsContextPresent("DeployBatchStack"))
        {
            DeployBatchStack();
        }

        // Deploy Step Functions state machines
        if (_app.IsContextPresent("DeployStepFunctionsStack"))
        {
            DeployStepFunctionsStack();
        }

        // Deploy SPLA usage tracking and cost monitoring
        if (_app.IsContextPresent("DeployMonitoringStack"))
        {
            DeployMonitoringStack();
        }

        // Deploy all stacks (convenience flag)
        if (_app.IsContextPresent("DeployAllStacks"))
        {
            DeployEcrStack();
            DeployFargateStack();
            DeployBatchStack();
            DeployStepFunctionsStack();
            DeployMonitoringStack();
        }

        return this;
    }

    private void DeployBootstrapStack()
    {
        var stackProperties = _stackPropertiesFactory.GetStackProperties<SDEConfiguration>();
        _ = new SDEBootstrapStack(_app, SDEBootstrapStack.STACK_NAME, stackProperties);
    }

    private void DeployEcrStack()
    {
        var stackProperties = _stackPropertiesFactory.GetStackProperties<SDEConfiguration>();
        var stackName = GetStackName("sde-ecr", stackProperties.StackTemplate);
        _ = new SDEEcrStack(_app, stackName, stackProperties);
    }

    private void DeployFargateStack()
    {
        var stackProperties = _stackPropertiesFactory.GetStackProperties<SDEConfiguration>();
        var stackName = GetStackName("sde-fargate", stackProperties.StackTemplate);
        _ = new SDEFargateStack(_app, stackName, stackProperties);
    }

    private void DeployBatchStack()
    {
        var stackProperties = _stackPropertiesFactory.GetStackProperties<SDEConfiguration>();
        var stackName = GetStackName("sde-batch", stackProperties.StackTemplate);
        _ = new SDEBatchStack(_app, stackName, stackProperties);
    }

    private void DeployStepFunctionsStack()
    {
        var stackProperties = _stackPropertiesFactory.GetStackProperties<SDEConfiguration>();
        var stackName = GetStackName("sde-stepfunctions", stackProperties.StackTemplate);
        _ = new SDEStepFunctionsStack(_app, stackName, stackProperties);
    }

    private void DeployMonitoringStack()
    {
        var stackProperties = _stackPropertiesFactory.GetStackProperties<SDEConfiguration>();
        var stackName = GetStackName("sde-monitoring", stackProperties.StackTemplate);
        _ = new SDEMonitoringStack(_app, stackName, stackProperties);
    }

    private static string GetStackName(string baseName, string? stackTemplate)
    {
        if (string.IsNullOrEmpty(stackTemplate))
        {
            return baseName;
        }
        return string.Format(stackTemplate, baseName);
    }
}
