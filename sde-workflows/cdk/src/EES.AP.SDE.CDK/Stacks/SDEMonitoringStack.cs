using Amazon.CDK;
using Amazon.CDK.AWS.CloudWatch;
using Amazon.CDK.AWS.CloudWatch.Actions;
using Amazon.CDK.AWS.DynamoDB;
using Amazon.CDK.AWS.Events;
using Amazon.CDK.AWS.Events.Targets;
using Amazon.CDK.AWS.IAM;
using Amazon.CDK.AWS.Lambda;
using Amazon.CDK.AWS.Logs;
using Amazon.CDK.AWS.SNS;
using Amazon.CDK.AWS.SNS.Subscriptions;
using Constructs;
using EES.AP.SDE.CDK.Models;
using EES.AWSCDK.Common;
using Verisk.EES.CDK.Common.Helpers;

namespace EES.AP.SDE.CDK.Stacks;

/// <summary>
/// Monitoring Stack — SPLA usage tracking, cost visibility, and CloudWatch dashboards.
///
/// Provisions:
///   • DynamoDB table  sde-spla-usage  (TTL 3 years, GSI on billing_month)
///   • Lambda          sde-spla-tracker        EventBridge → DynamoDB + CW metric
///   • Lambda          sde-spla-concurrent-pub 5-min cron → ConcurrentSPLACores CW metric
///   • Lambda          sde-spla-monthly-reporter 1st-of-month cron → S3 report
///   • EventBridge rules for each Lambda
///   • CloudWatch alarms on ConcurrentSPLACores
///   • CloudWatch dashboard sde-Cost
///   • SNS topic  sde-spla-alerts  (subscribe your finance/ops email via console)
/// </summary>
public class SDEMonitoringStack : Stack
{
    public const string STACK_NAME = "SDEMonitoringStack";

    /// <summary>Exported DynamoDB table ARN for downstream references.</summary>
    public string SplaTableArn => _splaTable.TableArn;

    private readonly Table _splaTable;

    public SDEMonitoringStack(Construct scope, string id, StackProperties<SDEConfiguration> props)
        : base(scope, id, new StackProps
        {
            Env = new Amazon.CDK.Environment
            {
                Account = props.AccountId,
                Region = props.Region
            },
            Description = "Synergy Data Exchange Pipeline - SPLA usage tracking, cost monitoring and reporting"
        })
    {
        var monSettings = props.StackConfigurationSettings?.Monitoring ?? new MonitoringConfiguration();
        var fargateSettings = props.StackConfigurationSettings?.Fargate ?? new FargateConfiguration();
        var stage = props.InstanceStage ?? "dev";

        // Determine if SPLA tracking is meaningful for this environment.
        // When SqlEdition is Developer (non-prod), all paths are free — no SPLA obligation —
        // so we disable DynamoDB writes and metric publishing to avoid noise in reports.
        bool splaTrackingEnabled = !string.Equals(
            fargateSettings.SqlEdition, "Developer", StringComparison.OrdinalIgnoreCase);
        Console.WriteLine($"SDEMonitoringStack: SPLA tracking {(splaTrackingEnabled ? "ENABLED (Standard edition)" : "DISABLED (Developer edition — no SPLA obligation)")} for stage={stage}");

        // ── Construct known resource names / ARNs ──────────────────────────────
        // State machine ARNs are constructed deterministically (same scheme as SDEStepFunctionsStack)
        var extractionSmArn = $"arn:aws:states:{Region}:{Account}:stateMachine:ss-cdk{stage}-sde-extraction";
        var ingestionSmArn  = $"arn:aws:states:{Region}:{Account}:stateMachine:ss-cdk{stage}-sde-ingestion";

        var reportBucketName = monSettings.ReportBucket.Replace("{stage}", stage);
        var reportPrefix     = monSettings.ReportPrefix.Replace("{stage}", stage);
        var reportBucketArn  = $"arn:aws:s3:::{reportBucketName}";

        // ── SNS topic for SPLA alerts (ops + finance subscription) ─────────────
        var alertTopic = new Topic(this, "SplaAlertTopic", new TopicProps
        {
            TopicName = $"ss-cdk{stage}-sde-spla-alerts",
            DisplayName = $"Synergy Data Exchange SPLA Alerts ({stage})"
        });

        // Optionally pre-subscribe an ops e-mail if configured
        if (!string.IsNullOrWhiteSpace(monSettings.AlertEmailAddress))
        {
            alertTopic.AddSubscription(new EmailSubscription(monSettings.AlertEmailAddress));
        }

        // ── DynamoDB table ─────────────────────────────────────────────────────
        _splaTable = new Table(this, "SplaUsageTable", new TableProps
        {
            TableName = $"ss-cdk{stage}-sde-spla-usage",
            PartitionKey = new Amazon.CDK.AWS.DynamoDB.Attribute { Name = "tenant_id", Type = AttributeType.STRING },
            SortKey        = new Amazon.CDK.AWS.DynamoDB.Attribute { Name = "run_id",    Type = AttributeType.STRING },
            BillingMode    = BillingMode.PAY_PER_REQUEST,
            TimeToLiveAttribute = "ttl_epoch",          // Lambda sets this to started_at + 3 years
            RemovalPolicy  = RemovalPolicy.RETAIN,      // Never auto-delete cost records
            PointInTimeRecovery = true
        });

        // GSI: query all runs for a billing month (used by monthly reporter & concurrent tracker)
        _splaTable.AddGlobalSecondaryIndex(new GlobalSecondaryIndexProps
        {
            IndexName      = "billing_month-index",
            PartitionKey   = new Amazon.CDK.AWS.DynamoDB.Attribute { Name = "billing_month", Type = AttributeType.STRING },
            SortKey        = new Amazon.CDK.AWS.DynamoDB.Attribute { Name = "started_at",    Type = AttributeType.STRING },
            ProjectionType = ProjectionType.ALL
        });

        _ = new CfnOutput(this, "SplaTableName", new CfnOutputProps
        {
            Value       = _splaTable.TableName,
            Description = "DynamoDB table for SPLA usage records (3-year retention)",
            ExportName  = $"{id}-SplaTableName"
        });

        // ── Shared Lambda environment variables ────────────────────────────────
        var sharedEnv = new Dictionary<string, string>
        {
            ["SPLA_TABLE_NAME"]      = _splaTable.TableName,
            ["EXTRACTION_SM_ARN"]    = extractionSmArn,
            ["INGESTION_SM_ARN"]     = ingestionSmArn,
            ["REPORT_BUCKET"]        = reportBucketName,
            ["REPORT_PREFIX"]        = reportPrefix,
            ["ENV"]                  = stage,
            // When Developer edition is deployed (non-prod), disable SPLA DynamoDB writes and metric
            // publishing. Keeps reports clean and avoids false SPLA obligations on non-prod environments.
            ["SPLA_TRACKING_ENABLED"] = splaTrackingEnabled ? "true" : "false"
        };

        // ── SPLA Tracker Lambda ────────────────────────────────────────────────
        // Fires on SFN terminal state changes → writes DynamoDB records (extraction + ingestion)
        var trackerRole = Utility.CreateRole(this, "spla-tracker", monSettings.TrackerRoleConfig, props.TokenReplacementDictionary);
        _splaTable.GrantReadWriteData(trackerRole);

        // Explicit log group avoids CDK LogRetention custom resource (which creates an auto-named
        // IAM role incompatible with the VA-PB-Standard permissions boundary).
        var trackerLogGroup = new Amazon.CDK.AWS.Logs.LogGroup(this, "SplaTrackerLogGroup", new Amazon.CDK.AWS.Logs.LogGroupProps
        {
            LogGroupName    = $"/aws/lambda/ss-cdk{stage}-sde-spla-tracker",
            Retention       = RetentionDays.ONE_YEAR,
            RemovalPolicy   = RemovalPolicy.RETAIN
        });

        var trackerLambda = new Function(this, "SplaTrackerLambda", new FunctionProps
        {
            FunctionName  = $"ss-cdk{stage}-sde-spla-tracker",
            Runtime       = Runtime.PYTHON_3_12,
            Handler       = "spla_tracker.lambda_handler",
            Code          = Code.FromAsset("src/EES.AP.SDE.CDK/lambda"),
            Role          = trackerRole,
            Timeout       = Duration.Seconds(60),
            MemorySize    = 256,
            Description   = "Records PATH 2/3 SPLA-liable SFN executions to DynamoDB and publishes concurrent-cores CW metric",
            Environment   = sharedEnv,
            LogGroup      = trackerLogGroup
        });

        // EventBridge rule: SFN terminal state changes for both state machines
        var sfnStateChangeRule = new Rule(this, "SfnTerminalStateRule", new RuleProps
        {
            RuleName    = $"ss-cdk{stage}-sde-spla-sfn-events",
            Description = "Fires on Synergy Data Exchange SFN SUCCEEDED/FAILED/TIMED_OUT/ABORTED → SPLA tracker Lambda",
            EventPattern = new EventPattern
            {
                Source     = new[] { "aws.states" },
                DetailType = new[] { "Step Functions Execution Status Change" },
                Detail     = new Dictionary<string, object>
                {
                    ["status"] = new[] { "SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED" },
                    ["stateMachineArn"] = new[] { extractionSmArn, ingestionSmArn }
                }
            }
        });
        sfnStateChangeRule.AddTarget(new LambdaFunction(trackerLambda));

        // EventBridge rule: 5-minute cron → publish ConcurrentSPLACores metric
        var concurrentCronRule = new Rule(this, "ConcurrentCoresCronRule", new RuleProps
        {
            RuleName    = $"ss-cdk{stage}-sde-spla-concurrent-cron",
            Description = "Every 5 minutes — queries running SFN executions and publishes ConcurrentSPLACores CW metric",
            Schedule    = Schedule.Rate(Duration.Minutes(5))
        });
        concurrentCronRule.AddTarget(new LambdaFunction(trackerLambda));

        // ── Monthly Reporter Lambda ────────────────────────────────────────────
        var reporterRole = Utility.CreateRole(this, "spla-reporter", monSettings.ReporterRoleConfig, props.TokenReplacementDictionary);

        var reporterLogGroup = new Amazon.CDK.AWS.Logs.LogGroup(this, "SplaReporterLogGroup", new Amazon.CDK.AWS.Logs.LogGroupProps
        {
            LogGroupName  = $"/aws/lambda/ss-cdk{stage}-sde-spla-monthly-reporter",
            Retention     = RetentionDays.TWO_YEARS,   // Retain reports for audit
            RemovalPolicy = RemovalPolicy.RETAIN
        });

        var reporterLambda = new Function(this, "SplaMonthlyReporterLambda", new FunctionProps
        {
            FunctionName = $"ss-cdk{stage}-sde-spla-monthly-reporter",
            Runtime      = Runtime.PYTHON_3_12,
            Handler      = "spla_monthly_reporter.lambda_handler",
            Code         = Code.FromAsset("src/EES.AP.SDE.CDK/lambda"),
            Role         = reporterRole,
            Timeout      = Duration.Seconds(300),   // 5 min: large Scan may be slow for high-volume months
            MemorySize   = 512,
            Description  = "Generates monthly SPLA core report, uploads to S3 for LSP declaration",
            Environment  = sharedEnv,
            LogGroup     = reporterLogGroup
        });

        _splaTable.GrantReadData(reporterRole);

        // EventBridge rule: monthly on 1st at 01:00 UTC
        var monthlyCronRule = new Rule(this, "MonthlyReporterCronRule", new RuleProps
        {
            RuleName    = $"ss-cdk{stage}-sde-spla-monthly-cron",
            Description = "1st of every month at 01:00 UTC — generates SPLA usage report and uploads to S3",
            Schedule    = Schedule.Cron(new CronOptions { Minute = "0", Hour = "1", Day = "1", Month = "*", Year = "*" })
        });
        monthlyCronRule.AddTarget(new LambdaFunction(reporterLambda));

        // ── CloudWatch Alarms ──────────────────────────────────────────────────
        var concurrentCoresMetric = new Metric(new MetricProps
        {
            Namespace   = "sde/SPLA",
            MetricName  = "ConcurrentSPLACores",
            DimensionsMap = new Dictionary<string, string> { ["Environment"] = stage },
            Period      = Duration.Minutes(5),
            Statistic   = "Maximum"
        });

        // Warning: approaching cost escalation threshold
        var warningAlarm = new Alarm(this, "ConcurrentCoresWarningAlarm", new AlarmProps
        {
            AlarmName          = $"ss-cdk{stage}-sde-spla-cores-warning",
            AlarmDescription   = $"Synergy Data Exchange SPLA concurrent cores >= {monSettings.ConcurrentCoresWarningThreshold} — review active PATH 2/3 jobs",
            Metric             = concurrentCoresMetric,
            Threshold          = monSettings.ConcurrentCoresWarningThreshold,
            EvaluationPeriods  = 1,
            ComparisonOperator = ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            TreatMissingData   = TreatMissingData.NOT_BREACHING
        });
        warningAlarm.AddAlarmAction(new SnsAction(alertTopic));

        // Critical: runaway concurrency — escalate immediately
        var criticalAlarm = new Alarm(this, "ConcurrentCoresCriticalAlarm", new AlarmProps
        {
            AlarmName          = $"ss-cdk{stage}-sde-spla-cores-critical",
            AlarmDescription   = $"Synergy Data Exchange SPLA concurrent cores >= {monSettings.ConcurrentCoresCriticalThreshold} — ESCALATE: SPLA cost spike",
            Metric             = concurrentCoresMetric,
            Threshold          = monSettings.ConcurrentCoresCriticalThreshold,
            EvaluationPeriods  = 1,
            ComparisonOperator = ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            TreatMissingData   = TreatMissingData.NOT_BREACHING
        });
        criticalAlarm.AddAlarmAction(new SnsAction(alertTopic));
        criticalAlarm.AddOkAction(new SnsAction(alertTopic));

        // ── CloudWatch Dashboard ───────────────────────────────────────────────
        CreateCostDashboard(stage, concurrentCoresMetric, warningAlarm, criticalAlarm);
    }

    // ── CloudWatch Dashboard ──────────────────────────────────────────────────

    private void CreateCostDashboard(string stage, Metric concurrentCoresMetric, Alarm warningAlarm, Alarm criticalAlarm)
    {
        var sfnExtractNamespace = "AWS/States";
        var sfnExtractName      = $"ss-cdk{stage}-sde-extraction";
        var sfnIngestName       = $"ss-cdk{stage}-sde-ingestion";

        _ = new Amazon.CDK.AWS.CloudWatch.Dashboard(this, "CostDashboard", new DashboardProps
        {
            DashboardName = $"sde-Cost-{stage}",
            Widgets = new IWidget[][]
            {
                // Row 1: SPLA concurrent cores
                new IWidget[]
                {
                    new GraphWidget(new GraphWidgetProps
                    {
                        Title  = "SPLA Concurrent Cores (PATH 2 + PATH 3)",
                        Width  = 12,
                        Height = 6,
                        Left   = new[] { concurrentCoresMetric },
                        LeftAnnotations = new[]
                        {
                            new HorizontalAnnotation { Value = 80,  Color = "#ff7f0e", Label = "Warning threshold" },
                            new HorizontalAnnotation { Value = 160, Color = "#d62728", Label = "Critical threshold" }
                        }
                    }),
                    new AlarmWidget(new AlarmWidgetProps
                    {
                        Title  = "SPLA Core Alarms",
                        Width  = 12,
                        Height = 6,
                        Alarm  = criticalAlarm
                    })
                },

                // Row 2: Step Functions executions per pipeline
                new IWidget[]
                {
                    new GraphWidget(new GraphWidgetProps
                    {
                        Title  = "SFN Executions — Extraction",
                        Width  = 8,
                        Height = 6,
                        Left   = new[]
                        {
                            new Metric(new MetricProps
                            {
                                Namespace  = sfnExtractNamespace,
                                MetricName = "ExecutionsStarted",
                                DimensionsMap = new Dictionary<string, string> { ["StateMachineArn"] = $"arn:aws:states:{Region}:{Account}:stateMachine:{sfnExtractName}" },
                                Period     = Duration.Minutes(60),
                                Statistic  = "Sum",
                                Label      = "Started"
                            }),
                            new Metric(new MetricProps
                            {
                                Namespace  = sfnExtractNamespace,
                                MetricName = "ExecutionsFailed",
                                DimensionsMap = new Dictionary<string, string> { ["StateMachineArn"] = $"arn:aws:states:{Region}:{Account}:stateMachine:{sfnExtractName}" },
                                Period     = Duration.Minutes(60),
                                Statistic  = "Sum",
                                Label      = "Failed"
                            })
                        }
                    }),
                    new GraphWidget(new GraphWidgetProps
                    {
                        Title  = "SFN Executions — Ingestion",
                        Width  = 8,
                        Height = 6,
                        Left   = new[]
                        {
                            new Metric(new MetricProps
                            {
                                Namespace  = sfnExtractNamespace,
                                MetricName = "ExecutionsStarted",
                                DimensionsMap = new Dictionary<string, string> { ["StateMachineArn"] = $"arn:aws:states:{Region}:{Account}:stateMachine:{sfnIngestName}" },
                                Period     = Duration.Minutes(60),
                                Statistic  = "Sum",
                                Label      = "Started"
                            }),
                            new Metric(new MetricProps
                            {
                                Namespace  = sfnExtractNamespace,
                                MetricName = "ExecutionsFailed",
                                DimensionsMap = new Dictionary<string, string> { ["StateMachineArn"] = $"arn:aws:states:{Region}:{Account}:stateMachine:{sfnIngestName}" },
                                Period     = Duration.Minutes(60),
                                Statistic  = "Sum",
                                Label      = "Failed"
                            })
                        }
                    }),
                    new SingleValueWidget(new SingleValueWidgetProps
                    {
                        Title   = "SPLA Cores Now",
                        Width   = 8,
                        Height  = 6,
                        Metrics = new[] { concurrentCoresMetric }
                    })
                }
            }
        });
    }
}
