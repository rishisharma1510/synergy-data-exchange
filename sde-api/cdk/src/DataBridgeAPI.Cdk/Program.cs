using Amazon.CDK;

namespace DataBridgeAPI.Cdk;

sealed class Program
{
    public static void Main()
    {
        var app = new App();

        new DataBridgeApiStack(app, "DataBridgeApiStack", new StackProps
        {
            // Pinned to account 451952076009 / us-east-1 where the Step Function
            // state machines (ss-cdkdev-databridge-extraction / -ingestion) live.
            Env = new Amazon.CDK.Environment
            {
                Account = "451952076009",
                Region  = "us-east-1"
            },
            Description = "DataBridge API — API Gateway + Lambda + Step Functions"
        });

        app.Synth();
    }
}
