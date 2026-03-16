# DataBridge API

.NET 8 API on AWS — API Gateway (REST v1) → Lambda → Step Functions

## Architecture

```
Client
  │
  ▼
API Gateway (REST API v1 — LambdaRestApi)   ← CDK-managed, stage: /api
  │  catch-all {proxy+} resource            ← no route mapping in CDK; routing is in the controllers
  ▼
Lambda  (DataBridgeApiFunction)             ← .NET 8 ASP.NET Core via LambdaEntryPoint
  │  Startup.cs → MVC controllers → services
  │
  ├── ExecutionsController  [Route: api/executions]
  │     POST /api/executions        → StepFunctionsService.StartExecutionAsync
  │     GET  /api/executions/{id}   → StepFunctionsService.GetExecutionStatusAsync
  │
  └── HealthController      [Route: health]
        GET  /health
  ▼
Step Functions Standard                     ← DataBridgeStateMachine
  ValidateInput → ProcessData → FormatOutput → Succeed
```

### Handler string (Amazon.Lambda.AspNetCoreServer pattern)
```
DataBridgeAPI.Lambda::DataBridgeAPI.Lambda.LambdaEntryPoint::FunctionHandlerAsync
```

### IAM Permissions (Lambda execution role)

| Permission | Purpose |
|---|---|
| `states:StartExecution` | POST /api/executions |
| `states:DescribeExecution` | Poll status |
| `states:GetExecutionHistory` | Stream progress events |
| `states:StopExecution` | Future cancel endpoint |
| `states:ListExecutions` | Future listing endpoint |
| `AWSLambdaBasicExecutionRole` | CloudWatch Logs |
| `AWSXRayDaemonWriteAccess` | X-Ray tracing |

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| .NET SDK | 8.x | `dotnet --version` |
| AWS CDK | 2.x | `npm i -g aws-cdk` |
| AWS CLI | 2.x | configured with deploy permissions |
| Docker | any | only needed if .NET SDK not available locally |

---

## Project Structure

```
DataBridgeAPI/
├── src/
│   └── DataBridgeAPI.Lambda/           ← ASP.NET Core Web API (Microsoft.NET.Sdk.Web)
│       ├── DataBridgeAPI.Lambda.csproj
│       ├── LambdaEntryPoint.cs         ← extends APIGatewayProxyFunction (Lambda runtime)
│       ├── LocalEntryPoint.cs          ← Kestrel entry point for local development
│       ├── Startup.cs                  ← DI registration & middleware pipeline
│       ├── appsettings.json
│       ├── aws-lambda-tools-defaults.json
│       ├── Controllers/
│       │   ├── ExecutionsController.cs ← POST /api/executions, GET /api/executions/{id}
│       │   └── HealthController.cs     ← GET /health
│       ├── Services/
│       │   ├── IStepFunctionsService.cs
│       │   └── StepFunctionsService.cs ← starts executions, polls status + history
│       └── Models/
│           ├── StartExecutionRequest.cs
│           ├── StartExecutionResponse.cs
│           └── ExecutionStatusResponse.cs
├── cdk/
│   ├── cdk.json
│   ├── global.json
│   └── src/
│       └── DataBridgeAPI.Cdk/
│           ├── DataBridgeAPI.Cdk.csproj
│           ├── Program.cs
│           ├── DataBridgeApiStack.cs   ← State Machine + Lambda + LambdaRestApi
│           └── LocalDotNetBundling.cs  ← local dotnet publish before Docker fallback
├── DataBridgeAPI.sln
└── README.md
```

---

## Run Locally

```powershell
cd src/DataBridgeAPI.Lambda

# STATE_MACHINE_ARN is only used for real AWS calls; any non-empty value works locally
$env:STATE_MACHINE_ARN = "arn:aws:states:us-east-1:000000000000:stateMachine:local"

dotnet run
# API running at https://localhost:5001
# Swagger UI at https://localhost:5001/swagger
```

---

## Deploy

```powershell
# 1. Bootstrap CDK (once per account/region)
cd cdk
cdk bootstrap

# 2. Synthesise (optional — inspect CloudFormation template)
cdk synth

# 3. Deploy
cdk deploy

# Outputs printed on success:
#   DataBridgeApiStack.ApiGatewayUrl          = https://<id>.execute-api.<region>.amazonaws.com/api/
#   DataBridgeApiStack.StartExecutionEndpoint = https://.../api/executions
#   DataBridgeApiStack.GetExecutionStatusEndpoint = https://.../api/executions/{executionId}
```

> **Windows note:** CDK tries local `dotnet publish` first (`LocalDotNetBundling`).
> If your .NET SDK is on PATH the build is fast and Docker is **not** required.
> Docker is used as a fallback for CI environments.

---

## API Usage

### Start an execution

```http
POST https://<api-url>/api/executions
Content-Type: application/json

{
  "executionName": "my-first-run",   // optional — auto-generated if omitted
  "input": {
    "orderId": "ORD-123",
    "customerId": "CUST-456"
  },
  "metadata": {
    "correlationId": "abc-def-ghi"
  }
}
```

**202 Accepted response:**
```json
{
  "executionArn": "arn:aws:states:us-east-1:123456789:execution:DataBridgeStateMachine:my-first-run",
  "executionId": "my-first-run",
  "startDate": "2026-03-04T10:00:00Z",
  "statusUrl": "https://<api-url>/api/executions/my-first-run"
}
```

### Poll execution status + progress

```http
GET https://<api-url>/api/executions/my-first-run
```

**200 OK response (in-progress):**
```json
{
  "executionArn": "arn:aws:states:...",
  "executionId": "my-first-run",
  "name": "my-first-run",
  "status": "RUNNING",
  "startDate": "2026-03-04T10:00:00Z",
  "stopDate": null,
  "output": null,
  "isComplete": false,
  "events": [
    { "id": 1, "type": "ExecutionStarted",   "timestamp": "...", "details": { "input": "..." } },
    { "id": 2, "type": "PassStateEntered",   "timestamp": "...", "details": { "name": "ValidateInput" } },
    { "id": 3, "type": "PassStateExited",    "timestamp": "...", "details": { "name": "ValidateInput" } }
  ]
}
```

**200 OK response (complete):**
```json
{
  "status": "SUCCEEDED",
  "isComplete": true,
  "output": { "requestId": "...", "status": "processed", "result": { "message": "Request processed successfully" } },
  "events": [ ... ]
}
```

> Poll until `isComplete === true`. Recommended interval: 2–5 seconds.

---

## Extending the Step Function

Open [cdk/src/DataBridgeAPI.Cdk/DataBridgeApiStack.cs](cdk/src/DataBridgeAPI.Cdk/DataBridgeApiStack.cs) and replace the `Pass` states with real `Task` states. Example — invoke a Lambda worker:

```csharp
using Amazon.CDK.AWS.StepFunctions.Tasks;

var processData = new LambdaInvoke(this, "ProcessData", new LambdaInvokeProps
{
    LambdaFunction = myWorkerLambda,
    OutputPath = "$.Payload"
});
```

---

## Destroy

```powershell
cd cdk
cdk destroy
```

> Log groups have `RemovalPolicy.DESTROY` set, so they are deleted with the stack during development.
> Change to `RemovalPolicy.RETAIN` for production.
