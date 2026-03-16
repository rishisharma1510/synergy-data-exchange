using System.Text.Json;
using System.Text.RegularExpressions;
using Amazon.CloudWatchLogs;
using Amazon.CloudWatchLogs.Model;
using Amazon.S3;
using Amazon.StepFunctions;
using DataBridgeAPI.Lambda.Models;

// Alias the AWS SDK model types that share names with our own models
using SfnStartRequest  = Amazon.StepFunctions.Model.StartExecutionRequest;
using SfnDescribeRequest = Amazon.StepFunctions.Model.DescribeExecutionRequest;
using SfnHistoryRequest  = Amazon.StepFunctions.Model.GetExecutionHistoryRequest;
using SfnHistoryEvent    = Amazon.StepFunctions.Model.HistoryEvent;
using SfnListRequest     = Amazon.StepFunctions.Model.ListExecutionsRequest;

namespace DataBridgeAPI.Lambda.Services;

public class StepFunctionsService : IStepFunctionsService
{
    private readonly IAmazonStepFunctions _sfnClient;
    private readonly IAmazonS3 _s3Client;
    private readonly IAmazonCloudWatchLogs _logsClient;
    private readonly string _extractionArn;
    private readonly string _ingestionArn;
    private readonly string _progressBucket;
    private readonly string _extractionLogGroup;
    private readonly string _ingestionLogGroup;
    private readonly ILogger<StepFunctionsService> _logger;

    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        WriteIndented = false
    };

    public StepFunctionsService(
        IAmazonStepFunctions sfnClient,
        IAmazonS3 s3Client,
        IAmazonCloudWatchLogs logsClient,
        ILogger<StepFunctionsService> logger)
    {
        _sfnClient = sfnClient;
        _s3Client  = s3Client;
        _logsClient = logsClient;
        _logger    = logger;
        _extractionArn = Environment.GetEnvironmentVariable("EXTRACTION_STATE_MACHINE_ARN")
            ?? throw new System.InvalidOperationException("EXTRACTION_STATE_MACHINE_ARN environment variable is not set.");
        _ingestionArn = Environment.GetEnvironmentVariable("INGESTION_STATE_MACHINE_ARN")
            ?? throw new System.InvalidOperationException("INGESTION_STATE_MACHINE_ARN environment variable is not set.");
        _progressBucket = Environment.GetEnvironmentVariable("PROGRESS_S3_BUCKET")
            ?? "rs-cdkdev-ssautoma03-s3";

        _extractionLogGroup = Environment.GetEnvironmentVariable("EXTRACTION_STEPFUNCTIONS_LOG_GROUP")
            ?? $"/aws/vendedlogs/states/{ExtractStateMachineName(_extractionArn)}";
        _ingestionLogGroup = Environment.GetEnvironmentVariable("INGESTION_STEPFUNCTIONS_LOG_GROUP")
            ?? $"/aws/vendedlogs/states/{ExtractStateMachineName(_ingestionArn)}";
    }

    // ── Start Execution ──────────────────────────────────────────────────────

    public async Task<StartExecutionResponse> StartExecutionAsync(
        StartExecutionRequest request,
        string requestId,
        string baseUrl,
        CancellationToken cancellationToken = default)
    {
        // Select state machine based on TransactionType
        var stateMachineArn = request.TransactionType switch
        {
            TransactionType.Extraction => _extractionArn,
            TransactionType.Ingestion  => _ingestionArn,
            _ => throw new ArgumentOutOfRangeException(nameof(request.TransactionType),
                $"Unsupported TransactionType: {request.TransactionType}")
        };

        var executionName = !string.IsNullOrWhiteSpace(request.ExecutionName)
            ? SanitiseName(request.ExecutionName)
            : $"{request.TransactionType.ToString().ToLowerInvariant()}-{DateTime.UtcNow:yyyyMMddHHmmss}-{Guid.NewGuid():N}";

        var sfnInput = request.TransactionType == TransactionType.Extraction
            ? JsonSerializer.Serialize(new
            {
                activity_id   = executionName,
                artifact_type = request.ArtifactType ?? "mdf",
                tenant_id     = request.TenantId,
                exposure_ids  = request.ExposureSetIds,
                S3outputpath  = request.S3OutputPath,
                run_id        = executionName,
                callback_url  = ""
            }, _jsonOptions)
            : JsonSerializer.Serialize(BuildIngestionInput(executionName, request), _jsonOptions);

        _logger.LogInformation("Starting {TransactionType} execution '{Name}' on {Arn}",
            request.TransactionType, executionName, stateMachineArn);

        var sfnResponse = await _sfnClient.StartExecutionAsync(
            new SfnStartRequest
            {
                StateMachineArn = stateMachineArn,
                Name = executionName,
                Input = sfnInput
            }, cancellationToken);

        var executionId = ExtractExecutionId(sfnResponse.ExecutionArn);

        _logger.LogInformation("{TransactionType} execution started: {Arn}",
            request.TransactionType, sfnResponse.ExecutionArn);

        // Build status URL — include executionArn so the server can avoid ARN reconstruction
        var statusUrl = $"{baseUrl.TrimEnd('/')}/api/executions/{executionId}" +
                        $"?transactionType={request.TransactionType}&executionArn={Uri.EscapeDataString(sfnResponse.ExecutionArn)}";

        return new StartExecutionResponse
        {
            TransactionType = request.TransactionType.ToString(),
            ExecutionArn = sfnResponse.ExecutionArn,
            ExecutionId = executionId,
            TenantId = request.TenantId,
            StartDate = sfnResponse.StartDate,
            StatusUrl = statusUrl
        };
    }

    // ── Get Execution Status ─────────────────────────────────────────────────

    public async Task<ExecutionStatusResponse> GetExecutionStatusAsync(
        string executionArn,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Fetching status for execution {Arn}", executionArn);

        // Fetch description and full event history in parallel
        var describeTask = _sfnClient.DescribeExecutionAsync(
            new SfnDescribeRequest { ExecutionArn = executionArn }, cancellationToken);

        var historyTask = _sfnClient.GetExecutionHistoryAsync(
            new SfnHistoryRequest
            {
                ExecutionArn = executionArn,
                MaxResults = 1000,   // retrieve full history for diagnostics
                ReverseOrder = false // chronological order
            }, cancellationToken);

        await Task.WhenAll(describeTask, historyTask);

        var describe = describeTask.Result;
        var history  = historyTask.Result;

        // ── Parse input ───────────────────────────────────────────────────────
        object? parsedInput = null;
        string? tenantId = null;
        if (!string.IsNullOrWhiteSpace(describe.Input))
        {
            try { parsedInput = JsonSerializer.Deserialize<object>(describe.Input, _jsonOptions); }
            catch { parsedInput = describe.Input; }
            tenantId = ExtractTenantResourceId(describe.Input);
        }

        // ── Parse output (only present when SUCCEEDED) ────────────────────────
        object? parsedOutput = null;
        if (!string.IsNullOrWhiteSpace(describe.Output))
        {
            try { parsedOutput = JsonSerializer.Deserialize<object>(describe.Output, _jsonOptions); }
            catch { parsedOutput = describe.Output; }
        }

        // ── Failure details ───────────────────────────────────────────────────
        ExecutionFailure? failure = null;
        var status = describe.Status?.Value ?? "UNKNOWN";
        if (status is "FAILED" or "TIMED_OUT" or "ABORTED")
        {
            // Walk the event log to find the state where the failure originated
            var failedAt = history.Events
                .Where(e => e.ExecutionFailedEventDetails != null
                         || e.TaskFailedEventDetails != null
                         || e.LambdaFunctionFailedEventDetails != null)
                .Select(e =>
                {
                    // Walk backwards to find the matching state-entered event
                    var enteredId = e.PreviousEventId;
                    return history.Events
                        .LastOrDefault(h => h.Id <= enteredId && h.StateEnteredEventDetails != null)
                        ?.StateEnteredEventDetails?.Name;
                })
                .FirstOrDefault(name => name != null);

            failure = new ExecutionFailure
            {
                Error   = describe.Error,
                Cause   = describe.Cause,
                FailedAt = failedAt
            };
        }

        // ── Duration ──────────────────────────────────────────────────────────
        long? durationMs = null;
        if (describe.StartDate != default && describe.StopDate != default)
            durationMs = (long)(describe.StopDate - describe.StartDate).TotalMilliseconds;

        // ── Derive TransactionType from the state machine name in the ARN ─────
        // ARN format: arn:aws:states:{region}:{account}:execution:{stateMachineName}:{execName}
        var transactionType = DeriveTransactionType(executionArn);

        return new ExecutionStatusResponse
        {
            ExecutionArn    = describe.ExecutionArn,
            ExecutionId     = ExtractExecutionId(describe.ExecutionArn),
            Name            = describe.Name,
            TransactionType = transactionType,
            TenantId = tenantId,
            Status          = status,
            StartDate       = describe.StartDate == default ? null : describe.StartDate,
            StopDate        = describe.StopDate  == default ? null : describe.StopDate,
            DurationMs      = durationMs,
            Input           = parsedInput,
            Output          = parsedOutput,
            Failure         = failure,
            Events          = history.Events.Select(MapEvent).ToList()
        };
    }

    // ── Get Execution Counts ──────────────────────────────────────────────────

    public async Task<ExecutionCountsResponse> GetExecutionCountsAsync(
        TransactionType? transactionType,
        string? tenantId,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Getting execution counts — type={Type} tenantId={TenantId}",
            transactionType?.ToString() ?? "All", tenantId ?? "Any");

        // When tenantId is specified, use describe-based tenant filtering
        if (!string.IsNullOrWhiteSpace(tenantId))
            return await CountByTenantResourceIdAsync(transactionType, tenantId, cancellationToken);

        // Statuses we count individually (PENDING_REDRIVE omitted — rarely used)
        var statuses = new[]
        {
            Amazon.StepFunctions.ExecutionStatus.RUNNING,
            Amazon.StepFunctions.ExecutionStatus.SUCCEEDED,
            Amazon.StepFunctions.ExecutionStatus.FAILED,
            Amazon.StepFunctions.ExecutionStatus.TIMED_OUT,
            Amazon.StepFunctions.ExecutionStatus.ABORTED
        };

        // Build list of (arn, txType, status) tuples to fan out
        var queries = new List<(string Arn, TransactionType TxType, Amazon.StepFunctions.ExecutionStatus Status)>();

        if (transactionType is null or TransactionType.Extraction)
            foreach (var s in statuses)
                queries.Add((_extractionArn, TransactionType.Extraction, s));

        if (transactionType is null or TransactionType.Ingestion)
            foreach (var s in statuses)
                queries.Add((_ingestionArn, TransactionType.Ingestion, s));

        // Fan out all (machine × status) count calls in parallel
        var countTasks = queries.Select(q => CountExecutionsAsync(q.Arn, q.TxType, q.Status, cancellationToken));
        var results = await Task.WhenAll(countTasks);

        // Aggregate into breakdown objects first, then roll up totals
        var ext = new StatusBreakdown();
        var ing = new StatusBreakdown();
        bool isTruncated = false;

        foreach (var r in results)
        {
            isTruncated |= r.IsTruncated;
            var b = r.TxType == TransactionType.Extraction ? ext : ing;

            switch (r.Status.Value)
            {
                case "RUNNING":   b.Running   = r.Count; break;
                case "SUCCEEDED": b.Succeeded = r.Count; break;
                case "FAILED":    b.Failed    = r.Count; break;
                case "TIMED_OUT": b.TimedOut  = r.Count; break;
                case "ABORTED":   b.Aborted   = r.Count; break;
            }
        }

        return new ExecutionCountsResponse
        {
            ExtractionBreakdown = ext,
            IngestionBreakdown  = ing,
            ExtractionCount     = ext.Total,
            IngestionCount      = ing.Total,
            TotalCount          = ext.Total + ing.Total,
            RunningCount        = ext.Running   + ing.Running,
            SucceededCount      = ext.Succeeded + ing.Succeeded,
            FailedCount         = ext.Failed    + ing.Failed,
            TimedOutCount       = ext.TimedOut  + ing.TimedOut,
            AbortedCount        = ext.Aborted   + ing.Aborted,
            IsTruncated         = isTruncated,
            AppliedFilters      = new CountFilters { TransactionType = transactionType?.ToString(), TenantId = null }
        };
    }

    // Counts all executions for one (state machine, status) pair, paginating until done.
    // Internal cap of 5 000 per bucket to avoid runaway pagination.
    private async Task<(TransactionType TxType, Amazon.StepFunctions.ExecutionStatus Status, int Count, bool IsTruncated)>
        CountExecutionsAsync(
            string stateMachineArn,
            TransactionType txType,
            Amazon.StepFunctions.ExecutionStatus status,
            CancellationToken cancellationToken)
    {
        const int pageSize = 1000;
        const int paginationCap = 5000;

        int total = 0;
        string? nextToken = null;

        do
        {
            var req = new SfnListRequest
            {
                StateMachineArn = stateMachineArn,
                StatusFilter    = status,
                MaxResults      = pageSize,
                NextToken       = nextToken
            };

            var page = await _sfnClient.ListExecutionsAsync(req, cancellationToken);
            total    += page.Executions.Count;
            nextToken = page.NextToken;

            if (total >= paginationCap)
                return (txType, status, total, IsTruncated: true);

        } while (!string.IsNullOrEmpty(nextToken));

        return (txType, status, total, IsTruncated: false);
    }

    // ── List Executions ──────────────────────────────────────────────────────

    public async Task<ListExecutionsResponse> ListExecutionsAsync(
        TransactionType? transactionType,
        string? statusFilter,
        int maxResults,
        string? tenantId,
        string? tenantResourceId,
        string baseUrl,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "Listing executions — type={Type} status={Status} max={Max} tenantId={TenantId} tenantResourceId={TenantResourceId}",
            transactionType?.ToString() ?? "All", statusFilter ?? "All", maxResults, tenantId ?? "Any", tenantResourceId ?? "Any");

        // When filtering by tenantId we fetch a larger batch so there are enough items after filtering
        int fetchLimit = (string.IsNullOrWhiteSpace(tenantId) && string.IsNullOrWhiteSpace(tenantResourceId))
            ? maxResults : Math.Min(maxResults * 20, 1000);

        // Fan out to the relevant state machine(s) in parallel
        var tasks = new List<Task<List<ExecutionListItem>>>();

        if (transactionType is null or TransactionType.Extraction)
            tasks.Add(ListFromStateMachineAsync(_extractionArn, TransactionType.Extraction,
                statusFilter, fetchLimit, cancellationToken));

        if (transactionType is null or TransactionType.Ingestion)
            tasks.Add(ListFromStateMachineAsync(_ingestionArn, TransactionType.Ingestion,
                statusFilter, fetchLimit, cancellationToken));

        await Task.WhenAll(tasks);

        var allItems = tasks
            .SelectMany(t => t.Result)
            .OrderByDescending(i => i.StartDate)
            .Take(fetchLimit)
            .ToList();

        // ── TenantId filter: describe all items in parallel to read their input JSON ──
        // Reuse describe responses below for failure enrichment to avoid double calls.
        var described = new Dictionary<string, Amazon.StepFunctions.Model.DescribeExecutionResponse>();

        if (!string.IsNullOrWhiteSpace(tenantId))
        {
            var describeTasks = allItems.Select(async item =>
            {
                try
                {
                    var desc = await _sfnClient.DescribeExecutionAsync(
                        new SfnDescribeRequest { ExecutionArn = item.ExecutionArn }, cancellationToken);
                    return (item.ExecutionArn, Desc: desc);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Could not describe execution {Arn} for tenantId filter", item.ExecutionArn);
                    return (item.ExecutionArn, Desc: (Amazon.StepFunctions.Model.DescribeExecutionResponse?)null);
                }
            });

            var describeResults = await Task.WhenAll(describeTasks);
            foreach (var (arn, desc) in describeResults)
                if (desc != null) described[arn] = desc;

            allItems = allItems
                .Where(item => described.TryGetValue(item.ExecutionArn, out var d)
                               && MatchesTenantResourceId(d.Input, tenantId))
                .Take(maxResults)
                .ToList();

            // Populate TenantResourceId on each matched item from its described input
            foreach (var item in allItems)
                if (described.TryGetValue(item.ExecutionArn, out var d))
                    item.TenantResourceId = ExtractTenantResourceId(d.Input);
        }

        // When tenantResourceId is explicitly provided, override item.TenantResourceId so that
        // S3 bucket resolution uses the right short name (e.g. "ruby000001") rather than the UUID tenant_id.
        if (!string.IsNullOrWhiteSpace(tenantResourceId))
            foreach (var item in allItems)
                item.TenantResourceId = tenantResourceId;

        // ── Enrich failed items with error/cause (reuse already-described responses if available) ──
        var failedItems = allItems
            .Where(i => i.Status is "FAILED" or "TIMED_OUT" or "ABORTED")
            .ToList();

        if (failedItems.Count > 0)
        {
            var detailTasks = failedItems.Select(async item =>
            {
                try
                {
                    var desc = described.TryGetValue(item.ExecutionArn, out var cached)
                        ? cached
                        : await _sfnClient.DescribeExecutionAsync(
                            new SfnDescribeRequest { ExecutionArn = item.ExecutionArn }, cancellationToken);

                    item.Failure = new ExecutionFailure { Error = desc.Error, Cause = desc.Cause };
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex,
                        "Could not fetch failure details for execution {Arn}", item.ExecutionArn);
                }
            });

            await Task.WhenAll(detailTasks);
        }

        // Attach per-item detail URLs
        var baseUrlTrimmed = baseUrl.TrimEnd('/');
        foreach (var item in allItems)
            item.DetailUrl = $"{baseUrlTrimmed}/api/executions/{item.ExecutionId}" +
                             $"?executionArn={Uri.EscapeDataString(item.ExecutionArn)}";

        // Fetch S3 progress reports for RUNNING executions (fire-and-forget failures ignored)
        var runningItems = allItems.Where(i => i.Status == "RUNNING").ToList();
        if (runningItems.Count > 0)
        {
            var tenantBucketCache = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (var item in runningItems)
            {
                var runIdCandidates = await ResolveProgressRunIdCandidatesAsync(item, described, cancellationToken);
                var bucketCandidates = await ResolveProgressBucketCandidatesAsync(
                    item,
                    runIdCandidates,
                    described,
                    tenantBucketCache,
                    cancellationToken);

                bool foundProgress = false;
                foreach (var bucket in bucketCandidates)
                {
                    foreach (var runId in runIdCandidates)
                    {
                        item.Progress = await FetchProgressAsync(bucket, runId, cancellationToken);
                        if (item.Progress is null)
                            continue;

                        foundProgress = true;
                        if (!string.IsNullOrWhiteSpace(item.TenantResourceId))
                            tenantBucketCache[item.TenantResourceId] = bucket;

                        if (item.Progress is JsonElement progressElement)
                        {
                            TryExtractRecordCounts(progressElement, out var ingested, out var extracted);
                            item.RecordsIngested ??= ingested;
                            item.RecordsExtracted ??= extracted;

                            if (item.RecordsIngested is null && item.RecordsExtracted is null
                                && TryExtractTotalRecordCount(progressElement, out var totalFromProgress))
                            {
                                ApplyTransactionSpecificTotal(item, totalFromProgress);
                            }
                        }

                        break;
                    }

                    if (foundProgress)
                        break;
                }
            }

            // Second pass: if a tenant bucket was learned from another execution,
            // retry unresolved items for that same tenant using the discovered bucket.
            foreach (var item in runningItems.Where(i => i.Progress is null && !string.IsNullOrWhiteSpace(i.TenantResourceId)))
            {
                if (!tenantBucketCache.TryGetValue(item.TenantResourceId!, out var tenantBucket))
                    continue;

                var runIdCandidates = await ResolveProgressRunIdCandidatesAsync(item, described, cancellationToken);
                foreach (var runId in runIdCandidates)
                {
                    item.Progress = await FetchProgressAsync(tenantBucket, runId, cancellationToken);
                    if (item.Progress is null)
                        continue;

                    if (item.Progress is JsonElement progressElement)
                    {
                        TryExtractRecordCounts(progressElement, out var ingested, out var extracted);
                        item.RecordsIngested ??= ingested;
                        item.RecordsExtracted ??= extracted;

                        if (item.RecordsIngested is null && item.RecordsExtracted is null
                            && TryExtractTotalRecordCount(progressElement, out var totalFromProgress))
                        {
                            ApplyTransactionSpecificTotal(item, totalFromProgress);
                        }
                    }

                    break;
                }
            }
        }

        // Enrich record counters from execution output for items where progress did not include them.
        await EnrichRecordCountsFromExecutionOutputAsync(allItems, described, cancellationToken);

        // Final fallback: scan Step Functions CloudWatch logs for record counters.
        await EnrichRecordCountsFromCloudWatchLogsAsync(allItems, cancellationToken);

        return new ListExecutionsResponse
        {
            Items           = allItems,
            TotalCount      = allItems.Count,
            ExtractionCount = allItems.Count(i => i.TransactionType == "Extraction"),
            IngestionCount  = allItems.Count(i => i.TransactionType == "Ingestion"),
            AppliedFilters  = new ListExecutionsFilters
            {
                TransactionType  = transactionType?.ToString(),
                Status           = statusFilter,
                TenantId         = string.IsNullOrWhiteSpace(tenantId) ? null : tenantId,
                TenantResourceId = string.IsNullOrWhiteSpace(tenantResourceId) ? null : tenantResourceId,
                MaxResults       = maxResults
            }
        };
    }

    private async Task<List<ExecutionListItem>> ListFromStateMachineAsync(
        string stateMachineArn,
        TransactionType txType,
        string? statusFilter,
        int maxResults,
        CancellationToken cancellationToken)
    {
        var request = new SfnListRequest
        {
            StateMachineArn = stateMachineArn,
            MaxResults = maxResults
        };

        // Apply optional status filter — Step Functions SDK uses ExecutionStatus enum
        if (!string.IsNullOrWhiteSpace(statusFilter))
        {
            request.StatusFilter = statusFilter.ToUpperInvariant() switch
            {
                "RUNNING"          => Amazon.StepFunctions.ExecutionStatus.RUNNING,
                "SUCCEEDED"        => Amazon.StepFunctions.ExecutionStatus.SUCCEEDED,
                "FAILED"           => Amazon.StepFunctions.ExecutionStatus.FAILED,
                "TIMED_OUT"        => Amazon.StepFunctions.ExecutionStatus.TIMED_OUT,
                "ABORTED"          => Amazon.StepFunctions.ExecutionStatus.ABORTED,
                "PENDING_REDRIVE"  => Amazon.StepFunctions.ExecutionStatus.PENDING_REDRIVE,
                _ => null
            };
        }

        var response = await _sfnClient.ListExecutionsAsync(request, cancellationToken);

        return response.Executions.Select(e =>
        {
            long? durationMs = null;
            if (e.StartDate != default && e.StopDate != default)
                durationMs = (long)(e.StopDate - e.StartDate).TotalMilliseconds;

            return new ExecutionListItem
            {
                ExecutionArn    = e.ExecutionArn,
                ExecutionId     = ExtractExecutionId(e.ExecutionArn),
                Name            = e.Name,
                TransactionType = txType.ToString(),
                Status          = e.Status?.Value ?? "UNKNOWN",
                StartDate       = e.StartDate == default ? null : e.StartDate,
                StopDate        = e.StopDate  == default ? null : e.StopDate,
                DurationMs      = durationMs
            };
        }).ToList();
    }

    // ── Cancel Execution ─────────────────────────────────────────────────────

    public async Task CancelExecutionAsync(
        string executionArn,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Stopping execution {Arn}", executionArn);

        await _sfnClient.StopExecutionAsync(
            new Amazon.StepFunctions.Model.StopExecutionRequest
            {
                ExecutionArn = executionArn,
                Cause = "Cancelled via DataBridge API",
                Error = "UserCancelled"
            }, cancellationToken);

        _logger.LogInformation("Execution stopped: {Arn}", executionArn);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────
    // Fetches progress.json from S3 for a given run_id.
    // Returns the parsed JSON as an object so it is forwarded as-is to the API consumer.
    // Returns null if the file doesn't exist (execution hasn't written progress yet) or on any error.
    private async Task<object?> FetchProgressAsync(string bucket, string runId, CancellationToken ct)
    {
        var key = $"sde/runs/{runId}/progress.json";

        try
        {
            var response = await _s3Client.GetObjectAsync(bucket, key, ct);
            using var reader = new StreamReader(response.ResponseStream);
            var json = await reader.ReadToEndAsync(ct);

            _logger.LogInformation(
                "Fetched progress.json for run {RunId} from s3://{Bucket}/{Key}: {ProgressJson}",
                runId,
                bucket,
                key,
                TruncateForLog(json, 4000));

            return System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(json, _jsonOptions);
        }
        catch (Amazon.S3.AmazonS3Exception ex)
            when (ex.StatusCode == System.Net.HttpStatusCode.NotFound
               || ex.StatusCode == System.Net.HttpStatusCode.BadRequest
               || ex.StatusCode == System.Net.HttpStatusCode.Forbidden)
        {
            _logger.LogDebug(
                "No progress file for run {RunId} at s3://{Bucket}/{Key}. StatusCode={StatusCode}",
                runId, bucket, key, ex.StatusCode);
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Could not fetch S3 progress for run {RunId} from s3://{Bucket}/{Key}",
                runId, bucket, key);
            return null;
        }
    }

    private async Task<IReadOnlyList<string>> ResolveProgressBucketCandidatesAsync(
        ExecutionListItem item,
        IReadOnlyList<string> runIdCandidates,
        IDictionary<string, Amazon.StepFunctions.Model.DescribeExecutionResponse> described,
        IDictionary<string, string> tenantBucketCache,
        CancellationToken cancellationToken)
    {
        var candidates = new List<string>();

        if (!string.IsNullOrWhiteSpace(item.TenantResourceId)
            && tenantBucketCache.TryGetValue(item.TenantResourceId, out var cachedBucket))
        {
            candidates.Add(cachedBucket);
        }

        try
        {
            var describe = described.TryGetValue(item.ExecutionArn, out var cached)
                ? cached
                : await _sfnClient.DescribeExecutionAsync(
                    new SfnDescribeRequest { ExecutionArn = item.ExecutionArn }, cancellationToken);

            if (!described.ContainsKey(item.ExecutionArn))
                described[item.ExecutionArn] = describe;

            foreach (var bucket in ExtractBucketCandidatesFromInput(describe.Input))
                candidates.Add(bucket);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Could not resolve tenant bucket from input for execution {Arn}", item.ExecutionArn);
        }

        if (!string.IsNullOrWhiteSpace(item.TenantResourceId))
            candidates.Add(BuildTenantBucketName(item.TenantResourceId));

        candidates.Add(_progressBucket);

        return candidates
            .Where(v => !string.IsNullOrWhiteSpace(v))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private async Task<IReadOnlyList<string>> ResolveProgressRunIdCandidatesAsync(
        ExecutionListItem item,
        IDictionary<string, Amazon.StepFunctions.Model.DescribeExecutionResponse> described,
        CancellationToken cancellationToken)
    {
        var candidates = new List<string> { item.ExecutionId };

        try
        {
            var describe = described.TryGetValue(item.ExecutionArn, out var cached)
                ? cached
                : await _sfnClient.DescribeExecutionAsync(
                    new SfnDescribeRequest { ExecutionArn = item.ExecutionArn }, cancellationToken);

            if (!described.ContainsKey(item.ExecutionArn))
                described[item.ExecutionArn] = describe;

            item.TenantResourceId ??= ExtractTenantResourceId(describe.Input);

            foreach (var id in ExtractRunIdentifiersFromInput(describe.Input))
                candidates.Add(id);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Could not resolve run_id/activity_id for execution {Arn}", item.ExecutionArn);
        }

        return candidates
            .Where(v => !string.IsNullOrWhiteSpace(v))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static IReadOnlyList<string> ExtractRunIdentifiersFromInput(string? input)
    {
        if (string.IsNullOrWhiteSpace(input))
            return [];

        try
        {
            using var doc = JsonDocument.Parse(input);
            var values = new List<string>();

            if (doc.RootElement.TryGetProperty("run_id", out var runId) && runId.ValueKind == JsonValueKind.String)
                values.Add(runId.GetString() ?? string.Empty);

            if (doc.RootElement.TryGetProperty("activity_id", out var activityId) && activityId.ValueKind == JsonValueKind.String)
                values.Add(activityId.GetString() ?? string.Empty);

            return values
                .Where(v => !string.IsNullOrWhiteSpace(v))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
        }
        catch
        {
            return [];
        }
    }

    private static string BuildTenantBucketName(string tenantResourceId) =>
        $"rs-cdkdev-{tenantResourceId}-s3";

    private static IReadOnlyList<string> ExtractBucketCandidatesFromInput(string? input)
    {
        if (string.IsNullOrWhiteSpace(input))
            return [];

        try
        {
            using var doc = JsonDocument.Parse(input);
            var buckets = new List<string>();
            var root = doc.RootElement;

            if (root.TryGetProperty("tenant_bucket", out var tenantBucket)
                && tenantBucket.ValueKind == JsonValueKind.String)
            {
                buckets.Add(tenantBucket.GetString() ?? string.Empty);
            }

            if (root.TryGetProperty("bucket_name", out var bucketName)
                && bucketName.ValueKind == JsonValueKind.String)
            {
                buckets.Add(bucketName.GetString() ?? string.Empty);
            }

            if (root.TryGetProperty("databridge_context", out var context)
                && context.ValueKind == JsonValueKind.Object
                && context.TryGetProperty("bucket_name", out var contextBucket)
                && contextBucket.ValueKind == JsonValueKind.String)
            {
                buckets.Add(contextBucket.GetString() ?? string.Empty);
            }

            if (root.TryGetProperty("S3outputpath", out var outputPath)
                && outputPath.ValueKind == JsonValueKind.String)
            {
                var parsed = ExtractBucketFromS3Uri(outputPath.GetString());
                if (!string.IsNullOrWhiteSpace(parsed))
                    buckets.Add(parsed);
            }

            return buckets
                .Where(v => !string.IsNullOrWhiteSpace(v))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
        }
        catch
        {
            return [];
        }
    }

    private static object BuildIngestionInput(string executionName, DataBridgeAPI.Lambda.Models.StartExecutionRequest request)
    {
        string? bucket = null;
        string? key = null;

        var fileName = request.FileName?.Trim();

        if (!string.IsNullOrWhiteSpace(fileName)
            && fileName.StartsWith("s3://", StringComparison.OrdinalIgnoreCase))
        {
            // Full S3 URI: s3://bucket/key/path
            var withoutScheme = fileName[5..];
            var slash = withoutScheme.IndexOf('/');
            if (slash >= 0)
            {
                bucket = withoutScheme[..slash];
                key    = withoutScheme[(slash + 1)..];
            }
            else
            {
                bucket = withoutScheme;
                key    = string.Empty;
            }
        }
        else
        {
            // Plain path — derive bucket from tenantResourceId
            if (!string.IsNullOrWhiteSpace(request.TenantResourceId))
                bucket = BuildTenantBucketName(request.TenantResourceId);

            key = fileName?.TrimStart('/') ?? string.Empty;
        }

        return new
        {
            activity_id      = executionName,
            run_id           = executionName,
            tenant_id        = request.TenantId,
            tenantResourceId = request.TenantResourceId,
            artifact_type    = request.ArtifactType ?? "mdf",
            source_s3_bucket = bucket,
            source_s3_key    = key
        };
    }

    private static string? ExtractBucketFromS3Uri(string? uri)
    {
        if (string.IsNullOrWhiteSpace(uri))
            return null;

        var trimmed = uri.Trim();
        if (!trimmed.StartsWith("s3://", StringComparison.OrdinalIgnoreCase))
            return null;

        var withoutScheme = trimmed[5..];
        var slash = withoutScheme.IndexOf('/');
        return slash >= 0 ? withoutScheme[..slash] : withoutScheme;
    }

    private async Task EnrichRecordCountsFromExecutionOutputAsync(
        List<ExecutionListItem> items,
        IDictionary<string, Amazon.StepFunctions.Model.DescribeExecutionResponse> described,
        CancellationToken cancellationToken)
    {
        var candidates = items
            .Where(i => i.RecordsIngested is null || i.RecordsExtracted is null)
            .ToList();

        if (candidates.Count == 0)
            return;

        using var throttler = new SemaphoreSlim(10);

        var tasks = candidates.Select(async item =>
        {
            await throttler.WaitAsync(cancellationToken);
            try
            {
                var describe = described.TryGetValue(item.ExecutionArn, out var cached)
                    ? cached
                    : await _sfnClient.DescribeExecutionAsync(
                        new SfnDescribeRequest { ExecutionArn = item.ExecutionArn }, cancellationToken);

                if (!described.ContainsKey(item.ExecutionArn))
                    described[item.ExecutionArn] = describe;

                if (string.IsNullOrWhiteSpace(describe.Output))
                    return;

                TryExtractRecordCountsFromJson(describe.Output, out var ingested, out var extracted);
                item.RecordsIngested ??= ingested;
                item.RecordsExtracted ??= extracted;

                if (item.RecordsIngested is null && item.RecordsExtracted is null
                    && TryExtractTotalRecordCount(describe.Output, out var totalFromOutput))
                {
                    ApplyTransactionSpecificTotal(item, totalFromOutput);
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Could not enrich record counters for execution {Arn}", item.ExecutionArn);
            }
            finally
            {
                throttler.Release();
            }
        });

        await Task.WhenAll(tasks);
    }

    private async Task EnrichRecordCountsFromCloudWatchLogsAsync(
        List<ExecutionListItem> items,
        CancellationToken cancellationToken)
    {
        var candidates = items
            .Where(i => i.RecordsIngested is null || i.RecordsExtracted is null)
            .ToList();

        if (candidates.Count == 0)
            return;

        using var throttler = new SemaphoreSlim(5);

        var tasks = candidates.Select(async item =>
        {
            await throttler.WaitAsync(cancellationToken);
            try
            {
                var logGroup = ResolveLogGroupFor(item);
                var windowStart = (item.StartDate ?? DateTime.UtcNow).AddMinutes(-5);
                var windowEnd = (item.StopDate ?? DateTime.UtcNow).AddMinutes(5);

                var request = new FilterLogEventsRequest
                {
                    LogGroupName = logGroup,
                    StartTime = new DateTimeOffset(windowStart).ToUnixTimeMilliseconds(),
                    EndTime = new DateTimeOffset(windowEnd).ToUnixTimeMilliseconds(),
                    FilterPattern = $"\"{item.ExecutionArn}\"",
                    Limit = 200
                };

                string? nextToken = null;
                do
                {
                    request.NextToken = nextToken;
                    var response = await _logsClient.FilterLogEventsAsync(request, cancellationToken);

                    foreach (var ev in response.Events)
                    {
                        if (TryExtractRecordCountsFromLogMessage(ev.Message, out var ingested, out var extracted))
                        {
                            item.RecordsIngested ??= ingested;
                            item.RecordsExtracted ??= extracted;

                            if (item.RecordsIngested is not null && item.RecordsExtracted is not null)
                                return;
                        }

                        if (item.RecordsIngested is null && item.RecordsExtracted is null
                            && TryExtractTotalRecordCountFromLogMessage(ev.Message, out var totalFromLog))
                        {
                            ApplyTransactionSpecificTotal(item, totalFromLog);
                            return;
                        }
                    }

                    nextToken = response.NextToken;
                }
                while (!string.IsNullOrWhiteSpace(nextToken));
            }
            catch (ResourceNotFoundException)
            {
                _logger.LogDebug("CloudWatch log group not found for execution {Arn}", item.ExecutionArn);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Could not read CloudWatch logs for execution {Arn}", item.ExecutionArn);
            }
            finally
            {
                throttler.Release();
            }
        });

        await Task.WhenAll(tasks);
    }

    private string ResolveLogGroupFor(ExecutionListItem item) =>
        string.Equals(item.TransactionType, "Extraction", StringComparison.OrdinalIgnoreCase)
            ? _extractionLogGroup
            : _ingestionLogGroup;

    private static void ApplyTransactionSpecificTotal(ExecutionListItem item, int totalCount)
    {
        if (string.Equals(item.TransactionType, "Extraction", StringComparison.OrdinalIgnoreCase))
            item.RecordsExtracted ??= totalCount;
        else if (string.Equals(item.TransactionType, "Ingestion", StringComparison.OrdinalIgnoreCase))
            item.RecordsIngested ??= totalCount;
    }

    private static void TryExtractRecordCountsFromJson(string json, out int? recordsIngested, out int? recordsExtracted)
    {
        recordsIngested = null;
        recordsExtracted = null;

        if (string.IsNullOrWhiteSpace(json))
            return;

        try
        {
            using var doc = JsonDocument.Parse(json);
            TryExtractRecordCounts(doc.RootElement, out recordsIngested, out recordsExtracted);
        }
        catch
        {
            // Ignore malformed/non-JSON output.
        }
    }

    private static bool TryExtractRecordCounts(
        JsonElement element,
        out int? recordsIngested,
        out int? recordsExtracted)
    {
        recordsIngested = null;
        recordsExtracted = null;

        TraverseForRecordCounts(element, ref recordsIngested, ref recordsExtracted);
        return recordsIngested is not null || recordsExtracted is not null;
    }

    private static void TraverseForRecordCounts(
        JsonElement element,
        ref int? recordsIngested,
        ref int? recordsExtracted)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                foreach (var property in element.EnumerateObject())
                {
                    var normalizedKey = NormalizeKey(property.Name);
                    var propertyValue = property.Value;

                    if (recordsIngested is null && IsIngestedKey(normalizedKey)
                        && TryGetInt(propertyValue, out var ingested))
                    {
                        recordsIngested = ingested;
                    }

                    if (recordsExtracted is null && IsExtractedKey(normalizedKey)
                        && TryGetInt(propertyValue, out var extracted))
                    {
                        recordsExtracted = extracted;
                    }

                    if (propertyValue.ValueKind == JsonValueKind.String)
                    {
                        var nestedJson = propertyValue.GetString();
                        if (!string.IsNullOrWhiteSpace(nestedJson)
                            && LooksLikeJson(nestedJson)
                            && TryExtractRecordCountsFromJsonToTuple(nestedJson, out var nestedIngested, out var nestedExtracted))
                        {
                            recordsIngested ??= nestedIngested;
                            recordsExtracted ??= nestedExtracted;
                        }
                    }

                    if (recordsIngested is not null && recordsExtracted is not null)
                        return;

                    TraverseForRecordCounts(propertyValue, ref recordsIngested, ref recordsExtracted);

                    if (recordsIngested is not null && recordsExtracted is not null)
                        return;
                }
                break;

            case JsonValueKind.Array:
                foreach (var item in element.EnumerateArray())
                {
                    TraverseForRecordCounts(item, ref recordsIngested, ref recordsExtracted);
                    if (recordsIngested is not null && recordsExtracted is not null)
                        return;
                }
                break;
        }
    }

    private static bool IsIngestedKey(string normalizedKey) => normalizedKey is
        "recordsingested"
        or "ingestedrecords"
        or "ingestedcount"
        or "rowsingested"
        or "totalingested"
        or "ingestioncount"
        or "ingested"
        or "recordsprocessed";

    private static bool IsExtractedKey(string normalizedKey) => normalizedKey is
        "recordsextracted"
        or "extractedrecords"
        or "extractedcount"
        or "rowsextracted"
        or "totalextracted"
        or "extractioncount"
        or "extracted"
        or "outputrecordcount";

    private static string NormalizeKey(string key)
    {
        var buffer = key.Where(char.IsLetterOrDigit)
            .Select(char.ToLowerInvariant)
            .ToArray();

        return new string(buffer);
    }

    private static bool TryGetInt(JsonElement value, out int result)
    {
        result = 0;

        if (value.ValueKind == JsonValueKind.Number)
            return value.TryGetInt32(out result);

        if (value.ValueKind == JsonValueKind.String
            && int.TryParse(value.GetString(), out var parsed))
        {
            result = parsed;
            return true;
        }

        return false;
    }

    private static bool LooksLikeJson(string value)
    {
        var trimmed = value.TrimStart();
        return trimmed.StartsWith('{') || trimmed.StartsWith('[');
    }

    private static bool TryExtractRecordCountsFromJsonToTuple(
        string json,
        out int? recordsIngested,
        out int? recordsExtracted)
    {
        recordsIngested = null;
        recordsExtracted = null;

        TryExtractRecordCountsFromJson(json, out recordsIngested, out recordsExtracted);
        return recordsIngested is not null || recordsExtracted is not null;
    }

    private static bool TryExtractTotalRecordCount(string json, out int totalCount)
    {
        totalCount = 0;

        if (string.IsNullOrWhiteSpace(json))
            return false;

        try
        {
            using var doc = JsonDocument.Parse(json);
            return TryExtractTotalRecordCount(doc.RootElement, out totalCount);
        }
        catch
        {
            return false;
        }
    }

    private static bool TryExtractTotalRecordCount(JsonElement element, out int totalCount)
    {
        totalCount = 0;

        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                foreach (var property in element.EnumerateObject())
                {
                    var normalizedKey = NormalizeKey(property.Name);
                    if (normalizedKey is "totalrecordcount" or "recordcount" or "totalrecords")
                    {
                        if (TryGetInt(property.Value, out var parsed))
                        {
                            totalCount = parsed;
                            return true;
                        }
                    }

                    if (property.Value.ValueKind == JsonValueKind.String)
                    {
                        var nested = property.Value.GetString();
                        if (!string.IsNullOrWhiteSpace(nested)
                            && LooksLikeJson(nested)
                            && TryExtractTotalRecordCount(nested, out totalCount))
                        {
                            return true;
                        }
                    }

                    if (TryExtractTotalRecordCount(property.Value, out totalCount))
                        return true;
                }
                break;

            case JsonValueKind.Array:
                foreach (var item in element.EnumerateArray())
                    if (TryExtractTotalRecordCount(item, out totalCount))
                        return true;
                break;
        }

        return false;
    }

    private static bool TryExtractTotalRecordCountFromLogMessage(string message, out int totalCount)
    {
        totalCount = 0;

        if (string.IsNullOrWhiteSpace(message))
            return false;

        if (TryExtractTotalRecordCount(message, out totalCount))
            return true;

        var regex = new Regex(@"(?i)(?:total[_\s-]*record[_\s-]*count|record[_\s-]*count|total[_\s-]*records)\s*[:=]\s*(\d+)");
        var match = regex.Match(message);
        if (!match.Success || match.Groups.Count < 2)
            return false;

        return int.TryParse(match.Groups[1].Value, out totalCount);
    }

    private static bool TryExtractRecordCountsFromLogMessage(
        string message,
        out int? recordsIngested,
        out int? recordsExtracted)
    {
        recordsIngested = null;
        recordsExtracted = null;

        if (string.IsNullOrWhiteSpace(message))
            return false;

        // Try full-message JSON parsing first.
        TryExtractRecordCountsFromJson(message, out recordsIngested, out recordsExtracted);
        if (recordsIngested is not null || recordsExtracted is not null)
            return true;

        // Then regex fallback for plain text log lines.
        if (TryExtractByRegex(message, "records?[_\\s-]*ingested|ingested[_\\s-]*records?", out var ingested))
            recordsIngested = ingested;

        if (TryExtractByRegex(message, "records?[_\\s-]*extracted|extracted[_\\s-]*records?", out var extracted))
            recordsExtracted = extracted;

        return recordsIngested is not null || recordsExtracted is not null;
    }

    private static bool TryExtractByRegex(string message, string keyPattern, out int? value)
    {
        value = null;
        var regex = new Regex($@"(?i)(?:{keyPattern})\s*[:=]\s*(\d+)");
        var match = regex.Match(message);
        if (!match.Success || match.Groups.Count < 2)
            return false;

        if (!int.TryParse(match.Groups[1].Value, out var parsed))
            return false;

        value = parsed;
        return true;
    }

    private static string ExtractStateMachineName(string stateMachineArn)
    {
        var marker = ":stateMachine:";
        var idx = stateMachineArn.IndexOf(marker, StringComparison.Ordinal);
        if (idx < 0)
            return stateMachineArn;
        return stateMachineArn[(idx + marker.Length)..];
    }

    private static string TruncateForLog(string value, int maxLength)
    {
        if (string.IsNullOrEmpty(value) || value.Length <= maxLength)
            return value;

        return value[..maxLength] + "... [truncated]";
    }

    // ── CountByTenantResourceIdAsync ───────────────────────────────────────────────────
    // Counts executions for a specific tenant resource ID. Fetches up to 1 000 executions per state machine,
    // describes each to read their input, filters by tenant resource ID, then aggregates by status.
    private async Task<ExecutionCountsResponse> CountByTenantResourceIdAsync(
        TransactionType? transactionType,
        string tenantId,
        CancellationToken cancellationToken)
    {
        const int fetchPerMachine = 1000;

        var listTasks = new List<Task<List<ExecutionListItem>>>();
        if (transactionType is null or TransactionType.Extraction)
            listTasks.Add(ListFromStateMachineAsync(_extractionArn, TransactionType.Extraction,
                null, fetchPerMachine, cancellationToken));
        if (transactionType is null or TransactionType.Ingestion)
            listTasks.Add(ListFromStateMachineAsync(_ingestionArn, TransactionType.Ingestion,
                null, fetchPerMachine, cancellationToken));

        await Task.WhenAll(listTasks);
        var allItems = listTasks.SelectMany(t => t.Result).ToList();

        bool isTruncated = allItems.Count >= fetchPerMachine;

        // Describe all in parallel to read tenant_id from input
        var describeTasks = allItems.Select(async item =>
        {
            try
            {
                var desc = await _sfnClient.DescribeExecutionAsync(
                    new SfnDescribeRequest { ExecutionArn = item.ExecutionArn }, cancellationToken);
                return (item.ExecutionArn, Desc: desc);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Could not describe execution {Arn} for tenant count", item.ExecutionArn);
                return (item.ExecutionArn, Desc: (Amazon.StepFunctions.Model.DescribeExecutionResponse?)null);
            }
        });

        var describeResults = await Task.WhenAll(describeTasks);
        var describeMap = describeResults
            .Where(r => r.Desc != null)
            .ToDictionary(r => r.ExecutionArn, r => r.Desc!);

        var matchedItems = allItems
            .Where(item => describeMap.TryGetValue(item.ExecutionArn, out var d)
                           && MatchesTenantResourceId(d.Input, tenantId))
            .ToList();

        var ext = new StatusBreakdown();
        var ing = new StatusBreakdown();

        foreach (var item in matchedItems)
        {
            var b = item.TransactionType == "Extraction" ? ext : ing;
            switch (item.Status)
            {
                case "RUNNING":   b.Running++;   break;
                case "SUCCEEDED": b.Succeeded++; break;
                case "FAILED":    b.Failed++;    break;
                case "TIMED_OUT": b.TimedOut++;  break;
                case "ABORTED":   b.Aborted++;   break;
            }
        }

        return new ExecutionCountsResponse
        {
            ExtractionBreakdown = ext,
            IngestionBreakdown  = ing,
            ExtractionCount     = ext.Total,
            IngestionCount      = ing.Total,
            TotalCount          = ext.Total + ing.Total,
            RunningCount        = ext.Running   + ing.Running,
            SucceededCount      = ext.Succeeded + ing.Succeeded,
            FailedCount         = ext.Failed    + ing.Failed,
            TimedOutCount       = ext.TimedOut  + ing.TimedOut,
            AbortedCount        = ext.Aborted   + ing.Aborted,
            IsTruncated         = isTruncated,
            AppliedFilters      = new CountFilters
            {
                TransactionType = transactionType?.ToString(),
                TenantId = tenantId
            }
        };
    }

    // Extracts tenant resource ID from Step Function input JSON.
    // Supported fields: tenant_resource_id, resource_id, tenant_id (legacy).
    private static string? ExtractTenantResourceId(string? input)
    {
        if (string.IsNullOrWhiteSpace(input)) return null;
        try
        {
            using var doc = JsonDocument.Parse(input);
            if (doc.RootElement.TryGetProperty("tenant_resource_id", out var tenantResourceId))
                return tenantResourceId.GetString();
            if (doc.RootElement.TryGetProperty("resource_id", out var resourceId))
                return resourceId.GetString();
            return doc.RootElement.TryGetProperty("tenant_id", out var tenantId) ? tenantId.GetString() : null;
        }
        catch { return null; }
    }

    // Returns true when Step Function input JSON contains matching tenant resource ID.
    private static bool MatchesTenantResourceId(string? input, string tenantResourceId)
    {
        if (string.IsNullOrWhiteSpace(input)) return false;
        try
        {
            var value = ExtractTenantResourceId(input);
            return string.Equals(value, tenantResourceId, StringComparison.OrdinalIgnoreCase);
        }
        catch { return false; }
    }

    // Reconstructs full execution ARN from the correct state machine base ARN + short execution ID.
    private string ReconstructArn(string executionId, TransactionType transactionType)
    {
        var baseArn = transactionType switch
        {
            TransactionType.Extraction => _extractionArn,
            TransactionType.Ingestion  => _ingestionArn,
            _ => throw new ArgumentOutOfRangeException(nameof(transactionType))
        };
        return baseArn.Replace(":stateMachine:", ":execution:") + $":{executionId}";
    }

    // Derives the TransactionType string from the state machine name embedded in the execution ARN.
    // ARN format: arn:aws:states:{region}:{account}:execution:{stateMachineName}:{execName}
    private static string? DeriveTransactionType(string executionArn)
    {
        // The state machine name is the 8th colon-delimited segment (index 7)
        var parts = executionArn.Split(':');
        if (parts.Length < 8) return null;
        var machineName = parts[7]; // e.g. ss-cdkdev-databridge-extraction
        if (machineName.Contains("extraction", StringComparison.OrdinalIgnoreCase)) return "Extraction";
        if (machineName.Contains("ingestion",  StringComparison.OrdinalIgnoreCase)) return "Ingestion";
        return null;
    }

    // Last colon-delimited segment is the execution name
    private static string ExtractExecutionId(string arn) =>
        arn.Split(':').LastOrDefault() ?? arn;

    // Step Functions names: [a-zA-Z0-9+!@.()-=_' /,;:?*#%^]{1,80}
    private static string SanitiseName(string raw)
    {
        var cleaned = new string(raw.Where(c => char.IsLetterOrDigit(c) || c is '-' or '_' or '.').ToArray());
        return cleaned[..Math.Min(cleaned.Length, 80)];
    }

    private static ExecutionEvent MapEvent(SfnHistoryEvent e)
    {
        var details = new Dictionary<string, string?>();
        string? stateName = null;

        if (e.ExecutionStartedEventDetails is { } started)
        {
            details["input"]   = started.Input;
            details["roleArn"] = started.RoleArn;
        }
        else if (e.ExecutionSucceededEventDetails is { } succeeded)
            details["output"] = succeeded.Output;
        else if (e.ExecutionFailedEventDetails is { } execFailed)
        {
            details["error"] = execFailed.Error;
            details["cause"] = execFailed.Cause;
        }
        else if (e.ExecutionTimedOutEventDetails is { } timedOut)
        {
            details["error"] = timedOut.Error;
            details["cause"] = timedOut.Cause;
        }
        else if (e.ExecutionAbortedEventDetails is { } aborted)
        {
            details["error"] = aborted.Error;
            details["cause"] = aborted.Cause;
        }
        else if (e.StateEnteredEventDetails is { } entered)
        {
            stateName        = entered.Name;
            details["input"] = entered.Input;
        }
        else if (e.StateExitedEventDetails is { } exited)
        {
            stateName         = exited.Name;
            details["output"] = exited.Output;
        }
        else if (e.TaskScheduledEventDetails is { } taskScheduled)
        {
            details["resourceType"] = taskScheduled.ResourceType;
            details["resource"]     = taskScheduled.Resource;
            details["parameters"]   = taskScheduled.Parameters;
        }
        else if (e.TaskSucceededEventDetails is { } taskSucceeded)
        {
            details["resourceType"] = taskSucceeded.ResourceType;
            details["resource"]     = taskSucceeded.Resource;
            details["output"]       = taskSucceeded.Output;
        }
        else if (e.TaskFailedEventDetails is { } taskFailed)
        {
            details["resourceType"] = taskFailed.ResourceType;
            details["resource"]     = taskFailed.Resource;
            details["error"]        = taskFailed.Error;
            details["cause"]        = taskFailed.Cause;
        }
        else if (e.TaskTimedOutEventDetails is { } taskTimedOut)
        {
            details["resourceType"] = taskTimedOut.ResourceType;
            details["resource"]     = taskTimedOut.Resource;
            details["error"]        = taskTimedOut.Error;
            details["cause"]        = taskTimedOut.Cause;
        }
        else if (e.LambdaFunctionScheduledEventDetails is { } lambdaScheduled)
        {
            details["resource"] = lambdaScheduled.Resource;
            details["input"]    = lambdaScheduled.Input;
        }
        else if (e.LambdaFunctionSucceededEventDetails is { } lambdaSucceeded)
            details["output"] = lambdaSucceeded.Output;
        else if (e.LambdaFunctionFailedEventDetails is { } lambdaFailed)
        {
            details["error"] = lambdaFailed.Error;
            details["cause"] = lambdaFailed.Cause;
        }
        else if (e.LambdaFunctionTimedOutEventDetails is { } lambdaTimedOut)
        {
            details["error"] = lambdaTimedOut.Error;
            details["cause"] = lambdaTimedOut.Cause;
        }

        return new ExecutionEvent
        {
            Id              = e.Id,
            PreviousEventId = e.PreviousEventId,
            Timestamp       = e.Timestamp,
            Type            = e.Type?.Value ?? "Unknown",
            StateName       = stateName,
            Details         = details
        };
    }
}
