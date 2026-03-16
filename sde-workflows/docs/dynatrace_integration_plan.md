# Dynatrace Integration Plan for Synergy Data Exchange

## Overview

This document outlines the integration strategy for Dynatrace observability with the Synergy Data Exchange extraction/ingestion pipeline running on AWS Fargate and Batch.

---

## 1. Integration Approaches

### Option A: OneAgent Container (Recommended for Fargate)
Deploy Dynatrace OneAgent as a sidecar container alongside the pipeline container.

### Option B: OpenTelemetry with Dynatrace Exporter
Instrument application code with OpenTelemetry and export to Dynatrace.

**Recommendation:** Use Option A for infrastructure monitoring + Option B for custom business metrics.

---

## 2. Implementation Phases

### Phase 1: Infrastructure Monitoring (Week 1-2)

#### 1.1 Dynatrace OneAgent Operator Setup

**CDK Changes Required:**
- Add Dynatrace environment variables to task definitions
- Configure IAM permissions for Secrets Manager access (Dynatrace tokens)

**Secrets Required:**
```
ss-cdkdev-dynatrace-paas-token     # PaaS token for OneAgent download
ss-cdkdev-dynatrace-api-token      # API token for metric ingestion
ss-cdkdev-dynatrace-environment-id # Dynatrace environment ID
```

#### 1.2 Fargate Task Definition Updates

Add to [appsettings.json](../cdk/src/EES.AP.SDE.CDK/appsettings.json):

```json
"Dynatrace": {
  "Enabled": true,
  "EnvironmentId": "{#DynatraceEnvironmentId}",
  "PaasTokenSecretArn": "arn:aws:secretsmanager:{region}:{account-id}:secret:ss-cdk{stage}-dynatrace-paas-token",
  "ApiTokenSecretArn": "arn:aws:secretsmanager:{region}:{account-id}:secret:ss-cdk{stage}-dynatrace-api-token",
  "OneAgentImage": "docker.io/dynatrace/oneagent:latest",
  "IngestEndpoint": "https://{environment-id}.live.dynatrace.com/api/v2/metrics/ingest"
}
```

#### 1.3 Sidecar Container Definition

```csharp
// Add to SDEFargateStack.cs
private void AddDynatraceSidecar(FargateTaskDefinition taskDef, DynatraceConfiguration config)
{
    taskDef.AddContainer("dynatrace-oneagent", new ContainerDefinitionOptions
    {
        Image = ContainerImage.FromRegistry(config.OneAgentImage),
        Essential = false,
        MemoryLimitMiB = 256,
        Environment = new Dictionary<string, string>
        {
            ["DT_LOGLEVEL"] = "info"
        },
        Secrets = new Dictionary<string, Secret>
        {
            ["DT_PAAS_TOKEN"] = Secret.FromSecretsManager(paasTokenSecret),
            ["DT_ENVIRONMENT_ID"] = Secret.FromSecretsManager(envIdSecret)
        }
    });
}
```

---

### Phase 2: Application Tracing (Week 2-3)

#### 2.1 Python OneAgent SDK Installation

Add to [requirements.txt](../requirements.txt):

```
oneagent-sdk==1.5.0
opentelemetry-api>=1.20.0
opentelemetry-sdk>=1.20.0
opentelemetry-exporter-otlp>=1.20.0
```

#### 2.2 Tracing Module

Create `src/core/tracing.py`:

```python
"""
Dynatrace tracing integration for Synergy Data Exchange pipeline.
"""
import os
import logging
from contextlib import contextmanager
from typing import Optional, Any
from functools import wraps

logger = logging.getLogger(__name__)

# Dynatrace SDK import with graceful fallback
try:
    import oneagent
    from oneagent.sdk import SDK
    DYNATRACE_AVAILABLE = True
except ImportError:
    DYNATRACE_AVAILABLE = False
    logger.info("Dynatrace OneAgent SDK not available, tracing disabled")


class DynatraceTracer:
    """Wrapper for Dynatrace tracing with graceful fallback."""
    
    def __init__(self):
        self.sdk: Optional[SDK] = None
        self.enabled = os.getenv("DYNATRACE_ENABLED", "false").lower() == "true"
        
        if self.enabled and DYNATRACE_AVAILABLE:
            try:
                init_result = oneagent.initialize()
                if init_result.status == 0:
                    self.sdk = oneagent.get_sdk()
                    logger.info("Dynatrace OneAgent initialized successfully")
                else:
                    logger.warning(f"Dynatrace init failed: {init_result.error}")
            except Exception as e:
                logger.warning(f"Dynatrace initialization error: {e}")
    
    @contextmanager
    def trace_database_operation(self, operation: str, database: str, statement: str):
        """Trace database operations."""
        if not self.sdk:
            yield
            return
            
        tracer = self.sdk.trace_sql_database_request(
            database_vendor="Microsoft SQL Server",
            database_name=database,
            statement=statement
        )
        with tracer:
            yield
    
    @contextmanager
    def trace_s3_operation(self, operation: str, bucket: str, key: str):
        """Trace S3 operations."""
        if not self.sdk:
            yield
            return
            
        tracer = self.sdk.trace_custom_service(
            service_method=operation,
            service_name="S3"
        )
        tracer.set_string_tag("s3.bucket", bucket)
        tracer.set_string_tag("s3.key", key)
        with tracer:
            yield
    
    @contextmanager  
    def trace_iceberg_operation(self, operation: str, table: str, partition_count: int = 0):
        """Trace Iceberg table operations."""
        if not self.sdk:
            yield
            return
            
        tracer = self.sdk.trace_custom_service(
            service_method=operation,
            service_name="ApacheIceberg"
        )
        tracer.set_string_tag("iceberg.table", table)
        tracer.set_int_tag("iceberg.partitions", partition_count)
        with tracer:
            yield
    
    def report_metric(self, metric_key: str, value: float, dimensions: dict = None):
        """Report custom metrics to Dynatrace."""
        if not self.sdk:
            return
            
        # Custom metrics via SDK
        self.sdk.create_float_gauge(metric_key).set(value, dimensions or {})


# Global tracer instance
_tracer: Optional[DynatraceTracer] = None


def get_tracer() -> DynatraceTracer:
    """Get or initialize the global tracer."""
    global _tracer
    if _tracer is None:
        _tracer = DynatraceTracer()
    return _tracer


def trace_operation(operation_name: str, service: str = "Synergy Data Exchange"):
    """Decorator for tracing function execution."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            tracer = get_tracer()
            if tracer.sdk:
                with tracer.sdk.trace_custom_service(
                    service_method=operation_name,
                    service_name=service
                ):
                    return func(*args, **kwargs)
            return func(*args, **kwargs)
        return wrapper
    return decorator
```

#### 2.3 Integration Points

Instrument these critical paths:

| Component | File | Operations to Trace |
|-----------|------|---------------------|
| Iceberg Reader | `src/core/iceberg_reader.py` | `read_table`, `get_partitions` |
| SQL Writer | `src/core/sql_writer.py` | `bulk_insert`, `create_table` |
| S3 Uploader | `src/fargate/s3_uploader.py` | `upload_file`, `multipart_upload` |
| Artifact Generator | `src/fargate/artifact_generator.py` | `generate_bak`, `generate_mdf` |
| Database Manager | `src/fargate/database_manager.py` | `init_database`, `backup_database` |

---

### Phase 3: Custom Metrics (Week 3-4)

#### 3.1 Business Metrics Definition

| Metric Key | Type | Description | Dimensions |
|------------|------|-------------|------------|
| `Synergy Data Exchange.extraction.duration` | Timer | Total extraction time | tenant_id, exposure_count |
| `Synergy Data Exchange.extraction.rows` | Counter | Rows extracted | tenant_id, table_name |
| `Synergy Data Exchange.artifact.size_mb` | Gauge | Generated artifact size | tenant_id, artifact_type |
| `Synergy Data Exchange.s3.upload_speed_mbps` | Gauge | Upload throughput | tenant_id |
| `Synergy Data Exchange.ingestion.duration` | Timer | Total ingestion time | tenant_id |
| `Synergy Data Exchange.ingestion.tables` | Counter | Tables ingested | tenant_id |

#### 3.2 Metrics Reporter Module

Create `src/core/metrics.py`:

```python
"""
Custom metrics reporting for Dynatrace.
"""
import os
import time
import logging
import requests
from typing import Dict, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class MetricPoint:
    key: str
    value: float
    dimensions: Dict[str, str]
    timestamp: Optional[int] = None


class DynatraceMetrics:
    """Push custom metrics to Dynatrace Metrics API v2."""
    
    def __init__(self):
        self.enabled = os.getenv("DYNATRACE_METRICS_ENABLED", "false").lower() == "true"
        self.endpoint = os.getenv("DYNATRACE_METRICS_ENDPOINT")
        self.api_token = os.getenv("DYNATRACE_API_TOKEN")
        self._buffer: list[MetricPoint] = []
        self._buffer_size = int(os.getenv("DYNATRACE_BUFFER_SIZE", "100"))
    
    def record(self, key: str, value: float, dimensions: Dict[str, str] = None):
        """Record a metric point."""
        if not self.enabled:
            return
            
        self._buffer.append(MetricPoint(
            key=f"custom.Synergy Data Exchange.{key}",
            value=value,
            dimensions=dimensions or {},
            timestamp=int(time.time() * 1000)
        ))
        
        if len(self._buffer) >= self._buffer_size:
            self.flush()
    
    def flush(self):
        """Flush buffered metrics to Dynatrace."""
        if not self._buffer or not self.enabled:
            return
            
        lines = []
        for m in self._buffer:
            dim_str = ",".join(f'{k}="{v}"' for k, v in m.dimensions.items())
            line = f"{m.key},{dim_str} {m.value} {m.timestamp}"
            lines.append(line)
        
        payload = "\n".join(lines)
        
        try:
            response = requests.post(
                f"{self.endpoint}/api/v2/metrics/ingest",
                headers={
                    "Authorization": f"Api-Token {self.api_token}",
                    "Content-Type": "text/plain"
                },
                data=payload,
                timeout=10
            )
            response.raise_for_status()
            logger.debug(f"Flushed {len(self._buffer)} metrics to Dynatrace")
        except Exception as e:
            logger.warning(f"Failed to flush metrics: {e}")
        finally:
            self._buffer.clear()
    
    def timer(self, key: str, dimensions: Dict[str, str] = None):
        """Context manager for timing operations."""
        return MetricTimer(self, key, dimensions)


class MetricTimer:
    """Context manager for timing operations."""
    
    def __init__(self, metrics: DynatraceMetrics, key: str, dimensions: Dict[str, str]):
        self.metrics = metrics
        self.key = key
        self.dimensions = dimensions or {}
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.perf_counter()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (time.perf_counter() - self.start_time) * 1000
        self.dimensions["success"] = str(exc_type is None).lower()
        self.metrics.record(f"{self.key}.duration_ms", duration_ms, self.dimensions)


# Global metrics instance
_metrics: Optional[DynatraceMetrics] = None


def get_metrics() -> DynatraceMetrics:
    global _metrics
    if _metrics is None:
        _metrics = DynatraceMetrics()
    return _metrics
```

---

### Phase 4: Log Integration (Week 4)

#### 4.1 Structured Logging Format

Update logging configuration for Dynatrace log ingestion:

```python
# src/core/logging_config.py
import json
import logging
import os
from datetime import datetime, timezone


class DynatraceJsonFormatter(logging.Formatter):
    """JSON formatter compatible with Dynatrace log ingestion."""
    
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service.name": "Synergy Data Exchange",
            "service.version": os.getenv("APP_VERSION", "1.0.0"),
            "tenant.id": getattr(record, "tenant_id", None),
            "run.id": getattr(record, "run_id", None),
            "trace.id": getattr(record, "trace_id", None),
        }
        
        # Remove None values
        log_entry = {k: v for k, v in log_entry.items() if v is not None}
        
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_entry)


def configure_logging(level: str = "INFO"):
    """Configure logging for Dynatrace compatibility."""
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level))
    
    handler = logging.StreamHandler()
    
    if os.getenv("DYNATRACE_LOGGING_ENABLED", "false").lower() == "true":
        handler.setFormatter(DynatraceJsonFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s"
        ))
    
    root_logger.addHandler(handler)
```

#### 4.2 CloudWatch Log Forwarding

Configure Dynatrace AWS integration to ingest from CloudWatch:

- Log groups: `/ecs/sde/*`, `/aws/batch/ss-cdk*-sde-*`
- Enable ActiveGate for private log streaming

---

## 3. CDK Implementation Tasks

### 3.1 Configuration Model Updates

Add to `SDEConfiguration.cs`:

```csharp
public class DynatraceConfiguration
{
    public bool Enabled { get; set; }
    public string EnvironmentId { get; set; }
    public string PaasTokenSecretArn { get; set; }
    public string ApiTokenSecretArn { get; set; }
    public string OneAgentImage { get; set; } = "docker.io/dynatrace/oneagent:latest";
    public string IngestEndpoint { get; set; }
}
```

### 3.2 IAM Policy Updates

Add Secrets Manager access for Dynatrace tokens:

```json
{
  "Sid": "DynatraceSecretsAccess",
  "Effect": "Allow",
  "Actions": [ "secretsmanager:GetSecretValue" ],
  "Resources": [ "arn:aws:secretsmanager:{region}:{account-id}:secret:ss-cdk{stage}-dynatrace-*" ]
}
```

### 3.3 Environment Variables

Inject into task definitions:

| Variable | Source | Description |
|----------|--------|-------------|
| `DYNATRACE_ENABLED` | Config | Enable/disable Dynatrace |
| `DYNATRACE_ENVIRONMENT_ID` | Secrets Manager | Dynatrace tenant ID |
| `DYNATRACE_API_TOKEN` | Secrets Manager | API token for metrics |
| `DYNATRACE_PAAS_TOKEN` | Secrets Manager | OneAgent download token |
| `DYNATRACE_METRICS_ENDPOINT` | Config | Metrics ingest URL |
| `DYNATRACE_LOGGING_ENABLED` | Config | Enable JSON logging |

---

## 4. Dashboard & Alerting

### 4.1 Recommended Dashboards

1. **Synergy Data Exchange Operations Overview**
   - Extraction/Ingestion success rate
   - Average duration by tenant
   - Active runs count

2. **Infrastructure Health**
   - Fargate task CPU/Memory utilization
   - Batch job queue depth
   - S3 upload latency

3. **Business Metrics**
   - Data volume extracted (GB/day)
   - Rows processed per tenant
   - Artifact generation frequency

### 4.2 Recommended Alerts

| Alert | Condition | Severity |
|-------|-----------|----------|
| Extraction Failure | Success rate < 95% over 15min | Critical |
| Long Running Task | Duration > 2 hours | Warning |
| High Memory Usage | > 85% memory for 10min | Warning |
| S3 Upload Failures | Any upload error | Critical |
| Database Connection Error | SQL connectivity loss | Critical |

---

## 5. Rollout Plan

| Week | Phase | Deliverables |
|------|-------|--------------|
| 1 | Setup | Dynatrace environment, secrets, IAM permissions |
| 2 | Infrastructure | OneAgent sidecar deployment, CloudWatch integration |
| 3 | Application | Python SDK integration, tracing instrumentation |
| 4 | Metrics & Logs | Custom metrics, structured logging, dashboards |
| 5 | Alerting | Alert rules, runbooks, team training |

---

## 6. Prerequisites

- [ ] Dynatrace SaaS or Managed environment provisioned
- [ ] PaaS token generated with `InstallerDownload` scope
- [ ] API token generated with `metrics.ingest`, `logs.ingest` scopes
- [ ] Secrets created in AWS Secrets Manager
- [ ] Network connectivity from Fargate/Batch to Dynatrace endpoints

---

## 7. Testing Strategy

1. **Dev Environment**: Full integration with verbose logging
2. **QA Environment**: Performance baseline comparison (with/without Dynatrace)
3. **Production**: Gradual rollout (10% → 50% → 100% of tasks)

---

## 8. Cost Considerations

| Component | Estimated Monthly Cost |
|-----------|------------------------|
| Dynatrace Host Units (Fargate) | ~$30/host unit |
| Custom Metrics Ingestion | ~$0.001/metric |
| Log Ingestion | ~$0.20/GB |
| **Estimated Total** | **$100-500/month** (depends on volume) |

---

## References

- [Dynatrace OneAgent for AWS Fargate](https://www.dynatrace.com/support/help/setup-and-configuration/setup-on-container-platforms/aws/aws-fargate)
- [Dynatrace Python OneAgent SDK](https://www.dynatrace.com/support/help/extend-dynatrace/oneagent-sdk/oneagent-sdk-python)
- [Dynatrace Metrics API v2](https://www.dynatrace.com/support/help/dynatrace-api/environment-api/metric-v2)
- [Dynatrace Log Monitoring](https://www.dynatrace.com/support/help/observe-and-explore/logs)
