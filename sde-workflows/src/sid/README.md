# SID Transformation Module

In-memory Source Identifier (SID) transformation for the Synergy Data Exchange ingestion pipeline.

## Overview

When migrating data between environments (e.g., tenant isolation), surrogate keys (SIDs) must be remapped to avoid collisions. This module handles:

- **Range Allocation**: Request contiguous SID ranges from central SID API
- **In-Memory Transform**: Transform PyArrow tables without modifying source SQL
- **FK Cascade**: Update foreign key columns based on parent→child relationships
- **Self-References**: Handle self-referencing tables (e.g., `tLocation.ParentLocationSID`)

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Synergy Data Exchange Pipeline                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   SQL Server ──► PyArrow ──► SID Transform ──► Iceberg                 │
│                                    │                                    │
│                                    ▼                                    │
│                          ┌─────────────────┐                           │
│                          │   SID API       │                           │
│                          │   (Central)     │                           │
│                          └─────────────────┘                           │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Components

### `transformer.py` - Core Transformation Logic

```python
from src.sid import SIDTransformer, MockSIDClient

# Create transformer with mock client (for testing)
client = MockSIDClient(starting_sid=1_000_000)
transformer = SIDTransformer(client)

# Transform a single table
result = transformer.transform_table("tCompany", arrow_table)

# Transform all tables in dependency order
job_result = transformer.transform_all(tables_dict)

# Access transformed tables
transformed = transformer._transformed_tables["tCompany"]

# Access SID mappings (old → new)
mapping = transformer.get_mapping("tCompany")
```

### `sid_client.py` - SID API Client

**MockSIDClient** - For testing:
```python
from src.sid import MockSIDClient

client = MockSIDClient(starting_sid=5_000_000)
response = client.allocate("tCompany", count=100)
# response.start = 5_000_000, response.count = 100
```

**SIDClient** - Production with full auth flow:
```python
from src.sid import SIDClient

client = SIDClient(
    sid_api_url="https://sid-api.verisk.com",
    tenant_context_lambda_name="rs-cdkdev-ap-tenant-context-lambda",
    app_token_secret_name="rs-cdkdev-app-token-secret",
    max_retries=3,
)
response = client.allocate("tCompany", count=100)
# Calls: POST {sid_api_url}/tCompany?count=100
#        Authorization: Bearer <jwt>
```

**Authentication Flow:**
```
Secrets Manager (App Token)
        ↓
Tenant Context Lambda (existing AWS service)
        ↓ returns: { clientID, clientSecret }
Okta Token API
        ↓ returns: Bearer token
SID API
```

### `config.py` - Table Configuration

Contains FK relationships and processing order derived from AIRExposure MDF analysis:

```python
from src.sid import SID_CONFIG, get_processing_order

# Get table config
config = SID_CONFIG["tExposureSet"]
# config.pk_column = "ExposureSetSID"
# config.fk_columns = {"CompanySID": "tCompany"}

# Get tables in dependency order
order = get_processing_order()
# ['tCompany', 'tLocation', ..., 'tContractLayer', ...]
```

**Processing Levels:**
| Level | Tables | Description |
|-------|--------|-------------|
| 0 | tCompany, tLocation, tCurrency, ... | Root tables (no internal FK deps) |
| 1 | tExposureSet, tPolicy, ... | Depend on Level 0 |
| 2 | tContract, tReinsuranceContract, ... | Depend on Level 1 |
| 3 | tLayer, tContractCoverage, ... | Depend on Level 2 |
| 4 | tLayerCondition, tLayerLimit, ... | Depend on Level 3 |
| 5 | tContractLayerExposureSet, ... | Junction tables |

### `metrics.py` - Telemetry

```python
from src.sid import MetricsCollector, timed_operation

# Create collector (optionally emit to CloudWatch)
metrics = MetricsCollector(
    namespace="SDE/SID",
    emit_to_cloudwatch=True,
)

# Track a job
job = metrics.start_job(job_id="run-123", tenant_id="tenant-456")
job.add_allocation("tCompany", sid_start=1000000, sid_end=1000099, row_count=100, duration_ms=50.5)
job.add_transform("tCompany", rows_processed=100, pk_column="CompanySID", fk_columns_updated=0, duration_ms=25.3)
metrics.complete_job()

# Use timed operations
with timed_operation("transform_table") as timer:
    do_work()
print(f"Duration: {timer.duration_ms}ms")
```

### `exposure_client.py` - Exposure Management API

Integrates with Exposure Management API to create new ExposureSet/ExposureView entities:

```python
from src.sid import create_exposure_client, extract_exposure_metadata

# Create client (mock for testing)
client = create_exposure_client(use_mock=True, mock_starting_sid=9_000_000)

# Or production client — fetches clientId/clientSecret from Tenant Context Lambda,
# calls IDENTITY_PROVIDER_TOKEN_ENDPOINT to get a bearer JWT, uses it automatically.
client = create_exposure_client(
    api_url="https://exposure-api.example.com",
    tenant_id="tenant-uuid",
    tenant_context_lambda_name="rs-cdkdev-ap-tenant-context-lambda",
    app_token_secret_name="rs-cdkdev-app-token-secret",
)

# Extract metadata from ingested tables
metadata = extract_exposure_metadata(
    tables_data={"tExposureSet": arrow_table, "tExposureView": view_table},
    tenant_id="tenant-123",
    run_id="run-456",
    source_artifact="backup.bak",
)

# Call API to create new exposure entities
response = client.create_exposure(metadata)
print(f"New ExposureSetSID: {response.exposure_set_sid}")
print(f"New ExposureViewSID: {response.exposure_view_sid}")
```

**Override SID Mapping:**

When Exposure API returns new SIDs, set them as overrides on the transformer:

```python
# All old ExposureSetSIDs will map to this single new SID
transformer.set_override_sid("tExposureSet", response.exposure_set_sid)

# Transform tables - FK references cascade to override SID
transformer.transform_table("tContract", contract_table)
# All tContract.ExposureSetSID values now point to the new SID
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SID_ENABLED` | Enable SID transformation | `false` |
| `SID_API_URL` | Base URL of SID API | - |
| `TENANT_CONTEXT_LAMBDA` | Lambda function name | `rs-cdkdev-ap-tenant-context-lambda` |
| `APP_TOKEN_SECRET` | Secrets Manager secret name | `rs-cdkdev-app-token-secret` |
| `IDENTITY_PROVIDER_TOKEN_ENDPOINT` | Full Okta token endpoint URL | `https://sso-dev-na.../v1/token` |
| `SID_MAX_RETRIES` | Max retry attempts | `3` |
| `SID_USE_MOCK` | Use mock client | `false` |
| `SID_MOCK_STARTING_SID` | Mock starting SID | `1000000` |
| `EXPOSURE_API_URL` | Exposure Management API URL | - |

## Self-Referencing Tables

Three tables have self-referencing FKs that require special handling:

| Table | FK Column | Description |
|-------|-----------|-------------|
| `tLocation` | `ParentLocationSID` | Location hierarchy |
| `tLayerCondition` | `ParentLayerConditionSID` | Condition hierarchy |
| `tCompanyLossAssociation` | Self-reference | Association hierarchy |

These are handled with a two-pass approach:
1. First pass: Update PK column, build mapping
2. Second pass: Update self-reference FK using the mapping

## Testing

```bash
# Unit tests - SID transformer (29 tests)
python -m pytest tests/unit/test_sid_transformer.py -v

# Unit tests - Exposure client (20 tests)
python -m pytest tests/unit/test_exposure_client.py -v

# Unit tests - Enhancements (24 tests)
python -m pytest tests/unit/test_enhancements.py -v

# All unit tests (88 tests)
python -m pytest tests/unit/ --ignore=tests/unit/test_column_mapper.py -v

# All SID-related tests
python -m pytest tests/ -k "sid or exposure" -v
```

## Performance

Tested with 16K+ rows across 4 tables:
- **Throughput**: ~100K rows/second
- **Memory**: O(N) for SID mappings, O(1) for streaming transforms
- **API Calls**: 1 call per table (range allocation)

## Error Handling

- **Retry Logic**: Exponential backoff for transient API failures
- **Auth Refresh**: Automatic token refresh on 401/403
- **Graceful Degradation**: Skip unknown tables, log warnings for missing columns
- **Validation**: Verify FK integrity before/after transformation
