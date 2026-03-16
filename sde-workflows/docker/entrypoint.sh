#!/bin/bash
# =============================================================================
# Entrypoint script for bidirectional data pipeline
# Supports all three paths:
#   PATH 1: Express (≤9GB) - Fargate
#   PATH 2: Standard (9-80GB) - Fargate
#   PATH 3: Standard (>80GB) - AWS Batch
# =============================================================================

set -e

# Pipeline mode: extraction or ingestion
PIPELINE_MODE=${PIPELINE_MODE:-extraction}

# Path identifier (set by Dockerfile)
PIPELINE_PATH=${PIPELINE_PATH:-UNKNOWN}

echo "=============================================="
echo "Data Pipeline Starting"
echo "=============================================="
echo "Mode: $PIPELINE_MODE"
echo "Path: $PIPELINE_PATH"
echo "SQL Edition: ${MSSQL_PID:-Standard}"
echo "=============================================="

# =============================================================================
# Start SQL Server
# =============================================================================
echo "Starting SQL Server (${MSSQL_PID:-Standard})..."

# Start SQL Server in background
/opt/mssql/bin/sqlservr &
SQL_PID=$!

# Wait for SQL Server to be ready
echo "Waiting for SQL Server to become available..."
STARTUP_TIMEOUT=${STARTUP_TIMEOUT:-120}
SQLCMD_PATH="/opt/mssql-tools18/bin/sqlcmd"

# Fallback to older path if needed
if [ ! -f "$SQLCMD_PATH" ]; then
    SQLCMD_PATH="/opt/mssql-tools/bin/sqlcmd"
fi

for i in $(seq 1 $STARTUP_TIMEOUT); do
    if $SQLCMD_PATH -S localhost -U sa -P "$SA_PASSWORD" -C -Q "SELECT 1" > /dev/null 2>&1; then
        echo "SQL Server is ready (took ${i}s)"
        break
    fi
    
    # Check if SQL Server process is still running
    if ! kill -0 $SQL_PID 2>/dev/null; then
        echo "ERROR: SQL Server process died unexpectedly"
        exit 1
    fi
    
    sleep 1
done

if [ $i -eq $STARTUP_TIMEOUT ]; then
    echo "ERROR: SQL Server did not start within ${STARTUP_TIMEOUT} seconds"
    exit 1
fi

# =============================================================================
# Path-specific initialization
# =============================================================================
case "$PIPELINE_PATH" in
    PATH1_EXPRESS)
        echo "PATH 1: Express edition - no additional configuration needed"
        ;;
    PATH2_STANDARD)
        echo "PATH 2: Standard edition - enabling compression"
        $SQLCMD_PATH -S localhost -U sa -P "$SA_PASSWORD" -C -Q "EXEC sp_configure 'backup compression default', 1; RECONFIGURE;" 2>/dev/null || true
        ;;
    PATH3_BATCH)
        echo "PATH 3: Batch - running optimization script"
        if [ -f /batch_init.sql ]; then
            $SQLCMD_PATH -S localhost -U sa -P "$SA_PASSWORD" -C -i /batch_init.sql 2>/dev/null || echo "Warning: batch_init.sql execution had errors"
        fi
        ;;
    *)
        echo "Unknown path: $PIPELINE_PATH - proceeding with defaults"
        ;;
esac

# =============================================================================
# Report resource allocation
# =============================================================================
echo ""
echo "Resource Allocation:"
echo "  CPU cores: $(nproc)"
echo "  Memory: $(free -h | awk '/^Mem:/ {print $2}')"
echo "  Disk space (/data): $(df -h /data 2>/dev/null | awk 'NR==2 {print $4}' || echo 'N/A')"
echo ""

# =============================================================================
# Run the appropriate pipeline
# =============================================================================
case "$PIPELINE_MODE" in
    extraction)
        echo "Starting EXTRACTION pipeline (Iceberg -> SQL Server -> MDF)..."
        python3 -m src.fargate.entrypoint "$@"
        EXIT_CODE=$?
        ;;
    ingestion)
        echo "Starting INGESTION pipeline (MDF -> SQL Server -> Iceberg)..."
        python3 -m src.fargate.ingestion_entrypoint "$@"
        EXIT_CODE=$?
        ;;
    *)
        echo "ERROR: Unknown pipeline mode: $PIPELINE_MODE"
        echo "Supported modes: extraction, ingestion"
        exit 1
        ;;
esac

# =============================================================================
# Cleanup
# =============================================================================
echo ""
echo "=============================================="
echo "Pipeline completed with exit code: $EXIT_CODE"
echo "=============================================="

# Gracefully stop SQL Server
echo "Stopping SQL Server..."
$SQLCMD_PATH -S localhost -U sa -P "$SA_PASSWORD" -C -Q "SHUTDOWN WITH NOWAIT" 2>/dev/null || true

# Wait for SQL Server to stop
wait $SQL_PID 2>/dev/null || true

exit $EXIT_CODE
