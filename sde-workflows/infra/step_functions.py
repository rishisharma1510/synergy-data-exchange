"""
Step Functions infrastructure for bidirectional data pipeline.

Defines state machines for both extraction and ingestion activities:
- Extraction: Iceberg → SQL Server → BAK → Tenant S3
- Ingestion: Tenant S3 → BAK → SQL Server → Iceberg

Three-Path Architecture (size-based routing):
- PATH 1: ≤ threshold_small_gb → Fargate + SQL Express (FREE)
- PATH 2: threshold_small_gb < size ≤ threshold_large_gb → Fargate + SQL Standard
- PATH 3: > threshold_large_gb → AWS Batch EC2 + SQL Standard

State machines are triggered by Activity Management.

Input Parameters:
- tables: "all" or JSON array of table names to process (e.g., ["TContract", "TExposure"])
- create_if_not_exists: (Ingestion only) Boolean flag to auto-create Iceberg tables if missing
"""

import json
from typing import Any

# Default size thresholds (GB) - configurable
DEFAULT_THRESHOLD_SMALL_GB = 9   # ≤9GB uses Fargate + Express (FREE)
DEFAULT_THRESHOLD_LARGE_GB = 80  # >80GB uses AWS Batch EC2


def _get_table_selection_env_vars() -> list[dict]:
    """Get environment variables for table selection (used by all state machines)."""
    return [
        {
            "Name": "TABLES",
            "Value.$": "States.JsonToString($.tables)",
        },
        {
            "Name": "CREATE_IF_NOT_EXISTS",
            "Value.$": "States.Format('{}', $.create_if_not_exists)",
        },
    ]


def _check_records_states(lambda_arn: str, next_state: str) -> dict:
    """
    Return the two shared pre-check states injected at the top of every extraction
    state machine:  CheckRecords → EvaluateRecordCount → (next_state | NoRecordsFound).
    """
    return {
        "CheckRecords": {
            "Type": "Task",
            "Comment": "Invoke Lambda to count Iceberg records for the given exposure IDs. Skips ECS if none found.",
            "Resource": "arn:aws:states:::lambda:invoke",
            "Parameters": {
                "FunctionName": lambda_arn,
                "Payload.$": "$",
            },
            "ResultSelector": {
                "result.$": "$.Payload",
            },
            "ResultPath": "$.lambda_result",
            "OutputPath": "$.lambda_result.result",
            "Retry": [
                {
                    "ErrorEquals": ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.TooManyRequestsException"],
                    "IntervalSeconds": 5,
                    "MaxAttempts": 2,
                    "BackoffRate": 2.0,
                }
            ],
            "Catch": [
                {
                    "ErrorEquals": ["States.ALL"],
                    "Comment": "On any Lambda error, proceed to extraction (fail-safe)",
                    "Next": next_state,
                }
            ],
            "Next": "EvaluateRecordCount",
        },
        "EvaluateRecordCount": {
            "Type": "Choice",
            "Comment": "Exit early when CheckRecords Lambda found no data to extract.",
            "Choices": [
                {
                    "Variable": "$.record_check.has_records",
                    "BooleanEquals": False,
                    "Next": "NoRecordsFound",
                }
            ],
            "Default": next_state,
        },
        "NoRecordsFound": {
            "Type": "Succeed",
            "Comment": "No Iceberg records matched the requested exposure IDs — nothing to extract.",
        },
    }


def get_extraction_state_machine_definition(
    cluster_arn: str,
    task_definition_arn: str,
    subnets: list[str],
    security_groups: list[str],
    check_records_lambda_arn: str = "",
) -> dict[str, Any]:
    """
    Get the state machine definition for extraction activity.
    
    Parameters
    ----------
    cluster_arn : str
        ECS cluster ARN.
    task_definition_arn : str
        Fargate task definition ARN.
    subnets : list[str]
        VPC subnet IDs.
    security_groups : list[str]
        Security group IDs.
    check_records_lambda_arn : str
        ARN of the CheckRecords Lambda. When provided the state machine starts
        with a Lambda check and exits early (Succeed) if no records are found,
        saving the cost of an ECS launch.
    
    Returns
    -------
    dict
        State machine definition in ASL (Amazon States Language).
    """
    start_at = "CheckRecords" if check_records_lambda_arn else "RunExtractionTask"
    extra_states = _check_records_states(check_records_lambda_arn, "RunExtractionTask") if check_records_lambda_arn else {}

    return {
        "Comment": "Extraction State Machine - Iceberg to SQL Server to BAK",
        "StartAt": start_at,
        "States": {
            **extra_states,
            "RunExtractionTask": {
                "Type": "Task",
                "Resource": "arn:aws:states:::ecs:runTask.sync",
                "Parameters": {
                    "Cluster": cluster_arn,
                    "TaskDefinition": task_definition_arn,
                    "LaunchType": "FARGATE",
                    "NetworkConfiguration": {
                        "AwsvpcConfiguration": {
                            "Subnets": subnets,
                            "SecurityGroups": security_groups,
                            "AssignPublicIp": "DISABLED",
                        }
                    },
                    "Overrides": {
                        "ContainerOverrides": [
                            {
                                "Name": "pipeline",
                                "Environment": [
                                    {
                                        "Name": "PIPELINE_MODE",
                                        "Value": "extraction",
                                    },
                                    {
                                        "Name": "TENANT_ID",
                                        "Value.$": "$.tenant_id",
                                    },
                                    {
                                        "Name": "RUN_ID",
                                        "Value.$": "$.run_id",
                                    },
                                    {
                                        "Name": "ACTIVITY_ID",
                                        "Value.$": "$.activity_id",
                                    },
                                    {
                                        "Name": "CALLBACK_URL",
                                        "Value.$": "$.callback_url",
                                    },
                                    {
                                        "Name": "TENANT_BUCKET",
                                        "Value.$": "$.tenant_bucket",
                                    },
                                    {
                                        "Name": "EXPOSURE_IDS",
                                        "Value.$": "States.JsonToString($.exposure_ids)",
                                    },
                                    {
                                        "Name": "ARTIFACT_TYPE",
                                        "Value.$": "$.artifact_type",
                                    },
                                    {
                                        "Name": "TABLES",
                                        "Value.$": "States.JsonToString($.tables)",
                                    },
                                ],
                            }
                        ]
                    },
                },
                "Retry": [
                    {
                        "ErrorEquals": ["States.TaskFailed"],
                        "IntervalSeconds": 30,
                        "MaxAttempts": 2,
                        "BackoffRate": 2.0,
                    }
                ],
                "Catch": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "Next": "ExtractionFailed",
                    }
                ],
                "End": True,
            },
            "ExtractionFailed": {
                "Type": "Fail",
                "Error": "ExtractionTaskFailed",
                "Cause": "Extraction Fargate task failed",
            },
        },
    }


def get_ingestion_state_machine_definition(
    cluster_arn: str,
    task_definition_arn: str,
    subnets: list[str],
    security_groups: list[str],
    sid_use_mock: bool = False,
) -> dict[str, Any]:
    """
    Get the state machine definition for ingestion activity.
    
    Parameters
    ----------
    cluster_arn : str
        ECS cluster ARN.
    task_definition_arn : str
        Fargate task definition ARN.
    subnets : list[str]
        VPC subnet IDs.
    security_groups : list[str]
        Security group IDs.
    sid_use_mock : bool
        Use mock SID client for testing (SID transformation is always enabled).
    
    Returns
    -------
    dict
        State machine definition in ASL (Amazon States Language).
    """
    # Base environment variables
    environment = [
        {
            "Name": "PIPELINE_MODE",
            "Value": "ingestion",
        },
        {
            "Name": "TENANT_ID",
            "Value.$": "$.tenant_id",
        },
        {
            "Name": "RUN_ID",
            "Value.$": "$.run_id",
        },
        {
            "Name": "ACTIVITY_ID",
            "Value.$": "$.activity_id",
        },
        {
            "Name": "CALLBACK_URL",
            "Value.$": "$.callback_url",
        },
        {
            "Name": "SOURCE_S3_BUCKET",
            "Value.$": "$.source_s3_bucket",
        },
        {
            "Name": "SOURCE_S3_KEY",
            "Value.$": "$.source_s3_key",
        },
        {
            "Name": "TARGET_NAMESPACE",
            "Value.$": "$.target_namespace",
        },
        {
            "Name": "WRITE_MODE",
            "Value.$": "$.write_mode",
        },
        {
            "Name": "CHECKSUM",
            "Value.$": "$.checksum",
        },
        # Table selection configuration
        {
            "Name": "TABLES",
            "Value.$": "States.JsonToString($.tables)",
        },
        {
            "Name": "CREATE_IF_NOT_EXISTS",
            "Value.$": "States.Format('{}', $.create_if_not_exists)",
        },
        # SID transformation is always enabled
        {
            "Name": "SID_USE_MOCK",
            "Value": str(sid_use_mock).lower(),
        },
    ]
    
    return {
        "Comment": "Ingestion State Machine - Tenant S3 to SQL Server to Iceberg",
        "StartAt": "RunIngestionTask",
        "States": {
            "RunIngestionTask": {
                "Type": "Task",
                "Resource": "arn:aws:states:::ecs:runTask.sync",
                "Parameters": {
                    "Cluster": cluster_arn,
                    "TaskDefinition": task_definition_arn,
                    "LaunchType": "FARGATE",
                    "NetworkConfiguration": {
                        "AwsvpcConfiguration": {
                            "Subnets": subnets,
                            "SecurityGroups": security_groups,
                            "AssignPublicIp": "DISABLED",
                        }
                    },
                    "Overrides": {
                        "ContainerOverrides": [
                            {
                                "Name": "pipeline",
                                "Environment": environment,
                            }
                        ]
                    },
                },
                "Retry": [
                    {
                        "ErrorEquals": ["States.TaskFailed"],
                        "IntervalSeconds": 30,
                        "MaxAttempts": 2,
                        "BackoffRate": 2.0,
                    }
                ],
                "Catch": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "Next": "IngestionFailed",
                    }
                ],
                "End": True,
            },
            "IngestionFailed": {
                "Type": "Fail",
                "Error": "IngestionTaskFailed",
                "Cause": "Ingestion Fargate task failed",
            },
        },
    }


def get_bidirectional_state_machine_definition(
    cluster_arn: str,
    task_definition_arn: str,
    subnets: list[str],
    security_groups: list[str],
    sid_use_mock: bool = False,
    check_records_lambda_arn: str = "",
) -> dict[str, Any]:
    """
    Get a combined state machine that handles both extraction and ingestion.
    
    Uses a Choice state to route to the appropriate task based on activity_type.
    
    Parameters
    ----------
    cluster_arn : str
        ECS cluster ARN.
    task_definition_arn : str
        Fargate task definition ARN.
    subnets : list[str]
        VPC subnet IDs.
    security_groups : list[str]
        Security group IDs.
    sid_use_mock : bool
        Use mock SID client (SID transformation is always enabled).
    check_records_lambda_arn : str
        ARN of the CheckRecords Lambda for the extraction branch. When provided,
        the extraction path checks for records before launching ECS.
    
    Returns
    -------
    dict
        State machine definition in ASL.
    """
    extraction_def = get_extraction_state_machine_definition(
        cluster_arn, task_definition_arn, subnets, security_groups,
        check_records_lambda_arn=check_records_lambda_arn,
    )
    ingestion_def = get_ingestion_state_machine_definition(
        cluster_arn, task_definition_arn, subnets, security_groups,
        sid_use_mock=sid_use_mock,
    )

    extraction_start = extraction_def["StartAt"]  # "CheckRecords" or "RunExtractionTask"
    extra_extraction_states = {
        k: v for k, v in extraction_def["States"].items()
        if k not in ("RunExtractionTask", "ExtractionFailed")
    }

    return {
        "Comment": "Bidirectional Pipeline - Extraction and Ingestion",
        "StartAt": "DetermineActivityType",
        "States": {
            "DetermineActivityType": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.activity_type",
                        "StringEquals": "extraction",
                        "Next": extraction_start,
                    },
                    {
                        "Variable": "$.activity_type",
                        "StringEquals": "ingestion",
                        "Next": "RunIngestionTask",
                    },
                ],
                "Default": "InvalidActivityType",
            },
            **extra_extraction_states,
            "RunExtractionTask": extraction_def["States"]["RunExtractionTask"],
            "RunIngestionTask": ingestion_def["States"]["RunIngestionTask"],
            "ExtractionFailed": extraction_def["States"]["ExtractionFailed"],
            "IngestionFailed": ingestion_def["States"]["IngestionFailed"],
            "InvalidActivityType": {
                "Type": "Fail",
                "Error": "InvalidActivityType",
                "Cause": "activity_type must be 'extraction' or 'ingestion'",
            },
        },
    }


def export_state_machine_json(definition: dict, filepath: str) -> None:
    """Export state machine definition to JSON file."""
    with open(filepath, "w") as f:
        json.dump(definition, f, indent=2)


def get_three_path_extraction_definition(
    fargate_express_cluster_arn: str,
    fargate_express_task_arn: str,
    fargate_standard_cluster_arn: str,
    fargate_standard_task_arn: str,
    batch_job_queue_arn: str,
    batch_job_definition_arn: str,
    subnets: list[str],
    security_groups: list[str],
    threshold_small_gb: float = DEFAULT_THRESHOLD_SMALL_GB,
    threshold_large_gb: float = DEFAULT_THRESHOLD_LARGE_GB,
    check_records_lambda_arn: str = "",
) -> dict[str, Any]:
    """
    Get the three-path extraction state machine definition with configurable thresholds.
    
    Parameters
    ----------
    fargate_express_cluster_arn : str
        ECS cluster ARN for Fargate + Express path.
    fargate_express_task_arn : str
        Task definition ARN for Fargate + Express (PATH 1).
    fargate_standard_cluster_arn : str
        ECS cluster ARN for Fargate + Standard path.
    fargate_standard_task_arn : str
        Task definition ARN for Fargate + Standard (PATH 2).
    batch_job_queue_arn : str
        AWS Batch job queue ARN for large datasets.
    batch_job_definition_arn : str
        AWS Batch job definition ARN (PATH 3).
    subnets : list[str]
        VPC subnet IDs.
    security_groups : list[str]
        Security group IDs.
    threshold_small_gb : float
        Size threshold for PATH 1 (Fargate + Express). Default: 7 GB.
    threshold_large_gb : float
        Size threshold for PATH 3 (Batch EC2). Default: 60 GB.
    check_records_lambda_arn : str
        ARN of the CheckRecords Lambda. When provided the state machine starts
        with a Lambda check and exits early (Succeed) if no records are found,
        saving the cost of an ECS/Batch launch.
    
    Returns
    -------
    dict
        State machine definition in ASL with size-based routing.
    """
    start_at = "CheckRecords" if check_records_lambda_arn else "RouteBySize"
    extra_states = _check_records_states(check_records_lambda_arn, "RouteBySize") if check_records_lambda_arn else {}

    return {
        "Comment": f"Extraction - Three-Path (thresholds: {threshold_small_gb}GB, {threshold_large_gb}GB)",
        "StartAt": start_at,
        "States": {
            **extra_states,
            "RouteBySize": {
                "Type": "Choice",
                "Comment": f"Route based on estimated_size_gb: ≤{threshold_small_gb}→Express, ≤{threshold_large_gb}→Standard, else→Batch",
                "Choices": [
                    {
                        "Variable": "$.estimated_size_gb",
                        "NumericLessThanEquals": threshold_small_gb,
                        "Next": "RunFargateExpress",
                    },
                    {
                        "Variable": "$.estimated_size_gb",
                        "NumericLessThanEquals": threshold_large_gb,
                        "Next": "RunFargateStandard",
                    },
                ],
                "Default": "RunBatchEC2",
            },
            "RunFargateExpress": {
                "Type": "Task",
                "Comment": "PATH 1: Fargate + SQL Express (FREE license, ≤7GB)",
                "Resource": "arn:aws:states:::ecs:runTask.sync",
                "Parameters": {
                    "Cluster": fargate_express_cluster_arn,
                    "TaskDefinition": fargate_express_task_arn,
                    "LaunchType": "FARGATE",
                    "NetworkConfiguration": {
                        "AwsvpcConfiguration": {
                            "Subnets": subnets,
                            "SecurityGroups": security_groups,
                            "AssignPublicIp": "DISABLED",
                        }
                    },
                    "Overrides": {
                        "ContainerOverrides": [
                            {
                                "Name": "pipeline",
                                "Environment": [
                                    {"Name": "PIPELINE_MODE", "Value": "extraction"},
                                    {"Name": "MSSQL_PID", "Value": "Express"},
                                    {"Name": "TENANT_ID", "Value.$": "$.tenant_id"},
                                    {"Name": "RUN_ID", "Value.$": "$.run_id"},
                                    {"Name": "ACTIVITY_ID", "Value.$": "$.activity_id"},
                                    {"Name": "CALLBACK_URL", "Value.$": "$.callback_url"},
                                    {"Name": "TENANT_BUCKET", "Value.$": "$.tenant_bucket"},
                                    {"Name": "EXPOSURE_IDS", "Value.$": "States.JsonToString($.exposure_ids)"},
                                    {"Name": "ARTIFACT_TYPE", "Value.$": "$.artifact_type"},
                                    {"Name": "TABLES", "Value.$": "States.JsonToString($.tables)"},
                                ],
                            }
                        ]
                    },
                },
                "Retry": [{"ErrorEquals": ["States.TaskFailed"], "IntervalSeconds": 30, "MaxAttempts": 2, "BackoffRate": 2.0}],
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "ExtractionFailed"}],
                "End": True,
            },
            "RunFargateStandard": {
                "Type": "Task",
                "Comment": "PATH 2: Fargate + SQL Standard (Licensed, 7-60GB)",
                "Resource": "arn:aws:states:::ecs:runTask.sync",
                "Parameters": {
                    "Cluster": fargate_standard_cluster_arn,
                    "TaskDefinition": fargate_standard_task_arn,
                    "LaunchType": "FARGATE",
                    "NetworkConfiguration": {
                        "AwsvpcConfiguration": {
                            "Subnets": subnets,
                            "SecurityGroups": security_groups,
                            "AssignPublicIp": "DISABLED",
                        }
                    },
                    "Overrides": {
                        "ContainerOverrides": [
                            {
                                "Name": "pipeline",
                                "Environment": [
                                    {"Name": "PIPELINE_MODE", "Value": "extraction"},
                                    {"Name": "MSSQL_PID", "Value": "Standard"},
                                    {"Name": "TENANT_ID", "Value.$": "$.tenant_id"},
                                    {"Name": "RUN_ID", "Value.$": "$.run_id"},
                                    {"Name": "ACTIVITY_ID", "Value.$": "$.activity_id"},
                                    {"Name": "CALLBACK_URL", "Value.$": "$.callback_url"},
                                    {"Name": "TENANT_BUCKET", "Value.$": "$.tenant_bucket"},
                                    {"Name": "EXPOSURE_IDS", "Value.$": "States.JsonToString($.exposure_ids)"},
                                    {"Name": "ARTIFACT_TYPE", "Value.$": "$.artifact_type"},
                                    {"Name": "TABLES", "Value.$": "States.JsonToString($.tables)"},
                                ],
                            }
                        ]
                    },
                },
                "Retry": [{"ErrorEquals": ["States.TaskFailed"], "IntervalSeconds": 30, "MaxAttempts": 2, "BackoffRate": 2.0}],
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "ExtractionFailed"}],
                "End": True,
            },
            "RunBatchEC2": {
                "Type": "Task",
                "Comment": "PATH 3: AWS Batch EC2 + SQL Standard (Licensed, >60GB)",
                "Resource": "arn:aws:states:::batch:submitJob.sync",
                "Parameters": {
                    "JobName.$": "States.Format('extraction-{}', $.run_id)",
                    "JobQueue": batch_job_queue_arn,
                    "JobDefinition": batch_job_definition_arn,
                    "ContainerOverrides": {
                        "Environment": [
                            {"Name": "PIPELINE_MODE", "Value": "extraction"},
                            {"Name": "MSSQL_PID", "Value": "Standard"},
                            {"Name": "TENANT_ID", "Value.$": "$.tenant_id"},
                            {"Name": "RUN_ID", "Value.$": "$.run_id"},
                            {"Name": "ACTIVITY_ID", "Value.$": "$.activity_id"},
                            {"Name": "CALLBACK_URL", "Value.$": "$.callback_url"},
                            {"Name": "TENANT_BUCKET", "Value.$": "$.tenant_bucket"},
                            {"Name": "EXPOSURE_IDS", "Value.$": "States.JsonToString($.exposure_ids)"},
                            {"Name": "ARTIFACT_TYPE", "Value.$": "$.artifact_type"},
                            {"Name": "TABLES", "Value.$": "States.JsonToString($.tables)"},
                        ],
                    },
                },
                "Retry": [{"ErrorEquals": ["States.TaskFailed"], "IntervalSeconds": 60, "MaxAttempts": 2, "BackoffRate": 2.0}],
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "ExtractionFailed"}],
                "End": True,
            },
            "ExtractionFailed": {
                "Type": "Fail",
                "Error": "ExtractionTaskFailed",
                "Cause": "Extraction task failed",
            },
        },
    }


def get_three_path_ingestion_definition(
    fargate_express_cluster_arn: str,
    fargate_express_task_arn: str,
    fargate_standard_cluster_arn: str,
    fargate_standard_task_arn: str,
    batch_job_queue_arn: str,
    batch_job_definition_arn: str,
    subnets: list[str],
    security_groups: list[str],
    size_estimator_lambda_arn: str,
    threshold_small_gb: float = DEFAULT_THRESHOLD_SMALL_GB,
    threshold_large_gb: float = DEFAULT_THRESHOLD_LARGE_GB,
) -> dict[str, Any]:
    """
    Get the three-path ingestion state machine definition.

    The first state calls the size_estimator Lambda with mode='ingestion';
    the Lambda does s3:HeadObject to measure the file and returns selected_path
    (EXPRESS/STANDARD/BATCH) along with sde_context (tenant role, glue db, etc.).
    The caller does NOT need to supply artifact_size_gb — it is measured automatically.

    Parameters
    ----------
    size_estimator_lambda_arn : str
        ARN of the size-estimator Lambda (same one used by extraction).
    threshold_small_gb : float
        Size threshold for PATH 1 (Fargate + Express).
    threshold_large_gb : float
        Size threshold for PATH 3 (Batch EC2).

    Returns
    -------
    dict
        State machine definition in ASL.
    """
    def _container_env(mssql_pid: str) -> list:
        """Shared container env vars injected into every ECS/Batch task."""
        return [
            {"Name": "PIPELINE_MODE",         "Value":  "ingestion"},
            {"Name": "MSSQL_PID",             "Value":  mssql_pid},
            {"Name": "TENANT_ID",             "Value.$": "$.tenant_id"},
            {"Name": "RUN_ID",                "Value.$": "$.run_id"},
            {"Name": "ACTIVITY_ID",           "Value.$": "$.activity_id"},
            {"Name": "CALLBACK_URL",          "Value.$": "$.callback_url"},
            {"Name": "SOURCE_S3_BUCKET",      "Value.$": "$.source_s3_bucket"},
            {"Name": "SOURCE_S3_KEY",         "Value.$": "$.source_s3_key"},
            {"Name": "TARGET_NAMESPACE",      "Value.$": "$.target_namespace"},
            {"Name": "WRITE_MODE",            "Value.$": "$.write_mode"},
            {"Name": "CHECKSUM",              "Value.$": "$.checksum"},
            {"Name": "TABLES",               "Value.$": "States.JsonToString($.tables)"},
            {"Name": "CREATE_IF_NOT_EXISTS",  "Value.$": "States.Format('{}', $.create_if_not_exists)"},
            # Cross-account access — tenant IAM role resolved by size-estimator Lambda
            {"Name": "TENANT_ROLE_ARN",       "Value.$": "$.sde_context.tenant_role"},
        ]

    return {
        "Comment": f"Ingestion - Three-Path, auto-size via Lambda (thresholds: {threshold_small_gb}GB, {threshold_large_gb}GB)",
        "StartAt": "PrepareIngestionInput",
        "States": {
            # ----------------------------------------------------------------
            # Step 1: inject mode field so the Lambda knows this is ingestion
            # ----------------------------------------------------------------
            "PrepareIngestionInput": {
                "Type": "Pass",
                "Comment": "Inject mode='ingestion' and build s3_input_path for size-estimator Lambda",
                "Parameters": {
                    "mode": "ingestion",
                    "tenant_id.$":           "$.tenant_id",
                    "run_id.$":              "$.run_id",
                    "activity_id.$":         "$.activity_id",
                    "callback_url.$":        "$.callback_url",
                    "source_s3_bucket.$":    "$.source_s3_bucket",
                    "source_s3_key.$":       "$.source_s3_key",
                    "s3_input_path.$":       "States.Format('s3://{}/{}', $.source_s3_bucket, $.source_s3_key)",
                    "artifact_type.$":       "$.artifact_type",
                    "checksum.$":            "$.checksum",
                    "target_namespace.$":    "$.target_namespace",
                    "write_mode.$":          "$.write_mode",
                    "tables.$":              "$.tables",
                    "create_if_not_exists.$": "$.create_if_not_exists",
                },
                "ResultPath": "$",
                "Next": "MeasureArtifactSize",
            },
            # ----------------------------------------------------------------
            # Step 2: call size-estimator Lambda → auto-measures S3 file size
            #   Lambda returns {selected_path, sde_context, ...}
            #   OutputPath extracts $.Payload so the enriched dict becomes the
            #   new state — no artifact_size_gb needed in SFN input.
            # ----------------------------------------------------------------
            "MeasureArtifactSize": {
                "Type": "Task",
                "Comment": "Auto-measure MDF/BAK file size from S3; determines EXPRESS/STANDARD/BATCH path",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": size_estimator_lambda_arn,
                    "Payload.$": "$",
                },
                "OutputPath": "$.Payload",
                "Retry": [
                    {
                        "ErrorEquals": ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.TooManyRequestsException"],
                        "IntervalSeconds": 5,
                        "MaxAttempts": 2,
                        "BackoffRate": 2.0,
                    }
                ],
                "Catch": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "Comment": "Size measurement failed — abort ingestion",
                        "Next": "IngestionFailed",
                    }
                ],
                "Next": "RouteBySize",
            },
            # ----------------------------------------------------------------
            # Step 3: route based on selected_path returned by Lambda
            # ----------------------------------------------------------------
            "RouteBySize": {
                "Type": "Choice",
                "Comment": "Route based on selected_path from size-estimator: EXPRESS→Fargate Express, STANDARD→Fargate Standard, else→Batch",
                "Choices": [
                    {
                        "Variable": "$.selected_path",
                        "StringEquals": "EXPRESS",
                        "Next": "RunFargateExpress",
                    },
                    {
                        "Variable": "$.selected_path",
                        "StringEquals": "STANDARD",
                        "Next": "RunFargateStandard",
                    },
                ],
                "Default": "RunBatchEC2",
            },
            "RunFargateExpress": {
                "Type": "Task",
                "Comment": "PATH 1: Fargate + SQL Express (FREE license)",
                "Resource": "arn:aws:states:::ecs:runTask.sync",
                "Parameters": {
                    "Cluster": fargate_express_cluster_arn,
                    "TaskDefinition": fargate_express_task_arn,
                    "LaunchType": "FARGATE",
                    "NetworkConfiguration": {
                        "AwsvpcConfiguration": {
                            "Subnets": subnets,
                            "SecurityGroups": security_groups,
                            "AssignPublicIp": "DISABLED",
                        }
                    },
                    "Overrides": {
                        "TaskRoleArn.$": "$.sde_context.tenant_role",
                        "ContainerOverrides": [
                            {
                                "Name": "pipeline",
                                "Environment": _container_env("Express"),
                            }
                        ],
                    },
                },
                "Retry": [{"ErrorEquals": ["States.TaskFailed"], "IntervalSeconds": 30, "MaxAttempts": 2, "BackoffRate": 2.0}],
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "IngestionFailed"}],
                "End": True,
            },
            "RunFargateStandard": {
                "Type": "Task",
                "Comment": "PATH 2: Fargate + SQL Standard (Licensed)",
                "Resource": "arn:aws:states:::ecs:runTask.sync",
                "Parameters": {
                    "Cluster": fargate_standard_cluster_arn,
                    "TaskDefinition": fargate_standard_task_arn,
                    "LaunchType": "FARGATE",
                    "NetworkConfiguration": {
                        "AwsvpcConfiguration": {
                            "Subnets": subnets,
                            "SecurityGroups": security_groups,
                            "AssignPublicIp": "DISABLED",
                        }
                    },
                    "Overrides": {
                        "TaskRoleArn.$": "$.sde_context.tenant_role",
                        "ContainerOverrides": [
                            {
                                "Name": "pipeline",
                                "Environment": _container_env("Standard"),
                            }
                        ],
                    },
                },
                "Retry": [{"ErrorEquals": ["States.TaskFailed"], "IntervalSeconds": 30, "MaxAttempts": 2, "BackoffRate": 2.0}],
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "IngestionFailed"}],
                "End": True,
            },
            "RunBatchEC2": {
                "Type": "Task",
                "Comment": "PATH 3: AWS Batch EC2 + SQL Standard (Licensed, large files)",
                "Resource": "arn:aws:states:::batch:submitJob.sync",
                "Parameters": {
                    "JobName.$": "States.Format('ingestion-{}', $.run_id)",
                    "JobQueue": batch_job_queue_arn,
                    "JobDefinition": batch_job_definition_arn,
                    "ContainerOverrides": {
                        "Environment": _container_env("Standard"),
                    },
                },
                "Retry": [{"ErrorEquals": ["States.TaskFailed"], "IntervalSeconds": 60, "MaxAttempts": 2, "BackoffRate": 2.0}],
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "IngestionFailed"}],
                "End": True,
            },
            "IngestionFailed": {
                "Type": "Fail",
                "Error": "IngestionTaskFailed",
                "Cause": "Ingestion task failed",
            },
        },
    }


# Example usage / testing
if __name__ == "__main__":
    # Example: Three-path extraction with custom thresholds
    three_path_def = get_three_path_extraction_definition(
        fargate_express_cluster_arn="arn:aws:ecs:us-east-1:123456789012:cluster/express",
        fargate_express_task_arn="arn:aws:ecs:us-east-1:123456789012:task-definition/express:1",
        fargate_standard_cluster_arn="arn:aws:ecs:us-east-1:123456789012:cluster/standard",
        fargate_standard_task_arn="arn:aws:ecs:us-east-1:123456789012:task-definition/standard:1",
        batch_job_queue_arn="arn:aws:batch:us-east-1:123456789012:job-queue/large",
        batch_job_definition_arn="arn:aws:batch:us-east-1:123456789012:job-definition/pipeline:1",
        subnets=["subnet-abc123", "subnet-def456"],
        security_groups=["sg-123456"],
        threshold_small_gb=7,   # Configurable!
        threshold_large_gb=60,  # Configurable!
    )
    
    print("=== Three-Path Extraction State Machine ===")
    print(json.dumps(three_path_def, indent=2))
