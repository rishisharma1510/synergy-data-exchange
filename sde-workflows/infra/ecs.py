"""
ECS/Fargate Infrastructure Configuration.

Defines:
- ECS cluster
- Task definition with SQL Server container
- Service configuration
- SID transformation environment variables
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class ContainerDefinition:
    """ECS container definition."""
    name: str
    image: str
    essential: bool = True
    cpu: int = 0
    memory: int = 0
    memory_reservation: int = 0
    environment: Dict[str, str] = field(default_factory=dict)
    secrets: List[Dict[str, str]] = field(default_factory=list)
    port_mappings: List[Dict[str, int]] = field(default_factory=list)
    log_configuration: Optional[Dict] = None
    
    def to_dict(self) -> dict:
        """Convert to ECS-compatible dict format."""
        result = {
            "name": self.name,
            "image": self.image,
            "essential": self.essential,
            "environment": [
                {"name": k, "value": v} for k, v in self.environment.items()
            ],
        }
        
        if self.cpu > 0:
            result["cpu"] = self.cpu
        if self.memory > 0:
            result["memory"] = self.memory
        if self.memory_reservation > 0:
            result["memoryReservation"] = self.memory_reservation
        if self.secrets:
            result["secrets"] = self.secrets
        if self.port_mappings:
            result["portMappings"] = self.port_mappings
        if self.log_configuration:
            result["logConfiguration"] = self.log_configuration
        
        return result


@dataclass
class TaskDefinition:
    """ECS task definition."""
    family: str
    cpu: str = "2048"  # 2 vCPU
    memory: str = "8192"  # 8 GB
    network_mode: str = "awsvpc"
    requires_compatibilities: List[str] = field(default_factory=lambda: ["FARGATE"])
    execution_role_arn: str = ""
    task_role_arn: str = ""
    containers: List[ContainerDefinition] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        """Convert to ECS-compatible dict format."""
        return {
            "family": self.family,
            "cpu": self.cpu,
            "memory": self.memory,
            "networkMode": self.network_mode,
            "requiresCompatibilities": self.requires_compatibilities,
            "executionRoleArn": self.execution_role_arn,
            "taskRoleArn": self.task_role_arn,
            "containerDefinitions": [c.to_dict() for c in self.containers],
        }


def create_pipeline_container(
    image_uri: str,
    tenant_id: str,
    stage: str = "dev",
    sid_enabled: bool = False,
    sid_api_url: str = "",
    sid_use_mock: bool = False,
) -> ContainerDefinition:
    """
    Create container definition for Synergy Data Exchange pipeline.
    
    Args:
        image_uri: ECR image URI for pipeline container.
        tenant_id: Tenant identifier.
        stage: Deployment stage (dev, staging, prod).
        sid_enabled: Whether SID transformation is enabled.
        sid_api_url: SID API URL (production only).
        sid_use_mock: Use mock SID client for testing.
    
    Returns:
        ContainerDefinition configured for pipeline.
    """
    # Stage-specific resource names
    tenant_context_lambda = f"rs-cdk{stage}-ap-tenant-context-lambda"
    app_token_secret = f"rs-cdk{stage}-app-token-secret"
    
    environment = {
        # SQL Server configuration
        "ACCEPT_EULA": "Y",
        "MSSQL_PID": "Standard",
        "MSSQL_COLLATION": "SQL_Latin1_General_CP1_CI_AS",
        
        # Pipeline configuration
        "TENANT_ID": tenant_id,
        "LOG_LEVEL": "INFO",
        
        # SID transformation configuration
        "SID_ENABLED": str(sid_enabled).lower(),
        "SID_USE_MOCK": str(sid_use_mock).lower(),
    }
    
    # Add SID resource names when enabled and not using mock
    if sid_enabled and not sid_use_mock:
        environment["TENANT_CONTEXT_LAMBDA"] = tenant_context_lambda
        environment["APP_TOKEN_SECRET"] = app_token_secret
    
    if sid_enabled and sid_api_url:
        environment["SID_API_URL"] = sid_api_url
    
    secrets = [
        {
            "name": "SA_PASSWORD",
            "valueFrom": f"arn:aws:secretsmanager:*:*:secret:sde/{tenant_id}/sql-password",
        },
    ]
    
    return ContainerDefinition(
        name="sde-pipeline",
        image=image_uri,
        essential=True,
        environment=environment,
        secrets=secrets,
        port_mappings=[{"containerPort": 1433, "protocol": "tcp"}],
        log_configuration={
            "logDriver": "awslogs",
            "options": {
                "awslogs-group": f"/ecs/sde-{tenant_id}",
                "awslogs-region": "us-east-1",
                "awslogs-stream-prefix": "pipeline",
            },
        },
    )


def create_ingestion_task_definition(
    account_id: str,
    region: str,
    tenant_id: str,
    image_uri: str,
    stage: str = "dev",
    sid_enabled: bool = False,
    sid_api_url: str = "",
    cpu: str = "2048",
    memory: str = "8192",
) -> TaskDefinition:
    """
    Create task definition for ingestion pipeline.
    
    Args:
        account_id: AWS account ID.
        region: AWS region.
        tenant_id: Tenant identifier.
        image_uri: ECR image URI.
        stage: Deployment stage (dev, staging, prod).
        sid_enabled: Whether SID transformation is enabled.
        sid_api_url: SID API URL.
        cpu: Task CPU units.
        memory: Task memory in MB.
    
    Returns:
        TaskDefinition for Fargate task.
    """
    container = create_pipeline_container(
        image_uri=image_uri,
        tenant_id=tenant_id,
        stage=stage,
        sid_enabled=sid_enabled,
        sid_api_url=sid_api_url,
    )
    
    return TaskDefinition(
        family=f"sde-ingestion-{tenant_id}",
        cpu=cpu,
        memory=memory,
        execution_role_arn=f"arn:aws:iam::{account_id}:role/sde-execution-role",
        task_role_arn=f"arn:aws:iam::{account_id}:role/sde-task-role",
        containers=[container],
    )


# Environment-specific configurations
ECS_CONFIGS = {
    "dev": {
        "cluster_name": "sde-dev",
        "cpu": "1024",
        "memory": "4096",
        "sid_enabled": True,
        "sid_use_mock": True,
    },
    "staging": {
        "cluster_name": "sde-staging",
        "cpu": "2048",
        "memory": "8192",
        "sid_enabled": True,
        "sid_use_mock": False,
        "sid_api_url": "https://sid-api-staging.verisk.com",
    },
    "prod": {
        "cluster_name": "sde-prod",
        "cpu": "4096",
        "memory": "16384",
        "sid_enabled": True,
        "sid_use_mock": False,
        "sid_api_url": "https://sid-api.verisk.com",
    },
}


def get_task_definition_for_env(
    env: str,
    account_id: str,
    region: str,
    tenant_id: str,
    image_uri: str,
) -> TaskDefinition:
    """
    Get task definition configured for specific environment.
    
    Args:
        env: Environment name (dev, staging, prod).
        account_id: AWS account ID.
        region: AWS region.
        tenant_id: Tenant identifier.
        image_uri: ECR image URI.
    
    Returns:
        TaskDefinition configured for environment.
    """
    config = ECS_CONFIGS.get(env, ECS_CONFIGS["dev"])
    
    return create_ingestion_task_definition(
        account_id=account_id,
        region=region,
        tenant_id=tenant_id,
        image_uri=image_uri,
        stage=env,
        sid_enabled=config.get("sid_enabled", False),
        sid_api_url=config.get("sid_api_url", ""),
        cpu=config.get("cpu", "2048"),
        memory=config.get("memory", "8192"),
    )
