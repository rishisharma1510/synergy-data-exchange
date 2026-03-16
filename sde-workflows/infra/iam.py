"""
IAM Roles and Policies for Synergy Data Exchange Pipeline.

Defines:
- Fargate execution role (ECR, CloudWatch, Secrets Manager)
- Task role (S3, STS, Lambda invoke for SID)
- Cross-account assume role policies
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
import json


@dataclass
class PolicyStatement:
    """IAM policy statement."""
    effect: str = "Allow"
    actions: List[str] = field(default_factory=list)
    resources: List[str] = field(default_factory=list)
    conditions: Optional[Dict] = None
    
    def to_dict(self) -> dict:
        result = {
            "Effect": self.effect,
            "Action": self.actions,
            "Resource": self.resources,
        }
        if self.conditions:
            result["Condition"] = self.conditions
        return result


@dataclass
class PolicyDocument:
    """IAM policy document."""
    version: str = "2012-10-17"
    statements: List[PolicyStatement] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        return {
            "Version": self.version,
            "Statement": [s.to_dict() for s in self.statements],
        }
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


def create_execution_role_policy(account_id: str, region: str) -> PolicyDocument:
    """
    Create execution role policy for Fargate task.
    
    Permissions:
    - Pull images from ECR
    - Write logs to CloudWatch
    - Read secrets from Secrets Manager
    """
    return PolicyDocument(
        statements=[
            # ECR permissions
            PolicyStatement(
                actions=[
                    "ecr:GetAuthorizationToken",
                ],
                resources=["*"],
            ),
            PolicyStatement(
                actions=[
                    "ecr:BatchCheckLayerAvailability",
                    "ecr:GetDownloadUrlForLayer",
                    "ecr:BatchGetImage",
                ],
                resources=[
                    f"arn:aws:ecr:{region}:{account_id}:repository/sde-*",
                ],
            ),
            # CloudWatch Logs
            PolicyStatement(
                actions=[
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=[
                    f"arn:aws:logs:{region}:{account_id}:log-group:/ecs/sde-*:*",
                ],
            ),
            # Secrets Manager
            PolicyStatement(
                actions=[
                    "secretsmanager:GetSecretValue",
                ],
                resources=[
                    f"arn:aws:secretsmanager:{region}:{account_id}:secret:sde/*",
                ],
            ),
        ]
    )


def create_task_role_policy(
    account_id: str,
    region: str,
    sid_enabled: bool = False,
    tenant_context_lambda_name: str = "rs-cdkdev-ap-tenant-context-lambda",
) -> PolicyDocument:
    """
    Create task role policy for pipeline execution.
    
    Permissions:
    - S3 read/write for data transfer
    - STS assume role for cross-account access
    - Secrets Manager for app tokens
    - Lambda invoke for SID tenant context (when enabled)
    - CloudWatch metrics (when enabled)
    """
    statements = [
        # S3 permissions for Iceberg data
        PolicyStatement(
            actions=[
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
            ],
            resources=[
                f"arn:aws:s3:::sde-*",
                f"arn:aws:s3:::sde-*/*",
            ],
        ),
        # Glue Catalog for Iceberg
        PolicyStatement(
            actions=[
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetPartitions",
                "glue:CreateTable",
                "glue:UpdateTable",
                "glue:DeleteTable",
                "glue:BatchCreatePartition",
                "glue:BatchDeletePartition",
            ],
            resources=[
                f"arn:aws:glue:{region}:{account_id}:catalog",
                f"arn:aws:glue:{region}:{account_id}:database/sde_*",
                f"arn:aws:glue:{region}:{account_id}:table/sde_*/*",
            ],
        ),
        # STS for cross-account access
        PolicyStatement(
            actions=[
                "sts:AssumeRole",
            ],
            resources=[
                f"arn:aws:iam::*:role/sde-tenant-access-role",
            ],
        ),
        # Secrets Manager for credentials
        PolicyStatement(
            actions=[
                "secretsmanager:GetSecretValue",
            ],
            resources=[
                f"arn:aws:secretsmanager:{region}:{account_id}:secret:sde/*",
            ],
        ),
    ]
    
    # Add SID-specific permissions when enabled
    if sid_enabled:
        # Lambda invoke for Tenant Context
        statements.append(
            PolicyStatement(
                actions=[
                    "lambda:InvokeFunction",
                ],
                resources=[
                    f"arn:aws:lambda:{region}:{account_id}:function:{tenant_context_lambda_name}",
                    # Also allow versioned invocations
                    f"arn:aws:lambda:{region}:{account_id}:function:{tenant_context_lambda_name}:*",
                ],
            )
        )
        
        # CloudWatch metrics for SID telemetry
        statements.append(
            PolicyStatement(
                actions=[
                    "cloudwatch:PutMetricData",
                ],
                resources=["*"],
                conditions={
                    "StringEquals": {
                        "cloudwatch:namespace": "SDE/SID",
                    },
                },
            )
        )
    
    return PolicyDocument(statements=statements)


def create_trust_policy_fargate() -> PolicyDocument:
    """Create trust policy allowing Fargate to assume role."""
    return PolicyDocument(
        statements=[
            PolicyStatement(
                effect="Allow",
                actions=["sts:AssumeRole"],
                resources=[],  # Not used in trust policies
            ),
        ]
    )


@dataclass
class RoleDefinition:
    """IAM role definition."""
    name: str
    description: str
    trust_policy: PolicyDocument
    policies: List[PolicyDocument] = field(default_factory=list)
    managed_policy_arns: List[str] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        return {
            "RoleName": self.name,
            "Description": self.description,
            "AssumeRolePolicyDocument": self.trust_policy.to_dict(),
            "AttachedPolicies": [p.to_dict() for p in self.policies],
            "ManagedPolicyArns": self.managed_policy_arns,
        }


def create_sde_roles(
    account_id: str,
    region: str,
    sid_enabled: bool = True,
    tenant_context_lambda_name: str = "rs-cdkdev-ap-tenant-context-lambda",
) -> Dict[str, RoleDefinition]:
    """
    Create all IAM roles needed for Synergy Data Exchange pipeline.
    
    Args:
        account_id: AWS account ID.
        region: AWS region.
        sid_enabled: Whether SID transformation is enabled.
        tenant_context_lambda_name: Name of Tenant Context Lambda.
    
    Returns:
        Dict of role name to RoleDefinition.
    """
    trust_policy = PolicyDocument(
        statements=[
            PolicyStatement(
                actions=["sts:AssumeRole"],
                resources=[],  # Replaced by Principal in actual policy
            )
        ]
    )
    
    return {
        "execution-role": RoleDefinition(
            name="sde-execution-role",
            description="ECS task execution role for Synergy Data Exchange pipeline",
            trust_policy=trust_policy,
            policies=[create_execution_role_policy(account_id, region)],
        ),
        "task-role": RoleDefinition(
            name="sde-task-role",
            description="ECS task role for Synergy Data Exchange pipeline with SID permissions",
            trust_policy=trust_policy,
            policies=[
                create_task_role_policy(
                    account_id,
                    region,
                    sid_enabled=sid_enabled,
                    tenant_context_lambda_name=tenant_context_lambda_name,
                )
            ],
        ),
    }


# Environment-specific IAM configurations
IAM_CONFIGS = {
    "dev": {
        "sid_enabled": True,
        "tenant_context_lambda_name": "rs-cdkdev-ap-tenant-context-lambda",
        "app_token_secret_name": "rs-cdkdev-app-token-secret",
    },
    "staging": {
        "sid_enabled": True,
        "tenant_context_lambda_name": "rs-cdkstaging-ap-tenant-context-lambda",
        "app_token_secret_name": "rs-cdkstaging-app-token-secret",
    },
    "prod": {
        "sid_enabled": True,
        "tenant_context_lambda_name": "rs-cdkprod-ap-tenant-context-lambda",
        "app_token_secret_name": "rs-cdkprod-app-token-secret",
    },
}


def get_roles_for_env(
    env: str,
    account_id: str,
    region: str,
) -> Dict[str, RoleDefinition]:
    """
    Get IAM roles configured for specific environment.
    
    Args:
        env: Environment name (dev, staging, prod).
        account_id: AWS account ID.
        region: AWS region.
    
    Returns:
        Dict of role definitions for environment.
    """
    config = IAM_CONFIGS.get(env, IAM_CONFIGS["dev"])
    
    return create_sde_roles(
        account_id=account_id,
        region=region,
        sid_enabled=config.get("sid_enabled", False),
        tenant_context_lambda_name=config.get("tenant_context_lambda_name", ""),
    )
