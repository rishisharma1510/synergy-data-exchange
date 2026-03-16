"""
Tenant Context Utility for Synergy Data Exchange Pipeline

Simple utility to fetch tenant context from the tenant context lambda and extract
only the fields Synergy Data Exchange needs. No rigid models - just read the raw response and
return what we need. If the upstream lambda changes, we automatically get new fields.

Synergy Data Exchange Architecture:
┌─────────────────────────────────────────────────────────────────────────────────┐
│  SHARED INFRASTRUCTURE (from Synergy Data Exchange CDK Stacks):                            │
│  - ECS Cluster, Fargate Task Definitions                                        │
│  - AWS Batch Compute Environment, Job Queue                                     │
│  - Step Functions State Machine                                                 │
│                                                                                 │
│  TENANT-SPECIFIC (from Tenant Context Lambda):                                  │
│  - tenant_role: IAM role to assume for cross-account access                     │
│  - bucket_name: Tenant S3 bucket                                                │
│  - glue_database: Tenant Iceberg/Glue database                                  │
│  - client_id/client_secret: OAuth credentials for API access                    │
└─────────────────────────────────────────────────────────────────────────────────┘

Multi-Tenant Credential Flow:
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ECS FARGATE (TaskRoleArn assigned at runtime - NO STS AssumeRole):             │
│  1. Step Functions passes TaskRoleArn = tenant_role                             │
│  2. Task runs directly with tenant credentials                                  │
│  3. For SID/Exposure API: Task calls get_okta_credentials() for OAuth token     │
│                                                                                 │
│  AWS BATCH (must use STS AssumeRole - job role can't be overridden):            │
│  1. Step Functions passes TENANT_ROLE_ARN as env var                            │
│  2. Task calls STS AssumeRole with TENANT_ROLE_ARN                              │
│  3. For SID/Exposure API: Task calls get_okta_credentials() for OAuth token     │
└─────────────────────────────────────────────────────────────────────────────────┘

Usage:
    # In Lambda handler:
    raw_context = fetch_tenant_context(tenant_id)  # Full dict from lambda
    db_config = get_sde_config(raw_context)  # Only what Synergy Data Exchange needs
    
    glue_db = db_config["glue_database"]  # "rs_cdkdev_dclegend01_exposure_db"
    bucket = db_config["bucket_name"]      # "rs-cdkdev-dclegend01-s3"
    tenant_role = db_config["tenant_role"] # "arn:aws:iam::451952076009:role/..."
    
    # For OAuth/API access:
    client = TenantContextClient()
    okta_creds = client.get_okta_credentials(tenant_id)
    # Use okta_creds.client_id, okta_creds.client_secret with Okta token API
"""

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Environment variables for configuration
# These MUST be injected by CDK as container env vars (TENANT_CONTEXT_LAMBDA / APP_TOKEN_SECRET).
# No hardcoded stage-specific defaults — they vary per stage (dev/qa/prod).
TENANT_CONTEXT_LAMBDA_NAME = (
    os.environ.get("TENANT_CONTEXT_LAMBDA_NAME")
    or os.environ.get("TENANT_CONTEXT_LAMBDA")
)
APP_TOKEN_SECRET_NAME = (
    os.environ.get("APP_TOKEN_SECRET_NAME")
    or os.environ.get("APP_TOKEN_SECRET")
)
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# Cache for tenant context (avoid repeated lambda invocations)
_context_cache: dict[str, dict[str, Any]] = {}
_cache_expiry: dict[str, float] = {}
CACHE_TTL_SECONDS = 3000  # 50 minutes (tokens valid ~1hr)


def get_app_token(secret_name: Optional[str] = None) -> str:
    """
    Retrieve the app token from Secrets Manager.
    
    Args:
        secret_name: Override secret name (default: from env var)
        
    Returns:
        App token string for authenticating with tenant context lambda
    """
    import boto3
    
    secret_name = secret_name or APP_TOKEN_SECRET_NAME
    
    client = boto3.client("secretsmanager", region_name=AWS_REGION)
    response = client.get_secret_value(SecretId=secret_name)
    
    secret_value = response.get("SecretString", "")
    
    # Handle JSON-formatted secrets
    try:
        secret_json = json.loads(secret_value)
        # Common patterns: {"token": "..."} or {"appToken": "..."} or plain string
        return (
            secret_json.get("token") or
            secret_json.get("appToken") or
            secret_json.get("app_token") or
            secret_value
        )
    except json.JSONDecodeError:
        return secret_value


def fetch_tenant_context(
    tenant_id: str,
    app_token: Optional[str] = None,
    lambda_name: Optional[str] = None,
    force_refresh: bool = False
) -> dict[str, Any]:
    """
    Invoke the tenant context lambda and return the raw response.
    
    Returns the FULL response dict - no filtering. Use get_sde_config()
    to extract only what Synergy Data Exchange needs.
    
    Args:
        tenant_id: UUID of the tenant
        app_token: App token for authentication (fetched from Secrets Manager if not provided)
        lambda_name: Override lambda name (default: from env var)
        force_refresh: Bypass cache and force a fresh lambda invocation
        
    Returns:
        Raw dict from tenant context lambda response (all fields as-is)
        
    Raises:
        RuntimeError: If lambda invocation fails or returns an error
    """
    import boto3
    
    lambda_name = lambda_name or TENANT_CONTEXT_LAMBDA_NAME
    
    # Check cache
    cache_key = f"{lambda_name}:{tenant_id}"
    if not force_refresh and cache_key in _context_cache:
        if time.time() < _cache_expiry.get(cache_key, 0):
            logger.debug("Using cached tenant context for %s", tenant_id)
            return _context_cache[cache_key]
    
    # Get app token if not provided
    if not app_token:
        app_token = get_app_token()
    
    # Invoke tenant context lambda
    lambda_client = boto3.client("lambda", region_name=AWS_REGION)
    
    payload = {
        "tenantId": tenant_id,
        "appToken": app_token
    }
    
    logger.info("Invoking tenant context lambda: %s for tenant: %s", lambda_name, tenant_id)
    
    response = lambda_client.invoke(
        FunctionName=lambda_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload)
    )
    
    # Parse response
    response_payload = json.loads(response["Payload"].read().decode("utf-8"))
    
    # Check for Lambda execution errors
    if "errorMessage" in response_payload:
        raise RuntimeError(f"Tenant context lambda error: {response_payload['errorMessage']}")
    
    # Check for application-level errors
    if response_payload.get("error"):
        raise RuntimeError(
            f"Tenant context lambda error: {response_payload.get('message', 'Unknown error')}"
        )
    
    if response_payload.get("statusCode") != 200:
        raise RuntimeError(
            f"Tenant context lambda failed with status {response_payload.get('statusCode')}: "
            f"{response_payload.get('message', 'Unknown error')}"
        )
    
    # Cache the response
    _context_cache[cache_key] = response_payload
    _cache_expiry[cache_key] = time.time() + CACHE_TTL_SECONDS
    
    logger.info("Tenant context retrieved, cached until: %d", int(_cache_expiry[cache_key]))
    return response_payload


def _derive_glue_database(bucket_name: str) -> str:
    """
    Derive the Glue/Iceberg database name from the tenant S3 bucket name.

    Used as a fallback when icebergDbConfiguration is absent from tenant context.
    Naming convention: "rs-cdkdev-bridgete01-s3" → "rs_cdkdev_bridgete01_exposure_db"
    """
    if not bucket_name:
        return ""
    # Strip the trailing "-s3" segment and replace hyphens with underscores
    parts = bucket_name.split("-")
    if parts and parts[-1] == "s3":
        parts = parts[:-1]
    derived = "_".join(parts) + "_exposure_db"
    logger.warning(
        "icebergDbConfiguration.exposure missing from tenant context — "
        "derived glue_database='%s' from bucket_name='%s'",
        derived,
        bucket_name,
    )
    return derived


def get_sde_config(tenant_context: dict[str, Any]) -> dict[str, Any]:
    """
    Extract only the fields Synergy Data Exchange needs from tenant context.
    
    This is the ONLY place that maps tenant context fields to Synergy Data Exchange config.
    If the upstream lambda changes field names, only update this function.
    
    The output is a simple dict - no models, no classes. Easy to serialize
    and pass through Step Functions state or container environment variables.
    
    NOTE: Synergy Data Exchange uses SHARED infrastructure (ECS cluster, Fargate, Batch)
    defined in Synergy Data Exchange CDK stacks - NOT the lossBatchConfiguration from
    tenant context (that's for loss processing).
    
    The key field from tenant context is `tenant_role` - Fargate/Batch tasks
    assume this role for cross-account access to tenant S3/Glue resources.
    
    Args:
        tenant_context: Raw response from tenant context lambda (full dict)
        
    Returns:
        Dict with only sde-relevant fields:
        {
            "tenant_id": "uuid",
            "resource_id": "dclegend01",
            "region": "us-east-1",
            "bucket_name": "rs-cdkdev-dclegend01-s3",
            "tenant_role": "arn:aws:iam::451952076009:role/rs-cdkdev-dclegend01-role",
            "glue_database": "rs_cdkdev_dclegend01_exposure_db",
            "aws_credentials": {...} or None
        }
    """
    # Navigate nested structures safely
    iceberg_config = tenant_context.get("icebergDbConfiguration", {})
    aws_keys = tenant_context.get("awsSessionKeys", {})
    tenant_profile = tenant_context.get("tenantProfile", {})
    
    return {
        # Core identification - always needed
        "tenant_id": tenant_profile.get("tenantId") or tenant_context.get("tenantId", ""),
        "resource_id": tenant_context.get("resourceId", ""),
        "region": tenant_context.get("region", "us-east-1"),
        
        # S3 bucket - tenant's data bucket
        "bucket_name": tenant_context.get("bucketName", ""),
        
        # IAM role for cross-account access - CRITICAL for Synergy Data Exchange
        # Fargate/Batch tasks assume this role to access tenant S3/Glue
        "tenant_role": tenant_context.get("tenantRole", ""),
        
        # Iceberg/Glue — THE KEY FIELD for Synergy Data Exchange queries.
        # Primary source: icebergDbConfiguration.exposure (set by Tenant Context service).
        # Fallback: derive from bucketName when the Iceberg config is absent (e.g. older tenants).
        # Naming convention: "rs-cdkdev-bridgete01-s3" → "rs_cdkdev_bridgete01_exposure_db"
        "glue_database": iceberg_config.get("exposure", "") or _derive_glue_database(tenant_context.get("bucketName", "")),
        
        # AWS credentials for cross-account access (optional, short-lived)
        # Tasks can also use tenant_role via STS AssumeRole (preferred)
        "aws_credentials": {
            "access_key": aws_keys.get("accessKey", ""),
            "secret_key": aws_keys.get("secretKey", ""),
            "session_token": aws_keys.get("sessionToken", ""),
        } if aws_keys.get("accessKey") else None,
        
        # Metadata
        "generated_on": tenant_context.get("generatedOn", 0),
    }


def get_sde_env_vars(config: dict[str, Any]) -> dict[str, str]:
    """
    Convert Synergy Data Exchange config to environment variables for containers.
    
    Use this when launching Fargate/Batch tasks via Step Functions.
    These env vars allow tasks to assume the tenant role for cross-account access.
    
    Flow:
    1. Step Functions passes sde_context to Fargate/Batch task
    2. Task reads TENANT_ROLE_ARN from env
    3. Task calls STS AssumeRole to get tenant credentials
    4. Task uses tenant credentials to access S3/Glue
    
    NOTE: Sensitive values (AWS credentials) are NOT included.
    Use TENANT_ROLE_ARN with STS AssumeRole for cross-account access.
    
    Args:
        config: Output from get_sde_config()
        
    Returns:
        Dict of env var name -> value (all strings, no sensitive data)
    """
    return {
        "TENANT_ID": config.get("tenant_id", ""),
        "TENANT_RESOURCE_ID": config.get("resource_id", ""),
        "AWS_REGION": config.get("region", "us-east-1"),
        "TENANT_ROLE_ARN": config.get("tenant_role", ""),  # Tasks assume this role
        "TENANT_BUCKET": config.get("bucket_name", ""),
        "GLUE_DATABASE": config.get("glue_database", ""),
    }


def get_boto3_credentials(config: dict[str, Any]) -> Optional[dict[str, str]]:
    """
    Get boto3-compatible credentials dict from Synergy Data Exchange config.
    
    Returns None if no AWS credentials were included in tenant context.
    Use tenant_role with STS AssumeRole as an alternative.
    
    Args:
        config: Output from get_sde_config()
        
    Returns:
        Dict for boto3 client initialization, or None
        {
            "aws_access_key_id": "...",
            "aws_secret_access_key": "...",
            "aws_session_token": "..."
        }
    """
    creds = config.get("aws_credentials")
    if not creds or not creds.get("access_key"):
        return None
    
    return {
        "aws_access_key_id": creds["access_key"],
        "aws_secret_access_key": creds["secret_key"],
        "aws_session_token": creds["session_token"],
    }


# =============================================================================
# One-shot helper for Lambda/Fargate entrypoints
# =============================================================================

def init_sde_context(tenant_id: str) -> dict[str, Any]:
    """
    One-shot helper: fetch tenant context and return Synergy Data Exchange config.
    
    Use this at the start of Lambda handlers or Fargate entrypoints:
    
        db_config = init_sde_context(event["tenant_id"])
        glue_db = db_config["glue_database"]
        bucket = db_config["bucket_name"]
    
    Args:
        tenant_id: UUID of the tenant
        
    Returns:
        Synergy Data Exchange config dict (see get_sde_config for fields)
    """
    raw_context = fetch_tenant_context(tenant_id)
    return get_sde_config(raw_context)


def get_raw_tenant_context(tenant_id: str) -> dict[str, Any]:
    """
    Get the full raw tenant context (all fields, no filtering).
    
    Use this when you need fields not in the standard Synergy Data Exchange config.
    
    Args:
        tenant_id: UUID of the tenant
        
    Returns:
        Full dict from tenant context lambda
    """
    return fetch_tenant_context(tenant_id)


# =============================================================================
# Dataclasses for structured credential types
# =============================================================================

@dataclass
class OktaCredentials:
    """OAuth credentials for Okta token generation."""
    client_id: str
    client_secret: str
    token_endpoint: str  # Full token URL, e.g. https://sso-dev.../v1/token
    
    def to_auth_tuple(self) -> tuple[str, str]:
        """Return (client_id, client_secret) for requests basic auth."""
        return (self.client_id, self.client_secret)


@dataclass  
class AWSCredentials:
    """AWS session credentials for cross-account access."""
    access_key: str
    secret_key: str
    session_token: str
    
    def to_boto3_config(self) -> dict[str, str]:
        """Return dict for boto3 client initialization."""
        return {
            "aws_access_key_id": self.access_key,
            "aws_secret_access_key": self.secret_key,
            "aws_session_token": self.session_token,
        }


# =============================================================================
# TenantContextClient - Stateful client for tasks needing ongoing access
# =============================================================================

class TenantContextClient:
    """
    Stateful client for Fargate/Batch tasks that need ongoing access
    to tenant context (credentials, config, etc).
    
    Use this when tasks need to:
    - Get Okta credentials for SID/Exposure API calls
    - Get AWS credentials for cross-account access (Batch only)
    - Refresh credentials during long-running jobs
    
    ECS Fargate Note:
        Fargate tasks have tenant role assigned via TaskRoleArn.
        Use this client ONLY for Okta credentials (SID/Exposure API).
        AWS credentials come from the task role automatically.
        
    AWS Batch Note:
        Batch jobs must assume tenant role via STS AssumeRole.
        Use this client for both Okta AND AWS credentials,
        or use STS AssumeRole with TENANT_ROLE_ARN env var.
    
    Example:
        client = TenantContextClient()
        
        # Get Okta token for API calls
        okta = client.get_okta_credentials(tenant_id)
        token = get_okta_token(okta.client_id, okta.client_secret, okta.token_endpoint)
        
        # For Batch jobs needing AWS credentials:
        aws = client.get_aws_credentials(tenant_id)
        s3 = boto3.client("s3", **aws.to_boto3_config())
    """
    
    def __init__(
        self,
        lambda_name: Optional[str] = None,
        app_token_secret: Optional[str] = None,
        token_endpoint: Optional[str] = None,
    ):
        """
        Initialize the tenant context client.
        
        Args:
            lambda_name: Tenant context lambda name (default: from env)
            app_token_secret: Secrets Manager secret name (default: from env)
            token_endpoint: Full Okta token endpoint URL (default: from IDENTITY_PROVIDER_TOKEN_ENDPOINT env var)
        """
        self._lambda_name = lambda_name or TENANT_CONTEXT_LAMBDA_NAME
        if not self._lambda_name:
            raise ValueError(
                "Tenant context lambda name is not set. "
                "Provide it explicitly or set the TENANT_CONTEXT_LAMBDA env var."
            )

        self._app_token_secret = app_token_secret or APP_TOKEN_SECRET_NAME
        if not self._app_token_secret:
            raise ValueError(
                "App token secret name is not set. "
                "Provide it explicitly or set the APP_TOKEN_SECRET env var."
            )

        self._token_endpoint = token_endpoint or os.environ.get("IDENTITY_PROVIDER_TOKEN_ENDPOINT", "")
        if not self._token_endpoint:
            raise ValueError(
                "Identity provider token endpoint is not set. "
                "Set the IDENTITY_PROVIDER_TOKEN_ENDPOINT env var."
            )
        
        # Cache tenant contexts by tenant_id
        self._context_cache: dict[str, dict[str, Any]] = {}
        self._cache_expiry: dict[str, float] = {}
    
    def _get_tenant_context(
        self,
        tenant_id: str,
        force_refresh: bool = False
    ) -> dict[str, Any]:
        """Fetch and cache tenant context."""
        cache_key = tenant_id
        
        if not force_refresh and cache_key in self._context_cache:
            if time.time() < self._cache_expiry.get(cache_key, 0):
                return self._context_cache[cache_key]
        
        # Fetch fresh context
        context = fetch_tenant_context(
            tenant_id=tenant_id,
            lambda_name=self._lambda_name,
        )
        
        # Cache for 50 minutes
        self._context_cache[cache_key] = context
        self._cache_expiry[cache_key] = time.time() + 3000
        
        return context
    
    def get_okta_credentials(
        self,
        tenant_id: Optional[str] = None,
        force_refresh: bool = False
    ) -> OktaCredentials:
        """
        Get Okta credentials for OAuth token generation.
        
        Used to authenticate with SID API, Exposure API, etc.
        
        Args:
            tenant_id: Tenant UUID (optional if credentials are tenant-agnostic)
            force_refresh: Bypass cache
            
        Returns:
            OktaCredentials with client_id, client_secret, token_endpoint
        """
        if tenant_id:
            context = self._get_tenant_context(tenant_id, force_refresh)

            # Credentials may be at top level OR nested under tenantSystemCredentials
            sys_creds = context.get("tenantSystemCredentials") or {}
            client_id = (
                context.get("clientId") or context.get("clientID")
                or sys_creds.get("clientId") or sys_creds.get("clientID", "")
            )
            client_secret = (
                context.get("clientSecret")
                or sys_creds.get("clientSecret", "")
            )
            token_endpoint = (
                context.get("tokenEndpoint")
                or sys_creds.get("tokenEndpoint")
                or self._token_endpoint
            )

            # Fallback to env vars when tenant context doesn't supply credentials
            # (e.g. dev tenants not yet registered with OAuth app credentials)
            if not client_id:
                client_id = os.environ.get("SID_CLIENT_ID", "")
            if not client_secret:
                client_secret = os.environ.get("SID_CLIENT_SECRET", "")
            if not token_endpoint:
                token_endpoint = os.environ.get(
                    "IDENTITY_PROVIDER_TOKEN_ENDPOINT", self._token_endpoint
                )

            return OktaCredentials(
                client_id=client_id,
                client_secret=client_secret,
                token_endpoint=token_endpoint,
            )
        else:
            # No tenant_id - try to get from app token directly
            # This path is for shared/non-tenant-specific credentials
            app_token = get_app_token(self._app_token_secret)
            
            # App token format may include client credentials
            # Try parsing as JSON first
            try:
                token_data = json.loads(app_token)
                return OktaCredentials(
                    client_id=token_data.get("clientId", token_data.get("client_id", "")),
                    client_secret=token_data.get("clientSecret", token_data.get("client_secret", "")),
                    token_endpoint=token_data.get("tokenEndpoint", self._token_endpoint),
                )
            except json.JSONDecodeError:
                raise ValueError("Cannot extract Okta credentials without tenant_id")
    
    def get_aws_credentials(
        self,
        tenant_id: str,
        force_refresh: bool = False
    ) -> Optional[AWSCredentials]:
        """
        Get AWS credentials for cross-account access.
        
        NOTE: For ECS Fargate tasks, you DON'T need this - the task role
        is assigned via TaskRoleArn and credentials are automatic.
        
        For AWS Batch jobs, you can either:
        1. Use this method with credentials from tenant context
        2. Use STS AssumeRole with TENANT_ROLE_ARN env var (preferred)
        
        Args:
            tenant_id: Tenant UUID
            force_refresh: Bypass cache
            
        Returns:
            AWSCredentials or None if not available
        """
        context = self._get_tenant_context(tenant_id, force_refresh)
        aws_keys = context.get("awsSessionKeys", {})
        
        if not aws_keys.get("accessKey"):
            return None
        
        return AWSCredentials(
            access_key=aws_keys.get("accessKey", ""),
            secret_key=aws_keys.get("secretKey", ""),
            session_token=aws_keys.get("sessionToken", ""),
        )
    
    def get_sde_config(
        self,
        tenant_id: str,
        force_refresh: bool = False
    ) -> dict[str, Any]:
        """
        Get Synergy Data Exchange config for a tenant.
        
        Convenience wrapper around the module-level function.
        
        Args:
            tenant_id: Tenant UUID
            force_refresh: Bypass cache
            
        Returns:
            Synergy Data Exchange config dict
        """
        context = self._get_tenant_context(tenant_id, force_refresh)
        return get_sde_config(context)
