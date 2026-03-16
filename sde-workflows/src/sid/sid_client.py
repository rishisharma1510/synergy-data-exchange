"""
SID API Client

Handles communication with external SID allocation API.
Uses range allocation model: one API call per table returns {start, count}.
"""

import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Optional
import time

logger = logging.getLogger(__name__)


@dataclass
class SIDAllocationResponse:
    """Response from SID API allocation request."""
    start: int  # Starting SID value
    count: int  # Number of SIDs allocated
    
    @property
    def end(self) -> int:
        """Last SID value (inclusive)."""
        return self.start + self.count - 1


class SIDClientBase(ABC):
    """Abstract base class for SID API clients."""
    
    @abstractmethod
    def allocate(self, table_name: str, count: int) -> SIDAllocationResponse:
        """
        Allocate a range of SIDs for a table.
        
        Args:
            table_name: Name of the table requiring SIDs.
            count: Number of SIDs to allocate.
            
        Returns:
            SIDAllocationResponse with start and count.
        """
        pass
    
    @abstractmethod
    def health_check(self) -> bool:
        """Check if the SID API is available."""
        pass


class MockSIDClient(SIDClientBase):
    """
    Mock SID API client for local development and testing.
    
    Simulates range allocation by maintaining state per table.
    """
    
    def __init__(self, starting_sid: int = 1_000_000):
        """
        Initialize mock client.
        
        Args:
            starting_sid: Base SID to start allocations from.
        """
        self._next_sid: Dict[str, int] = {}
        self._starting_sid = starting_sid
        self._allocation_history: list = []
    
    def allocate(self, table_name: str, count: int) -> SIDAllocationResponse:
        """
        Allocate a mock range of SIDs.
        
        Args:
            table_name: Name of the table.
            count: Number of SIDs to allocate.
            
        Returns:
            SIDAllocationResponse with contiguous range.
        """
        if count <= 0:
            raise ValueError(f"Count must be positive, got {count}")
        
        # Get or initialize the next SID for this table
        if table_name not in self._next_sid:
            # Different starting ranges per table for easier debugging
            table_offset = hash(table_name) % 1000 * 100_000
            self._next_sid[table_name] = self._starting_sid + abs(table_offset)
        
        start = self._next_sid[table_name]
        self._next_sid[table_name] = start + count
        
        response = SIDAllocationResponse(start=start, count=count)
        
        # Track allocation history for testing
        self._allocation_history.append({
            "table_name": table_name,
            "start": start,
            "count": count,
            "timestamp": time.time(),
        })
        
        logger.info(f"Mock SID allocation: {table_name} -> start={start}, count={count}")
        return response
    
    def health_check(self) -> bool:
        """Mock health check always returns True."""
        return True
    
    def get_allocation_history(self) -> list:
        """Get history of all allocations (for testing)."""
        return self._allocation_history.copy()
    
    def reset(self) -> None:
        """Reset all state (for testing)."""
        self._next_sid.clear()
        self._allocation_history.clear()


class SIDClient(SIDClientBase):
    """
    Production SID API client.
    
    Authenticates via Tenant Context Lambda and calls external SID API.
    
    Authentication flow:
        Secrets Manager (App Token)
            ↓
        Tenant Context Lambda (aws-sd-lambda-tenant-context)
            ↓ returns: { clientID, clientSecret }
        Okta Token API
            ↓ returns: Bearer token
        SID API
    """
    
    def __init__(
        self,
        sid_api_url: str,
        tenant_id: Optional[str] = None,
        tenant_context_lambda_name: Optional[str] = None,
        app_token_secret_name: Optional[str] = None,
        max_retries: int = 3,
        retry_backoff_base: float = 1.0,
    ):
        """
        Initialize production SID client.

        Args:
            sid_api_url: Base URL of the SID API.
            tenant_id: Tenant UUID for fetching OAuth credentials. If not provided,
                       reads from TENANT_ID environment variable.
            tenant_context_lambda_name: Name of Tenant Context Lambda.
                       Falls back to TENANT_CONTEXT_LAMBDA env var.
            app_token_secret_name: Name of app token secret in Secrets Manager.
                       Falls back to APP_TOKEN_SECRET env var.
            max_retries: Maximum retry attempts on failure.
            retry_backoff_base: Base delay for exponential backoff (seconds).
        """
        self._sid_api_url = sid_api_url.rstrip("/")
        self._tenant_id = tenant_id or os.environ.get("TENANT_ID")
        self._tenant_context_lambda_name = (
            tenant_context_lambda_name
            or os.environ.get("TENANT_CONTEXT_LAMBDA")
            or os.environ.get("TENANT_CONTEXT_LAMBDA_NAME")
        )
        if not self._tenant_context_lambda_name:
            raise ValueError(
                "Tenant context lambda name is not set. "
                "Provide it explicitly or set the TENANT_CONTEXT_LAMBDA env var."
            )

        self._app_token_secret_name = (
            app_token_secret_name
            or os.environ.get("APP_TOKEN_SECRET")
            or os.environ.get("APP_TOKEN_SECRET_NAME")
        )
        if not self._app_token_secret_name:
            raise ValueError(
                "App token secret name is not set. "
                "Provide it explicitly or set the APP_TOKEN_SECRET env var."
            )
        self._max_retries = max_retries
        self._retry_backoff_base = retry_backoff_base
        
        # Use shared TenantContextClient
        self._tenant_context_client = None
        
        # Cached auth token
        self._bearer_token: Optional[str] = None
        self._token_expiry: Optional[float] = None
    
    def _get_tenant_context_client(self):
        """Get or create the shared TenantContextClient."""
        if self._tenant_context_client is None:
            from src.core.tenant_context import TenantContextClient
            self._tenant_context_client = TenantContextClient(
                lambda_name=self._tenant_context_lambda_name,
                app_token_secret=self._app_token_secret_name,
            )
        return self._tenant_context_client
    
    def _get_okta_token(self, client_id: str, client_secret: str, token_endpoint: str) -> str:
        """
        Exchange client credentials for Okta bearer token.
        
        Returns:
            Bearer token string.
        """
        import requests

        masked_id = (client_id[:6] + '...') if client_id else '(empty)'
        masked_secret = ('***set***' if client_secret else '(empty)')
        logger.info(
            "SID Okta token request: endpoint=%s client_id=%s client_secret=%s scope=default",
            token_endpoint, masked_id, masked_secret,
        )

        response = requests.post(
            token_endpoint,
            data={
                "grant_type": "client_credentials",
                "scope": "default",
            },
            auth=(client_id, client_secret),
            timeout=30,
        )
        logger.info(
            "SID Okta token response: status=%d body=%s",
            response.status_code, response.text[:300],
        )
        response.raise_for_status()
        return response.json()["access_token"]
    
    def _ensure_authenticated(self) -> str:
        """
        Ensure we have a valid bearer token.
        
        Returns:
            Valid bearer token.
        """
        # Check if cached token is still valid
        if self._bearer_token and self._token_expiry:
            if time.time() < self._token_expiry - 60:  # 60 second buffer
                return self._bearer_token
        
        # Get fresh token using shared TenantContextClient
        tenant_ctx = self._get_tenant_context_client()
        okta_creds = tenant_ctx.get_okta_credentials(self._tenant_id)
        logger.info(
            "SID auth: tenant_id=%s client_id=%s token_endpoint=%s",
            self._tenant_id,
            (okta_creds.client_id[:6] + '...') if okta_creds.client_id else '(empty)',
            okta_creds.token_endpoint or '(empty)',
        )

        self._bearer_token = self._get_okta_token(
            okta_creds.client_id,
            okta_creds.client_secret,
            okta_creds.token_endpoint,
        )
        self._token_expiry = time.time() + 3600  # Assume 1 hour validity
        
        return self._bearer_token
    
    def allocate(self, table_name: str, count: int) -> SIDAllocationResponse:
        """
        Allocate a range of SIDs from the production API.

        Endpoint: POST {sid_api_url}/sids/{table_name.lower()}?count={count}

        Args:
            table_name: Name of the table (object name in the SID API).
            count: Number of SIDs to allocate.

        Returns:
            SIDAllocationResponse with start and count.

        Raises:
            RuntimeError: If allocation fails after retries.
        """
        import requests

        if count <= 0:
            raise ValueError(f"Count must be positive, got {count}")

        last_error = None

        for attempt in range(self._max_retries):
            try:
                token = self._ensure_authenticated()

                url = f"{self._sid_api_url}/sids/{table_name.lower()}"
                logger.info(
                    "SID allocate request: POST %s?count=%d token_prefix=%s",
                    url, count, token[:12] + '...' if token else '(empty)',
                )
                response = requests.post(
                    url,
                    params={"count": count},
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                    },
                    timeout=30,
                )
                logger.info(
                    "SID allocate response: status=%d body=%s",
                    response.status_code, response.text[:300],
                )
                response.raise_for_status()

                logger.debug(
                    "SID API response [%s]: status=%d body=%s",
                    table_name, response.status_code, response.text,
                )

                data = response.json()
                # API returns {lowerBound, upperBound, count} — map to our model
                start = data.get("start") or data.get("lowerBound")
                count_returned = data.get("count") or (data["upperBound"] - data["lowerBound"] + 1)
                result = SIDAllocationResponse(
                    start=start,
                    count=count_returned,
                )

                logger.info(
                    "SID allocation: table=%s start=%d count=%d end=%d",
                    table_name, result.start, result.count, result.end,
                )
                return result
                
            except Exception as e:
                last_error = e
                logger.warning(f"SID allocation attempt {attempt + 1} failed: {e}")
                
                if attempt < self._max_retries - 1:
                    delay = self._retry_backoff_base * (2 ** attempt)
                    time.sleep(delay)
                    
                    # Clear cached token on auth errors
                    if "401" in str(e) or "403" in str(e):
                        self._bearer_token = None
                        self._token_expiry = None
        
        raise RuntimeError(f"SID allocation failed after {self._max_retries} attempts: {last_error}")
    
    def health_check(self) -> bool:
        """Check if the SID API is available."""
        import requests
        
        try:
            response = requests.get(
                f"{self._sid_api_url}/health",
                timeout=10,
            )
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"SID API health check failed: {e}")
            return False
