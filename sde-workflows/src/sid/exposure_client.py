"""
Exposure Management API Client

Handles communication with Exposure Management API to create new
ExposureSet/ExposureView entities and retrieve their SIDs.

The returned SIDs are used as "override mappings" during SID transformation,
so all FK references to ExposureSetSID/ExposureViewSID in the ingested data
will point to the newly created entities.
"""

import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Any
import time

import pyarrow as pa

logger = logging.getLogger(__name__)


@dataclass
class ExposureMetadata:
    """Metadata about exposure data being ingested."""

    # tExposureSet metadata
    exposure_set_names: List[str]
    exposure_set_count: int

    # tExposureView metadata (if present)
    exposure_view_names: List[str]
    exposure_view_count: int

    # Exposure type sent to the API for every set in this run.
    # Defaults to "Detailed"; can be overridden via Step Function input.
    exposure_type: str = "Detailed"

    # Additional context
    tenant_id: str = ""
    run_id: str = ""
    source_artifact: str = ""  # e.g., "tenant-backup.bak"


@dataclass
class ExposureSetResult:
    """Result for a single ExposureSet created via POST /sets."""
    name: str
    sid: int
    exposure_type: str = "Detailed"


@dataclass
class ExposureAllocationResponse:
    """Aggregated response from creating all ExposureSets via the Exposure API."""

    # One entry per ExposureSet created (same order as metadata.exposure_set_names)
    exposure_sets: List[ExposureSetResult]

    @property
    def exposure_set_sid(self) -> Optional[int]:
        """Primary (first) ExposureSet SID — convenience accessor."""
        return self.exposure_sets[0].sid if self.exposure_sets else None

    @property
    def exposure_set_sids(self) -> List[int]:
        """All created ExposureSet SIDs in creation order."""
        return [s.sid for s in self.exposure_sets]


class ExposureClientBase(ABC):
    """Abstract base class for Exposure Management API clients."""
    
    @abstractmethod
    def create_exposure(self, metadata: ExposureMetadata) -> ExposureAllocationResponse:
        """
        Create new ExposureSet/ExposureView entities in target system.
        
        Args:
            metadata: Information about the exposure data being ingested.
            
        Returns:
            ExposureAllocationResponse with new SIDs.
        """
        pass
    
    @abstractmethod
    def health_check(self) -> bool:
        """Check if the Exposure Management API is available."""
        pass


class MockExposureClient(ExposureClientBase):
    """
    Mock Exposure Management API client for local development and testing.
    
    Simulates exposure creation by returning incrementing SIDs.
    """
    
    def __init__(self, starting_sid: int = 9_000_000):
        """
        Initialize mock client.
        
        Args:
            starting_sid: Base SID to start allocations from.
        """
        self._next_sid = starting_sid
        self._allocation_history: list = []
    
    def create_exposure(self, metadata: ExposureMetadata) -> ExposureAllocationResponse:
        """
        Create mock exposure entities — returns incrementing SIDs without calling any API.
        """
        results: List[ExposureSetResult] = []

        names = metadata.exposure_set_names or [f"MockSet_{metadata.tenant_id}"]
        for name in names:
            result = ExposureSetResult(
                name=name,
                sid=self._next_sid,
                exposure_type=metadata.exposure_type,
            )
            self._next_sid += 1
            results.append(result)

        response = ExposureAllocationResponse(exposure_sets=results)

        # Track allocation history for testing
        self._allocation_history.append({
            "metadata": metadata,
            "response": response,
            "timestamp": time.time(),
        })

        logger.info(
            "MockExposureClient: Created ExposureSets %s",
            {r.name: r.sid for r in results},
        )

        return response
    
    def health_check(self) -> bool:
        """Mock health check always returns True."""
        return True
    
    @property
    def allocation_history(self) -> list:
        """Get history of allocations for testing."""
        return self._allocation_history


class ExposureClient(ExposureClientBase):
    """
    Production Exposure Management API client.

    Authentication flow (mirrors SIDClient):
        Secrets Manager (App Token)
            ↓
        Tenant Context Lambda (aws-sd-lambda-tenant-context)
            ↓ returns: { clientId, clientSecret }
        IDENTITY_PROVIDER_TOKEN_ENDPOINT  (Okta client-credentials grant)
            ↓ returns: Bearer JWT
        Exposure Management API
    """

    def __init__(
        self,
        api_url: str,
        tenant_id: Optional[str] = None,
        tenant_context_lambda_name: Optional[str] = None,
        app_token_secret_name: Optional[str] = None,
        max_retries: int = 3,
        retry_backoff_base: float = 1.0,
        timeout: float = 30.0,
    ):
        """
        Initialize production client.

        Args:
            api_url: Base URL of Exposure Management API.
            tenant_id: Tenant UUID for fetching OAuth credentials.
                       Falls back to TENANT_ID env var if not provided.
            tenant_context_lambda_name: Name of Tenant Context Lambda.
                       Falls back to TENANT_CONTEXT_LAMBDA env var.
            app_token_secret_name: Name of app token secret in Secrets Manager.
                       Falls back to APP_TOKEN_SECRET env var.
            max_retries: Maximum number of retry attempts.
            retry_backoff_base: Base seconds for exponential backoff.
            timeout: Request timeout in seconds.
        """
        self._api_url = api_url.rstrip("/")
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
        self._timeout = timeout

        # Shared TenantContextClient (lazy init)
        self._tenant_context_client = None

        # Cached bearer token
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

    def _ensure_authenticated(self) -> str:
        """
        Return a valid bearer token, refreshing via Okta if needed.

        Flow:
          TenantContextClient → OktaCredentials(client_id, client_secret, token_endpoint)
          POST token_endpoint (client_credentials) → access_token (JWT)
        """
        if self._bearer_token and self._token_expiry:
            if time.time() < self._token_expiry - 60:  # 60 s buffer
                return self._bearer_token  # type: ignore[return-value]  # guarded by truthiness check above

        import requests

        tenant_ctx = self._get_tenant_context_client()
        creds = tenant_ctx.get_okta_credentials(self._tenant_id)

        response = requests.post(
            creds.token_endpoint,
            data={
                "grant_type": "client_credentials",
                "scope": "exposure:write",
            },
            auth=(creds.client_id, creds.client_secret),
            timeout=30,
        )
        response.raise_for_status()

        self._bearer_token = response.json()["access_token"]
        self._token_expiry = time.time() + 3600  # assume 1 h validity
        return self._bearer_token

    def _make_request(self, method: str, endpoint: str, expected_status: int = 200, **kwargs) -> Dict[str, Any]:
        """
        Make HTTP request with retry logic and automatic token refresh on 401.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path.
            expected_status: Expected success HTTP status code (default 200).
            **kwargs: Additional arguments for requests.

        Returns:
            JSON response data.

        Raises:
            ExposureAPIError: If request fails after retries.
        """
        import requests

        url = f"{self._api_url}/{endpoint.lstrip('/')}"

        last_error = None
        for attempt in range(self._max_retries):
            try:
                token = self._ensure_authenticated()
                response = requests.request(
                    method=method,
                    url=url,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                    },
                    timeout=self._timeout,
                    **kwargs,
                )

                logger.debug(
                    "Exposure API response [%s %s]: status=%d body=%s",
                    method, url, response.status_code, response.text,
                )

                if response.status_code != expected_status:
                    # Parse RFC 7807 / ProblemDetails error body if present
                    try:
                        err = response.json()
                        detail = err.get("detail") or err.get("title") or response.text
                    except Exception:
                        detail = response.text
                    raise ExposureAPIError(
                        f"Exposure API {method} {url} returned {response.status_code}: {detail}"
                    )

                return response.json()

            except ExposureAPIError:
                raise  # don't retry on known API errors (wrong status)
            except Exception as e:
                last_error = e
                if attempt < self._max_retries - 1:
                    # Expire cached token on auth errors so next attempt re-fetches
                    if "401" in str(e) or "403" in str(e):
                        self._bearer_token = None
                        self._token_expiry = None
                    sleep_time = self._retry_backoff_base * (2 ** attempt)
                    logger.warning(
                        "Exposure API request failed (attempt %d/%d): %s. Retrying in %.1fs",
                        attempt + 1,
                        self._max_retries,
                        str(e),
                        sleep_time,
                    )
                    time.sleep(sleep_time)

        raise ExposureAPIError(f"Request failed after {self._max_retries} attempts: {last_error}")
    
    def create_exposure(self, metadata: ExposureMetadata) -> ExposureAllocationResponse:
        """
        Create one ExposureSet per name in the source data via POST {api_url}/sets.

        Endpoint: POST {exposure_api_url}/sets
        Request:  {"name": "<ExposureSetName>", "type": "Detailed"}
        Success:  201 — response body contains {"sid": <int>, "name": ..., ...}
        Error:    non-201 — response body is RFC 7807 ProblemDetails

        Args:
            metadata: Exposure metadata extracted from tExposureSet table.

        Returns:
            ExposureAllocationResponse with one ExposureSetResult per name created.
        """
        if not metadata.exposure_set_names:
            raise ExposureAPIError(
                "No ExposureSet names found in source data — cannot call Exposure API"
            )

        logger.info(
            "Creating %d ExposureSet(s) via Exposure API: %s",
            len(metadata.exposure_set_names),
            metadata.exposure_set_names,
        )

        results: List[ExposureSetResult] = []

        for name in metadata.exposure_set_names:
            request_body = {
                "name": name,
                "type": metadata.exposure_type,
            }

            logger.info("Creating ExposureSet: name=%s", name)

            data = self._make_request(
                method="POST",
                endpoint="/sets",
                expected_status=201,
                json=request_body,
            )

            result = ExposureSetResult(
                name=data["name"],
                sid=data["sid"],
                exposure_type=data.get("type", "Detailed"),
            )
            results.append(result)

            logger.info(
                "ExposureSet created: name=%s sid=%d type=%s",
                result.name, result.sid, result.exposure_type,
            )

        logger.info(
            "All ExposureSets created — SIDs: %s",
            {r.name: r.sid for r in results},
        )

        return ExposureAllocationResponse(exposure_sets=results)
    
    def health_check(self) -> bool:
        """Check if the Exposure Management API is available."""
        import requests
        try:
            token = self._ensure_authenticated()
            response = requests.get(
                f"{self._api_url}/health",
                headers={"Authorization": f"Bearer {token}"},
                timeout=10,
            )
            return response.status_code == 200
        except Exception as e:
            logger.warning("Exposure API health check failed: %s", str(e))
            return False


class ExposureAPIError(Exception):
    """Exception raised when Exposure API calls fail."""
    pass


def extract_exposure_metadata(
    tables_data: Dict[str, pa.Table],
    tenant_id: str,
    run_id: str,
    source_artifact: str,
) -> ExposureMetadata:
    """
    Extract exposure metadata from ingested table data.

    ExposureSet names are always read from the tExposureSet table in the
    restored source artifact.  A new SID is generated via the Exposure API
    for each ExposureSet found.

    Args:
        tables_data: Dict mapping table name to PyArrow Table.
        tenant_id: Tenant identifier.
        run_id: Run/activity identifier.
        source_artifact: Source artifact path (e.g., "backup.bak").

    Returns:
        ExposureMetadata with information extracted from tExposureSet.
    """
    exposure_view_names = []
    exposure_view_count = 0

    # --- ExposureSet names — always from tExposureSet table ---
    exposure_set_names = []
    exposure_set_count = 0
    if "tExposureSet" in tables_data:
        table = tables_data["tExposureSet"]
        exposure_set_count = table.num_rows
        if "ExposureSetName" in table.column_names:
            exposure_set_names = [
                n for n in table.column("ExposureSetName").to_pylist() if n is not None
            ]
    logger.info(
        "ExposureSet names extracted from tExposureSet table: %s", exposure_set_names
    )

    # --- ExposureView (always from table, no user override) ---
    if "tExposureView" in tables_data:
        table = tables_data["tExposureView"]
        exposure_view_count = table.num_rows
        if "ExposureViewName" in table.column_names:
            exposure_view_names = [
                n for n in table.column("ExposureViewName").to_pylist() if n is not None
            ]

    return ExposureMetadata(
        exposure_set_names=exposure_set_names,
        exposure_set_count=exposure_set_count,
        exposure_view_names=exposure_view_names,
        exposure_view_count=exposure_view_count,
        exposure_type="Detailed",
        tenant_id=tenant_id,
        run_id=run_id,
        source_artifact=source_artifact,
    )


def create_exposure_client(
    api_url: Optional[str] = None,
    use_mock: bool = False,
    mock_starting_sid: int = 9_000_000,
    tenant_id: Optional[str] = None,
    tenant_context_lambda_name: Optional[str] = None,
    app_token_secret_name: Optional[str] = None,
    max_retries: int = 3,
) -> ExposureClientBase:
    """
    Factory function to create appropriate Exposure client.

    Args:
        api_url: Exposure Management API URL (required if use_mock=False).
        use_mock: If True, return MockExposureClient.
        mock_starting_sid: Starting SID for mock client.
        tenant_id: Tenant UUID (falls back to TENANT_ID env var).
        tenant_context_lambda_name: Lambda name (falls back to env/default).
        app_token_secret_name: Secret name (falls back to env/default).
        max_retries: Max retries for production client.

    Returns:
        ExposureClientBase implementation.
    """
    if use_mock:
        logger.info("Creating MockExposureClient (starting_sid=%d)", mock_starting_sid)
        return MockExposureClient(starting_sid=mock_starting_sid)

    if not api_url:
        raise ValueError("api_url is required when use_mock=False")

    logger.info("Creating ExposureClient (api_url=%s)", api_url)
    return ExposureClient(
        api_url=api_url,
        tenant_id=tenant_id,
        tenant_context_lambda_name=tenant_context_lambda_name or None,
        app_token_secret_name=app_token_secret_name or None,
        max_retries=max_retries,
    )
