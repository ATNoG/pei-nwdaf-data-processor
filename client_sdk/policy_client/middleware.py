"""
FastAPI Middleware for policy enforcement on HTTP endpoints.
"""
import json
import logging
from typing import Optional, Callable

from fastapi import Request, Response, HTTPException, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from .client import PolicyClient

logger = logging.getLogger(__name__)


class PolicyMiddleware(BaseHTTPMiddleware):
    """
    FastAPI middleware that enforces policy on incoming requests.

    Extracts component ID from headers and checks access permissions.
    Can also transform response data based on policy rules.
    """

    def __init__(
        self,
        app: ASGIApp,
        policy_service_url: str,
        component_id: str,
        enable_policy: bool = False,
        source_header: str = "X-Component-ID",
        anonymous_fallback: str = "anonymous"
    ):
        """
        Initialize the Policy Middleware.

        Args:
            app: ASGI application
            policy_service_url: URL of Policy Service
            component_id: This component's ID
            enable_policy: Whether to enable policy enforcement
            source_header: Header name to extract source component ID from
            anonymous_fallback: Component ID to use if header is missing
        """
        super().__init__(app)
        self.component_id = component_id
        self.enable_policy = enable_policy
        self.source_header = source_header
        self.anonymous_fallback = anonymous_fallback

        self.policy_client = PolicyClient(
            service_url=policy_service_url,
            component_id=component_id,
            enable_policy=enable_policy
        )

        logger.info(
            f"PolicyMiddleware initialized: component={component_id}, enabled={enable_policy}"
        )

    async def dispatch(self, request: Request, call_next) -> Response:
        """
        Process request with policy enforcement.

        Args:
            request: Incoming request
            call_next: Next middleware/handler in chain

        Returns:
            Response
        """
        # Extract source component ID from header
        source_id = request.headers.get(self.source_header, self.anonymous_fallback)

        # Build resource path from request
        resource = request.url.path
        action = request.method.lower()

        # Check policy if enabled
        if self.enable_policy:
            try:
                allowed = await self.policy_client.check_access(
                    source_id=source_id,
                    sink_id=self.component_id,
                    resource=resource,
                    action=action
                )

                if not allowed:
                    return JSONResponse(
                        status_code=status.HTTP_403_FORBIDDEN,
                        content={
                            "error": "forbidden",
                            "message": f"Access denied: {source_id} not allowed to {action} {resource}"
                        }
                    )

            except Exception as e:
                logger.warning(f"Policy check failed: {e}")
                if not self.policy_client.fail_open:
                    return JSONResponse(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        content={"error": "policy_error", "message": "Policy check failed"}
                    )

        # Process request
        response = await call_next(request)

        # Transform response data if applicable
        if self.enable_policy and response.headers.get("content-type", "").startswith("application/json"):
            response = await self._transform_response(response, source_id)

        return response

    async def _transform_response(self, response: Response, source_id: str) -> Response:
        """
        Transform response data based on policy rules.

        Args:
            response: Original response
            source_id: Source component ID

        Returns:
            Transformed response
        """
        try:
            # Get response body
            body = b""
            async for chunk in response.body_iterator:
                body += chunk

            # Try to parse as JSON
            try:
                data = json.loads(body.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                # Not JSON, return as-is
                return Response(
                    content=body,
                    status_code=response.status_code,
                    headers=dict(response.headers),
                    media_type=response.media_type
                )

            # Apply transformation (reverse direction: component -> source)
            result = await self.policy_client.process_data(
                source_id=self.component_id,
                sink_id=source_id,
                data=data
            )

            if result.allowed:
                # Return transformed data
                return JSONResponse(
                    content=result.data,
                    status_code=response.status_code,
                    headers=dict(response.headers)
                )
            else:
                # Access denied on response transformation
                logger.warning(f"Response transformation denied: {result.reason}")
                return JSONResponse(
                    content={"error": "transformation_denied", "message": result.reason},
                    status_code=status.HTTP_403_FORBIDDEN
                )

        except Exception as e:
            logger.warning(f"Response transformation failed: {e}")
            # Return original response on failure
            return response


class PolicyRequirement:
    """
    Decorator/dependency for requiring specific policy on endpoints.

    Usage:
        @app.get("/api/data")
        @policy_required(resource="api_data", action="read")
        async def get_data():
            return {"data": "sensitive"}
    """

    def __init__(
        self,
        resource: str,
        action: str = "read",
        policy_client: Optional[PolicyClient] = None
    ):
        """
        Initialize policy requirement.

        Args:
            resource: Resource identifier
            action: Required action
            policy_client: Policy client (will use request.state if None)
        """
        self.resource = resource
        self.action = action
        self.policy_client = policy_client

    async def __call__(
        self,
        source_id: str = None,
        policy_client: PolicyClient = None
    ) -> bool:
        """
        Check if policy requirement is met.

        Args:
            source_id: Source component ID
            policy_client: Policy client

        Returns:
            True if allowed
        """
        client = policy_client or self.policy_client
        if not client:
            return True  # No policy client, allow

        # This would be called via FastAPI Depends
        return await client.check_access(
            source_id=source_id,
            sink_id=client.component_id,
            resource=self.resource,
            action=self.action
        )


def policy_required(resource: str, action: str = "read"):
    """
    Decorator for requiring policy on an endpoint.

    Args:
        resource: Resource identifier
        action: Required action

    Usage:
        @app.get("/api/data")
        @policy_required(resource="api_data", action="read")
        async def get_data(request: Request):
            source_id = request.headers.get("X-Component-ID", "anonymous")
            # ... endpoint logic
    """
    def decorator(func: Callable):
        async def wrapper(*args, **kwargs):
            # Extract request from kwargs
            request = kwargs.get('request')
            if request:
                # Get policy client from app state
                policy_client = request.app.state.policy_client
                source_id = request.headers.get("X-Component-ID", "anonymous")

                if policy_client and policy_client.enable_policy:
                    allowed = await policy_client.check_access(
                        source_id=source_id,
                        sink_id=policy_client.component_id,
                        resource=resource,
                        action=action
                    )
                    if not allowed:
                        raise HTTPException(
                            status_code=status.HTTP_403_FORBIDDEN,
                            detail=f"Access denied: {source_id} not allowed to {action} {resource}"
                        )

            return await func(*args, **kwargs)
        return wrapper
    return decorator
