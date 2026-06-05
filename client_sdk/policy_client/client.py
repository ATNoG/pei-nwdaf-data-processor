"""
Policy Client - Main client class for components to interact with Policy Service.

This client now supports local data transformation. Instead of sending data to the
policy service for transformation, it fetches the transformation pipeline configuration
and applies transformations locally. This improves privacy, performance, and scalability.
"""
import asyncio
import json
import logging
import threading
import time
from typing import Any, Callable, Coroutine

import aiohttp

from cachetools import TTLCache

logger = logging.getLogger(__name__)


async def retry_with_backoff(
    coro_func,
    max_retries: int = 5,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    backoff_factor: float = 2.0
):
    """
    Retry a coroutine with exponential backoff.

    Args:
        coro_func: A callable that returns a coroutine (function, not coroutine object)
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds
        max_delay: Maximum delay between retries
        backoff_factor: Multiplier for delay after each retry

    Returns:
        The result of the coroutine

    Raises:
        The last exception if all retries fail
    """
    last_exception = None
    delay = initial_delay

    for attempt in range(max_retries + 1):
        try:
            # Create a new coroutine on each attempt by calling the function
            coro = coro_func()
            return await coro
        except Exception as e:
            last_exception = e
            if attempt < max_retries:
                logger.warning(
                    f"Request failed (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                    f"Retrying in {delay:.1f}s..."
                )
                await asyncio.sleep(min(delay, max_delay))
                delay *= backoff_factor
            else:
                logger.error(f"Request failed after {max_retries + 1} attempts: {e}")

    raise last_exception

# Type alias for field source - can be a list, URL string, or callable function
FieldSource = (
    list[str]  # Direct list of field names
    | str  # URL to fetch fields from
    | Callable[[], list[str]]  # Sync function returning field names
    | Callable[[], Coroutine[None, None, list[str]]]  # Async function returning field names
)


class ProcessResult:
    """Result of policy data processing."""

    def __init__(
        self,
        allowed: bool,
        data: dict[str, Any] | None = None,
        reason: str = "",
        transformations: list[str] | None = None
    ):
        self.allowed = allowed
        self.data = data
        self.reason = reason
        self.transformations = transformations or []


class PolicyClient:
    """
    Lightweight client SDK for policy enforcement.

    This client provides caching and fail-open functionality
    for components to interact with the Policy Service.
    """

    def __init__(
        self,
        service_url: str,
        component_id: str,
        fields: FieldSource | None = None,
        cache_ttl: int = 60,
        cache_size: int = 1000,
        timeout: int = 5,
        registration_timeout: int = 60,
        heartbeat_interval: int = 30,
        enable_policy: bool = False,
        fail_open: bool = True
    ):
        """
        Initialize the Policy Client.

        Args:
            service_url: URL of the Policy Service (e.g., "http://policy-service:8000")
            component_id: This component's ID
            fields: Field source - can be:
                - list[str]: Direct list of field names
                - str: URL to fetch fields from (HTTP GET expected to return JSON with 'fields' key)
                - Callable: Function that returns field names (sync or async)
            cache_ttl: Cache TTL in seconds
            cache_size: Maximum cache size
            timeout: Request timeout in seconds for hot-path calls (policy checks, field fetches)
            registration_timeout: Request timeout in seconds for component registration.
                Registration involves N sequential Permit.io attribute creations on the server
                and can take much longer than a hot-path check.  Keep this well above the
                expected wall-clock time (default 60 s).
            enable_policy: Whether policy enforcement is enabled (policyless mode if False)
            fail_open: If True, allow data through on policy check failures
        """
        self.service_url = service_url.rstrip("/")
        self.component_id = component_id
        self.timeout = timeout
        self.registration_timeout = registration_timeout
        self.heartbeat_interval = heartbeat_interval
        self.enable_policy = enable_policy
        self.fail_open = fail_open
        self._fields_source = fields

        # Decision cache
        self.cache = TTLCache(maxsize=cache_size, ttl=cache_ttl)

        # Pipeline version tracking — used to detect stale cached configs
        self._last_pipeline_version: int | None = None  # None = never checked
        self._last_version_check_time: float = 0.0  # monotonic timestamp

        # Compiled pipeline object cache — plain dict, invalidated on version change.
        # Avoids rebuilding TransformerPipeline from config on every record.
        self._pipeline_obj_cache: dict[str, Any] = {}

        # Field cache key for callable sources
        self._fields_cache_key = f"{self.component_id}:fields"

        # Heartbeat state — populated on first successful register_component call
        self._last_register_kwargs: dict | None = None
        self._heartbeat_task: asyncio.Task | None = None

        logger.info(
            f"PolicyClient initialized: service={service_url}, "
            f"component={component_id}, enabled={enable_policy}"
        )

    async def _get_fields(self) -> list[str]:
        """
        Resolve field names from the configured source.

        For callable sources, uses TTL cache to allow fields to be refreshed
        as the underlying data changes.

        Returns:
            List of field names
        """
        if self._fields_source is None:
            return []

        # Direct list - no caching needed (static)
        if isinstance(self._fields_source, list):
            return self._fields_source

        # Check TTL cache for callable sources (URLs and functions)
        # This allows fields to be refreshed when the cache expires
        cached_fields = self.cache.get(self._fields_cache_key)
        if cached_fields is not None:
            return cached_fields

        # URL string - fetch from endpoint
        if isinstance(self._fields_source, str):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(self._fields_source, timeout=aiohttp.ClientTimeout(total=self.timeout)) as response:
                        response.raise_for_status()
                        data = await response.json()
                        # Support both direct list and nested 'fields' key
                        if isinstance(data, list):
                            fields = data
                        elif isinstance(data, dict) and "fields" in data:
                            fields = data["fields"]
                        else:
                            logger.warning(f"Unexpected response format from fields endpoint: {data}")
                            fields = []

                        # Cache the result
                        self.cache[self._fields_cache_key] = fields
                        return fields
            except Exception as e:
                logger.error(f"Failed to fetch fields from URL {self._fields_source}: {e}")
                return []

        # Callable - invoke and get fields
        result = self._fields_source()
        if asyncio.iscoroutine(result):
            # Async callable
            fields = await result
        else:
            # Sync callable
            fields = result

        # Cache the result with TTL
        self.cache[self._fields_cache_key] = fields
        return fields

    async def register_component(
        self,
        component_type: str,
        role: str | None = None,
        additional_roles: list[str] | None = None,
        permit_user_key: str | None = None,
        data_columns: list[str] | None = None,
        auto_create_attributes: bool = True,
        allowed_fields: dict[str, list[str]] | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> bool:
        """
        Register this component with the Policy Service.

        Fields are automatically resolved from the configured source if data_columns is not provided.
        Uses exponential backoff retry to handle temporary failures like Policy Service restarts.

        Args:
            component_type: Type of component
            role: Optional primary role to assign
            additional_roles: Optional additional roles beyond primary
            permit_user_key: Optional Permit.io user key override (for sharing)
            data_columns: Optional data columns (if not provided, uses resolved fields)
            auto_create_attributes: Whether to auto-create attributes
            allowed_fields: Optional allowed fields per sink
            attributes: Optional additional component attributes

        Returns:
            True if successful
        """
        # Resolve fields if data_columns not provided
        if data_columns is None:
            data_columns = await self._get_fields()

        async def _do_register():
            payload = {
                "component_id": self.component_id,
                "component_type": component_type,
                "role": role,
                "data_columns": data_columns,
                "auto_create_attributes": auto_create_attributes,
                "allowed_fields": allowed_fields,
            }
            if additional_roles is not None:
                payload["additional_roles"] = additional_roles
            if permit_user_key is not None:
                payload["permit_user_key"] = permit_user_key
            if attributes is not None:
                payload["attributes"] = attributes
            await self._request(
                "POST",
                "/api/v1/components",
                payload,
                timeout_override=self.registration_timeout,
            )

        try:
            await retry_with_backoff(_do_register)
            logger.info(f"Component registered: {self.component_id}")

            # Remember kwargs so start_heartbeat() / the sync heartbeat thread
            # can re-register with identical params after a Policy restart.
            self._last_register_kwargs = {
                "component_type": component_type,
                "role": role,
                "additional_roles": additional_roles,
                "permit_user_key": permit_user_key,
                "data_columns": data_columns,
                "auto_create_attributes": auto_create_attributes,
                "allowed_fields": allowed_fields,
                "attributes": attributes,
            }

            return True

        except Exception as e:
            logger.error(f"Failed to register component after retries: {e}")
            return False

    async def _heartbeat_loop(self) -> None:
        """
        Background task that periodically re-registers this component so the
        Policy Service registry stays populated after a Policy restart.
        """
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            if not self._last_register_kwargs:
                continue
            try:
                await self._request(
                    "POST",
                    "/api/v1/components",
                    {"component_id": self.component_id, **self._last_register_kwargs},
                    timeout_override=self.registration_timeout,
                )
                logger.debug(f"Heartbeat: re-registered {self.component_id}")
            except Exception as e:
                logger.debug(
                    f"Heartbeat re-registration failed for {self.component_id} "
                    f"(will retry in {self.heartbeat_interval}s): {e}"
                )

    async def start_heartbeat(self) -> None:
        """
        Start a background asyncio task that periodically re-registers this
        component so the Policy Service registry stays populated after a
        Policy restart.

        Call this once from your async lifespan / startup after the first
        successful ``register_component``.  It is a no-op if the heartbeat
        is already running or ``heartbeat_interval`` is 0.
        """
        if self.heartbeat_interval <= 0:
            return
        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            return  # Already running
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info(
            f"Heartbeat started for {self.component_id}: "
            f"re-registering every {self.heartbeat_interval}s"
        )

    async def stop_heartbeat(self) -> None:
        """Cancel the background heartbeat task if it is running."""
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        self._heartbeat_task = None
        logger.info(f"Heartbeat stopped for {self.component_id}")

    async def _request(
        self,
        method: str,
        endpoint: str,
        data: dict[str, Any] | None = None,
        timeout_override: int | None = None,
    ) -> dict:
        """
        Make an HTTP request to the Policy Service.

        Args:
            method: HTTP method
            endpoint: API endpoint
            data: Request body
            timeout_override: If given, use this timeout instead of ``self.timeout``.
                Pass ``self.registration_timeout`` for registration calls so they
                are not prematurely killed by the short hot-path timeout.

        Returns:
            Response JSON
        """
        url = f"{self.service_url}{endpoint}"
        effective_timeout = timeout_override if timeout_override is not None else self.timeout

        try:
            async with aiohttp.ClientSession() as session:
                async with session.request(
                    method=method,
                    url=url,
                    json=data,
                    timeout=aiohttp.ClientTimeout(total=effective_timeout)
                ) as response:
                    # Capture response body for better error messages
                    response_text = await response.text()
                    if response.status >= 400:
                        raise Exception(
                            f"HTTP {response.status}, message='{response.reason}', "
                            f"url='{url}', body='{response_text[:500]}'"
                        )
                    return json.loads(response_text) if response_text else {}

        except asyncio.TimeoutError:
            raise Exception(f"Timeout connecting to Policy Service: {url}")

        except aiohttp.ClientError as e:
            raise Exception(f"Failed to connect to Policy Service: {e}")

    def _generate_cache_key(self, *args) -> str:
        """Generate a cache key from arguments."""
        key_data = json.dumps(args, sort_keys=True, default=str)
        return f"{self.component_id}:{hash(key_data)}"

    async def check_access(
        self,
        source_id: str,
        sink_id: str,
        resource: str = "data",
        action: str = "read"
    ) -> bool:
        """
        Check if access is permitted.

        Args:
            source_id: Source component ID
            sink_id: Sink component ID
            resource: Resource type
            action: Action to perform

        Returns:
            True if access is permitted
        """
        if not self.enable_policy:
            return True

        # Check cache
        cache_key = self._generate_cache_key("check", source_id, sink_id, resource, action)
        if cache_key in self.cache:
            return self.cache[cache_key]

        try:
            response = await self._request(
                "POST",
                "/api/v1/policy/check",
                {
                    "source_id": source_id,
                    "sink_id": sink_id,
                    "resource": resource,
                    "action": action
                }
            )

            allowed = response.get("allowed", False)
            self.cache[cache_key] = allowed
            return allowed

        except Exception as e:
            logger.warning(f"Policy check failed: {e}")
            return self.fail_open

    async def process_data(
        self,
        source_id: str,
        sink_id: str,
        data: dict[str, Any],
        action: str = "read"
    ) -> ProcessResult:
        """
        Check policy and transform data locally.

        Fetches pipeline configuration, checks authorization via Permit.io
        (no data sent), and applies transformations locally.

        Args:
            source_id: Source component ID
            sink_id: Sink component ID
            data: Data to process
            action: Action type

        Returns:
            ProcessResult with allowed status and transformed data
        """
        if not self.enable_policy:
            return ProcessResult(
                allowed=True,
                data=data,
                reason="Policy disabled"
            )

        # Check authorization (no data sent to server)
        allowed = await self.check_access(source_id, sink_id, action=action)
        if not allowed:
            return ProcessResult(
                allowed=False,
                reason="Authorization denied"
            )

        # Get pipeline configuration (cached)
        try:
            pipeline_config = await self._get_pipeline_config(source_id, sink_id)
        except Exception as e:
            logger.warning(f"Failed to get pipeline config: {e}")
            # If we can't get config but auth passed, return data as-is
            return ProcessResult(
                allowed=True,
                data=data,
                reason=f"Pipeline config unavailable, using raw data: {e}"
            )

        # Apply transformations locally
        if pipeline_config.get("steps"):
            try:
                transformed_data = self._apply_pipeline_cached(source_id, sink_id, data, pipeline_config)
                transformations = [s["type"] for s in pipeline_config["steps"]]
            except Exception as e:
                logger.error(f"Failed to apply pipeline: {e}")
                if self.fail_open:
                    return ProcessResult(
                        allowed=True,
                        data=data,
                        reason=f"Pipeline application failed, using raw data: {e}"
                    )
                return ProcessResult(
                    allowed=False,
                    reason=f"Pipeline application failed: {e}"
                )
        else:
            transformed_data = data
            transformations = []

        return ProcessResult(
            allowed=True,
            data=transformed_data,
            reason="Data processed locally",
            transformations=transformations
        )

    async def receive_data(
        self,
        source_id: str,
        data: dict[str, Any],
        action: str = "read"
    ) -> ProcessResult:
        """
        Apply ingress-side policy when this component receives data from source_id.

        Equivalent to process_data(source_id, self.component_id, data) — the
        component is the sink. Enables pipelines like "kafka_to_<component>"
        to transform data before the component consumes it, without affecting
        other consumers of the same source.
        """
        return await self.process_data(source_id, self.component_id, data, action)

    async def _get_pipeline_config(self, source_id: str, sink_id: str) -> dict:
        """Fetch and cache pipeline configuration.

        Checks the server-side pipeline version before returning a cached
        result.  If the version has changed since the last fetch, all
        pipeline cache entries are evicted so the next fetch hits the server.
        """
        cache_key = f"pipeline:{source_id}:{sink_id}"

        # Before returning a cached pipeline, verify the version is still current.
        # The version check itself is cached for 5 seconds to avoid hammering
        # the server on every data record.
        if cached := self.cache.get(cache_key):
            await self._check_pipeline_version()
            # Re-check — _check_pipeline_version may have cleared the cache
            if cached := self.cache.get(cache_key):
                return cached

        response = await self._request(
            "GET",
            f"/api/v1/transformers/pipelines/{source_id}/{sink_id}"
        )
        self.cache[cache_key] = response
        return response

    async def _check_pipeline_version(self) -> None:
        """Check the server-side pipeline version.

        If it differs from the last-seen version, evict all ``pipeline:*``
        cache entries so subsequent calls fetch fresh configs.
        Rate-limited to at most once every 5 seconds.
        """
        # Rate-limit: only check at most once every 5 seconds
        now = time.monotonic()
        if now - self._last_version_check_time < 5.0:
            return

        try:
            resp = await self._request(
                "GET",
                "/api/v1/transformers/version",
            )
            self._last_version_check_time = time.monotonic()
            current_version = resp.get("version", 0)

            if self._last_pipeline_version is None:
                # First check — just remember the version
                self._last_pipeline_version = current_version
                return

            if current_version != self._last_pipeline_version:
                # Version changed — evict all pipeline cache entries
                pipeline_keys = [
                    k for k in list(self.cache.keys())
                    if isinstance(k, str) and k.startswith("pipeline:")
                ]
                for k in pipeline_keys:
                    del self.cache[k]
                # Also evict compiled pipeline objects
                evicted_objs = len(self._pipeline_obj_cache)
                self._pipeline_obj_cache.clear()
                self._last_pipeline_version = current_version
                logger.info(
                    f"Pipeline version changed to {current_version}, "
                    f"evicted {len(pipeline_keys)} cached config(s) "
                    f"and {evicted_objs} compiled pipeline(s)"
                )

        except Exception as e:
            # Version check failure should not break the hot path
            logger.debug(f"Pipeline version check failed: {e}")

    def _apply_pipeline_cached(self, source_id: str, sink_id: str, data: dict, pipeline_config: dict) -> dict:
        """Apply transformations using a cached TransformerPipeline object.

        The compiled pipeline is cached by (source_id, sink_id) and reused
        across records instead of rebuilding from config on every call.
        """
        from policy_client.transformers import TransformerPipeline

        cache_key = f"pipeline_obj:{source_id}:{sink_id}"
        pipeline = self._pipeline_obj_cache.get(cache_key)
        if pipeline is None:
            pipeline = TransformerPipeline.from_config(pipeline_config)
            self._pipeline_obj_cache[cache_key] = pipeline

        return pipeline.execute_sync(data)

    async def register_ml_model(
        self,
        model_id: str,
        model_name: str,
        input_fields: list[str],
        output_fields: list[str],
        data_type: str,
        architecture: str | None = None,
        permit_user_key: str | None = None,
        additional_roles: list[str] | None = None,
        window_duration_seconds: int | None = None,
    ) -> bool:
        """
        Register an ML model with the Policy Service.

        The model is registered as a first-class policy component
        (type ``ml_model``) via the ``POST /api/v1/ml/models`` endpoint.

        Args:
            model_id: Unique model identifier (e.g. UUID from MLflow)
            model_name: Human-readable model name
            input_fields: Input field names the model reads
            output_fields: Output field names the model produces
            data_type: Data type for role assignment
            architecture: Optional model architecture label (ann, lstm, …)
            permit_user_key: Optional shared Permit.io user key
            additional_roles: Optional additional roles beyond default ML role
            window_duration_seconds: Optional window duration in seconds

        Returns:
            True if successful
        """
        try:
            payload: dict[str, Any] = {
                "model_id": model_id,
                "model_name": model_name,
                "input_fields": input_fields,
                "output_fields": output_fields,
                "data_type": data_type,
            }
            if architecture is not None:
                payload["architecture"] = architecture
            if permit_user_key is not None:
                payload["permit_user_key"] = permit_user_key
            if additional_roles is not None:
                payload["additional_roles"] = additional_roles
            if window_duration_seconds is not None:
                payload["window_duration_seconds"] = window_duration_seconds

            await self._request(
                "POST",
                "/api/v1/ml/models",
                payload,
                timeout_override=self.registration_timeout,
            )
            logger.info(f"ML model registered: {model_id} (name={model_name})")
            return True

        except Exception as e:
            logger.error(f"Failed to register ML model: {e}")
            return False

    async def unregister_ml_model(self, model_name: str) -> bool:
        """
        Unregister an ML model from the Policy Service.

        Calls ``DELETE /api/v1/ml/models/{model_name}``.

        Args:
            model_name: Human-readable model name (used to derive component ID).

        Returns:
            True if successful, False on error.
        """
        try:
            await self._request(
                "DELETE",
                f"/api/v1/ml/models/{model_name}",
            )
            logger.info(f"ML model unregistered: {model_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to unregister ML model: {e}")
            return False

    def clear_cache(self) -> None:
        """Clear the decision cache."""
        self.cache.clear()

    def __repr__(self) -> str:
        return f"PolicyClient(component_id={self.component_id}, enabled={self.enable_policy})"


class SyncPolicyClient:
    """
    Synchronous wrapper for PolicyClient for non-async contexts.

    This wrapper works both inside and outside of running event loops:
    - Outside a running loop: uses asyncio.run() to create a new loop
    - Inside a running loop: submits the task and waits for it with run_coroutine_threadsafe
    """

    def __init__(self, async_client: PolicyClient):
        self._async_client = async_client
        self._loop = None
        self._heartbeat_stop: "threading.Event | None" = None
        self._heartbeat_thread: "threading.Thread | None" = None

    def _run_coroutine(self, coro, timeout: int | None = None):
        """
        Run a coroutine, handling both cases: with or without a running event loop.

        Args:
            coro: Coroutine to run.
            timeout: How long (seconds) to wait for the result when the call must
                run in a background thread (i.e. when a loop is already running).
                Defaults to ``self._async_client.timeout`` (the short hot-path
                timeout).  Pass ``self._async_client.registration_timeout`` for
                registration calls so the thread is not killed prematurely.
        """
        try:
            # Try to get the running loop
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop, use asyncio.run()
            return asyncio.run(coro)

        # We're inside a running loop - run in separate thread with own event loop
        import concurrent.futures

        def run_in_thread():
            """Run async code in a new thread with its own event loop."""
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            try:
                return new_loop.run_until_complete(coro)
            finally:
                new_loop.close()

        effective_timeout = timeout if timeout is not None else self._async_client.timeout

        # Use ThreadPoolExecutor to run in background thread
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(run_in_thread)
            return future.result(timeout=effective_timeout)

    def check_access(
        self,
        source_id: str,
        sink_id: str,
        resource: str = "data",
        action: str = "read"
    ) -> bool:
        """Synchronous version of check_access."""
        return self._run_coroutine(self._async_client.check_access(
            source_id, sink_id, resource, action
        ))

    def process_data(
        self,
        source_id: str,
        sink_id: str,
        data: dict[str, Any],
        action: str = "read"
    ) -> ProcessResult:
        """Synchronous version of process_data."""
        return self._run_coroutine(self._async_client.process_data(
            source_id, sink_id, data, action
        ))

    def receive_data(
        self,
        source_id: str,
        data: dict[str, Any],
        action: str = "read"
    ) -> ProcessResult:
        """Synchronous version of receive_data."""
        return self._run_coroutine(self._async_client.receive_data(source_id, data, action))

    def register_component(
        self,
        component_type: str,
        role: str | None = None,
        additional_roles: list[str] | None = None,
        permit_user_key: str | None = None,
        data_columns: list[str] | None = None,
        auto_create_attributes: bool = True,
        allowed_fields: dict[str, list[str]] | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> bool:
        """Synchronous version of register_component.

        Uses ``registration_timeout`` instead of the short hot-path ``timeout``
        so that the background thread is not killed before the Policy Service
        finishes processing the request.

        Also auto-starts the sync heartbeat thread on the first successful
        registration if ``heartbeat_interval > 0`` on the underlying client.
        """
        result = self._run_coroutine(
            self._async_client.register_component(
                component_type,
                role,
                additional_roles,
                permit_user_key,
                data_columns,
                auto_create_attributes,
                allowed_fields,
                attributes,
            ),
            timeout=self._async_client.registration_timeout,
        )
        if result and self._async_client.heartbeat_interval > 0:
            if self._heartbeat_thread is None or not self._heartbeat_thread.is_alive():
                self.start_heartbeat()
        return result

    def start_heartbeat(self, interval: int | None = None) -> None:
        """
        Start a daemon thread that periodically re-registers this component.

        Called automatically by ``register_component`` when
        ``heartbeat_interval > 0``.  Safe to call manually as well — calling
        it while a heartbeat thread is already alive is a no-op.

        Args:
            interval: Override the ``heartbeat_interval`` set on the async
                client (seconds).  Defaults to ``async_client.heartbeat_interval``.
        """
        effective_interval = interval if interval is not None else self._async_client.heartbeat_interval
        if effective_interval <= 0:
            return
        if self._heartbeat_thread is not None and self._heartbeat_thread.is_alive():
            return  # Already running

        stop_event = threading.Event()
        self._heartbeat_stop = stop_event

        def _loop() -> None:
            while not stop_event.wait(timeout=effective_interval):
                kwargs = self._async_client._last_register_kwargs
                if not kwargs:
                    continue
                try:
                    self.register_component(**kwargs)
                    logger.debug(
                        f"Sync heartbeat: re-registered {self._async_client.component_id}"
                    )
                except Exception as e:
                    logger.debug(
                        f"Sync heartbeat re-registration failed for "
                        f"{self._async_client.component_id} "
                        f"(will retry in {effective_interval}s): {e}"
                    )

        hb_thread = threading.Thread(
            target=_loop,
            daemon=True,
            name=f"policy-heartbeat-{self._async_client.component_id}",
        )
        self._heartbeat_thread = hb_thread
        hb_thread.start()
        logger.info(
            f"Sync heartbeat started for {self._async_client.component_id}: "
            f"re-registering every {effective_interval}s"
        )

    def stop_heartbeat(self) -> None:
        """Stop the background heartbeat thread if it is running."""
        stop_event = self._heartbeat_stop
        if stop_event is not None:
            stop_event.set()
        hb_thread = self._heartbeat_thread
        if hb_thread is not None:
            hb_thread.join(timeout=5)
        self._heartbeat_thread = None
        self._heartbeat_stop = None
        logger.info(f"Sync heartbeat stopped for {self._async_client.component_id}")

    def register_ml_model(
        self,
        model_id: str,
        model_name: str,
        input_fields: list[str],
        output_fields: list[str],
        data_type: str,
        architecture: str | None = None,
        permit_user_key: str | None = None,
        additional_roles: list[str] | None = None,
        window_duration_seconds: int | None = None,
    ) -> bool:
        """Synchronous version of register_ml_model."""
        return self._run_coroutine(
            self._async_client.register_ml_model(
                model_id,
                model_name,
                input_fields,
                output_fields,
                data_type,
                architecture,
                permit_user_key,
                additional_roles,
                window_duration_seconds,
            ),
            timeout=self._async_client.registration_timeout,
        )

    def unregister_ml_model(self, model_name: str) -> bool:
        """Synchronous version of unregister_ml_model."""
        return self._run_coroutine(
            self._async_client.unregister_ml_model(model_name),
            timeout=self._async_client.registration_timeout,
        )

    def clear_cache(self) -> None:
        """Clear the decision cache."""
        self._async_client.clear_cache()

    def ensure_pipeline_ready(
        self,
        source_id: str,
        sink_id: str,
        action: str = "read"
    ) -> None:
        """Synchronous version of ensure_pipeline_ready."""
        self._run_coroutine(
            self._async_client.ensure_pipeline_ready(source_id, sink_id, action),
            timeout=self._async_client.registration_timeout,
        )

    def __repr__(self) -> str:
        return f"SyncPolicyClient({self._async_client})"
