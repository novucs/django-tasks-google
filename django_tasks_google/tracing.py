"""Optional OpenTelemetry tracing.

No-ops unless ``opentelemetry-api`` is installed and
``DJANGO_TASKS_GOOGLE_OTEL_ENABLED`` is not False. The OTel API does nothing until
the app configures an SDK, so it is safe to leave on by default.

Each task gets a PRODUCER span at enqueue and a CONSUMER span at execute. Context is
propagated through the transport (Cloud Tasks HTTP headers, Cloud Run Jobs env vars),
so an ``opentelemetry-instrumentation-django`` target links the whole
request-to-task chain into one trace.
"""

import contextlib
from importlib.metadata import PackageNotFoundError, version

from django.conf import settings

# Standard W3C env vars for Cloud Run Jobs (env name -> carrier key).
_TRACE_ENV_VARS = {
    "TRACEPARENT": "traceparent",
    "TRACESTATE": "tracestate",
}

try:
    from opentelemetry import context as otel_context
    from opentelemetry import propagate, trace
    from opentelemetry.trace import SpanKind, Status, StatusCode

    _OTEL_INSTALLED = True
except ImportError:  # pragma: no cover - exercised via the no-op path
    _OTEL_INSTALLED = False


def _enabled():
    return _OTEL_INSTALLED and getattr(
        settings, "DJANGO_TASKS_GOOGLE_OTEL_ENABLED", True
    )


def _get_tracer():
    try:
        lib_version = version("django-tasks-google")
    except PackageNotFoundError:  # pragma: no cover - only when running uninstalled
        lib_version = ""
    return trace.get_tracer("django_tasks_google", lib_version)


@contextlib.contextmanager
def producer_span(task, messaging_system):
    """Start a PRODUCER span and yield a carrier with its injected context.

    Yields an empty carrier and starts no span when tracing is disabled.
    """
    if not _enabled():
        yield {}
        return

    tracer = _get_tracer()
    backend = task.backend
    queue_name = task.queue_name
    with tracer.start_as_current_span(
        f"send {backend}/{queue_name}", kind=SpanKind.PRODUCER
    ) as span:
        span.set_attribute("messaging.system", messaging_system)
        span.set_attribute("messaging.destination.name", queue_name)
        span.set_attribute("messaging.operation.type", "send")
        span.set_attribute("messaging.operation.name", "enqueue")
        span.set_attribute("django_tasks_google.backend", backend)
        span.set_attribute("django_tasks_google.task", task.module_path)
        carrier = {}
        # Inject inside the span scope so the carrier points at this span.
        propagate.inject(carrier)
        yield carrier


@contextlib.contextmanager
def consumer_span(execution, messaging_system, carrier=None):
    """Start a CONSUMER span linked back to the producer.

    Yields the span, or ``None`` when disabled.
    """
    if not _enabled():
        yield None
        return

    tracer = _get_tracer()
    token = None
    # Nest under the ambient span if one exists (e.g. a django SERVER span from the
    # incoming header); otherwise extract the producer context from the carrier.
    if not trace.get_current_span().get_span_context().is_valid:
        otel_ctx = propagate.extract(carrier or {})
        token = otel_context.attach(otel_ctx)
    try:
        backend = execution.backend_alias
        queue_name = execution.queue_name
        with tracer.start_as_current_span(
            f"process {backend}/{queue_name}", kind=SpanKind.CONSUMER
        ) as span:
            span.set_attribute("messaging.system", messaging_system)
            span.set_attribute("messaging.destination.name", queue_name)
            span.set_attribute("messaging.operation.type", "process")
            span.set_attribute("messaging.operation.name", "execute")
            span.set_attribute("messaging.message.id", str(execution.pk))
            span.set_attribute("django_tasks_google.backend", execution.backend_alias)
            span.set_attribute("django_tasks_google.task", execution.module_path)
            yield span
    finally:
        if token is not None:
            otel_context.detach(token)


def carrier_to_env(carrier):
    """Encode a W3C trace carrier as standard env vars (or ``{}`` when empty)."""
    if not carrier:
        return {}
    return {
        env_name: carrier[key]
        for env_name, key in _TRACE_ENV_VARS.items()
        if carrier.get(key)
    }


def carrier_from_env(environ):
    """Decode the W3C trace carrier from standard env vars (or ``{}`` when absent)."""
    return {
        key: environ[env_name]
        for env_name, key in _TRACE_ENV_VARS.items()
        if environ.get(env_name)
    }


def record_exception(exception):
    """Record an exception on the current span and mark it errored.

    No-op when tracing is disabled or no span is recording.
    """
    if not _enabled():
        return
    span = trace.get_current_span()
    if not span.is_recording():
        return
    span.record_exception(exception)
    cls = type(exception)
    span.set_attribute("error.type", f"{cls.__module__}.{cls.__qualname__}")
    span.set_status(Status(StatusCode.ERROR))
