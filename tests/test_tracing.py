import pytest
from django.tasks import task_backends
from opentelemetry import trace
from opentelemetry.propagate import inject
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode

from django_tasks_google import tracing
from django_tasks_google.executor import execute_task
from django_tasks_google.models import TaskExecution

# A global provider/exporter pair: opentelemetry only honours the *first*
# set_tracer_provider() call per process, so we install one provider and clear
# the exporter between tests.
_EXPORTER = InMemorySpanExporter()
_PROVIDER = TracerProvider()
_PROVIDER.add_span_processor(SimpleSpanProcessor(_EXPORTER))


@pytest.fixture
def spans():
    trace.set_tracer_provider(_PROVIDER)  # once-only; later calls are ignored
    _EXPORTER.clear()
    yield _EXPORTER
    _EXPORTER.clear()


def _span_of_kind(exporter, kind):
    return next(s for s in exporter.get_finished_spans() if s.kind == kind)


def _make_process_execution():
    return TaskExecution.objects.create(
        module_path="tests.fake_tasks.process_task",
        backend_alias="process",
        queue_name="default",
        args=[],
        kwargs={},
    )


def _carrier_for_new_span(label):
    """Open a span, inject it into a carrier; return (carrier, span_context)."""
    tracer = trace.get_tracer("test")
    with tracer.start_as_current_span(label) as span:
        carrier = {}
        inject(carrier)
        return carrier, span.get_span_context()


@pytest.mark.django_db
def test_consumer_links_to_carrier_context(spans):
    # Simulate the transport carrying a producer's context (HTTP header / env var).
    carrier, producer_ctx = _carrier_for_new_span("producer")
    execution = _make_process_execution()

    execute_task(
        execution.pk,
        attempt=1,
        backend=task_backends["process"],
        trace_carrier=carrier,
    )

    consumer = _span_of_kind(spans, SpanKind.CONSUMER)
    assert consumer.name == "process process/default"
    # CONSUMER links to the producer carried by the transport.
    assert consumer.context.trace_id == producer_ctx.trace_id
    assert consumer.parent.span_id == producer_ctx.span_id
    assert consumer.attributes["messaging.system"] == "gcp_cloud_tasks"
    assert consumer.attributes["messaging.operation.type"] == "process"
    assert consumer.attributes["messaging.message.id"] == str(execution.pk)


@pytest.mark.django_db
def test_consumer_nests_under_ambient_span_ignoring_carrier(spans):
    # A carrier from an unrelated trace, which must be IGNORED when a span is
    # already current (i.e. the SERVER span opentelemetry-instrumentation-django
    # opened from the incoming traceparent header).
    other_carrier, _ = _carrier_for_new_span("unrelated")
    execution = _make_process_execution()

    tracer = trace.get_tracer("test")
    with tracer.start_as_current_span("server") as server:
        server_ctx = server.get_span_context()
        execute_task(
            execution.pk,
            attempt=1,
            backend=task_backends["process"],
            trace_carrier=other_carrier,
        )

    consumer = _span_of_kind(spans, SpanKind.CONSUMER)
    # Nests under the ambient SERVER span, not the (ignored) carrier.
    assert consumer.context.trace_id == server_ctx.trace_id
    assert consumer.parent.span_id == server_ctx.span_id


@pytest.mark.django_db
def test_enqueue_threads_trace_carrier_to_enqueue_gcp(
    spans, django_capture_on_commit_callbacks, monkeypatch
):
    from tests.fake_tasks import process_task

    backend = task_backends["process"]
    captured = {}

    def fake_enqueue_gcp(execution_id, trace_context=None):
        captured["carrier"] = trace_context

    monkeypatch.setattr(backend, "enqueue_gcp", fake_enqueue_gcp)

    with django_capture_on_commit_callbacks(execute=True):
        process_task.enqueue()

    # The PRODUCER span (opened in enqueue) yields a carrier with the W3C header.
    assert "traceparent" in captured["carrier"]


@pytest.mark.django_db
def test_failing_task_marks_consumer_span_error(spans):
    from tests.fake_tasks import always_fails

    result = always_fails.enqueue()
    TaskExecution.objects.filter(pk=result.id).update(max_attempts=1)

    execute_task(result.id, attempt=1, backend=task_backends["process"])

    consumer = _span_of_kind(spans, SpanKind.CONSUMER)
    assert consumer.status.status_code == StatusCode.ERROR
    assert consumer.attributes["error.type"].endswith("ValueError")


@pytest.mark.django_db
def test_disabled_emits_no_spans(spans, settings):
    from tests.fake_tasks import process_task

    settings.DJANGO_TASKS_GOOGLE_OTEL_ENABLED = False

    result = process_task.enqueue()
    execute_task(result.id, attempt=1, backend=task_backends["process"])

    assert spans.get_finished_spans() == ()


def test_messaging_system_is_per_backend():
    from django_tasks_google.backends import CloudRunJobsBackend, CloudTasksBackend

    assert CloudTasksBackend.otel_messaging_system == "gcp_cloud_tasks"
    assert CloudRunJobsBackend.otel_messaging_system == "gcp_cloud_run_jobs"
    # ProcessBackend mirrors its emulated mode.
    assert task_backends["process"].otel_messaging_system == "gcp_cloud_tasks"
    assert task_backends["process_jobs"].otel_messaging_system == "gcp_cloud_run_jobs"


def test_carrier_env_round_trip():
    carrier = {"traceparent": "00-abc-def-01", "tracestate": "rojo=00f067"}
    env = tracing.carrier_to_env(carrier)
    # Standard W3C env var names.
    assert env == {"TRACEPARENT": "00-abc-def-01", "TRACESTATE": "rojo=00f067"}
    assert tracing.carrier_from_env(env) == carrier
    # Empty / absent cases degrade to {}.
    assert tracing.carrier_to_env({}) == {}
    assert tracing.carrier_from_env({}) == {}
