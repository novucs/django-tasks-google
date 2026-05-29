# Observability

[Back to README](../README.md)

## OpenTelemetry tracing

Optional distributed tracing. Each task gets a span when it's enqueued and another when it
runs, linked into a single trace.

```bash
pip install 'django-tasks-google[otel]'
```

These spans turn on automatically, but they only go anywhere once your app has a configured
OpenTelemetry SDK and exporter (see the
[OpenTelemetry Python docs](https://opentelemetry.io/docs/languages/python/)). To also
trace the enqueueing request through to the task, set up
[`opentelemetry-instrumentation-django`](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/django/django.html).

To turn the task spans off:

```python
# settings.py
DJANGO_TASKS_GOOGLE_OTEL_ENABLED = False
```
