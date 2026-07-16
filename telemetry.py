from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterator

_configured = False


def configure_telemetry(app: Any) -> None:
    global _configured
    if _configured or os.environ.get("OTEL_SDK_DISABLED", "false").lower() == "true":
        return

    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    from opentelemetry.instrumentation.logging import LoggingInstrumentor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    resource = Resource.create(
        {
            "service.name": "runner-orchestrator",
            "service.namespace": "infra-cluster",
            "deployment.environment": "homelab",
            "lxc.ctid": "150",
            "pve.node": "pve01",
            "infra.managed_by": "infra-cluster",
            "infra.resource.type": "runner-orchestrator",
            "infra.resource.role": "control-plane",
        }
    )
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(
        endpoint=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4317"),
        insecure=os.environ.get("OTEL_EXPORTER_OTLP_INSECURE", "true").lower() == "true",
    )
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    FastAPIInstrumentor.instrument_app(
        app,
        excluded_urls="/health/live,/health/ready,/metrics,/ui.*,/api/v1/.*",
    )
    HTTPXClientInstrumentor().instrument()
    LoggingInstrumentor().instrument(set_logging_format=False)
    _configured = True


def set_current_span_attributes(attributes: dict[str, str | int | bool]) -> None:
    try:
        from opentelemetry import trace

        current = trace.get_current_span()
        if current.is_recording():
            for key, value in attributes.items():
                current.set_attribute(key, value)
    except ImportError:
        return


@contextmanager
def span(name: str, attributes: dict[str, str | int | bool] | None = None) -> Iterator[None]:
    try:
        from opentelemetry import trace
    except ImportError:
        yield
        return
    with trace.get_tracer("infra-cluster.runner-orchestrator").start_as_current_span(
        name, attributes=attributes or {}
    ):
        yield
