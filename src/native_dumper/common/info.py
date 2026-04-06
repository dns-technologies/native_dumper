from ast import literal_eval

from base_dumper import DebugInfo

from ..core.pyo3http import HttpResponse


def info_from_headers(
    host: str,
    kind: str,
    response: HttpResponse,
) -> DebugInfo:
    """Get DebugInfo from response.headers."""

    summary = literal_eval(response.get_header("X-ClickHouse-Summary")) or {}
    duration = round(int(summary.get("elapsed_ns", 0)) * 1e-9, 3)
    memory = int(summary.get("memory_usage", 0))
    storage = int(summary.get("result_bytes", 0))
    rows = int(summary.get("result_rows", 0))
    return DebugInfo(host, kind, duration, memory, storage, rows)
