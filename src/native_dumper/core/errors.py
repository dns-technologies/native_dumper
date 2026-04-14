from base_dumper import (
    BaseDumperError,
    BaseDumperValueError,
)

from .pyo3http import (
    HttpConnectionError,
    HttpError,
    HttpProtocolError,
    HttpTimeoutError,
    HttpTypeError,
)


class NativeDumperError(BaseDumperError):
    """NativeDumper base error."""


class NativeDumperReadError(NativeDumperError):
    """NativeDumper read error."""


class NativeDumperWriteError(NativeDumperError):
    """NativeDumper write error."""


class NativeDumperValueError(NativeDumperError, BaseDumperValueError):
    """NativeDumper value error."""


class ClickhouseServerError(BaseDumperValueError):
    """Clickhouse errors."""


class ClickhouseHTTPError(ClickhouseServerError):
    """Base class for HTTP-level errors from ClickHouse."""


class ClickhouseTimeoutError(ClickhouseHTTPError):
    """Timeout occurred while communicating with ClickHouse server."""


class ClickhouseConnectionError(ClickhouseHTTPError):
    """Connection to ClickHouse server failed or was reset."""


class ClickhouseProtocolError(ClickhouseHTTPError):
    """HTTP protocol violation by ClickHouse server
    (e.g., connection reset)."""


class ClickhouseSSLError(ClickhouseHTTPError):
    """SSL/TLS error during connection to ClickHouse."""


class RustHTTPError(HttpError, NativeDumperError):
    """Base class for errors originating from the Rust pyo3http backend."""


class RustHTTPTimeoutError(
    RustHTTPError,
    HttpTimeoutError,
    ClickhouseTimeoutError,
):
    """Timeout from Rust reqwest client (converted to IOError)."""


class RustHTTPConnectionError(
    RustHTTPError,
    HttpConnectionError,
    ClickhouseConnectionError,
):
    """Connection error from Rust reqwest client."""


class RustHTTPRuntimeError(RustHTTPError):
    """Runtime error in Rust pyo3http
    (e.g., failed to create Tokio runtime)."""


class RustHTTPTypeError(RustHTTPError, HttpTypeError, NativeDumperValueError):
    """Type error in Rust pyo3http (e.g., invalid data type for POST body)."""


class RustHTTPPanicError(RustHTTPError, HttpProtocolError):
    """Panic occurred in Rust pyo3http code (indicates a bug)."""


class Urllib3HTTPError(ClickhouseHTTPError):
    """Base class for errors originating from urllib3."""


class Urllib3TimeoutError(Urllib3HTTPError, ClickhouseTimeoutError):
    """Timeout from urllib3 client (connect or read)."""


class Urllib3ConnectionError(Urllib3HTTPError, ClickhouseConnectionError):
    """Connection error from urllib3 client."""


class Urllib3ProtocolError(Urllib3HTTPError, ClickhouseProtocolError):
    """Protocol error from urllib3 client (e.g., connection reset)."""


class Urllib3SSLError(Urllib3HTTPError, ClickhouseSSLError):
    """SSL error from urllib3 client."""
