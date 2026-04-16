from ast import literal_eval
from io import BytesIO
from logging import Logger
from typing import Iterable
from uuid import uuid4

from light_compressor import (
    CompressionMethod,
    define_reader,
)
from base_dumper import (
    CSVStreamReader,
    DBMetadata,
    ReaderType,
)
from nativelib import NativeReader

from ..version import __version__
from .connector import CHConnector
from .errors import ClickhouseServerError
from .session import (
    HttpResponse,
    HttpSession,
)
from .query import query_template
from .stream import NativeStreamReader
from .pyo3http import HttpRustResponse


ERROR_BUFFER = BytesIO()


def string_error(data: bytes) -> str:
    """Bytes to string decoder."""

    ERROR_BUFFER.seek(0)
    ERROR_BUFFER.truncate()
    ERROR_BUFFER.write(data)
    ERROR_BUFFER.seek(0)

    return define_reader(ERROR_BUFFER).read(len(data)).decode(
        "utf-8",
        errors="replace",
    ).strip()


def define_stream(stream_type: str) -> str:
    """Serialize stream type."""

    if stream_type == "native":
        return stream_type.capitalize()

    return stream_type.upper()


class HTTPCursor:
    """Class for send queryes to Clickhouse server
    and read/write Native format."""

    def __init__(
        self,
        connector: CHConnector,
        compression_method: CompressionMethod,
        compression_level: int,
        logger: Logger,
        timeout: int,
        stream_type: str,
        user_agent: str | None = None,
    ) -> None:
        """Class initialization."""

        if not user_agent:
            user_agent = f"{self.__class__.__name__}/{__version__}"

        self.connector = connector
        self.compression_method = compression_method
        self.logger = logger
        self.timeout = timeout
        self.stream_type = define_stream(stream_type)
        self.user_agent = user_agent
        self.session = HttpSession(timeout=self.timeout)
        self.headers = {
            "Accept": "*/*",
            "Connection": "keep-alive",
            "User-Agent": f"{self.user_agent}",
            "Accept-Encoding": self.compression_method.method,
            "Content-Encoding": self.compression_method.method,
            "X-ClickHouse-User": self.connector.user,
            "X-ClickHouse-Key": self.connector.password,
            "X-ClickHouse-Format": self.stream_type,
            "X-Content-Type-Options": "nosniff",
        }
        self.stream_reader = {
            "CSV": CSVStreamReader,
            "Native": NativeStreamReader,
        }
        self.mode = {
            443: "https",
            8123: "http",
            8443: "https",
        }.get(int(self.connector.port), "http")
        self.url = (
            f"{self.mode}://{self.connector.host}:{self.connector.port}"
        )
        self.params = {
            "database": connector.dbname,
            "query": "",
            "query_id": "",
            "session_id": str(uuid4()),
            "enable_http_compression": "1",
            "wait_end_of_query": "1",
        }
        self.server_version = ""
        self.headers_memory = False
        self.is_readonly = False
        self.is_connected = False
        self._compression_level = 0
        self.compression_level = compression_level

    def __enter__(self) -> "HTTPCursor":
        """Enter context manager."""

        return self

    def __exit__(
        self,
        exc_type: object,
        exc_val: object,
        exc_tb: object,
    ) -> bool:
        """Exit context manager."""

        _ = exc_type, exc_val, exc_tb
        self.close()
        return False

    @property
    def compression_level(self) -> int:
        """Get server compression level."""

        return self._compression_level

    @compression_level.setter
    def compression_level(self, level_value: int) -> int:
        """Set server compression level."""

        if not self.is_readonly:
            self._compression_level = (
                1 if level_value < 1
                else 9 if level_value > 9
                else level_value
            )

        return self._compression_level

    def send_hello(self) -> tuple[bool, str]:
        """Get server version."""

        self.headers["X-ClickHouse-Format"] = "Native"
        query_version = query_template("system_version")
        reader = self.get_stream(query_version)
        self.headers_memory, self.server_version = next(reader.to_rows())
        reader.close()
        write_params = {
            "async_insert": "1",
            "async_insert_busy_timeout_ms": f"{self.timeout}000",
            "http_response_buffer_size": "0",
            "http_headers_progress_interval_ms": f"{self.timeout}000",
            "http_zlib_compression_level": f"{self.compression_level}",
            "http_receive_timeout": str(self.timeout),
            "http_send_timeout": str(self.timeout),
            "max_execution_time": str(self.timeout),
            "send_progress_in_http_headers": "1",
            "wait_for_async_insert": "1",
        }
        self.params.update(write_params)
        query_log = query_template("log_access")
        stream = self.post(query_log)
        status = stream.get_status()

        if status != 200:
            self.is_readonly = True
            self._compression_level = 3

            for key in write_params:
                self.params.pop(key, None)

        else:
            bufferobj = define_reader(stream, self.compression_method)
            reader = NativeReader(bufferobj)
            self.is_readonly = not next(reader.to_rows())[0]
            reader.close()

        self.headers["X-ClickHouse-Format"] = self.stream_type
        self.is_connected = True
        return self.is_readonly, self.server_version

    def post(
        self,
        query: str,
        data: Iterable[bytes] | None = None,
    ) -> HttpResponse | HttpRustResponse:
        """Post response from clickhouse server."""

        self.params["query"] = query
        self.params["query_id"] = str(uuid4())

        return self.session.post(
            url=self.url,
            params=self.params,
            headers=self.headers,
            timeout=self.timeout,
            data=data,
        )

    def get_response(
        self,
        query: str,
        data: Iterable[bytes] | None = None,
    ) -> HttpResponse | HttpRustResponse:
        """Get response from clickhouse server."""

        response = self.post(query, data)
        status = response.get_status()

        if status != 200:
            error = string_error(response.read())
            response.close()
            self.logger.error(f"ClickhouseServerError: {error}")
            raise ClickhouseServerError(error)

        return response

    def get_stream(
        self,
        query: str,
        db_metadata: DBMetadata | None = None,
    ) -> ReaderType:
        """Get answer from server as unpacked stream file."""

        stream = self.get_response(query)

        try:
            bufferobj = define_reader(stream, self.compression_method)
        except EOFError:
            """Empty data."""

            bufferobj = BytesIO()

        if not db_metadata:
            return NativeReader(bufferobj)

        return self.stream_reader[self.stream_type](bufferobj, db_metadata)

    def upload_data(
        self,
        table: str,
        data: Iterable[bytes],
    ) -> None:
        """Download data into table."""

        response = self.get_response(
            query=f"INSERT INTO {table} FORMAT {self.stream_type}",
            data=data,
        )
        summary = literal_eval(
            response.get_header("X-ClickHouse-Summary"),
        ) or {}
        duration = round(int(summary.get("elapsed_ns", 0)) * 1e-9, 3)
        self.logger.info(f"Duration time is {duration} seconds.")

    def metadata(
        self,
        table: str,
    ) -> list[dict[str, str]]:
        """Get table metadata."""

        self.headers["X-ClickHouse-Format"] = "Native"
        reader = self.get_stream(f"DESCRIBE TABLE {table}")
        self.headers["X-ClickHouse-Format"] = self.stream_type
        return [
            {column: dtype}
            for column, dtype, *_ in tuple(reader.to_rows())
        ]

    def execute(
        self,
        query: str,
    ) -> HttpResponse | HttpRustResponse:
        """Exetute method with return HttpResponse."""

        return self.get_response(query)

    def last_query(self) -> str:
        """Show last query."""

        return self.params["query"]

    def refresh(self) -> None:
        """Refresh Session ID."""

        if self.session.closed:
            self.session = HttpSession(timeout=self.timeout)

        self.params["session_id"] = str(uuid4())

    def close(self) -> None:
        """Close HTTPCursor session."""

        self.session.close()
        self.is_connected = False
