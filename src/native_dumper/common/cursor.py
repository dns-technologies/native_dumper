from io import BytesIO
from logging import Logger
from typing import Iterable
from uuid import uuid4

from base_dumper import AbstractCursor
from light_compressor import (
    CompressionMethod,
    define_reader,
)
from nativelib import (
    Column,
    NativeReader,
)

from ..version import __version__
from .connector import CHConnector
from .errors import ClickhouseServerError
from .pyo3http import (
    HttpResponse,
    HttpSession,
)


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


class HTTPCursor(AbstractCursor):
    """Class for send queryes to Clickhouse server
    and read/write Native format."""

    def __init__(
        self,
        connector: CHConnector,
        compression_method: CompressionMethod,
        compression_level: int,
        logger: Logger,
        timeout: int,
        user_agent: str | None = None,
    ) -> None:
        """Class initialization."""

        if not user_agent:
            user_agent = self.__class__.__name__

        self.connector = connector
        self.compression_method = compression_method
        self.compression_level = (
            1 if compression_level < 1
            else 9 if compression_level > 9
            else compression_level
        )
        self.logger = logger
        self.timeout = timeout
        self.user_agent = user_agent
        self.session = HttpSession(timeout=self.timeout)
        self.is_connected = False
        self.headers = {
            "Accept": "*/*",
            "Connection": "keep-alive",
            "User-Agent": f"{self.user_agent}/{__version__}",
            "Accept-Encoding": self.compression_method.method,
            "Content-Encoding": self.compression_method.method,
            "X-ClickHouse-User": self.connector.user,
            "X-ClickHouse-Key": self.connector.password,
            "X-ClickHouse-Compression": self.compression_method.method,
            "X-ClickHouse-Format": "Native",
            "X-Content-Type-Options": "nosniff",
        }
        self.mode = {
            443: "https",
        }.get(int(self.connector.port), "http")
        self.url = (
            f"{self.mode}://{self.connector.host}:{self.connector.port}/"
            "?enable_http_compression=1&http_zlib_compression_level="
            f"{self.compression_level}"
        )
        self.params = {
            "database": connector.dbname,
            "query": "",
            "query_id": "",
            "session_id": str(uuid4()),
        }
        self.server_version = None

    def send_hello(self) -> str:
        """Get server version."""

        reader = self.get_stream("SELECT version()")
        server_version = tuple(reader.to_rows())[0][0]
        self.is_connected = True
        self.server_version = server_version
        return self.server_version

    def get_response(
        self,
        query: str,
        data: Iterable[bytes] | None = None,
    ) -> HttpResponse:
        """Get response from clickhouse server."""

        self.params["query"] = query
        self.params["query_id"] = str(uuid4())

        response = self.session.post(
            url=self.url,
            params=self.params,
            headers=self.headers,
            timeout=self.timeout,
            data=data,
        )
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
        waiting_data: bool = False,
    ) -> NativeReader:
        """Get answer from server as unpacked stream file."""

        stream = self.get_response(query)

        try:
            bufferobj = define_reader(stream, self.compression_method)
        except EOFError as error:
            if waiting_data:
                raise error

            error_message = (
                "Code: 92. DB::Exception: (EMPTY_DATA_PASSED) "
                f"(version {self.server_version} (official build))"
            )
            self.logger.error(f"ClickhouseServerError: {error_message}")
            raise ClickhouseServerError(error_message)

        return NativeReader(bufferobj)

    def upload_data(
        self,
        table: str,
        data: Iterable[bytes],
    ) -> None:
        """Download data into table."""

        self.get_response(
            query=f"INSERT INTO {table} FORMAT Native",
            data=data,
        )

    def metadata(
        self,
        table: str,
    ) -> list[Column]:
        """Get table metadata."""

        reader = self.get_stream(f"DESCRIBE TABLE {table}")
        return [
            Column(*describe[:2])
            for describe in reader.to_rows()
        ]

    def execute(
        self,
        query: str,
    ) -> None:
        """Simple exetute method without return."""

        self.get_response(query)

    def last_query(self) -> str:
        """Show last query."""

        return self.params["query"]

    def refresh(self) -> None:
        """Refresh Session ID."""

        self.params["session_id"] = str(uuid4())

    def close(self) -> None:
        """Close HTTPCursor session."""

        self.session.close()
        self.is_connected = False
