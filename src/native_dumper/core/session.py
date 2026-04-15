from urllib3 import (
    HTTPConnectionPool,
    HTTPResponse,
    PoolManager,
    Timeout,
)
from urllib.parse import urlencode
from typing import Iterable

from .pyo3http import (
    HttpRustResponse,
    HttpRustSession,
)


class HttpResponse:
    """HttpResponse with fileobject methods."""

    def __init__(
        self,
        response: HTTPResponse,
    ) -> None:
        """Class initialization."""

        self._response = response
        self._is_closed = False
        self._is_complete = False
        self._status_code = response.status
        self._headers = {k.lower(): v for k, v in response.headers.items()}
        self._content_length = response.headers.get("content-length")

        if self._content_length:
            self._content_length = int(self._content_length)

        self._url = response.geturl()
        self._seek_allowed = False
        self._has_used_seek = False

    def read(self, *_: int) -> bytes: return b""
    def read1(self) -> bytes: return b""
    def seek(self, *_: int) -> None: ...
    def tell(self) -> int: return 0

    def close(self) -> None:
        """Close the response and release resources."""

        if not self._is_closed:
            self._response.close()
            self._is_closed = True
            self._is_complete = True

    def get_status(self) -> int | None:
        """Get the HTTP status code.

        Returns:
            HTTP status code or None if not available.
        """

        return self._status_code

    def get_headers(self) -> dict[str, str] | None:
        """Get all response headers.

        Returns:
            Dictionary of header names (lowercase)
            to values, or None if not available.
        """

        return self._headers.copy() if self._headers else None

    def get_header(self, name: str) -> str | None:
        """Get a specific response header.

        Args:
            name: Header name (case-insensitive).

        Returns:
            Header value or None if header doesn't exist.
        """

        return self._headers.get(name.lower())

    def get_content_length(self) -> int | None:
        """Get the content length from response headers.

        Returns:
            Content length in bytes or None if not specified.
        """

        return self._content_length

    def is_success(self) -> bool:
        """Check if the response indicates success (2xx status code).

        Returns:
            True if status code is in 200-299 range.
        """

        return 200 <= self._status_code < 300 if self._status_code else False

    def is_redirect(self) -> bool:
        """Check if the response indicates a redirect (3xx status code).

        Returns:
            True if status code is in 300-399 range.
        """

        return 300 <= self._status_code < 400 if self._status_code else False

    def is_client_error(self) -> bool:
        """Check if the response indicates a client error (4xx status code).

        Returns:
            True if status code is in 400-499 range.
        """

        return 400 <= self._status_code < 500 if self._status_code else False

    def is_server_error(self) -> bool:
        """Check if the response indicates a server error (5xx status code).

        Returns:
            True if status code is in 500-599 range.
        """

        return 500 <= self._status_code < 600 if self._status_code else False

    def get_content_type(self) -> str | None:
        """Get the Content-Type header value.

        Returns:
            Content-Type value or None if not specified.
        """

        return self.get_header("content-type")

    def get_url(self) -> str | None:
        """Get the final URL of the response (after redirects).

        Returns:
            URL string or None if not available.
        """

        return self._url

    def seekable(self) -> bool:
        """Check if the response stream supports seeking.

        Returns:
            True if seeking to position 0 is allowed.
        """

        return self._seek_allowed and not self._has_used_seek

    def is_closed(self) -> bool:
        """Check if the response is closed.

        Returns:
            True if response is closed.
        """

        return self._is_closed

    def get_info(self) -> dict[str, str]:
        """Get comprehensive information about the response.

        Returns:
            Dictionary containing response metadata.
        """

        info = {}

        if self._status_code:
            info["status"] = str(self._status_code)
            info["status_text"] = "OK" if self.is_success() else "Error"

        if self._content_length:
            info["content_length"] = str(self._content_length)

        content_type = self.get_content_type()
        if content_type:
            info["content_type"] = content_type

        if self._url:
            info["url"] = self._url

        info["closed"] = str(self._is_closed)
        info["complete"] = str(self._is_complete)
        info["seek_allowed"] = str(self._seek_allowed)
        info["seek_used"] = str(self._has_used_seek)
        info["seekable"] = str(self.seekable())

        return info


class HttpSession:
    """HttpSession with post method only."""

    _pool: HTTPConnectionPool
    _session: HttpRustSession
    _client_closed: bool

    def __init__(
        self,
        timeout: float | int | None = None,
    ) -> None:
        """Initialize an HTTP session.

        Args:
            timeout: Request timeout in seconds. Default is 30 seconds.

        Raises:
            RuntimeError: If HTTP client creation fails.
        """

        timeout_val = timeout if timeout is not None else 30.0
        self._pool = PoolManager(
            timeout=Timeout(connect=timeout_val, read=timeout_val),
            retries=False,
        )
        self._session = HttpRustSession(timeout)
        self._client_closed = False

    @property
    def closed(self) -> bool:
        """Check closed session."""

        return self._client_closed

    def post(
        self,
        url: str,
        headers: dict[str, str] | None,
        params: dict[str, str] | None,
        data: bytes | Iterable[bytes | bytearray] | None,
        timeout: float | int | None,
    ) -> HttpResponse | HttpRustResponse:
        """Send a POST request.

        Args:
            url: Target URL.
            headers: HTTP headers as key-value pairs.
            params: URL parameters as key-value pairs.
            data: Request body data. Can be:
                - bytes
                - list of bytes objects
                - byte array
                - iterable/generator yielding bytes
            timeout: Request timeout in seconds (overrides session timeout).

        Returns:
            HttpResponse object.

        Raises:
            IOError: If HTTP request fails.
            TypeError: If data type is not supported.
        """

        if self._client_closed:
            raise RuntimeError("Session is closed")

        if params:
            query_string = urlencode(params)
            url = f"{url}?{query_string}"

        request_headers = headers.copy() if headers else {}
        body: bytes | bytearray | str | None = None

        if data is not None:
            if hasattr(data, "__iter__") and not isinstance(
                data, (bytes, bytearray, str)
            ):
                request_headers["Transfer-Encoding"] = "chunked"
                body = data
            else:
                if isinstance(data, (bytes, bytearray)):
                    body = data
                    request_headers["Content-Length"] = str(len(data))
                else:
                    body = b"".join(data)
                    request_headers["Content-Length"] = str(len(body))

            request_timeout = (
                timeout
                if timeout is not None
                else self._pool.timeout.read_timeout
            )
            response = self._pool.request(
                "POST",
                url,
                headers=request_headers,
                body=body,
                chunked=("Transfer-Encoding" in request_headers),
                timeout=Timeout(
                    connect=request_timeout,
                    read=request_timeout,
                ),
                preload_content=False,
            )
            return HttpResponse(response)

        return self._session.post(url, headers, params, data, timeout)

    def post_stream(
        self,
        url: str,
        headers: dict[str, str] | None,
        params: dict[str, str] | None,
        data: bytes | Iterable[bytes | bytearray] | None,
        timeout: float | int | None,
    ) -> HttpResponse | HttpRustResponse:
        """Send a POST request (alias for post method).

        Args:
            url: Target URL.
            headers: HTTP headers as key-value pairs.
            params: URL parameters as key-value pairs.
            data: Request body data.
            timeout: Request timeout in seconds.

        Returns:
            HttpResponse object.
        """

        return self.post(url, headers, params, data, timeout)

    def close(self) -> None:
        """Close the HTTP session and release resources.

        This method should be called when the session is no longer needed
        to properly clean up connections and resources.
        """

        self._session.close()

        if not self._client_closed:
            self._pool.clear()
            self._client_closed = True
