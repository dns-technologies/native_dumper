from gc import collect
from io import (
    BufferedReader,
    BufferedWriter,
)
from logging import Logger
from typing import (
    Any,
    BinaryIO,
    Iterable,
    NoReturn,
    Union,
)

from base_dumper import (
    BaseDumper,
    DBMetadata,
    DumperMode,
    IsolationLevel,
    multiquery,
    timeouts,
    transfer_diagram,
)
from light_compressor import (
    CompressionMethod,
    auto_detector,
    define_reader,
    define_writer,
)
from nativelib import (
    NativeReader,
    NativeWriter,
)

from .common import (
    CHUNK_SIZE,
    CHConnector,
    ClickhouseServerError,
    HTTPCursor,
    NativeDumperError,
    NativeDumperReadError,
    NativeDumperValueError,
    NativeDumperWriteError,
    file_writer,
    make_columns,
)
from .version import __version__


class NativeDumper(BaseDumper):
    """Class for read and write Clickhouse Native format."""

    cursor: HTTPCursor

    def __init__(
        self,
        connector: CHConnector,
        compression_method: CompressionMethod = CompressionMethod.ZSTD,
        logger: Logger | None = None,
        timeout: int | None = None,
        isolation: IsolationLevel = IsolationLevel.committed,
        mode: DumperMode = DumperMode.PROD,
    ) -> None:
        """Class initialization."""


        if int(connector.port) == 9000:
            raise NativeDumperValueError(
                "NativeDumper don't support port 9000, please, use 8123."
            )

        if timeout is None:
            timeout = timeouts.CLICKHOUSE_DEFAULT_TIMEOUT

        self.version = __version__
        self.stream_type = "native"

        super().__init__(
            connector,
            compression_method,
            logger,
            timeout,
            isolation,
            mode,
        )

        try:
            self.cursor = HTTPCursor(
                connector=self.connector,
                compression_method=self.compression_method,
                logger=self.logger,
                timeout=timeout,
                user_agent=self.__class__.__name__,
            )
            self.version = self.cursor.send_hello()
            self._dbmeta: DBMetadata | None = None
        except ClickhouseServerError as error:
            raise error
        except Exception as error:
            logger.error(f"NativeDumperError: {error}")
            raise NativeDumperError(error)

        self.dbname = "clickhouse"
        self.logger.info(
            f"NativeDumper initialized for host {self.connector.host}"
            f"[{self.dbname} {self.version}]"
        )

    @property
    def timeout(self) -> int:
        """Property method for get session timeout."""

        return self._timeout

    @timeout.setter
    def timeout(self, timeout_value: int) -> int:
        """Property method for set statement_timeout."""

        self._timeout = timeout_value
        self.cursor.timeout = self._timeout
        return self._timeout

    @property
    def isolation(self) -> NoReturn:
        """Property method for get current
        server transaction isolation level."""

        raise NativeDumperValueError(
            "Clickhouse server don't have transaction isolation level.",
        )

    @multiquery
    def _read_dump(
        self,
        fileobj: BufferedWriter,
        query: str | None,
        table_name: str | None,
    ) -> bool:
        """Internal method read_dump for generate kwargs to decorator."""

        if not query and not table_name:
            error_message = "Query or table name not defined."
            self.logger.error(f"NativeDumperValueError: {error_message}")
            raise NativeDumperValueError(error_message)

        if not query:
            query = f"SELECT * FROM {table_name}"

        self.logger.info(f"Start read from {self.connector.host}.")

        try:
            self.logger.info(
                "Reading native dump with compression "
                f"{self.compression_method.name}."
            )
            columns = make_columns(self.cursor.metadata(f"({query}\n)"))
            source = DBMetadata(
                name=self.dbname,
                version=self.version,
                columns=columns,
            )
            destination = DBMetadata(
                name="file",
                version=fileobj.name,
                columns=columns,
            )
            self.logger.info(transfer_diagram(source, destination))
            stream = self.cursor.get_response(query)
            size = 0

            while chunk := stream.read(CHUNK_SIZE):
                size += fileobj.write(chunk)
                del chunk

            stream.close()
            fileobj.close()
            self.logger.info(f"Successfully read {size} bytes.")

            if not size:
                self.logger.warning("Empty data read!")

            self.logger.info(f"Read from {self.connector.host} done.")
            return True
        except ClickhouseServerError as error:
            raise error
        except Exception as error:
            self.logger.error(f"NativeDumperReadError: {error}")
            raise NativeDumperReadError(error)

    @multiquery
    def _write_between(
        self,
        table_dest: str,
        table_src: str | None,
        query_src: str | None,
        dumper_src: Union["NativeDumper", object],
    ) -> bool:
        """Internal method write_between for generate kwargs to decorator."""

        if not query_src and not table_src:
            error_message = "Source query or table name not defined."
            self.logger.error(f"NativeDumperValueError: {error_message}")
            raise NativeDumperValueError(error_message)

        if not table_dest:
            error_message = "Destination table name not defined."
            self.logger.error(f"NativeDumperValueError: {error_message}")
            raise NativeDumperValueError(error_message)

        if not dumper_src:
            cursor = self.cursor
            src_dbname = self.dbname
            src_version = self.version
        elif dumper_src.__class__ is NativeDumper:
            cursor = dumper_src.cursor
            src_dbname = dumper_src.dbname
            src_version = dumper_src.version
        else:
            if query_src:
                query_src = query_src.strip().strip(";")

            reader = dumper_src.to_reader(
                query=query_src,
                table_name=table_src,
            )
            dtype_data = reader.to_rows()
            self.from_rows(
                dtype_data=dtype_data,
                table_name=table_dest,
                source=dumper_src._dbmeta,
            )
            size = reader.tell()
            self.logger.info(f"Successfully sending {size} bytes.")

            if not size:
                self.logger.warning("Empty data send!")

            return reader.close()

        if not query_src:
            query_src = f"SELECT * FROM {table_src}"
        else:
            query_src = query_src.strip().strip(";")

        source = DBMetadata(
            name=src_dbname,
            version=src_version,
            columns=make_columns(cursor.metadata(f"({query_src})")),
        )
        destination = DBMetadata(
            name=self.dbname,
            version=self.version,
            columns=make_columns(self.cursor.metadata(table_dest)),
        )
        self.logger.info(transfer_diagram(source, destination))
        stream = cursor.get_response(query_src)
        self.write_dump(stream, table_dest, cursor.compression_method)

    @multiquery
    def _to_reader(
        self,
        query: str | None,
        table_name: str | None,
    ) -> NativeReader:
        """Internal method to_reader for generate kwargs to decorator."""

        if not query and not table_name:
            error_message = "Query or table name not defined."
            self.logger.error(f"NativeDumperValueError: {error_message}")
            raise NativeDumperValueError(error_message)

        if not query:
            query = f"SELECT * FROM {table_name}"

        self.logger.info(
            f"Get NativeReader object from {self.connector.host}."
        )
        self._dbmeta = DBMetadata(
            name=self.dbname,
            version=self.version,
            columns=make_columns(self.cursor.metadata(f"({query}\n)")),
        )
        return self.cursor.get_stream(query)

    def write_dump(
        self,
        fileobj: BufferedReader | BinaryIO,
        table_name: str,
        compression_method: CompressionMethod | None = None,
    ) -> None:
        """Write Native dump into Clickhouse."""

        if not table_name:
            error_message = "Table name not defined."
            self.logger.error(f"NativeDumperValueError: {error_message}")
            raise NativeDumperValueError(error_message)

        self.logger.info(
            f"Start write into {self.connector.host}.{table_name}."
        )

        try:
            if not compression_method:
                compression_method = auto_detector(fileobj)

            if compression_method != self.compression_method:
                reader = define_reader(fileobj, compression_method)
                data = define_writer(
                    file_writer(reader),
                    self.compression_method,
                )
            else:
                reader = fileobj
                data = file_writer(reader)

            self.cursor.upload_data(
                table=table_name,
                data=data,
            )
            collect()
            size = reader.tell()
            self.logger.info(f"Successfully sending {size} bytes.")

            if not size:
                self.logger.warning("Empty data send!")

            reader.close()
        except ClickhouseServerError as error:
            raise error
        except Exception as error:
            self.logger.error(f"NativeDumperWriteError: {error}")
            raise NativeDumperWriteError(error)

        self.logger.info(
            f"Write into {self.connector.host}.{table_name} done."
        )
        self.refresh()

    def from_rows(
        self,
        dtype_data: Iterable[Any],
        table_name: str,
        source: DBMetadata | None = None,
    ) -> None:
        """Write from python list into Clickhouse table."""

        if not table_name:
            error_message = "Table name not defined."
            self.logger.error(f"NativeDumperValueError: {error_message}")
            raise NativeDumperValueError(error_message)

        if not source:
            source = DBMetadata(
                name="python",
                version="iterable object",
                columns={"Unknown": "Unknown"},
            )

        column_list = self.cursor.metadata(table_name)
        writer = NativeWriter(column_list)
        data = define_writer(
            writer.from_rows(dtype_data),
            self.compression_method,
        )

        destination = DBMetadata(
            name=self.dbname,
            version=self.version,
            columns=make_columns(column_list),
        )

        self.logger.info(transfer_diagram(source, destination))
        collect()
        self.logger.info(
            f"Start write into {self.connector.host}.{table_name}."
        )

        try:
            self.cursor.upload_data(
                table=table_name,
                data=data,
            )
        except ClickhouseServerError as error:
            raise error
        except Exception as error:
            self.logger.error(f"NativeDumperWriteError: {error}")
            raise NativeDumperWriteError(error)

        self.logger.info(
            f"Write into {self.connector.host}.{table_name} done."
        )
        self.refresh()

    def refresh(self) -> None:
        """Refresh session."""

        self.cursor.refresh()
        self.logger.info(f"Connection to host {self.connector.host} updated.")
