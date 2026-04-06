from collections import OrderedDict
from gc import collect
from io import (
    BufferedReader,
    BufferedWriter,
)
from logging import Logger
from types import MethodType
from typing import (
    Any,
    Iterable,
    NoReturn,
)

from base_dumper import (
    BaseDumper,
    DBMetadata,
    DebugInfo,
    DumperMode,
    DumperType,
    DumpFormat,
    IsolationLevel,
    ReaderType,
    Timeout,
    get_query_kind,
    log_table,
    multiquery,
)
from csvpack import (
    CSVPackMeta,
    CSVPackReader,
    CSVPackWriter,
    CSVWriter,
)
from light_compressor import (
    CompressionLevel,
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
    Size,
    file_writer,
    info_from_headers,
)
from .core import (
    CHConnector,
    ClickhouseServerError,
    HTTPCursor,
    NativeDumperError,
    NativeDumperReadError,
    NativeDumperValueError,
    NativeDumperWriteError,
    query_template,
)
from .version import __version__


class NativeDumper(BaseDumper):
    """Class for read and write Clickhouse Native format."""

    connector: CHConnector
    compression_method: CompressionMethod
    logger: Logger
    timeout: int
    isolation: IsolationLevel
    mode: DumperMode
    dump_format: DumpFormat
    s3_file: bool
    user_agent: str
    cursor: HTTPCursor

    def __init__(
        self,
        connector: CHConnector,
        compression_method: CompressionMethod = CompressionMethod.ZSTD,
        compression_level: int = CompressionLevel.ZSTD_DEFAULT,
        logger: Logger | None = None,
        timeout: int | None = None,
        isolation: IsolationLevel = IsolationLevel.committed,
        mode: DumperMode = DumperMode.PROD,
        dump_format: DumpFormat = DumpFormat.BINARY,
        s3_file: bool = False,
    ) -> None:
        """Class initialization."""

        if int(connector.port) == 9000:
            raise NativeDumperValueError(
                "NativeDumper don't support port 9000, please, use 8123."
            )

        if timeout is None:
            timeout = Timeout.CLICKHOUSE_DEFAULT_TIMEOUT

        self.dumper_version = __version__
        self.user_agent = f"{self.__class__.__name__}/{self.dumper_version}"

        super().__init__(
            connector,
            compression_method,
            compression_level,
            logger,
            timeout,
            isolation,
            mode,
            dump_format,
            s3_file,
        )

        try:
            self.dbname = "clickhouse"
            self.cursor = HTTPCursor(
                connector=self.connector,
                compression_method=self.compression_method,
                compression_level=self._compression_level,
                logger=self.logger,
                timeout=self._timeout,
                stream_type=self.stream_type,
                user_agent=self.user_agent,
            )
            self.is_readonly, self.version = self.cursor.send_hello()
            self._compression_level = self.cursor.compression_level
        except ClickhouseServerError as error:
            raise error
        except Exception as error:
            logger.error(f"NativeDumperError: {error}")
            raise NativeDumperError(error)

        self.logger.info(
            f"NativeDumper initialized for host {self.connector.host}"
            f"[{self.dbname} {self.version}]"
        )

        if self.mode is not DumperMode.PROD:
            if self.dump_format is DumpFormat.BINARY:
                dump_format = f"{self.dump_format.name} [{self.stream_type}]"
            else:
                dump_format = self.dump_format.name

            self.logger.info(
                "NativeDumper additional info:\n"
                f"Version: {self.dumper_version}\n"
                f"User agent: {self.user_agent}\n"
                f"Compression method: {self.compression_method.name}\n"
                f"Compression level: {self.compression_level}\n"
                f"Dump format: {dump_format}\n"
                f"Statement timeout: {self.timeout} seconds\n"
            )

            if self.is_readonly:
                self.logger.warning("Read-only session. Write don't work!")

    def __dbmeta(self, metadata: list[dict[str, str]]) -> DBMetadata:
        """Generate DBMetadata from Native metadata."""

        columns = OrderedDict()

        for column_dtype in metadata:
            columns.update(column_dtype)

        return DBMetadata(
            name=self.dbname,
            version=self.version,
            columns=columns,
        )

    def __validate_action(
        self,
        query: str | None = None,
        table_name: str | None = None,
        error_message: str = "Query or table name not defined.",
    ) -> None:
        """Validate action parameters."""

        if not query and not table_name:
            self.logger.error(f"NativeDumperValueError: {error_message}")
            raise NativeDumperValueError(error_message)

    def __make_query(
        self,
        query: str | None = None,
        table_name: str | None = None,
    ) -> str:
        """Make query from table name."""

        if not query:
            query = f"SELECT * FROM {table_name}"

        return query

    @property
    def compression_level(self) -> int:
        """Property method for get compression_level value."""

        return self._compression_level

    @compression_level.setter
    def compression_level(self, compression_value: int) -> int:
        """Property method for set compression_level value."""

        self.cursor.compression_level = compression_value
        self._compression_level = self.cursor.compression_level
        return self._compression_level

    @property
    def dump_format(self) -> DumpFormat:
        """Property method for get dump_format value."""

        return self._dump_format

    @dump_format.setter
    def dump_format(self, dump_format_value: DumpFormat) -> DumpFormat:
        """Property method for set dump_format value."""

        self._dump_format = dump_format_value
        self.cursor.stream_type = self.stream_type
        self.cursor.headers["X-ClickHouse-Format"] = self.cursor.stream_type
        return self._dump_format

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

    def mode_action(
        self,
        action_data: str | MethodType | None = None,
        *args: Any,
        **kwargs: dict[str, Any],
    ) -> None:
        """DumperMode.DEBUG or DumperMode.TEST action."""

        if action_data:
            if isinstance(action_data, str):

                response = self.cursor.execute(action_data)

                if self.mode is DumperMode.PROD:
                    return

                self.logger.info("Get query debug info.")

                if self.cursor.headers_memory or self.is_readonly:
                    host = self.connector.host
                    kind = get_query_kind(action_data)
                    debug_info = info_from_headers(host, kind, response)
                else:
                    query_id = self.cursor.params["query_id"]
                    query_info = query_template("query_info").format(
                        user_agent=self.user_agent,
                        query_id=query_id,
                    )

                    for _ in range(180):
                        try:
                            reader = self.cursor.get_stream(query_info)
                            debug_info = DebugInfo(*next(reader.to_rows()))
                            reader.close()
                            break
                        except EOFError:
                            """Try again without waiting."""

                return self.logger.info(debug_info)

            return action_data(*args, **kwargs)

    def metadata(
        self,
        query: str | None = None,
        table_name: str | None = None,
        reader_meta: bool = False,
    ) -> DBMetadata | list[dict[str, str]]:
        """Read metadata from Server."""

        self.__validate_action(query, table_name)

        if query:
            table_name = f"({query}\n)"

        host = self.connector.host
        self.logger.info(f"Start read metadata from host {host}.")
        metadata = self.cursor.metadata(table_name)
        self.logger.info(f"Read metadata from host {host} done.")

        if reader_meta:
            return metadata

        return self.__dbmeta(metadata)

    @multiquery
    def _read_dump(
        self,
        fileobj: BufferedWriter,
        query: str | None,
        table_name: str | None,
    ) -> bool:
        """Internal method read_dump for generate kwargs to decorator."""

        self.__validate_action(query, table_name)
        self.logger.info(f"Start read from {self.connector.host}.")

        try:
            self.logger.info(
                "Reading native dump with compression "
                f"{self.compression_method.name}."
            )
            source = self.metadata(query, table_name)
            destination = DBMetadata(
                name="file",
                version=fileobj.name,
                columns=source.columns,
            )
            log_table(self.logger, self.mode, source, destination)

            if self.mode is DumperMode.TEST:
                return

            query = self.__make_query(query, table_name)
            stream = self.cursor.get_response(query)

            if self.dump_format is DumpFormat.CSV:
                csv_metadata = CSVPackMeta.from_params(
                    source.name,
                    source.version,
                    [column for column, _ in source.columns.items()],
                    [dtype for _, dtype in source.columns.items()],
                )
                writer = CSVPackWriter(
                    csv_metadata,
                    fileobj,
                    self.compression_method,
                    self.compression_level,
                    self.s3_file,
                )
                uncompress_stream = define_reader(
                    stream,
                    self.compression_method,
                )
                bytes_data = file_writer(uncompress_stream)
                writer.from_bytes(bytes_data)
                size = writer.compressed_length
                writer.close()
            else:
                size = 0

                while chunk := stream.read(Size.CHUNK_SIZE):
                    size += fileobj.write(chunk)
                    del chunk

            stream.close()
            fileobj.close()
            self.logger.info(f"Successfully read {size} bytes.")

            if size < 5:
                self.logger.warning("Empty data read!")

            self.logger.info(f"Read from {self.connector.host} done.")
        except ClickhouseServerError as error:
            raise error
        except Exception as error:
            self.logger.error(f"NativeDumperReadError: {error}")
            raise NativeDumperReadError(error)

    def write_between(
        self,
        table_dest: str,
        table_src: str | None = None,
        query_src: str | None = None,
        dumper_src: DumperType | None = None,
    ) -> None:
        """Write stream between Servers."""

        self.__validate_action(
            query_src,
            table_src,
            "Source query or table name not defined.",
        )
        super().write_between(
            table_dest,
            table_src,
            query_src,
            dumper_src,
        )

    @multiquery
    def _to_reader(
        self,
        query: str | None,
        table_name: str | None,
        metadata: DBMetadata | list[dict[str, str]] | None = None,
    ) -> ReaderType:
        """Internal method to_reader for generate kwargs to decorator."""

        self.__validate_action(query, table_name)

        if not metadata:
            db_metadata = self.metadata(query, table_name)
        elif not isinstance(metadata, DBMetadata):
            db_metadata = self.__dbmeta(metadata)
        else:
            db_metadata = metadata

        if self.mode is DumperMode.TEST and not self.is_between:
            log_table(self.logger, self.mode, db_metadata)
            return db_metadata

        self.logger.info(f"Get stream from {self.connector.host}.")
        query = self.__make_query(query, table_name)
        return self.cursor.get_stream(query, db_metadata)

    def _to_fileobj(
        self,
        query: str | None,
        table_name: str | None,
        metadata: DBMetadata | object | None = None,
    ) -> BufferedReader | DBMetadata:
        """Internal method to_fileobj for generate kwargs to decorator."""

        self.__validate_action(query, table_name)

        if self.mode is DumperMode.TEST and not self.is_between:

            if not metadata:
                metadata = self.metadata(query, table_name)
            elif not isinstance(metadata, DBMetadata):
                metadata = self.__dbmeta(metadata)

            log_table(self.logger, self.mode, metadata)
            return metadata

        query = self.__make_query(query, table_name)
        return self.cursor.get_response(query)

    def write_dump(
        self,
        fileobj: BufferedReader,
        table_name: str,
    ) -> None:
        """Write CSVPack/Native dump into Clickhouse."""

        self.__validate_action(None, table_name, "Table name not defined.")
        self.logger.info(
            f"Start write into {self.connector.host}.{table_name}."
        )

        try:
            if self.dump_format is DumpFormat.CSV:
                reader = CSVPackReader(fileobj)
                compression_method = reader.compression_method
            else:
                compression_method = auto_detector(fileobj)
                raw_file = define_reader(fileobj, compression_method)
                reader = NativeReader(raw_file)
                reader.read_info()
                raw_file.seek(0)

            source = DBMetadata(
                name="file",
                version=fileobj.name,
                columns={
                    column: dtype
                    for column, dtype in zip(reader.columns, reader.dtypes)
                },
            )
            destination = self.metadata(table_name=table_name)
            log_table(self.logger, self.mode, source, destination)

            if self.mode is DumperMode.TEST:
                return reader.close()

            bytes_data = reader.to_bytes()

            self.from_bytes(
                bytes_data,
                table_name,
                source,
                destination,
                CompressionMethod.NONE,
            )
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

    def from_rows(
        self,
        dtype_data: Iterable[Any],
        table_name: str,
        source: DBMetadata | object | None = None,
    ) -> None:
        """Write from python list into Clickhouse table."""

        self.__validate_action(None, table_name, "Table name not defined.")

        if not source:
            source, dtype_data = self._db_meta_from_iter(dtype_data)

        metadata = self.metadata(table_name=table_name, reader_meta=True)
        destination = self.__dbmeta(metadata)

        if self.dump_format is DumpFormat.BINARY:
            writer = NativeWriter(metadata)
        elif self.dump_format is DumpFormat.CSV:
            csvpack_meta = CSVPackMeta.from_params(
                destination.name,
                destination.version,
                [column for column, _ in destination.columns.items()],
                [dtype for _, dtype in destination.columns.items()],
            )
            writer = CSVWriter(*csvpack_meta[4:])
        else:
            error = f"Unknown dump format {self.dump_format}"
            self.logger.error(f"NativeDumperWriteError: {error}")
            raise NativeDumperWriteError(error)

        bytes_data = writer.from_rows(dtype_data)
        self.from_bytes(
            bytes_data,
            table_name,
            source,
            destination,
            CompressionMethod.NONE,
        )
        collect()

    def from_bytes(
        self,
        bytes_data: Iterable[bytes],
        table_name: str,
        source: DBMetadata | object | None = None,
        destination: DBMetadata | None = None,
        compression_method: CompressionMethod | None = None,
    ) -> None:
        """Write from iterable bytes into Server object."""

        if not source:
            raise NativeDumperWriteError("Source metadata not define.")

        if not isinstance(source, DBMetadata):
            source = self.__dbmeta(source)

        if not destination:
            destination = self.metadata(table_name=table_name)

        log_table(self.logger, self.mode, source, destination)

        if self.mode is DumperMode.TEST:
            return

        if not compression_method:
            compression_method = self.compression_method

        if compression_method != self.compression_method:
            bytes_data = define_writer(
                bytes_data,
                self.compression_method,
                self.compression_level,
            )

        self.cursor.upload_data(
            table=table_name,
            data=bytes_data,
        )
        collect()

    def refresh(self) -> None:
        """Refresh session."""

        self.cursor.refresh()
        self.logger.info(f"Connection to host {self.connector.host} updated.")
