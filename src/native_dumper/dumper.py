from gc import collect
from io import (
    BufferedReader,
    BufferedWriter,
)
from logging import Logger
from types import MethodType
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
    DebugInfo,
    DumperMode,
    DumpFormat,
    IsolationLevel,
    Timeout,
    log_diagram,
    multiquery,
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
    query_template,
)
from .version import __version__


class NativeDumper(BaseDumper):
    """Class for read and write Clickhouse Native format."""

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
        dump_format: DumpFormat = DumpFormat.RAW,
    ) -> None:
        """Class initialization."""


        if int(connector.port) == 9000:
            raise NativeDumperValueError(
                "NativeDumper don't support port 9000, please, use 8123."
            )

        if timeout is None:
            timeout = Timeout.CLICKHOUSE_DEFAULT_TIMEOUT

        self.__version__ = __version__
        self.stream_type = "native"
        self.user_agent = f"{self.__class__.__name__}/{self.__version__}"

        super().__init__(
            connector,
            compression_method,
            compression_level,
            logger,
            timeout,
            isolation,
            mode,
            dump_format,
        )

        try:
            self.cursor = HTTPCursor(
                connector=self.connector,
                compression_method=self.compression_method,
                compression_level=self.compression_level,
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

        if self.mode is not DumperMode.PROD:
            self.logger.info(
                "NativeDumper additional info:\n"
                f"Version: {self.__version__}\n"
                f"User agent: {self.user_agent}\n"
                f"Compression method: {self.compression_method.name}\n"
                f"Compression level: {self.compression_level}\n"
                f"Dump format: {self.dump_format.name}\n"
                f"Statement timeout: {self.timeout} seconds\n"
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

    def mode_action(
        self,
        action_data: str | MethodType | None = None,
        *args: Any,
        **kwargs: dict[str, Any],
    ) -> None:
        """DumperMode.DEBUG or DumperMode.TEST action."""

        if action_data:
            if isinstance(action_data, str):

                self.cursor.execute(action_data)

                if self.mode is DumperMode.PROD:
                    return

                query_id = self.cursor.params["query_id"]
                query_info = query_template("query_info").format(
                    user_agent=self.user_agent,
                    query_id=query_id,
                )
                self.logger.info("Get query debug info.")

                for _ in range(180):
                    try:
                        reader = self.cursor.get_stream(query_info, True)
                        debug_data = next(reader.to_rows())
                        reader.close()
                        break
                    except EOFError:
                        """Try again without waiting."""

                return self.logger.info(DebugInfo(
                    *debug_data,
                ))

            return action_data(*args, **kwargs)

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
            log_diagram(self.logger, self.mode, source, destination)

            if self.mode is DumperMode.TEST:
                return

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

        if not dumper_src:
            cursor = self.cursor
            src_dbname = self.dbname
            src_version = self.version
        elif dumper_src.__class__ is NativeDumper:
            cursor = dumper_src.cursor
            src_dbname = dumper_src.dbname
            src_version = dumper_src.version
        else:
            reader = dumper_src.to_reader(
                query=query_src,
                table_name=table_src,
            )

            if self.mode is DumperMode.TEST:

                if reader.__class__ is not DBMetadata:
                    reader.close()

                return self.from_rows(
                    dtype_data=None,
                    table_name=table_dest,
                    source=dumper_src._dbmeta,
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
        log_diagram(self.logger, self.mode, source, destination)

        if self.mode is DumperMode.TEST:
            return

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

        if self.mode is DumperMode.TEST:
            log_diagram(self.logger, self.mode, self._dbmeta)
            return self._dbmeta

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

            raw_file = define_reader(fileobj, compression_method)
            reader = NativeReader(raw_file)
            reader.read_info()
            src_columns = make_columns(reader.block_reader.column_list)
            source = DBMetadata(
                name="file",
                version=fileobj.name,
                columns=src_columns,
            )
            destination = DBMetadata(
                name=self.dbname,
                version=self.version,
                columns=make_columns(self.cursor.metadata(table_name)),
            )
            log_diagram(self.logger, self.mode, source, destination)

            if self.mode is DumperMode.TEST:
                return reader.close()

            raw_file.seek(0)
            bytes_data = file_writer(raw_file)

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
            size = raw_file.tell()
            self.logger.info(f"Successfully sending {size} bytes.")

            if not size:
                self.logger.warning("Empty data send!")

            raw_file.close()
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

        log_diagram(self.logger, self.mode, source, destination)

        if self.mode is DumperMode.TEST:
            return

        self.logger.info(
            f"Start write into {self.connector.host}.{table_name}."
        )

        try:
            collect()
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

    def refresh(self) -> None:
        """Refresh session."""

        self.cursor.refresh()
        self.logger.info(f"Connection to host {self.connector.host} updated.")
