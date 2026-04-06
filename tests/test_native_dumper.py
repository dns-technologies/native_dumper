import logging

from pathlib import Path

from base_dumper import (
    DumpFormat,
    CompressionMethod,
    CompressionLevel,
    DumperMode,
)
from native_dumper import NativeDumper


logger = logging.getLogger(__name__)


class TestNativeDumper:
    """Тесты для NativeDumper с реальным ClickHouse."""

    def test_initialization(self, ch_connector):
        """Тестирует инициализацию дампера."""

        dumper = NativeDumper(
            connector=ch_connector,
            compression_method=CompressionMethod.ZSTD,
            compression_level=CompressionLevel.ZSTD_DEFAULT,
            dump_format=DumpFormat.BINARY,
            mode=DumperMode.DEBUG,
            logger=logger,
        )

        assert dumper is not None  # noqa: S101
        assert dumper.connector == ch_connector  # noqa: S101
        assert dumper.dump_format == DumpFormat.BINARY  # noqa: S101
        assert dumper.mode == DumperMode.DEBUG  # noqa: S101
        dumper.close()

    def test_timeout_property(self, ch_dumper: NativeDumper):
        """Тестирует установку и получение timeout."""

        original_timeout = ch_dumper.timeout
        ch_dumper.timeout = 60
        assert ch_dumper.timeout == 60  # noqa: S101
        ch_dumper.timeout = original_timeout

    def test_compression_level_property(self, ch_dumper: NativeDumper):
        """Тестирует установку уровня сжатия."""

        original_level = ch_dumper.compression_level
        ch_dumper.compression_level = 5
        assert ch_dumper.compression_level == 5  # noqa: S101
        ch_dumper.compression_level = original_level

    def test_metadata_table(self, ch_dumper: NativeDumper, test_table: str):
        """Тестирует получение метаданных таблицы."""

        metadata = ch_dumper.metadata(table_name=test_table)
        assert metadata is not None  # noqa: S101
        assert hasattr(metadata, "columns")  # noqa: S101
        assert "name" in metadata.columns  # noqa: S101
        assert "age" in metadata.columns  # noqa: S101

    def test_metadata_query(self, ch_dumper: NativeDumper):
        """Тестирует получение метаданных из запроса."""

        query = "SELECT 1 as num, 'test' as text"
        metadata = ch_dumper.metadata(query=query)
        assert metadata is not None  # noqa: S101
        assert "num" in metadata.columns  # noqa: S101
        assert "text" in metadata.columns  # noqa: S101

    def test_write_and_read_dump(
        self,
        ch_dumper: NativeDumper,
        test_table: str,
        test_rows: list,
        tmp_path: Path,
    ):
        """Тестирует запись и чтение дампа."""

        dump_file = tmp_path / "test_dump.native"
        ch_dumper.from_rows(test_rows, test_table)

        with open(dump_file, "wb") as f:
            ch_dumper.read_dump(f, table_name=test_table)

        assert dump_file.exists()  # noqa: S101
        assert dump_file.stat().st_size > 0  # noqa: S101

        new_table = f"{test_table}_copy"

        with ch_dumper.cursor as cur:
            cur.execute(f"""
                CREATE TABLE {new_table} (
                    name String,
                    age Int32
                ) ENGINE = Memory
            """)

        with open(dump_file, "rb") as f:
            ch_dumper.write_dump(f, new_table)

        with ch_dumper:
            reader = ch_dumper.to_reader(f"SELECT name, age FROM {new_table}")
            results = list(reader.to_rows())

            assert len(results) == len(test_rows)  # noqa: S101
            for result, expected in zip(results, test_rows):
                assert result[0] == expected[0]  # noqa: S101
                assert result[1] == expected[1]  # noqa: S101

    def test_write_between(
        self,
        ch_dumper: NativeDumper,
        test_table: str,
        test_rows: list,
    ):
        """Тестирует копирование между таблицами."""

        ch_dumper.from_rows(test_rows, test_table)
        dest_table = f"{test_table}_dest"

        with ch_dumper.cursor as cur:
            cur.execute(f"""
                CREATE TABLE {dest_table} (
                    name String,
                    age Int32
                ) ENGINE = Memory
            """)

        ch_dumper.refresh()
        ch_dumper.write_between(dest_table, table_src=test_table)

        with ch_dumper:
            reader = ch_dumper.to_reader(f"SELECT name, age FROM {dest_table}")
            results = list(reader.to_rows())

            assert len(results) == len(test_rows)  # noqa: S101
            for result, expected in zip(results, test_rows):
                assert result[0] == expected[0]  # noqa: S101
                assert result[1] == expected[1]  # noqa: S101

    def test_write_between_with_query(
        self,
        ch_dumper: NativeDumper,
        test_table: str,
        test_rows: list,
    ):
        """Тестирует копирование с использованием запроса."""

        ch_dumper.from_rows(test_rows, test_table)
        dest_table = f"{test_table}_filtered"

        query = f"""
                CREATE TABLE {dest_table} (
                    name String,
                    age Int32
                ) ENGINE = Memory;
                SELECT name, age FROM {test_table} WHERE age > 30"""
        ch_dumper.write_between(dest_table, query_src=query)

        with ch_dumper:
            reader = ch_dumper.to_reader(f"SELECT name, age FROM {dest_table}")
            results = list(reader.to_rows())

            expected_count = len([r for r in test_rows if r[1] > 30])
            assert len(results) == expected_count  # noqa: S101
            for result in results:
                assert result[1] > 30  # noqa: S101

    def test_from_rows(
        self,
        ch_dumper: NativeDumper,
        test_table: str,
        test_rows: list,
    ):
        """Тестирует запись из строк (кортежей)."""

        ch_dumper.from_rows(test_rows, test_table)

        with ch_dumper:
            reader = ch_dumper.to_reader(f"SELECT name, age FROM {test_table}")
            results = list(reader.to_rows())

            assert len(results) == len(test_rows)  # noqa: S101
            for result, expected in zip(results, test_rows):
                assert result[0] == expected[0]  # noqa: S101
                assert result[1] == expected[1]  # noqa: S101

    def test_from_bytes(self, ch_dumper: NativeDumper, test_table: str):
        """Тестирует запись из байтов в CSV формате."""

        csv_data = b"Alice,30\nBob,25\nCharlie,35\n"
        original_format = ch_dumper.dump_format
        ch_dumper.dump_format = DumpFormat.CSV

        try:
            metadata = ch_dumper.metadata(table_name=test_table)
            ch_dumper.from_bytes(
                [csv_data],
                test_table,
                source=metadata,
                compression_method=CompressionMethod.NONE,
            )

            with ch_dumper:
                reader = ch_dumper.to_reader(
                    f"SELECT name, age FROM {test_table}"
                )
                results = list(reader.to_rows())

                assert len(results) == 3  # noqa: S101
                assert results[0][0] == "Alice"  # noqa: S101
                assert results[0][1] == 30  # noqa: S101

        finally:
            ch_dumper.dump_format = original_format

    def test_mode_debug_queries(self, ch_dumper: NativeDumper, capsys: object):
        """Тестирует выполнение запросов в DEBUG режиме."""

        _ = capsys
        original_mode = ch_dumper.mode
        ch_dumper.mode = DumperMode.DEBUG

        try:
            result = ch_dumper.mode_action("SELECT 1")
            assert result is None  # noqa: S101
        finally:
            ch_dumper.mode = original_mode

    def test_refresh_connection(self, ch_dumper: NativeDumper):
        """Тестирует обновление соединения."""

        original_session_id = ch_dumper.cursor.params["session_id"]
        ch_dumper.refresh()
        assert ch_dumper.cursor.params["session_id"] != original_session_id  # noqa: S101

    def test_close_connection(self, ch_dumper: NativeDumper):
        """Тестирует закрытие соединения."""

        assert ch_dumper.cursor.is_connected  # noqa: S101
        ch_dumper.close()
        assert not ch_dumper.cursor.is_connected  # noqa: S101
