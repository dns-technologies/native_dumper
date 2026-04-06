import logging

from pathlib import Path

import pytest

from native_dumper import (
    NativeDumper,
    CHConnector,
    DumpFormat,
    CompressionMethod,
    CompressionLevel,
    DumperMode,
)


test_logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)


class TestCompressionFormats:
    """Тесты различных форматов сжатия."""

    @pytest.mark.parametrize(
        "compression_method",
        [
            CompressionMethod.ZSTD,
            CompressionMethod.GZIP,
            CompressionMethod.LZ4,
            # CompressionMethod.SNAPPY,  # ClickHouse не поддерживает Snappy
        ],
    )
    def test_compression_methods(
        self,
        ch_connector: CHConnector,
        compression_method: CompressionMethod,
        test_table: str,
        test_rows: list,
        tmp_path: Path,
    ):
        """Тестирует методы сжатия при создании дампа."""

        dumper = NativeDumper(
            connector=ch_connector,
            compression_method=compression_method,
            compression_level=CompressionLevel.ZSTD_DEFAULT,
            dump_format=DumpFormat.BINARY,
            mode=DumperMode.PROD,
            logger=test_logger,
        )

        try:
            # Записываем данные
            dumper.from_rows(test_rows, test_table)
            # Создаем дамп
            dump_file = tmp_path / f"dump_{compression_method.name}.native"
            with open(dump_file, "wb") as f:
                dumper.read_dump(f, table_name=test_table)

            assert dump_file.exists()  # noqa: S101
            assert dump_file.stat().st_size > 0  # noqa: S101
            # Восстанавливаем дамп в новую таблицу
            new_table = f"{test_table}_{compression_method.name}"

            with dumper.cursor as cur:
                cur.execute(f"""
                    CREATE TABLE {new_table} (
                        name String,
                        age Int32
                    ) ENGINE = Memory
                """)

            dumper.refresh()

            with open(dump_file, "rb") as f:
                dumper.write_dump(f, new_table)

            # Проверяем данные через to_reader
            reader = dumper.to_reader(table_name=new_table)
            results = list(reader.to_rows())
            assert len(results) == len(test_rows)  # noqa: S101

            for result, expected in zip(results, test_rows):
                assert result[0] == expected[0]  # noqa: S101
                assert result[1] == expected[1]  # noqa: S101

        finally:
            dumper.close()

    @pytest.mark.parametrize(
        "dump_format", [DumpFormat.BINARY, DumpFormat.CSV]
    )
    def test_dump_formats(
        self,
        ch_connector: CHConnector,
        dump_format: DumpFormat,
        test_table: str,
        test_rows: list,
        tmp_path: Path,
    ):
        """Тестирует форматы дампа BINARY и CSV."""

        dumper = NativeDumper(
            connector=ch_connector,
            dump_format=dump_format,
            mode=DumperMode.PROD,
            logger=test_logger,
        )

        try:
            # Записываем данные
            dumper.from_rows(test_rows, test_table)
            # Создаем дамп
            dump_file = tmp_path / f"dump_{dump_format.name}.native"

            with open(dump_file, "wb") as f:
                dumper.read_dump(f, table_name=test_table)

            assert dump_file.exists()  # noqa: S101
            assert dump_file.stat().st_size > 0  # noqa: S101
            # Восстанавливаем дамп в новую таблицу
            new_table = f"{test_table}_{dump_format.name}"

            with dumper.cursor as cur:
                cur.execute(f"""
                    CREATE TABLE {new_table} (
                        name String,
                        age Int32
                    ) ENGINE = Memory
                """)

            dumper.refresh()

            with open(dump_file, "rb") as f:
                dumper.write_dump(f, new_table)

            # Проверяем данные через to_reader
            reader = dumper.to_reader(table_name=new_table)
            results = list(reader.to_rows())

            assert len(results) == len(test_rows)  # noqa: S101

            for result, expected in zip(results, test_rows):
                assert result[0] == expected[0]  # noqa: S101
                assert result[1] == expected[1]  # noqa: S101

        finally:
            dumper.close()
