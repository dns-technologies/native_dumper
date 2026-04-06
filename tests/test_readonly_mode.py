import logging

from pathlib import Path

import pytest

from native_dumper import (
    CHConnector,
    DumperMode,
    NativeDumper,
)
from nativelib import NativeReader
from light_compressor import define_reader


logger = logging.getLogger(__name__)


class TestReadOnlyMode:
    """Тесты для read-only режима ClickHouse."""

    def test_readonly_initialization(self, demo_ch_connector: CHConnector):
        """Тестирует инициализацию в read-only режиме."""

        dumper = NativeDumper(
            connector=demo_ch_connector,
            mode=DumperMode.PROD,
            logger=logger,
        )
        assert dumper is not None  # noqa: S101
        assert dumper.is_readonly is True  # noqa: S101
        dumper.close()

    def test_readonly_can_read_data(self, demo_ch_connector: CHConnector):
        """Тестирует, что в read-only режиме можно читать данные."""

        readonly_dumper = NativeDumper(
            connector=demo_ch_connector,
            mode=DumperMode.PROD,
            logger=logger,
        )
        # Читаем из таблицы, которая точно существует на демо-сервере
        reader = readonly_dumper.to_reader("SELECT 1 as num, 'test' as text")
        rows = list(reader.to_rows())
        assert len(rows) == 1  # noqa: S101
        assert rows[0][0] == 1  # noqa: S101
        assert rows[0][1] == "test"  # noqa: S101
        readonly_dumper.close()

    def test_readonly_cannot_write_data(self, demo_ch_connector: CHConnector):
        """Тестирует, что в read-only режиме нельзя записать данные."""

        readonly_dumper = NativeDumper(
            connector=demo_ch_connector,
            mode=DumperMode.PROD,
            logger=logger,
        )

        # Попытка вставить данные должна вызвать ошибку
        with pytest.raises(Exception):  # noqa: PT011
            readonly_dumper.from_rows([("test", 1)], "some_table")

        readonly_dumper.close()

    def test_readonly_read_dump(
        self,
        demo_ch_connector: CHConnector,
        tmp_path: Path,
    ):
        """Тестирует создание дампа в read-only режиме."""

        readonly_dumper = NativeDumper(
            connector=demo_ch_connector,
            mode=DumperMode.PROD,
            logger=logger,
        )
        # Читаем дамп из запроса
        dump_file = tmp_path / "readonly_dump.native"

        with open(dump_file, "wb") as f:
            readonly_dumper.read_dump(
                f, query="SELECT 1 as num, 'test' as text"
            )

        assert dump_file.exists()  # noqa: S101
        assert dump_file.stat().st_size > 0  # noqa: S101

        with open(dump_file, "rb") as f:
            decompressed = define_reader(f, readonly_dumper.compression_method)
            reader = NativeReader(decompressed)
            rows = list(reader.to_rows())

            assert len(rows) == 1  # noqa: S101
            assert rows[0][0] == 1  # noqa: S101
            assert rows[0][1] == "test"  # noqa: S101

        readonly_dumper.close()
