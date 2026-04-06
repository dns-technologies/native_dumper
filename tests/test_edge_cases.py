import pytest

from pathlib import Path
from nativelib import NativeReader
from native_dumper import (
    ClickhouseServerError,
    NativeDumper,
)
from light_compressor import define_reader


class TestEdgeCases:
    """Тесты граничных случаев и ошибок."""

    def test_empty_table_dump(
        self,
        ch_dumper: NativeDumper,
        test_table: str,
        tmp_path: Path,
    ):
        """Тестирует создание дампа пустой таблицы."""

        dump_file = tmp_path / "empty_dump.native"

        with open(dump_file, "wb") as f:
            ch_dumper.read_dump(f, table_name=test_table)

        assert dump_file.exists()  # noqa: S101
        assert dump_file.stat().st_size == 0  # noqa: S101

        with open(dump_file, "rb") as f:
            reader = NativeReader(f)
            reader.read_info()
            assert reader.columns == []  # noqa: S101

        with open(dump_file, "rb") as f:
            reader = NativeReader(f)
            df = reader.to_pandas()
            assert df.empty  # noqa: S101

        with open(dump_file, "rb") as f:
            reader = NativeReader(f)
            pl_df = reader.to_polars()
            assert pl_df.is_empty()  # noqa: S101

    def test_nonexistent_table_error(self, ch_dumper: NativeDumper):
        """Тестирует ошибку при работе с несуществующей таблицей."""

        with pytest.raises(ClickhouseServerError):
            ch_dumper.metadata(table_name="nonexistent_table_12345")

    def test_invalid_query_error(self, ch_dumper):
        """Тестирует ошибку при невалидном запросе."""

        with pytest.raises(ClickhouseServerError):
            ch_dumper.metadata(query="SELECT FROM invalid_syntax")

    def test_large_data_transfer(
        self,
        ch_dumper: NativeDumper,
        test_table: str,
        tmp_path: Path,
    ):
        """Тестирует передачу большого объема данных."""

        large_data = [(f"User_{i}", i % 100) for i in range(10000)]
        ch_dumper.from_rows(large_data, test_table)

        dump_file = tmp_path / "test_fileobj.native"

        with open(dump_file, "wb") as f:
            ch_dumper.read_dump(f, f"SELECT COUNT(*) FROM {test_table}")

        with open(dump_file, "rb") as f:
            uncompressed = define_reader(f, ch_dumper.compression_method)
            reader = NativeReader(uncompressed)
            count = next(reader.to_rows())[0]
            assert count == 10000  # noqa: S101

        ch_dumper.refresh()
        dump_file = tmp_path / "large_dump.native"

        with open(dump_file, "wb") as f:
            ch_dumper.read_dump(f, table_name=test_table)

        assert dump_file.stat().st_size > 0  # noqa: S101
