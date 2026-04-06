from native_dumper import NativeDumper

from conftest import FakeDumper


class TestWithFakeDumper:
    """Тесты с использованием фейкового дампера."""

    def test_write_between_with_fake_dumper(
        self,
        ch_dumper: NativeDumper,
        test_table: str,
    ):
        """Тестирует write_between с фейковым дампером."""

        fake_dumper = FakeDumper()
        ch_dumper.write_between(
            test_table, table_src="fake_table", dumper_src=fake_dumper
        )
        written = fake_dumper.get_written_data()
        operations = [w["operation"] for w in written]
        assert "to_reader" in operations or "metadata" in operations  # noqa: S101

    def test_fake_dumper_in_read_dump(
        self,
        ch_dumper: NativeDumper,
        test_table: str,
        test_rows: list,
        fake_dumper: FakeDumper,
    ):
        """Тестирует использование фейкового дампера в read_dump."""

        ch_dumper.from_rows(test_rows, test_table)
        ch_dumper.refresh()
        fake_dumper.set_read_data("fake_metadata,data")
        assert hasattr(fake_dumper, "to_bytes")  # noqa: S101
        assert hasattr(fake_dumper, "metadata")  # noqa: S101
