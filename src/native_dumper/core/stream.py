from io import BufferedReader

from base_dumper import DBMetadata
from nativelib import (
    BlockReader,
    NativeReader,
)
from nativelib.common.repr import table_repr


class NativeStreamReader(NativeReader):
    """Class for manipulate uncompressed stream native object."""

    fileobj: BufferedReader
    db_metadata: DBMetadata
    block_reader: BlockReader
    num_blocks: int
    num_rows: int
    _metadata: list[dict[str, str]]

    def __init__(
        self,
        fileobj: BufferedReader,
        db_metadata: DBMetadata,
    ) -> None:
        """Class initialization."""

        self.fileobj = fileobj
        self.db_metadata = db_metadata
        self.block_reader = BlockReader(self.fileobj)
        self.num_blocks = 0
        self.num_rows = 0
        self._metadata = [
            {column: dtype}
            for column, dtype in self.db_metadata.columns.items()
        ]

    @property
    def columns(self) -> list[str]:
        """Get column names."""

        return [
            column
            for column, _ in self.db_metadata.columns.items()
        ]

    @property
    def dtypes(self) -> list[str]:
        """Get column data types."""

        return [
            dtype
            for _, dtype in self.db_metadata.columns.items()
        ]

    @property
    def num_columns(self) -> int:
        """Get number of columns."""

        return len(self.metadata)

    @property
    def metadata(self) -> list[dict[str, str]]:
        """Get metadata."""

        return self._metadata

    def __repr__(self) -> str:
        """String representation of NativeStreamReader."""

        return table_repr(
            self.columns,
            self.dtypes,
            "<Native stream reader>",
            [
                f"Total columns: {self.num_columns}",
                f"Readed rows: {self.num_rows}",
                f"Readed blocks: {self.num_blocks}",
                f"Source: {self.db_metadata.name}",
                f"Version: {self.db_metadata.version}",
            ],
        )
