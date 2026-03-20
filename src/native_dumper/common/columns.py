from collections import OrderedDict

from nativelib import (
    Array,
    Column,
    ColumnInfo,
    DType,
    LowCardinality,
)


def make_columns(
    column_list: list[Column],
) -> OrderedDict[str, str]:
    """Make DBMetadata.columns dictionary."""

    def __col_type(
        dtype: Array | DType | LowCardinality,
        info: ColumnInfo,
    ) -> str:

        if dtype.__class__ is Array:
            return f"Array({__col_type(dtype.dtype, info)})"

        if dtype.__class__ is LowCardinality:
            return f"LowCardinality({__col_type(dtype.dtype, info)})"

        if dtype.__class__ is DType:
            if dtype.name == "FixedString":
                return f"{dtype.name}({info.length})"
            if dtype.name == "Decimal":
                return f"{dtype.name}({info.precision}, {info.scale})"
            if dtype.name == "DateTime64":
                if info.tzinfo:
                    return f"{dtype.name}({info.precision}, {info.tzinfo})"
                return f"{dtype.name}({info.precision})"
            if dtype.name in ("Enum8", "Enum16"):
                return f"{dtype.name}({info.enumcase})"
            if dtype.name == "Time64":
                return f"{dtype.name}({info.precision})"
            return dtype.name

    columns = OrderedDict()

    for column in column_list:
        dtype = column.dtype
        info = column.info
        columns[column.column] = __col_type(dtype, info)

    return columns
