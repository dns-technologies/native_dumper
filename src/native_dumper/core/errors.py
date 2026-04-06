from base_dumper import (
    BaseDumperError,
    BaseDumperValueError,
)


class ClickhouseServerError(BaseDumperValueError):
    """Clickhouse errors."""


class NativeDumperError(BaseDumperError):
    """NativeDumper base error."""


class NativeDumperReadError(NativeDumperError):
    """NativeDumper read error."""


class NativeDumperWriteError(NativeDumperError):
    """NativeDumper write error."""


class NativeDumperValueError(NativeDumperError, BaseDumperValueError):
    """NativeDumper value error."""
