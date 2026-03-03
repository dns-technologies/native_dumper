from base_dumper import DBConnector


DEFAULT_DATABASE = ""
DEFAULT_USER = "default"
DEFAULT_PASSWORD = ""
DEFAULT_PORT = 8123


class CHConnector(DBConnector):
    """Connector for Clickhouse."""

    def __new__(
        cls,
        host: str,
        dbname: str = DEFAULT_DATABASE,
        user: str = DEFAULT_USER,
        password: str = DEFAULT_PASSWORD,
        port: int = DEFAULT_PORT,
    ):
        return super().__new__(
            cls,
            host,
            dbname,
            user,
            password,
            port,
        )
