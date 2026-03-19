from pathlib import Path


QUERIES_PATH = f"{Path(__file__).parent.absolute()}/queries/{{}}.sql"


def query_template(query_name: str) -> str:
    """Get query template for his name."""

    with open(QUERIES_PATH.format(query_name), encoding="utf-8") as query:
        return query.read()
