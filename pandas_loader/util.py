from sqlalchemy import Table, MetaData, Column
from typing import Iterator, List

from sqlalchemy.engine.base import Engine
from sqlalchemy.inspection import inspect
from .models import ColumnRole
import logging


def _filter_columns(
    columns: List[Column], include_columns: List[str] = None
) -> Iterator[Column]:
    for col in columns:
        role = col.info.get("role")
        if role and (role == ColumnRole.TRACK_INSERT or ColumnRole.TRACK_UPDATE):
            continue
        if include_columns:
            if col.name in include_columns:
                yield col
        else:
            yield col


def derive_staging(table: Table, include_columns: List[str] = None, schema="staging"):
    """Derives staging table for defined input table

    Args:
        table (Table): Reference table
        include_columns (List[str], optional): List of columns to include
        schema (str, optional): DB schema. Defaults to "staging".
    """
    meta = MetaData(schema=schema)
    temp_table = Table(
        table.name,
        meta,
        *[
            Column(c.name, c.type)
            for c in _filter_columns(table.columns, include_columns=include_columns)
        ],
    )
    return temp_table


def recreate_table(table: Table, engine: Engine):
    if inspect(engine).has_table(table.name, schema=table.schema):
        logging.info(f"Dropping table {table}")
        table.drop(bind=engine)
    logging.info(f"Creating table {table}")
    table.create(bind=engine)