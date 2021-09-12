from enum import Enum, auto
from sqlalchemy import Table, Column, exists, and_
from typing import Any, Iterator, List, Set

from sqlalchemy.sql.dml import Delete, Insert
from sqlalchemy.sql.elements import literal_column
from sqlalchemy.sql.expression import select
from sqlalchemy.sql.selectable import Alias
from .models import ColumnRole, MergePart


def _primary_cols(table):
    return [c for c in table.columns if c.primary_key]


def _filter_insert_cols(columns: List[Column], source: Table) -> Iterator[Any]:
    """Filters out columns that do not exist in target or that have a tracking role"""
    for col in columns:
        try:
            role = col.info.get("role")
            if role and role == ColumnRole.TRACK_UPDATE:
                continue
            elif role and role == ColumnRole.TRACK_INSERT:
                yield col, literal_column("CURRENT_TIMESTAMP")
            else:
                source_col = source.c[col.name]
                yield col, source_col
        except KeyError:
            pass


def gen_insert_orm(
    source: Table, target: Table, join_columns: List[Column] = None
) -> Insert:
    """Generate a declarative SQL INSERT statement to insert only new items to the target

    Args:
        source (Table): Source table
        target (Table): Target table
        join_columns (List[Column]): Columns to join, defaults to Primary Keys in target

    Raises:
        ValueError: if no join columns are found

    Returns:
        Insert: The SQL statement
    """
    if not join_columns:
        join_columns = _primary_cols(target)
    if len(join_columns) == 0:
        raise ValueError("There must be at least one join column")
    source_alias = source.alias("s")
    cols_assign = list(_filter_insert_cols(target.columns, source_alias))
    expr = target.insert().from_select(
        [c[0].name for c in cols_assign],
        select([c[1] for c in cols_assign])
        .where(
            ~exists().where(and_(*[c == source_alias.c[c.name] for c in join_columns]))
        )
        .correlate(source_alias),
    )
    return expr


def _generate_update_set(
    source: Alias, target: Table, join_columns: List[Column] = None
) -> Iterator[str]:
    # TODO: Generalize MSSQL expression CURRENT_TIMESTAMP
    for col in target.columns:
        try:
            role = col.info.get("role")
            if role and role == ColumnRole.TRACK_INSERT:
                continue
            elif role and role == ColumnRole.TRACK_UPDATE:
                yield f"[{col.name}] = CURRENT_TIMESTAMP"
            elif col in join_columns:
                continue
            else:
                source_col = source.c[col.name]
                yield f"[{col.name}] = {source.name}.[{source_col.name}]"
        except KeyError:
            pass


def gen_update_orm(
    source: Table, target: Table, join_columns: List[Column] = None
) -> str:
    if not join_columns:
        join_columns = _primary_cols(target.alias("t"))

    update_set = "\n,".join(
        _generate_update_set(source.alias("s"), target, join_columns)
    )

    # TODO: Check if aliasing is neccessary here
    update_stmt = f"""
        UPDATE t
        SET 
        {update_set}
        FROM {target} t
        JOIN {source} s
        ON 
        {and_(*[c == source.alias("s").c[c.name] for c in join_columns])}
    """
    return update_stmt


def gen_delete_orm(
    source: Table, target: Table, join_columns: List[Column] = None
) -> Delete:
    """Creates a declarative SQL DELETE stmt to delete all that is not in the source compared by join_columns.

    Args:
        source (Table): Source table
        target (Table): Target table
        join_columns (List[Column], optional): Join columns, defaults to primary keys.

    Returns:
        [Delete]: Delete statement
    """
    if not join_columns:
        join_columns = _primary_cols(target)
    expr = target.delete().where(
        ~exists(
            source.select().where(and_(*[c == source.c[c.name] for c in join_columns]))
        ).correlate(target)
    )
    return expr


def generate(source: Table, target: Table, join_columns: List[Column] = None):
    return [
        (MergePart.DELETE, gen_delete_orm(source, target, join_columns)),
        (MergePart.UPDATE, gen_update_orm(source, target, join_columns)),
        (MergePart.INSERT, gen_insert_orm(source, target, join_columns)),
    ]
