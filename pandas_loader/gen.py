from abc import abstractmethod
from sqlalchemy import Table, Column, exists, and_
from typing import Any, Iterator, List, Union

from sqlalchemy.sql.dml import Delete, Executable
from sqlalchemy.sql.elements import literal_column
from sqlalchemy.sql.expression import select
from sqlalchemy.sql.selectable import Alias
from .models import ColumnRole


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


class AbstractExecutionGenerator:
    @abstractmethod
    def __call__(
        self, source: Table, target: Table, join_columns: List[Column] = None
    ) -> List[Union[str, Executable]]:
        pass


class StandardExecutionGenerator(AbstractExecutionGenerator):
    def __init__(self, insert: bool = True, update: bool = True, delete: bool = True):
        self.enable_insert = insert
        self.enable_update = update
        self.enable_delete = delete

    def insert(self, source: Table, target: Table, join_columns: List[Column] = None):
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
                ~exists().where(
                    and_(*[c == source_alias.c[c.name] for c in join_columns])
                )
            )
            .correlate(source_alias),
        )
        return expr

    def delete(
        self, source: Table, target: Table, join_columns: List[Column] = None
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
                source.select().where(
                    and_(*[c == source.c[c.name] for c in join_columns])
                )
            ).correlate(target)
        )
        return expr

    def _generate_update_set(
        self, source: Alias, target: Table, join_columns: List[Column] = None
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

    def update(
        self, source: Table, target: Table, join_columns: List[Column] = None
    ) -> str:
        """Creates a declarative SQL UPDATE stmt to update all values in the target table.

        Args:
            source (Table): Source table
            target (Table): Target table
            join_columns (List[Column], optional): Join columns, defaults to primary keys.

        Returns:
            [str]: Update statement
        """
        if not join_columns:
            join_columns = _primary_cols(target.alias("t"))

        update_set = "\n,".join(
            self._generate_update_set(source.alias("s"), target, join_columns)
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

    def __call__(
        self, source: Table, target: Table, join_columns: List[Column] = None
    ) -> List[Union[str, Executable]]:
        if self.enable_delete:
            yield self.delete(source, target, join_columns)
        if self.enable_update:
            yield self.update(source, target, join_columns)
        if self.enable_insert:
            yield self.insert(source, target, join_columns)


FullMerge = StandardExecutionGenerator(insert=True, update=True, delete=True)
Upsert = StandardExecutionGenerator(insert=True, update=True, delete=False)
