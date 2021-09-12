from abc import abstractmethod
from pandas.core.frame import DataFrame
from sqlalchemy import Table
from sqlalchemy.engine.base import Engine
from .gen import AbstractExecutionGenerator, FullMerge
from .util import derive_staging, recreate_table
import logging


class AbstractDbLoader:
    @abstractmethod
    def __call__(self, df: DataFrame, table: Table, engine: Engine):
        pass


class PandasDbLoader(AbstractDbLoader):
    def __init__(self, **kwargs):
        self.load_kwargs = {"index": False, "chunksize": 20000, "if_exists": "append"}
        self.load_kwargs.update(kwargs)

    def __call__(self, df: DataFrame, table: Table, engine: Engine):
        df.to_sql(table.name, engine, schema=table.schema, **self.load_kwargs)


class PandasLoad:
    def __init__(self, engine):
        self.engine = engine

    def load_db(
        self,
        target: Table,
        dataframe: DataFrame,
        execution_generator: AbstractExecutionGenerator = FullMerge,
        db_loader: AbstractDbLoader = PandasDbLoader(),
    ):
        source = derive_staging(target, list(dataframe.columns))
        recreate_table(source, self.engine)

        db_loader(dataframe, source, self.engine)

        with self.engine.begin() as conn:
            for stmt in execution_generator(source, target):
                logging.info(f"Executing: {stmt}")
                conn.execute(stmt)
