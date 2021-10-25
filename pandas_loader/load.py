from abc import abstractmethod
from typing import final
from pandas.core.frame import DataFrame
from sqlalchemy import Table
from sqlalchemy.engine.base import Engine
from .gen import AbstractExecutionGenerator, FullMerge
from .util import derive_staging, recreate_table
import logging


class AbstractDbLoader:
    @abstractmethod
    def __call__(self, df: DataFrame, table: Table):
        pass


class PandasDbLoader(AbstractDbLoader):
    def __init__(self, engine, **kwargs):
        self.engine = engine
        self.load_kwargs = {"index": False, "chunksize": 20000, "if_exists": "append"}
        self.load_kwargs.update(kwargs)

    def __call__(self, df: DataFrame, table: Table):
        df.to_sql(table.name, self.engine, schema=table.schema, **self.load_kwargs)

class PandasBulkDbLoader(AbstractDbLoader):
    def __init__(self, engine, **kwargs):
        self.engine = engine

    def __call__(self, df: DataFrame, table: Table):     
        def insert_bulk(table, conn, keys, data_iter):
            import ctds
            con = ctds.connect(server=conn.engine.url.host, user=conn.engine.url.username, password=conn.engine.url.password, database=conn.engine.url.database)
            try:
                n = con.bulk_insert(f"[{table.schema}].[{table.name}]", data_iter)
                con.commit()
                print(n)
            except Exception as e:
                print(e)
            finally:
                con.close()
            
        df.to_sql(table.name, self.engine, schema=table.schema, if_exists="append", index=False, method=insert_bulk)

class PandasLoad:
    def __init__(self, engine):
        self.engine = engine

    def load_db(
        self,
        target: Table,
        dataframe: DataFrame,
        execution_generator: AbstractExecutionGenerator = FullMerge,
        db_loader: AbstractDbLoader = None,
    ):
        source = derive_staging(target, list(dataframe.columns))
        recreate_table(source, self.engine)

        if not db_loader:
            db_loader = PandasDbLoader(self.engine)
        db_loader(dataframe, source)

        with self.engine.begin() as conn:
            for stmt in execution_generator(source, target):
                logging.info(f"Executing: {stmt}")
                conn.execute(stmt)
