from pandas.core.frame import DataFrame
from sqlalchemy import Table
from .models import MergePart
from .expr import generate
from .util import derive_staging, recreate_table
from typing import Set
import logging


class PandasLoad:
    def __init__(self, engine):
        self.engine = engine

    def load_db(
        self,
        target: Table,
        dataframe: DataFrame,
        merge_conf: Set[MergePart] = MergePart.all(),
        chunksize=20000,
    ):
        source = derive_staging(target, list(dataframe.columns))
        recreate_table(source, self.engine)

        dataframe.to_sql(
            source.name,
            self.engine,
            schema=source.schema,
            index=False,
            if_exists="append",
            chunksize=chunksize,
        )
        with self.engine.begin() as conn:
            for part, stmt in generate(source, target):
                if part in merge_conf:
                    logging.info(f"Executing {part} as {stmt}")
                    conn.execute(stmt)
