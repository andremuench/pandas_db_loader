from sqlalchemy import Table, MetaData, Column, Integer, String, create_engine
import pandas as pd
from sqlalchemy.inspection import inspect
from pandas_loader.load import PandasLoad
from pandas_loader.gen import FullMerge, Upsert
from pandas_loader.util import recreate_table
import os


engine = create_engine(
    f"mssql+pymssql://{os.environ.get('DB_USER')}:{os.environ.get('DB_PASSWD')}@localhost:1433/local"
)

meta = MetaData(bind=engine, schema="dbo")

# Assuming that this table already exists in the DB
table = Table(
    "sample_table",
    meta,
    Column("id", Integer, primary_key=True),
    Column("value", String(200)),
)

recreate_table(table, engine)

df = pd.DataFrame({"id": [1, 2], "value": ["val", "valval"]})

loader = PandasLoad(engine)

# Full Merge
loader.load_db(table, df, FullMerge)