from sqlalchemy import MetaData, Table
from sqlalchemy.engine import create_engine
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import DateTime, String
from pandasdb.core import PandasLoad
from pandasdb.models import ColumnRole
import pandas as pd
import logging
import os

logging.basicConfig(level=logging.DEBUG)

engine = create_engine(
    f"mssql+pymssql://{os.environ.get('DB_USER')}:{os.environ.get('DB_PASSWD')}@localhost:1433/local"
)

dbo_meta = MetaData(schema="dbo")

country = Table(
    "test_country",
    dbo_meta,
    Column("code", String(2), primary_key=True),
    Column("name", String(200)),
    Column("description", String(200)),
    Column("_inserted_on", DateTime, info=dict(role=ColumnRole.TRACK_INSERT)),
    Column("_updated_on", DateTime, info=dict(role=ColumnRole.TRACK_UPDATE)),
)

df = pd.DataFrame({"code": ["DE", "IT"], "name": ["Deutschland", "Italien"]})

if not country.exists(bind=engine):
    country.create(bind=engine)

loader = PandasLoad(engine=engine)

loader.load_db(country, df)
