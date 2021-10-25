from pandas_loader.gen import FullMerge
from pandas_loader.util import recreate_table
from sqlalchemy import MetaData, Table
from sqlalchemy.engine import create_engine
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import DateTime, String
from pandas_loader.load import PandasBulkDbLoader, PandasLoad
from pandas_loader.models import ColumnRole
import pandas as pd
import logging
import os
import pytest
from datetime import datetime


logging.basicConfig(level=logging.DEBUG)


@pytest.fixture
def country_fix():
    engine = create_engine(
        f"mssql+pymssql://{os.environ.get('DB_USER') or 'sa'}:{os.environ.get('DB_PASSWD') or 'Random123!'}@localhost:1433/local"
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

    df0 = pd.DataFrame({"code": ["DE", "??"], "name": ["Doitschland", "blubblub"]})

    recreate_table(country, engine=engine)
    print(f"Recreating {country.name}")
    df0.to_sql(
        country.name, engine, schema=country.schema, if_exists="append", index=False
    )

    df = pd.DataFrame({"code": ["DE", "IT"], "name": ["Deutschland", "Italien"]})

    n = datetime.utcnow()
    loader = PandasLoad(engine=engine)

    loader.load_db(country, df, FullMerge, db_loader=PandasBulkDbLoader(engine))

    df1 = pd.read_sql_table(country.name, engine, schema=country.schema)
    df1 = df1.set_index("code")

    yield n, df1
    #country.drop(bind=engine)


def test_update_val(country_fix):
    n, df = country_fix
    assert df.loc["DE", "name"] == "Deutschland"


def test_update_tracking(country_fix):
    n, df = country_fix
    assert df.loc["DE", "_updated_on"] > n


def test_insert_val(country_fix):
    n, df = country_fix
    assert df.loc["IT", "name"] == "Italien"


def test_insert_tracking(country_fix):
    n, df = country_fix
    assert df.loc["IT", "_inserted_on"] > n


def test_delete(country_fix):
    n, df = country_fix
    with pytest.raises(KeyError):
        df.loc["??"]
