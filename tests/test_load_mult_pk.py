from pandas_loader.gen import Upsert
from pandas_loader.util import recreate_table
from sqlalchemy import MetaData, Table
from sqlalchemy.engine import create_engine
from sqlalchemy.sql.schema import Column, PrimaryKeyConstraint
from sqlalchemy.sql.sqltypes import DateTime, Float, Integer, String
from pandas_loader.load import PandasLoad
import pandas as pd
import logging
import os
import pytest


logging.basicConfig(level=logging.DEBUG)


@pytest.fixture
def init_fix():
    engine = create_engine(
        f"mssql+pymssql://{os.environ.get('DB_USER')}:{os.environ.get('DB_PASSWD')}@localhost:1433/local"
    )

    dbo_meta = MetaData(schema="dbo")

    yearly_values = Table(
        "test_values_year",
        dbo_meta,
        Column("code", String(2)),
        Column("year", Integer),
        Column("value", Float),
        PrimaryKeyConstraint("code", "year"),
    )

    df0 = pd.DataFrame(
        {"code": ["DE", "DE"], "year": [2019, 2020], "value": [19.1, 20.0]}
    )

    recreate_table(yearly_values, engine)

    df0.to_sql(
        yearly_values.name,
        engine,
        schema=yearly_values.schema,
        if_exists="append",
        index=False,
    )

    df = pd.DataFrame({"code": ["DE", "DE"], "year": [2020, 2021], "value": [21, 22.1]})

    loader = PandasLoad(engine=engine)
    loader.load_db(yearly_values, df, Upsert)

    df1 = pd.read_sql_table(yearly_values.name, engine, yearly_values.schema)

    yield df1

    yearly_values.drop(bind=engine)


def test_old_exists(init_fix):
    assert len(init_fix["year"].unique()) == 3


def test_updated_val(init_fix):
    df = init_fix.set_index(["code", "year"])
    assert df.loc[("DE", 2020), "value"] == 21
