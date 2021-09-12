# Pandas DB Loader
Utility to load [Pandas](https://pandas.pydata.org/) Dataframes to [SQLAlchemy](https://www.sqlalchemy.org/) backed databases in a concise and structural manner that relies on declarative SQLAlchemy table definitions.

## Execution
The main class is `PandasLoad`. See below the common execution pattern:
```python
from pandas_loader.load import PandasLoad
from pandas_loader.gen import FullMerge, Upsert

# Assuming that this table already exists in the DB
table = Table(
    "sample_table",
    meta,
    Column("id", Integer, primary_key=True),
    Column("value", String(200))
)

df = pd.DataFrame({"id":[1,4], "value":["val", "valval"]})

loader = PandasLoad(engine)

# Full Merge
loader.load_db(table, df, FullMerge)
# OR: Upsert
loader.load_db(table, df, Upsert)

```
## Configuration
The library can be extended by deriving from the base class `pandas_loader.gen.AbstractExecutionGenerator` or `pandas_loader.gen.StandardExecutionGenerator`. 
The same holds true for the DB Loader.
Within the `PandasLoad` call both can be adjusted:
```python
loader.load_db(table, df, execution_generator=your_generator, db_loader=your_loader)
```  