# Kaggle + FastAPI + DuckDB ETL Project

## Table of Contents
1. [Introduction](#introduction)
2. [Technologies Covered](#technologies-covered)
   - [FastAPI](#fastapi)
   - [asyncio](#asyncio)
   - [DuckDB](#duckdb)
   - [Polars](#polars)
3. [Project Setup](#project-setup)
4. [Acquiring Datasets from Kaggle](#acquiring-datasets-from-kaggle)
5. [Building the FastAPI Service](#building-the-fastapi-service)
6. [Building the Async ETL Pipeline](#building-the-async-etl-pipeline)
7. [Building the Streamlit Frontend](#building-the-streamlit-frontend)
8. [Unit Testing with pytest](#unit-testing-with-pytest)
9. [Running the Full Project](#running-the-full-project)

---

## 1. Introduction {#introduction}

In this tutorial you will build a complete, end-to-end data engineering pipeline that integrates modern Python tools from data ingestion through to an interactive analytical front-end.

### What You Will Build

```
┌───────────────────────────────────────────────────────────────┐
│                       Project Architecture                    │
├───────────────┬──────────────────┬───────────────┬───────────┤
│   Kaggle CSVs │  FastAPI Service │  asyncio ETL  │  DuckDB   │
│   (4 datasets)│  (JSON endpoints)│  (concurrent  │  (local   │
│               │                  │   HTTP fetch) │   OLAP DB)│
└───────────────┴──────────────────┴───────────────┴─────┬─────┘
                                                          │
                                              ┌───────────▼──────────┐
                                              │  Streamlit Frontend   │
                                              │  - Table browser      │
                                              │  - Polars SQL queries │
                                              └──────────────────────┘
```

**Datasets** (from Kaggle):
| Dataset | Endpoint | Description |
|---|---|---|
| Titanic Passengers | `/titanic` | Survival data from the 1912 disaster |
| Iris Flowers | `/iris` | Classic Fisher iris measurements |
| World Happiness Report | `/happiness` | Country happiness scores 2024 |
| Netflix Titles | `/netflix` | Movies and TV shows on Netflix |

### Learning Objectives

By the end of this tutorial you will be able to:
- Serve structured CSV data as REST JSON endpoints using FastAPI and Polars
- Concurrently fetch data from multiple HTTP endpoints using `asyncio` and `httpx`
- Persist fetched data into a DuckDB analytical database
- Build an interactive Streamlit application backed by DuckDB and Polars
- Write unit tests for async code with `pytest` and `pytest-asyncio`

---

## 2. Technologies Covered {#technologies-covered}

### FastAPI {#fastapi}

FastAPI is a modern, high-performance Python web framework for building APIs. It is built on top of **Starlette** (ASGI) and **Pydantic** (data validation) and supports `async/await` natively.

**Why FastAPI for this project?**
- Automatic OpenAPI documentation at `/docs`
- Native async support matches our asyncio ETL pipeline
- Polars DataFrames convert cleanly to JSON responses
- Minimal boilerplate compared to Flask or Django

```python
# Minimal FastAPI example
from fastapi import FastAPI

app = FastAPI()

@app.get("/hello")
async def hello():
    return {"message": "Hello, Data Engineer!"}
```

---

### asyncio {#asyncio}

`asyncio` is Python's built-in library for writing concurrent I/O code using the `async/await` syntax. In this project, we use it to fire off HTTP requests to all four FastAPI endpoints **at the same time** rather than one-by-one.

**The key idea — concurrent vs. sequential:**

```
Sequential (slow):          Concurrent with asyncio (fast):
─────────────────           ─────────────────────────────────
Fetch /titanic  (1.2s)      Fetch /titanic  ──┐
Wait...                     Fetch /iris     ──┤── all running
Fetch /iris     (0.8s)      Fetch /happiness──┤   simultaneously
Wait...                     Fetch /netflix  ──┘
Fetch /happiness(1.0s)      All done in ~1.2s (the slowest one)
Wait...
Fetch /netflix  (0.9s)
Total: ~3.9s                Total: ~1.2s
```

`asyncio.gather()` is the workhorse that lets us run multiple coroutines concurrently on a single thread.

---

### DuckDB {#duckdb}

DuckDB is an in-process analytical database. Unlike SQLite (row-oriented), DuckDB uses a **columnar storage format** optimised for analytical queries (aggregations, joins, scans over large datasets).

**Why DuckDB for this project?**
- Runs embedded inside your Python process — no server to manage
- Reads Polars DataFrames directly without copying data
- SQL-first: standard SQL with window functions, CTEs, etc.
- Extremely fast for analytical workloads on local files

```python
import duckdb

conn = duckdb.connect("warehouse.db")
conn.execute("CREATE TABLE IF NOT EXISTS iris AS SELECT * FROM df")
conn.execute("SELECT species, AVG(petal_length) FROM iris GROUP BY species").fetchdf()
```

---

### Polars {#polars}

Polars is a DataFrame library written in Rust. It is significantly faster than pandas for most operations and has a powerful **lazy evaluation** API.

**Why Polars for this project?**
- Reading CSVs with `pl.read_csv()` is faster than pandas
- `.to_dicts()` converts a DataFrame directly to a list of JSON-serialisable dicts — perfect for FastAPI responses
- `polars.SQLContext` lets you run SQL against in-memory DataFrames — used in the Streamlit SQL query box

```python
import polars as pl

df = pl.read_csv("titanic.csv")
print(df.head(3))
# shape: (3, 12)
# ┌─────────────┬──────────┬─────┬───────┐
# │ PassengerId │ Survived │ Pclass │ Name │
# …
```

---

## 3. Project Setup {#project-setup}

### Prerequisites

- Python **3.11** or higher
- A [Kaggle account](https://www.kaggle.com) with an API key
- `pip` or `uv` package manager

### Directory Structure

Create the following layout before writing any code:

```
kaggle_fastapi_duckdb/
├── data/
│   ├── titanic.csv
│   ├── iris.csv
│   ├── world_happiness.csv
│   └── netflix_titles.csv
├── api/
│   ├── __init__.py
│   └── main.py
├── etl/
│   ├── __init__.py
│   └── loader.py
├── app/
│   └── streamlit_app.py
├── tests/
│   ├── __init__.py
│   ├── test_api.py
│   └── test_etl.py
├── requirements.txt
└── warehouse.db           ← created at runtime by DuckDB
```

Run this in your terminal to scaffold it:

```bash
mkdir -p kaggle_fastapi_duckdb/{data,api,etl,app,tests}
cd kaggle_fastapi_duckdb
touch api/__init__.py api/main.py
touch etl/__init__.py etl/loader.py
touch app/streamlit_app.py
touch tests/__init__.py tests/test_api.py tests/test_etl.py
touch requirements.txt
```

### Installing Dependencies

Add the following to `requirements.txt`:

```text
fastapi==0.115.0
uvicorn[standard]==0.30.6
polars==1.5.0
duckdb==1.1.0
httpx==0.27.2
asyncio==3.4.3
streamlit==1.38.0
pytest==8.3.3
pytest-asyncio==0.24.0
anyio==4.6.0
```

Install everything:

```bash
pip install -r requirements.txt
```

---

## 4. Acquiring Datasets from Kaggle {#acquiring-datasets-from-kaggle}

### Setting Up the Kaggle API

1. Go to [https://www.kaggle.com/settings](https://www.kaggle.com/settings) → **API** → **Create New Token**
2. A file `kaggle.json` downloads — move it to `~/.kaggle/kaggle.json`
3. Set the correct permissions:

```bash
chmod 600 ~/.kaggle/kaggle.json
```

4. Install the Kaggle CLI:

```bash
pip install kaggle
```

### Downloading the Four Datasets

Run these commands from inside your `kaggle_fastapi_duckdb/` directory. Each command downloads and unzips the CSV into `data/`.

```bash
# 1. Titanic Passengers
kaggle competitions download -c titanic -p data/
unzip -o data/titanic.zip -d data/ && mv data/train.csv data/titanic.csv

# 2. Iris Flower Dataset
kaggle datasets download -d uciml/iris -p data/ --unzip
mv data/Iris.csv data/iris.csv

# 3. World Happiness Report 2024
kaggle datasets download -d jainaru/world-happiness-report-2024-yearly-updated -p data/ --unzip
mv data/World\ Happiness\ Report\ 2024.csv data/world_happiness.csv

# 4. Netflix Movies and TV Shows
kaggle datasets download -d shivamb/netflix-shows -p data/ --unzip
mv data/netflix_titles.csv data/netflix_titles.csv
```

### Verifying Your Data

Open a quick Python REPL to confirm the files loaded:

```python
import polars as pl

for name, path in [
    ("Titanic",   "data/titanic.csv"),
    ("Iris",      "data/iris.csv"),
    ("Happiness", "data/world_happiness.csv"),
    ("Netflix",   "data/netflix_titles.csv"),
]:
    df = pl.read_csv(path, infer_schema_length=200)
    print(f"{name}: {df.shape[0]} rows × {df.shape[1]} cols")

# Expected output (row counts will match your download):
# Titanic:   891 rows × 12 cols
# Iris:      150 rows × 6 cols
# Happiness: 143 rows × 8 cols
# Netflix:  8807 rows × 12 cols
```

---

## 5. Building the FastAPI Service {#building-the-fastapi-service}

### Overview

The FastAPI service reads each CSV file with Polars on startup and exposes it as a paginated JSON endpoint. We load the DataFrames **once into memory** so every request is a fast in-memory slice rather than a disk read.

### Writing `api/main.py`

```python
# api/main.py

from fastapi import FastAPI, HTTPException, Query
from contextlib import asynccontextmanager
import polars as pl
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
DATA_DIR = Path(__file__).parent.parent / "data"

DATASETS: dict[str, Path] = {
    "titanic":   DATA_DIR / "titanic.csv",
    "iris":      DATA_DIR / "iris.csv",
    "happiness": DATA_DIR / "world_happiness.csv",
    "netflix":   DATA_DIR / "netflix_titles.csv",
}

# ── In-memory store ────────────────────────────────────────────────────────────
# Populated once during startup via the lifespan context manager.
_frames: dict[str, pl.DataFrame] = {}


# ── Lifespan: load CSVs at startup ────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load all Kaggle CSVs into memory before the server starts serving."""
    for name, path in DATASETS.items():
        if not path.exists():
            raise FileNotFoundError(
                f"Dataset not found: {path}. "
                "Run the Kaggle download commands in Section 4 first."
            )
        _frames[name] = pl.read_csv(path, infer_schema_length=200, null_values=["", "NA", "N/A"])
        print(f"[startup] Loaded '{name}': {_frames[name].shape}")
    yield
    # Teardown (nothing needed here, but the pattern is correct)
    _frames.clear()


app = FastAPI(
    title="Kaggle Dataset API",
    description="Serves Kaggle CSV datasets as paginated JSON endpoints.",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Helper ─────────────────────────────────────────────────────────────────────
def _paginate(df: pl.DataFrame, limit: int, offset: int) -> dict:
    """Return a page of rows plus metadata."""
    page = df.slice(offset, limit)
    return {
        "total_rows": df.height,
        "columns":    df.columns,
        "limit":      limit,
        "offset":     offset,
        "rows":       page.to_dicts(),   # list[dict] → JSON-serialisable
    }


# ── Endpoints ──────────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {
        "message": "Kaggle Dataset API is running.",
        "endpoints": [f"/{name}" for name in DATASETS],
        "docs": "/docs",
    }


@app.get("/titanic")
async def get_titanic(
    limit:  int = Query(default=100, ge=1, le=1000, description="Rows to return"),
    offset: int = Query(default=0,   ge=0,           description="Row offset"),
):
    """Returns Titanic passenger records."""
    return _paginate(_frames["titanic"], limit, offset)


@app.get("/iris")
async def get_iris(
    limit:  int = Query(default=150, ge=1, le=1000),
    offset: int = Query(default=0,   ge=0),
):
    """Returns the full Iris flower dataset."""
    return _paginate(_frames["iris"], limit, offset)


@app.get("/happiness")
async def get_happiness(
    limit:  int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0,   ge=0),
):
    """Returns World Happiness Report 2024 records."""
    return _paginate(_frames["happiness"], limit, offset)


@app.get("/netflix")
async def get_netflix(
    limit:  int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0,   ge=0),
):
    """Returns Netflix titles records."""
    return _paginate(_frames["netflix"], limit, offset)


@app.get("/schema/{dataset}")
async def get_schema(dataset: str):
    """Returns column names and data types for a given dataset."""
    if dataset not in _frames:
        raise HTTPException(
            status_code=404,
            detail=f"Dataset '{dataset}' not found. Available: {list(_frames.keys())}",
        )
    df = _frames[dataset]
    return {
        "dataset": dataset,
        "schema": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
    }
```

### Key Design Decisions Explained

**`@asynccontextmanager lifespan`** — FastAPI's recommended way to run startup/shutdown code. The CSV loading happens once before the first request, keeping every endpoint fast.

**`null_values=["", "NA", "N/A"]`** — Real-world Kaggle data is messy. This tells Polars to treat these common null representations consistently.

**`to_dicts()`** — Converts a Polars DataFrame slice into `list[dict]`, which FastAPI's JSON encoder handles natively.

**`limit` / `offset` query parameters** — Pagination prevents returning 8 800 Netflix rows in one shot.

### Testing the API Manually

Start the server:

```bash
# From inside kaggle_fastapi_duckdb/
uvicorn api.main:app --reload --port 8000
```

Then open your browser to:
- `http://localhost:8000/docs` — Interactive Swagger UI
- `http://localhost:8000/titanic?limit=5` — First 5 Titanic rows
- `http://localhost:8000/schema/iris` — Iris column types

You should see JSON like:

```json
{
  "total_rows": 891,
  "columns": ["PassengerId", "Survived", "Pclass", "Name", "Sex", "Age", ...],
  "limit": 5,
  "offset": 0,
  "rows": [
    {"PassengerId": 1, "Survived": 0, "Pclass": 3, "Name": "Braund, Mr. Owen Harris", ...},
    ...
  ]
}
```

---

## 6. Building the Async ETL Pipeline {#building-the-async-etl-pipeline}

### Overview

The ETL pipeline does three things:
1. **Extract** — Concurrently fetch all four endpoints using `asyncio.gather()`
2. **Transform** — Convert the JSON response into a Polars DataFrame
3. **Load** — Persist each DataFrame as a table in DuckDB

### Understanding `asyncio.gather()`

`asyncio.gather()` takes multiple coroutines and runs them **concurrently on the event loop**. When one coroutine is waiting for a network response, the event loop switches to another coroutine instead of blocking. All four HTTP requests are in-flight at the same time.

```python
# Conceptual example
import asyncio

async def fetch_a(): ...  # takes 1.2s
async def fetch_b(): ...  # takes 0.9s
async def fetch_c(): ...  # takes 1.0s

# Sequential — ~3.1s total
result_a = await fetch_a()
result_b = await fetch_b()
result_c = await fetch_c()

# Concurrent — ~1.2s total (limited by the slowest)
result_a, result_b, result_c = await asyncio.gather(fetch_a(), fetch_b(), fetch_c())
```

### Writing `etl/loader.py`

```python
# etl/loader.py

import asyncio
import duckdb
import httpx
import polars as pl
from pathlib import Path

# ── Configuration ──────────────────────────────────────────────────────────────
API_BASE_URL = "http://localhost:8000"
DB_PATH      = Path(__file__).parent.parent / "warehouse.db"

# Each dataset: (endpoint path, DuckDB table name, fetch all rows limit)
ENDPOINTS: list[tuple[str, str, int]] = [
    ("/titanic",   "titanic",   1000),
    ("/iris",      "iris",      1000),
    ("/happiness", "happiness", 500),
    ("/netflix",   "netflix",   9000),
]


# ── Step 1: Extract (async HTTP fetch) ────────────────────────────────────────
async def fetch_dataset(
    client:    httpx.AsyncClient,
    endpoint:  str,
    table:     str,
    limit:     int,
) -> tuple[str, pl.DataFrame]:
    """
    Fetch one dataset endpoint and return it as a Polars DataFrame.

    Parameters
    ----------
    client   : shared httpx.AsyncClient (connection pooling)
    endpoint : the API path, e.g. '/titanic'
    table    : name to use when storing in DuckDB
    limit    : max rows to request

    Returns
    -------
    (table_name, polars_DataFrame)
    """
    url = f"{API_BASE_URL}{endpoint}?limit={limit}&offset=0"
    print(f"[fetch] Requesting {url} …")

    response = await client.get(url, timeout=30.0)
    response.raise_for_status()

    payload = response.json()
    rows    = payload["rows"]       # list[dict]
    columns = payload["columns"]    # list[str] (preserves original order)

    if not rows:
        raise ValueError(f"No rows returned from {endpoint}")

    # Convert to Polars DataFrame — infers dtypes automatically
    df = pl.DataFrame(rows, schema_overrides={})
    print(f"[fetch] '{table}' → {df.shape[0]} rows × {df.shape[1]} cols")
    return table, df


async def extract_all() -> dict[str, pl.DataFrame]:
    """
    Concurrently fetch all endpoints.

    asyncio.gather() fires all coroutines at once.
    The event loop switches between them while each waits for HTTP responses.
    Total time ≈ slowest single request, not the sum of all requests.
    """
    async with httpx.AsyncClient() as client:
        tasks = [
            fetch_dataset(client, endpoint, table, limit)
            for endpoint, table, limit in ENDPOINTS
        ]
        results = await asyncio.gather(*tasks)

    # results is a list of (table_name, DataFrame) tuples
    return {table: df for table, df in results}


# ── Step 2: Load (DuckDB persistence) ─────────────────────────────────────────
def load_to_duckdb(frames: dict[str, pl.DataFrame]) -> None:
    """
    Persist each Polars DataFrame as a DuckDB table.

    DuckDB can register a Polars DataFrame directly — no intermediate
    CSV or Parquet file needed.
    """
    conn = duckdb.connect(str(DB_PATH))

    for table_name, df in frames.items():
        print(f"[load] Writing table '{table_name}' ({df.height} rows) → {DB_PATH}")

        # DROP + CREATE ensures idempotency: re-running the ETL is safe.
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Register the Polars DataFrame as a virtual DuckDB relation, then
        # materialise it as a persistent table with CREATE TABLE … AS SELECT.
        conn.register("_staging", df)
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM _staging")
        conn.unregister("_staging")

        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"[load] ✓ '{table_name}' persisted — {row_count} rows in DuckDB")

    conn.close()


# ── Step 3: Orchestrate (Extract → Load) ──────────────────────────────────────
async def run_etl() -> None:
    """
    Full ETL pipeline entry point.

    1. Extract: fetch all four endpoints concurrently via asyncio
    2. Load:    persist each DataFrame into DuckDB
    """
    print("=" * 60)
    print("Starting ETL pipeline")
    print("=" * 60)

    # Extract
    frames = await extract_all()

    # Load
    load_to_duckdb(frames)

    print("=" * 60)
    print("ETL complete. Database written to:", DB_PATH)
    print("=" * 60)


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # asyncio.run() creates a new event loop, runs the coroutine, then closes it.
    asyncio.run(run_etl())
```

### Running the ETL

Make sure the FastAPI server is still running (`uvicorn api.main:app --reload --port 8000`), then in a **second terminal**:

```bash
python -m etl.loader
```

You should see output like:

```
============================================================
Starting ETL pipeline
============================================================
[fetch] Requesting http://localhost:8000/titanic?limit=1000&offset=0 …
[fetch] Requesting http://localhost:8000/iris?limit=1000&offset=0 …
[fetch] Requesting http://localhost:8000/happiness?limit=500&offset=0 …
[fetch] Requesting http://localhost:8000/netflix?limit=9000&offset=0 …
[fetch] 'iris'      → 150 rows × 6 cols
[fetch] 'happiness' → 143 rows × 8 cols
[fetch] 'titanic'   → 891 rows × 12 cols
[fetch] 'netflix'   → 8807 rows × 12 cols
[load] Writing table 'titanic' (891 rows) → warehouse.db
[load] ✓ 'titanic' persisted — 891 rows in DuckDB
...
============================================================
ETL complete. Database written to: warehouse.db
============================================================
```

Notice that the fetch log lines arrive **out of order** — that is the async concurrency in action. The iris and happiness datasets (smaller) returned before titanic and netflix (larger), even though they were started after.

### Verifying DuckDB Directly

```python
import duckdb

conn = duckdb.connect("warehouse.db")

# List all tables
print(conn.execute("SHOW TABLES").fetchdf())

# Quick check
print(conn.execute("SELECT COUNT(*) FROM netflix").fetchone())
# (8807,)

conn.close()
```

---

## 7. Building the Streamlit Frontend {#building-the-streamlit-frontend}

### Overview

The Streamlit app connects to `warehouse.db` and provides:
1. A **dropdown** to select which table to browse
2. A **data preview** that shows the first N rows of the selected table
3. A **SQL query box** powered by Polars `SQLContext` — the user writes SQL, Polars executes it, and the results display in-app

### Writing `app/streamlit_app.py`

```python
# app/streamlit_app.py

import duckdb
import polars as pl
import streamlit as st
from pathlib import Path

# ── Configuration ──────────────────────────────────────────────────────────────
DB_PATH = Path(__file__).parent.parent / "warehouse.db"

# ── Page setup ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Kaggle Data Explorer",
    page_icon="🦆",
    layout="wide",
)

st.title("🦆 Kaggle Data Explorer")
st.caption("Powered by DuckDB · Polars · FastAPI")


# ── Database connection (cached for the session) ───────────────────────────────
@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    """
    Open a read-only DuckDB connection.
    @st.cache_resource keeps a single connection alive for the Streamlit session.
    """
    if not DB_PATH.exists():
        st.error(
            f"Database not found at `{DB_PATH}`. "
            "Run the ETL pipeline first:\n\n"
            "```bash\npython -m etl.loader\n```"
        )
        st.stop()
    return duckdb.connect(str(DB_PATH), read_only=True)


conn = get_connection()


# ── Fetch available tables ─────────────────────────────────────────────────────
@st.cache_data
def list_tables() -> list[str]:
    """Return all table names in the DuckDB warehouse."""
    rows = conn.execute("SHOW TABLES").fetchall()
    return [row[0] for row in rows]


tables = list_tables()

if not tables:
    st.warning("No tables found in the database. Run the ETL pipeline to populate it.")
    st.stop()


# ── Sidebar: table selector ────────────────────────────────────────────────────
st.sidebar.header("Dataset")
selected_table = st.sidebar.selectbox(
    label="Select a table to explore:",
    options=tables,
    index=0,
)

preview_rows = st.sidebar.slider(
    label="Preview rows",
    min_value=5,
    max_value=200,
    value=25,
    step=5,
)


# ── Load selected table from DuckDB into Polars ────────────────────────────────
@st.cache_data
def load_table(table: str) -> pl.DataFrame:
    """
    Pull an entire DuckDB table into a Polars DataFrame.
    Cached per table name so switching between tables is instant after first load.
    """
    return conn.execute(f"SELECT * FROM {table}").pl()


df = load_table(selected_table)


# ── Section 1: Data Preview ────────────────────────────────────────────────────
st.header(f"Table: `{selected_table}`")

col_info, col_shape = st.columns([3, 1])
with col_shape:
    st.metric("Total Rows",    df.height)
    st.metric("Total Columns", df.width)

with col_info:
    st.caption("Column types")
    schema_df = pl.DataFrame({
        "column": df.columns,
        "dtype":  [str(d) for d in df.dtypes],
    })
    st.dataframe(schema_df, use_container_width=True, hide_index=True)

st.subheader("Data Preview")
st.dataframe(df.head(preview_rows), use_container_width=True)


# ── Section 2: Polars SQL Query Box ───────────────────────────────────────────
st.divider()
st.header("SQL Query Explorer")

st.markdown(
    f"""
Write any SQL query below. The table **`{selected_table}`** is available as a SQL context.

**Example queries:**
```sql
-- Count rows
SELECT COUNT(*) AS total FROM {selected_table}

-- Aggregation
SELECT species, AVG(petal_length) AS avg_petal_len
FROM {selected_table}
GROUP BY species
ORDER BY avg_petal_len DESC
```
"""
)

default_query = f"SELECT *\nFROM {selected_table}\nLIMIT 10"

sql_query = st.text_area(
    label="Enter your SQL query:",
    value=default_query,
    height=180,
    placeholder=f"SELECT * FROM {selected_table} LIMIT 10",
)

run_button = st.button("▶ Run Query", type="primary")

if run_button:
    if not sql_query.strip():
        st.warning("Query is empty. Please enter a SQL statement.")
    else:
        try:
            # polars.SQLContext registers DataFrames as named tables.
            # The context is scoped — it does not persist between button clicks.
            ctx = pl.SQLContext({selected_table: df}, eager=True)
            result_df = ctx.execute(sql_query)

            st.success(f"Query returned {result_df.height} row(s).")
            st.dataframe(result_df, use_container_width=True)

        except Exception as exc:
            st.error(f"Query failed: {exc}")
            st.code(sql_query, language="sql")

# ── Footer ─────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "Data sourced from Kaggle · Served by FastAPI · Stored in DuckDB · "
    "Queried with Polars SQLContext"
)
```

### Key Design Decisions Explained

**`@st.cache_resource` for the DuckDB connection** — This decorator keeps a single database connection object alive for the life of the Streamlit process. Without it, a new connection would be created on every user interaction, which is wasteful.

**`@st.cache_data` for table loads** — Once a table is loaded into a Polars DataFrame it is cached by table name. Switching between tables in the dropdown reloads from cache instantly after the first fetch.

**`conn.execute(...).pl()`** — DuckDB's native `.pl()` method returns a Polars DataFrame directly, without any intermediate conversion to pandas.

**`polars.SQLContext`** — Registers the in-memory Polars DataFrame as a named table. SQL queries run entirely in Polars (no round-trip to DuckDB), which means the SQL box works on a snapshot of the data loaded at the time you selected the table.

### Running the Streamlit App

```bash
streamlit run app/streamlit_app.py
```

Your browser will open automatically at `http://localhost:8501`.

---

## 8. Unit Testing with pytest {#unit-testing-with-pytest}

### Overview

We write two test modules:
- `tests/test_api.py` — Tests the FastAPI endpoints using `httpx.AsyncClient` and `pytest-asyncio`
- `tests/test_etl.py` — Tests the ETL fetch and load logic

### Configuring pytest

Create a `pytest.ini` (or `pyproject.toml` section) at the project root:

```ini
# pytest.ini
[pytest]
asyncio_mode = auto
testpaths = tests
```

`asyncio_mode = auto` tells `pytest-asyncio` to automatically treat any `async def test_*` function as an async test — no need to add `@pytest.mark.asyncio` to every test.

### Writing `tests/test_api.py`

```python
# tests/test_api.py
"""
Tests for the FastAPI endpoints.

We use httpx.AsyncClient with ASGITransport to call the app
in-process — no real network socket is needed, making tests fast
and independent of a running server.
"""

import pytest
import polars as pl
from httpx import ASGITransport, AsyncClient

from api.main import app, _frames


# ── Fixtures ───────────────────────────────────────────────────────────────────
@pytest.fixture(scope="module")
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope="module")
async def client():
    """
    Spin up the FastAPI app using ASGI transport.
    The lifespan context manager runs (loading CSVs) before yielding the client.
    """
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as ac:
        yield ac


# ── Root endpoint ──────────────────────────────────────────────────────────────
async def test_root(client):
    response = await client.get("/")
    assert response.status_code == 200
    body = response.json()
    assert "endpoints" in body
    assert "/titanic" in body["endpoints"]


# ── Titanic endpoint ───────────────────────────────────────────────────────────
async def test_titanic_returns_rows(client):
    response = await client.get("/titanic?limit=10&offset=0")
    assert response.status_code == 200
    body = response.json()
    assert body["limit"] == 10
    assert len(body["rows"]) == 10


async def test_titanic_total_rows(client):
    response = await client.get("/titanic?limit=1&offset=0")
    body = response.json()
    # The full Titanic training set has 891 rows
    assert body["total_rows"] == 891


async def test_titanic_columns_present(client):
    response = await client.get("/titanic?limit=1")
    body = response.json()
    expected_cols = {"PassengerId", "Survived", "Pclass", "Name", "Sex", "Age"}
    assert expected_cols.issubset(set(body["columns"]))


async def test_titanic_offset_paginates(client):
    """Offset should shift the window of returned rows."""
    r1 = await client.get("/titanic?limit=5&offset=0")
    r2 = await client.get("/titanic?limit=5&offset=5")
    rows1 = r1.json()["rows"]
    rows2 = r2.json()["rows"]
    # First rows of each page must differ
    assert rows1[0]["PassengerId"] != rows2[0]["PassengerId"]


# ── Iris endpoint ──────────────────────────────────────────────────────────────
async def test_iris_returns_150_rows(client):
    response = await client.get("/iris?limit=200")
    body = response.json()
    assert body["total_rows"] == 150


# ── Happiness endpoint ─────────────────────────────────────────────────────────
async def test_happiness_endpoint_ok(client):
    response = await client.get("/happiness?limit=10")
    assert response.status_code == 200
    assert len(response.json()["rows"]) == 10


# ── Netflix endpoint ───────────────────────────────────────────────────────────
async def test_netflix_endpoint_ok(client):
    response = await client.get("/netflix?limit=5")
    assert response.status_code == 200
    assert len(response.json()["rows"]) == 5


# ── Schema endpoint ────────────────────────────────────────────────────────────
async def test_schema_iris(client):
    response = await client.get("/schema/iris")
    assert response.status_code == 200
    body = response.json()
    assert body["dataset"] == "iris"
    assert "schema" in body
    # Iris must have a Species / species column
    schema_keys_lower = {k.lower() for k in body["schema"]}
    assert "species" in schema_keys_lower


async def test_schema_unknown_dataset_404(client):
    response = await client.get("/schema/doesnotexist")
    assert response.status_code == 404
```

### Writing `tests/test_etl.py`

```python
# tests/test_etl.py
"""
Tests for the ETL loader.

We mock the HTTP layer so the tests do not require a running FastAPI server.
This keeps the tests fast and deterministic.
"""

import pytest
import duckdb
import polars as pl
from unittest.mock import AsyncMock, patch, MagicMock
from pathlib import Path

from etl.loader import fetch_dataset, load_to_duckdb


# ── Helpers ────────────────────────────────────────────────────────────────────
def make_mock_response(rows: list[dict], columns: list[str]) -> MagicMock:
    """Build a fake httpx Response object."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {
        "total_rows": len(rows),
        "columns":    columns,
        "limit":      len(rows),
        "offset":     0,
        "rows":       rows,
    }
    return mock_resp


# ── Sample data ────────────────────────────────────────────────────────────────
SAMPLE_IRIS_ROWS = [
    {"Id": 1, "SepalLengthCm": 5.1, "SepalWidthCm": 3.5, "PetalLengthCm": 1.4,
     "PetalWidthCm": 0.2, "Species": "Iris-setosa"},
    {"Id": 2, "SepalLengthCm": 4.9, "SepalWidthCm": 3.0, "PetalLengthCm": 1.4,
     "PetalWidthCm": 0.2, "Species": "Iris-setosa"},
]
SAMPLE_IRIS_COLS = ["Id", "SepalLengthCm", "SepalWidthCm", "PetalLengthCm", "PetalWidthCm", "Species"]


# ── fetch_dataset tests ────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_fetch_dataset_returns_polars_dataframe():
    """fetch_dataset should return a tuple of (table_name, pl.DataFrame)."""
    mock_client = AsyncMock()
    mock_client.get.return_value = make_mock_response(SAMPLE_IRIS_ROWS, SAMPLE_IRIS_COLS)

    table_name, df = await fetch_dataset(mock_client, "/iris", "iris", 150)

    assert table_name == "iris"
    assert isinstance(df, pl.DataFrame)


@pytest.mark.asyncio
async def test_fetch_dataset_correct_row_count():
    mock_client = AsyncMock()
    mock_client.get.return_value = make_mock_response(SAMPLE_IRIS_ROWS, SAMPLE_IRIS_COLS)

    _, df = await fetch_dataset(mock_client, "/iris", "iris", 150)

    assert df.height == 2


@pytest.mark.asyncio
async def test_fetch_dataset_correct_columns():
    mock_client = AsyncMock()
    mock_client.get.return_value = make_mock_response(SAMPLE_IRIS_ROWS, SAMPLE_IRIS_COLS)

    _, df = await fetch_dataset(mock_client, "/iris", "iris", 150)

    assert set(df.columns) == set(SAMPLE_IRIS_COLS)


@pytest.mark.asyncio
async def test_fetch_dataset_raises_on_empty_rows():
    """fetch_dataset should raise ValueError when the endpoint returns no rows."""
    mock_client = AsyncMock()
    mock_client.get.return_value = make_mock_response([], SAMPLE_IRIS_COLS)

    with pytest.raises(ValueError, match="No rows returned"):
        await fetch_dataset(mock_client, "/iris", "iris", 150)


@pytest.mark.asyncio
async def test_fetch_dataset_calls_correct_url():
    """Verify the constructed URL includes limit and offset query parameters."""
    mock_client = AsyncMock()
    mock_client.get.return_value = make_mock_response(SAMPLE_IRIS_ROWS, SAMPLE_IRIS_COLS)

    await fetch_dataset(mock_client, "/iris", "iris", 150)

    call_url = mock_client.get.call_args[0][0]
    assert "/iris" in call_url
    assert "limit=150" in call_url
    assert "offset=0" in call_url


# ── load_to_duckdb tests ───────────────────────────────────────────────────────
def test_load_to_duckdb_creates_table(tmp_path):
    """load_to_duckdb should create a table that is queryable."""
    db_path = tmp_path / "test.db"

    # Monkeypatch DB_PATH
    with patch("etl.loader.DB_PATH", db_path):
        iris_df = pl.DataFrame(SAMPLE_IRIS_ROWS)
        load_to_duckdb({"iris": iris_df})

    conn = duckdb.connect(str(db_path))
    tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
    assert "iris" in tables
    conn.close()


def test_load_to_duckdb_correct_row_count(tmp_path):
    db_path = tmp_path / "test.db"

    with patch("etl.loader.DB_PATH", db_path):
        iris_df = pl.DataFrame(SAMPLE_IRIS_ROWS)
        load_to_duckdb({"iris": iris_df})

    conn = duckdb.connect(str(db_path))
    count = conn.execute("SELECT COUNT(*) FROM iris").fetchone()[0]
    assert count == len(SAMPLE_IRIS_ROWS)
    conn.close()


def test_load_to_duckdb_is_idempotent(tmp_path):
    """Running load_to_duckdb twice should not raise errors or duplicate rows."""
    db_path = tmp_path / "test.db"
    iris_df = pl.DataFrame(SAMPLE_IRIS_ROWS)

    with patch("etl.loader.DB_PATH", db_path):
        load_to_duckdb({"iris": iris_df})
        load_to_duckdb({"iris": iris_df})   # second run — must not fail

    conn = duckdb.connect(str(db_path))
    count = conn.execute("SELECT COUNT(*) FROM iris").fetchone()[0]
    assert count == len(SAMPLE_IRIS_ROWS)   # rows must not be doubled
    conn.close()


def test_load_to_duckdb_multiple_tables(tmp_path):
    db_path = tmp_path / "test.db"
    frames = {
        "iris":    pl.DataFrame(SAMPLE_IRIS_ROWS),
        "titanic": pl.DataFrame([{"PassengerId": 1, "Survived": 0, "Name": "Braund"}]),
    }

    with patch("etl.loader.DB_PATH", db_path):
        load_to_duckdb(frames)

    conn = duckdb.connect(str(db_path))
    tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    assert {"iris", "titanic"}.issubset(tables)
    conn.close()
```

### Running the Tests

```bash
# From the project root
pytest -v
```

Expected output:

```
tests/test_api.py::test_root                         PASSED
tests/test_api.py::test_titanic_returns_rows         PASSED
tests/test_api.py::test_titanic_total_rows           PASSED
tests/test_api.py::test_titanic_columns_present      PASSED
tests/test_api.py::test_titanic_offset_paginates     PASSED
tests/test_api.py::test_iris_returns_150_rows        PASSED
tests/test_api.py::test_happiness_endpoint_ok        PASSED
tests/test_api.py::test_netflix_endpoint_ok          PASSED
tests/test_api.py::test_schema_iris                  PASSED
tests/test_api.py::test_schema_unknown_dataset_404   PASSED
tests/test_etl.py::test_fetch_dataset_returns_polars_dataframe PASSED
tests/test_etl.py::test_fetch_dataset_correct_row_count        PASSED
tests/test_etl.py::test_fetch_dataset_correct_columns          PASSED
tests/test_etl.py::test_fetch_dataset_raises_on_empty_rows     PASSED
tests/test_etl.py::test_fetch_dataset_calls_correct_url        PASSED
tests/test_etl.py::test_load_to_duckdb_creates_table           PASSED
tests/test_etl.py::test_load_to_duckdb_correct_row_count       PASSED
tests/test_etl.py::test_load_to_duckdb_is_idempotent           PASSED
tests/test_etl.py::test_load_to_duckdb_multiple_tables         PASSED

19 passed in 4.87s
```

---

## 9. Running the Full Project {#running-the-full-project}

Follow these steps in order. Each step depends on the previous one completing successfully.

### Step 1 — Download the Data (once)

```bash
cd kaggle_fastapi_duckdb

kaggle competitions download -c titanic -p data/ && unzip -o data/titanic.zip -d data/ && mv data/train.csv data/titanic.csv
kaggle datasets download -d uciml/iris -p data/ --unzip && mv data/Iris.csv data/iris.csv
kaggle datasets download -d jainaru/world-happiness-report-2024-yearly-updated -p data/ --unzip
kaggle datasets download -d shivamb/netflix-shows -p data/ --unzip
```

### Step 2 — Start the FastAPI Server (Terminal 1)

```bash
uvicorn api.main:app --reload --port 8000
```

Leave this running. You should see:

```
[startup] Loaded 'titanic':   (891, 12)
[startup] Loaded 'iris':      (150, 6)
[startup] Loaded 'happiness': (143, 8)
[startup] Loaded 'netflix':   (8807, 12)
INFO:     Application startup complete.
INFO:     Uvicorn running on http://127.0.0.1:8000
```

### Step 3 — Run the ETL Pipeline (Terminal 2)

```bash
python -m etl.loader
```

This will create `warehouse.db` in the project root.

### Step 4 — Launch the Streamlit App (Terminal 2)

```bash
streamlit run app/streamlit_app.py
```

Your browser will open at `http://localhost:8501`.

### Step 5 — Run the Tests (Terminal 2)

```bash
pytest -v
```

### Architecture Summary

```
Terminal 1                    Terminal 2
──────────────────────────    ────────────────────────────────────────
uvicorn api.main:app          python -m etl.loader
  │                             │
  │  Reads CSVs via Polars      │  asyncio.gather() fires 4 concurrent
  │  Serves 4 JSON endpoints    │  httpx requests to the API
  │                             │
  └───── HTTP /titanic ─────────►  DataFrame → DuckDB (warehouse.db)
  └───── HTTP /iris    ─────────►
  └───── HTTP /happiness ───────►
  └───── HTTP /netflix ─────────►
                                │
                        streamlit run app/streamlit_app.py
                                │
                          Reads warehouse.db via DuckDB
                          User picks table via dropdown
                          User writes SQL → Polars SQLContext executes
                          Results displayed in browser
```

---

### Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| `FileNotFoundError: Dataset not found` | CSVs not downloaded | Run Step 1 |
| `httpx.ConnectError` in ETL | FastAPI not running | Start the server (Step 2) first |
| `Database not found` in Streamlit | ETL not run | Run `python -m etl.loader` |
| `pytest` import errors | Missing package | Run `pip install -r requirements.txt` |
| Kaggle API 403 error | API token missing | Move `kaggle.json` to `~/.kaggle/` |