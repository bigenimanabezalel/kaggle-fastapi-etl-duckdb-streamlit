
import asyncio
import duckdb
import httpx
import polars as pl
from os import getcwd

# ── Configuration ──────────────────────────────────────────────────────────────

API_BASE_URL = "http://localhost:8000"

# Database path (relative to this script)
DB_PATH      = getcwd() + "\\data\\projects\\kaggle_fastapi_etl\\etl_results.db"

# Each dataset: (endpoint path, DuckDB table name, fetch all rows limit)
ENDPOINTS: list[tuple[str, str]] = [
    ("/titanic",   "titanic"),
    ("/iris",      "iris"),
    ("/world_happiness_report", "happiness"),
    ("/netflix_titles",   "netflix"),
]


# ── Step 1: Extract (async HTTP fetch) ────────────────────────────────────────
async def fetch_dataset(
    client:    httpx.AsyncClient,
    endpoint:  str,
    table:     str,

) -> tuple[str, pl.DataFrame]:
    """
    Fetch one dataset endpoint and return it as a Polars DataFrame.

    Parameters
    ----------
    client   : shared httpx.AsyncClient (connection pooling)
    endpoint : the API path, e.g. '/titanic'
    table    : name to use when storing in DuckDB

    Returns
    -------
    (table_name, polars_DataFrame)
    """
    url = f"{API_BASE_URL}{endpoint}"
    print(f"[fetch] Requesting {url} …")

    response = await client.get(url, timeout=30.0)
    response.raise_for_status()

    payload = response.json()
    rows    = len(payload)     # list[dict]
    columns = list(payload[0].keys())    # list[str] (preserves original order)

    if not rows:
        raise ValueError(f"No rows returned from {endpoint}")

    # Convert to Polars DataFrame — infers dtypes automatically
    df = pl.json_normalize(payload)
    print(f"[fetch] '{table}' → {rows} rows × {len(columns)} cols \n columns: {",".join(columns)}")
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
            fetch_dataset(client, endpoint, table)
            for endpoint, table, in ENDPOINTS
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
    
    
    
