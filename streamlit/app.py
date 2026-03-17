import duckdb
import polars as pl
import streamlit as st
from os import getcwd
from os.path import exists

from pathlib import Path

# ── Configuration ──────────────────────────────────────────────────────────────

DB_PATH = getcwd() + r"\data\projects\kaggle_fastapi_etl\etl_results.db"
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
    if not Path(DB_PATH).exists(): #if not exists(path=DB_PATH):
        st.error(
            f"Database not found at `{DB_PATH}`. "
            "Run the ETL pipeline first:\n\n"
            "```bash\npython -m etl.loader\n```"
        )
        st.stop()
    return duckdb.connect(DB_PATH, read_only=True)


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