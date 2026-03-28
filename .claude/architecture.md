# Architecture

## Data Layer — `src/database.py`

DuckDB persists to `data/warehouse.duckdb` (configured via `DB_PATH`). On first startup, `get_connection()` runs two phases:

1. **CSV materialization** — reads `data/*.csv` and creates native DuckDB tables (filename without extension = table name). Skipped on subsequent runs via `CREATE TABLE IF NOT EXISTS`.
2. **Derived table build** — creates pre-aggregated tables from raw CSV tables. Build order is fixed: `order_products` → `product_metrics`/`order_metrics` → `user_metrics`/`department_metrics`.

The five derived tables (tracked in `DERIVED_TABLES` frozenset, exported for the agent) are preferred over raw tables:

| Table | Purpose |
|---|---|
| `order_products` | Unified prior+train basket rows (test partition excluded) |
| `product_metrics` | Per-product order counts, reorder rate, avg cart position |
| `order_metrics` | Per-order basket size and reorder metrics |
| `user_metrics` | Per-user aggregates derived from order_metrics |
| `department_metrics` | Per-department aggregates derived from product_metrics |

`validate_sql()` rejects bare `SELECT *` at the query top-level before execution. Results are capped at `MAX_ROWS` (default 10,000); the `QueryResult` namedtuple carries a `truncated` flag.

## Agent Layer — `src/agent.py`

Single-phase: schema + question → XML → SQL → DataFrame. The LLM **never sees data rows** — it only receives schema metadata (table names, column types, row counts) and outputs a structured XML response:

```xml
<response>
  <reasoning>...</reasoning>
  <chart_type>bar|line|scatter|pie|table</chart_type>
  <sql>SELECT ...</sql>
</response>
```

- `_parse_response()` extracts the three fields. On format failure, raw LLM text is surfaced as reasoning with no chart.
- `_schema_to_text()` annotates each table: `DERIVED — PREFER THIS`, `RAW — avoid`, or row count.
- System prompt enforces: prefer derived tables, filter before join, always LIMIT on Top-N queries.
- `answer_question()` returns `(sql, dataframe, truncated, reasoning, chart_type, raw_llm_response)`.

## Visualization Layer — `src/visualization.py`

Pure mapper: takes the `chart_type` string from the LLM and returns a Plotly figure. No heuristics — chart selection is entirely the LLM's decision. Returns `None` for `"table"` or unrecognised types, causing the caller to fall back to `st.dataframe`.

## Config — `src/config.py`

Loads `.env` via `python-dotenv`. Exposes:

- `ANTHROPIC_API_KEY` — required
- `DATA_DIR` (default: `data/`)
- `DB_PATH` (default: `data/warehouse.duckdb`)
- `MAX_ROWS` (default: `10000`) — result cap before truncation warning
- `MODEL` (hardcoded: `claude-sonnet-4-6`)

## UI — `app.py`

Streamlit app. DuckDB connection is stored in `st.session_state.con` (initialized once per session). Chat history lives in `st.session_state.messages`, each entry carrying the DataFrame, truncated flag, and chart_type alongside the message text.
