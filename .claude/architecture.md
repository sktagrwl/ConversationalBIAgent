# Architecture

## Data Layer — `src/database.py`

DuckDB persists to `data/warehouse.duckdb` (configured via `DB_PATH`). On startup, `get_connection()` runs two phases:

1. **CSV → Parquet materialization** — reads `data/*.csv`, converts each to `<name>.parquet`, then creates a DuckDB table named `<filename_without_extension>`. Skipped for files already converted. Fully automatic and filename-based — works with any CSV.

2. **Derived table build** — reads `data/derived_tables.sql` (if it exists), parses `-- TABLE: <name>` sentinels, and executes each `CREATE TABLE IF NOT EXISTS` in order. If the file doesn't exist, this phase is skipped silently and the app works on raw tables.

### `derived_tables.sql` format

```sql
-- TABLE: table_name
CREATE TABLE IF NOT EXISTS table_name AS
SELECT ...;

-- TABLE: another_table
CREATE TABLE IF NOT EXISTS another_table AS
SELECT ...;
```

Each statement is preceded by a `-- TABLE: <name>` comment. The parser splits on these sentinels to extract `(table_name, sql)` pairs.

### Key functions

| Function | Purpose |
|---|---|
| `_load_derived_table_sql(data_dir)` | Reads `derived_tables.sql`, returns `list[tuple[str, str]]`. Returns `[]` if file missing. |
| `DERIVED_TABLES` | Frozenset of derived table names, computed at module import from the SQL file. Exported for the agent. |
| `get_schema(con)` | Returns `{table: {columns, row_count}}` for all tables — fully dynamic, no hardcoding. |
| `validate_sql(sql)` | Rejects bare `SELECT *` at query top-level before execution. |
| `run_query(con, sql)` | Validates + executes SQL, caps results at `MAX_ROWS`, returns `QueryResult(df, truncated)`. |

## Agent Layer — `src/agent.py`

Tool-use agent: schema + question → tool calls (run_sql) → XML → SQL → DataFrame. **The LLM never sees data rows** — only schema metadata and tool_result summaries.

### System prompt

Built dynamically each request by `_build_system_prompt(schema)`:

1. **Schema text** — `_schema_to_text(schema)` lists every table with columns, types, row count, and a label: `DERIVED — PREFER THIS`, `RAW — avoid`, or row count only.
2. **Generic query rules** — `run_sql` usage guidance, query planning steps, table selection rules, SQL performance rules, chart type guide.
3. **Dynamic hints** — `_build_dynamic_guidance(schema)` inspects the live schema to add dataset-specific notes:
   - No date/timestamp columns detected → warns LLM not to use calendar fields
   - Large raw tables detected → reminds LLM to filter aggressively

### Response format

```xml
<response>
  <reasoning>Step 1/2/3 planning...</reasoning>
  <chart_type>bar|line|scatter|pie|table</chart_type>
  <sql>SELECT ...</sql>
  <insight>2-3 sentence interpretation.</insight>
</response>
```

`_parse_response()` extracts all four fields using regex (not XML parser, so SQL with `<`/`>` operators doesn't break parsing).

### Tool use loop

`answer_question()` runs up to `MAX_TOOL_ITERATIONS` (10) iterations:
- `stop_reason == "tool_use"` → execute `run_sql` calls, feed results back, continue
- `stop_reason == "end_turn"` → parse XML, execute final SQL, return result
- SQL execution error → one self-correction retry with error appended to history
- Exceeded iterations → surface error

## Visualization Layer — `src/visualization.py`

Pure mapper: `chart_type` string → Plotly figure. Fully generic — detects column types dynamically (`df.select_dtypes()`), no hardcoded column names. Chart selection is entirely the LLM's decision. Returns `None` for `"table"` or unrecognised types; caller falls back to `st.dataframe`.

## Setup Script — `setup_dataset.py`

One-time dataset onboarding. Three phases:

1. **Load** — optional Kaggle download or `--source` copy, then CSV → Parquet conversion
2. **Analyze** — calls Claude with schema + sample rows (3 per table) → generates `data/derived_tables.sql`
   - Instacart: uses known-good pre-defined SQL directly, no API call
   - Other datasets: single API call, ~$0.01–0.05 one-time cost
3. **Build** — optionally runs `get_connection()` to materialize derived tables immediately

Flags: `--source`, `--kaggle`, `--reset`, `--skip-analysis`

## Config — `src/config.py`

Loads `.env` via `python-dotenv`. Exposes:

- `ANTHROPIC_API_KEY` — required
- `DATA_DIR` (default: `data/`)
- `DB_PATH` (default: `data/warehouse.duckdb`)
- `MAX_ROWS` (default: `10000`) — result cap before truncation warning
- `MODEL` (hardcoded: `claude-sonnet-4-6`)

## UI — `app.py`

Streamlit app. DuckDB connection stored in `st.session_state.con` (initialized once per session). Chat history in `st.session_state.messages`, each entry carrying `df`, `truncated`, `chart_type`, and `insight` alongside message text.
