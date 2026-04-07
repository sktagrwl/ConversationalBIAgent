# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

A Conversational BI Agent: natural language → SQL → charts/tables, backed by DuckDB and Streamlit.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Download the dataset (one-time, requires kaggle credentials)
python setup_dataset.py

# Run the app
streamlit run app.py

# Run tests (tests/ directory exists but is currently empty)
python -m pytest tests/ -v
```

## Key Conventions

- **Model**: Always use `claude-sonnet-4-6` for agent calls
- **Environment**: Copy `.env.example` to `.env` and set `ANTHROPIC_API_KEY`
- **Derived tables first**: Never query raw CSV tables directly — they have 32M+ rows
- **eval_set**: Never filter `WHERE eval_set = 'prior'` — this silently discards valid `train` rows. Any filter on `eval_set` in a generated query is almost certainly a bug.
- **Persistent DB**: Delete `data/warehouse.duckdb` and cached `.parquet` files to force full re-materialization from CSVs
- **days_since_prior_order NULLs**: NULL = first order ("no prior order"), not 0 days. NEVER use `COALESCE(days_since_prior_order, 0)` in AVG or distribution queries — corrupts interval metrics. Only valid inside cumulative `SUM OVER (...)` windows (already materialized as `fact_orders.days_since_first_order`).

## Architecture

### Data flow
```
data/*.csv  →  DuckDB raw tables  →  derived tables  →  agent schema  →  LLM  →  SQL  →  DataFrame  →  Plotly/st.dataframe
```

### Agent (`src/agent.py`)
Single-phase: schema metadata + question → LLM → XML → SQL → execute. **The LLM never sees data rows.**

LLM output format (parsed by `_parse_response`):
```xml
<response>
  <reasoning>Step 1/2/3 planning...</reasoning>
  <chart_type>bar|line|scatter|pie|table</chart_type>
  <sql>SELECT ...</sql>
</response>
```

On SQL execution failure, `answer_question()` makes one retry: it appends the error to the message history and asks the LLM to self-correct. After two failures it gives up and surfaces the error.

### Database (`src/database.py`)
`get_connection()` runs two phases on startup (both are no-ops once tables exist):
1. **Parquet/CSV materialization** — Parquet are preferred. `data/<name>.csv` are auto-converted to `<name>.parquet` and then ingested as DuckDB tables.
2. **Derived table build** — fixed build order (each depends on the previous):

| Table | Built from | Purpose |
|---|---|---|
| `order_products` | prior + train CSVs | Unified basket rows (test excluded) |
| `fact_orders` | order_products + lookup joins | Full hierarchy denormalized |
| `aisle_metrics` | fact_orders | Per-aisle aggregates |
| `product_metrics` | order_products + lookups | Per-product stats |
| `order_metrics` | orders + order_products | Per-order basket stats |
| `user_metrics` | orders + order_metrics | Per-user aggregates |
| `department_metrics` | fact_orders | Per-department aggregates (reorder_rate computed correctly here) |

`_schema_to_text()` annotates tables as `DERIVED — PREFER THIS`, `RAW — avoid`, or row count. `validate_sql()` rejects bare `SELECT *` at query top-level. Results are capped at `MAX_ROWS` (default 10,000) with a `truncated` flag.

### Visualization (`src/visualization.py`)
Pure mapper from `chart_type` string → Plotly figure. Chart selection is entirely the LLM's decision. Returns `None` for `"table"` or unrecognised types; `app.py` falls back to `st.dataframe`.

### UI (`app.py`)
DuckDB connection stored in `st.session_state.con` (initialized once per browser session). Chat history in `st.session_state.messages`, each entry carrying `df`, `truncated`, and `chart_type` alongside the message text.

### Config (`src/config.py`)
Loads `.env` via `python-dotenv`. Key vars: `ANTHROPIC_API_KEY` (required), `DATA_DIR` (default `data/`), `DB_PATH` (default `data/warehouse.duckdb`), `MAX_ROWS` (default `10000`). `MODEL` is hardcoded to `claude-sonnet-4-6`.

## Reference

| File | Contents |
|---|---|
| `.claude/architecture.md` | Per-module architecture detail |
| `.claude/dataset.md` | Data model rules: scale constraints, eval_set partition |
| `.claude/requirements.md` | Phased feature roadmap |
