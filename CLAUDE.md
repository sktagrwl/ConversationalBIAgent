# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Conversational BI Agent that accepts natural language questions and returns SQL query results, charts, and insights — backed by e-commerce CSV files queried via DuckDB.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run the app
streamlit run app.py

# Run a single test
python -m pytest tests/test_database.py -v

# Run all tests
python -m pytest tests/ -v
```

## Architecture

### Data Layer — `src/database.py`
DuckDB is the SQL engine. On startup it scans `data/*.csv` and registers each file as a table (filename without extension = table name). All queries run against this in-memory DuckDB connection. `get_schema()` returns a dict of `{table: [{column, type}]}` used to build agent prompts.

### Agent Layer — `src/agent.py`
Two-phase reasoning via Anthropic SDK (`claude-sonnet-4-6`):
1. **Planning phase** — given the schema + user question, the LLM decides which tables/joins are needed and produces a reasoning trace
2. **Execution phase** — LLM generates SQL, which is executed against DuckDB; results are returned as a Pandas DataFrame

The full schema is injected into every system prompt so the agent can plan multi-table joins without guessing column names.

### Visualization Layer — `src/visualization.py`
Takes a DataFrame + the original question and selects an appropriate Plotly chart type (bar, line, scatter, pie, table). Chart type selection is LLM-driven based on the question intent.

### Config — `src/config.py`
Loads `.env` via `python-dotenv`. Exposes `ANTHROPIC_API_KEY` and `DATA_DIR` (default: `data/`).

### UI — `app.py`
Streamlit app. Chat input → agent → renders DataFrame or Plotly chart in the response area.

## Key Conventions

- **CSV → table name**: `data/order_items.csv` becomes the `order_items` table in DuckDB
- **No data import step**: DuckDB reads CSVs directly; dropping a new CSV into `data/` and restarting registers it automatically
- **Environment**: Copy `.env.example` to `.env` and set `ANTHROPIC_API_KEY`
- **Model**: Always use `claude-sonnet-4-6` for agent calls
