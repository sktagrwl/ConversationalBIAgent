# Feature Requirements

## Phase 1 — Foundation
- [x] Project scaffolding: src/, data/, app.py
- [x] DuckDB: auto-register CSVs as tables on startup
- [x] Schema introspection: expose table/column/type to agent
- [x] Agent: NL → SQL via Anthropic SDK
- [x] Visualization: chart selection (bar, line, pie, scatter)
- [x] Streamlit UI: chat input + response + SQL expander + schema sidebar

## Phase 2 — Multi-step Reasoning
- [x] Tool-use agent: give Claude a `run_sql` tool for intermediate queries
- [x] Conversation memory: pass prior Q&A turns into agent context
- [x] Error recovery: if SQL fails, agent retries with the error message

## Phase 3 — Richer Output
- [x] LLM-driven chart type selection (replaced heuristic)
- [x] Insight narration: agent writes 2-3 sentence interpretation below every chart
- [x] Export: download query results as CSV

## Phase 4 — Dataset-Agnostic
- [x] Remove all Instacart-specific hardcoding from `database.py` and `agent.py`
- [x] `data/derived_tables.sql` — file-driven derived table definitions, git-ignored
- [x] `setup_dataset.py` — generic onboarding CLI: CSV → Parquet + AI-generated derived tables
- [x] Dynamic system prompt — `_build_system_prompt(schema)` built from live schema at runtime
- [x] Auto-detect dataset characteristics (date columns, large tables) for contextual hints

## Phase 5 — Production Hardening
- [ ] SQL safety: block DDL/DML statements
- [ ] Caching: cache query results for identical questions within a session
