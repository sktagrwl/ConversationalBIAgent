# Feature Requirements

## Phase 1 — Foundation (current)
- [x] Project scaffolding: src/, data/, app.py
- [x] DuckDB: auto-register CSVs as tables on startup
- [x] Schema introspection: expose table/column/type to agent
- [x] Agent: two-phase NL → SQL via Anthropic SDK
- [x] Visualization: heuristic chart selection (bar, line, pie, scatter)
- [x] Streamlit UI: chat input + response + SQL expander + schema sidebar

## Phase 2 — Multi-step Reasoning ✓
- [x] Tool-use agent: give Claude a `run_sql` tool so it can execute intermediate queries
- [x] Conversation memory: pass prior Q&A turns into agent context
- [x] Error recovery: if SQL fails, agent re-tries with the error message

## Phase 3 — Richer Output
- [x] LLM-driven chart type selection (replace heuristic)
- [x] Insight narration: agent writes a 2-3 sentence interpretation below every chart
- [x] Export: download query results as CSV

## Phase 4 — Production Hardening
- [ ] SQL safety: block DDL/DML statements
- [ ] Caching: cache query results for identical questions within a session