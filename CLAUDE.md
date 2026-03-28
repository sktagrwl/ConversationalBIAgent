# CLAUDE.md

A Conversational BI Agent: natural language → SQL → charts/tables, backed by DuckDB and Streamlit.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Download the dataset (one-time, requires kaggle credentials)
python setup_dataset.py

# Run the app
streamlit run app.py

# Run tests
python -m pytest tests/ -v
```

## Key Conventions

- **Model**: Always use `claude-sonnet-4-6` for agent calls
- **Environment**: Copy `.env.example` to `.env` and set `ANTHROPIC_API_KEY`
- **Derived tables first**: Never query raw CSV tables directly — see `.claude/dataset.md`
- **eval_set**: Never filter `WHERE eval_set = 'prior'` — see `.claude/dataset.md`
- **Persistent DB**: Delete `data/warehouse.duckdb` to force full re-materialization from CSVs

## Reference

| File | Contents |
|---|---|
| `.claude/architecture.md` | Per-module architecture detail (database, agent, viz, config, UI) |
| `.claude/dataset.md` | Data model rules: scale constraints, eval_set partition |
| `.claude/requirements.md` | Phased feature roadmap |
