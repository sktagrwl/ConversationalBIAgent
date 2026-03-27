import xml.etree.ElementTree as ET
import anthropic
import pandas as pd
from src.config import ANTHROPIC_API_KEY, MODEL
from src.database import get_schema, run_query, DERIVED_TABLES


client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


def _schema_to_text(schema: dict) -> str:
    lines = []
    for table, info in schema.items():
        row_count = info["row_count"]

        if table in DERIVED_TABLES:
            size_label = f"DERIVED — pre-aggregated, {row_count:,} rows — PREFER THIS"
        elif row_count >= 1_000_000:
            size_label = f"RAW — {row_count:,} rows — avoid, use derived table instead"
        elif row_count >= 10_000:
            size_label = f"{row_count:,} rows"
        else:
            size_label = f"{row_count:,} rows (lookup table)"

        col_str = ", ".join(f"{c['column']} ({c['type']})" for c in info["columns"])
        lines.append(f"  {table} [{size_label}]: {col_str}")

    return "\n".join(lines)


def _parse_response(text: str) -> tuple[str, str, str] | None:
    """
    Parse the LLM's structured XML response.
    Returns (reasoning, chart_type, sql) or None on parse failure.

    LLM role boundary — enforced by this parser:
      - LLM receives: schema metadata + question
      - LLM outputs: reasoning, chart_type, SQL
      - LLM NEVER sees data rows
    """
    try:
        root = ET.fromstring(text.strip())
        reasoning = (root.findtext("reasoning") or "").strip()
        chart_type = (root.findtext("chart_type") or "table").strip().lower()
        sql = (root.findtext("sql") or "").strip()
        if sql:
            return reasoning, chart_type, sql
        return None
    except ET.ParseError:
        return None


def answer_question(
    question: str, con
) -> tuple[str | None, pd.DataFrame | None, bool, str, str, str]:
    """
    LLM plans (schema + question → reasoning + chart_type + SQL).
    DuckDB executes (SQL → DataFrame).
    LLM never sees data rows.

    Returns (sql, dataframe, truncated, reasoning, chart_type, raw_llm_response).
    """
    schema = get_schema(con)

    if not schema:
        return None, None, False, "No CSV files found in data/. Add your CSVs and restart.", "table", ""

    schema_text = _schema_to_text(schema)

    system_prompt = f"""You are a SQL expert and data analyst. You have access to the following tables:

{schema_text}

Table selection rules — follow these strictly:
- Tables marked DERIVED are pre-aggregated. Use them as your first choice for any analytical question.
- Tables marked RAW have tens of millions of rows. Never query them directly — use the corresponding DERIVED table instead.
- Lookup tables (aisles, departments, products) may be joined freely.
- When no derived table covers the question, aggregate the RAW table immediately with GROUP BY + COUNT/SUM/AVG. Never SELECT * from a RAW table.

SQL performance rules — follow these strictly:
- Filter before joining: place WHERE on the larger table BEFORE the JOIN, or pre-filter with a CTE/subquery.
- Join order: smaller table LEFT, larger table RIGHT.
- Top-N: whenever the user asks for "top N", "most popular", "highest", or "best" — always include ORDER BY <metric_column> DESC LIMIT 10 (adjust N if the user specifies a different number).
- Never write bare SELECT * — select specific columns or aggregate. SELECT COUNT(*) is allowed.
- When result size is uncertain, add LIMIT 100.

Output your response in this EXACT XML format — no text outside the tags:
<response>
  <reasoning>1-3 sentences: which tables you'll use and why</reasoning>
  <chart_type>bar</chart_type>
  <sql>
    SELECT ...
  </sql>
</response>

Chart type guide:
- bar: comparisons, rankings, top-N (most common choice)
- line: time-series or trends over an ordered sequence
- scatter: correlation between two numeric dimensions
- pie: part-of-whole proportions with ≤8 categories
- table: exact values matter or no meaningful visual pattern

Data partitioning context (Instacart dataset):
- 'order_products' is the canonical product table — it combines prior + train orders (complete purchase history).
- 'prior' orders = all orders except each user's last order. 'train' = last order for ~75% of users.
- 'test' orders are excluded from order_products — their basket data does not exist.
- Never filter by eval_set unless the user explicitly asks about it. Do not write WHERE eval_set = 'prior'.

Use DuckDB SQL syntax. All table names are exactly as listed above."""

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=1024,
            system=system_prompt,
            messages=[{"role": "user", "content": question}],
        )
    except anthropic.APIError as e:
        return None, None, False, f"**Anthropic API Error:** {e.message}", "table", ""

    raw = response.content[0].text
    parsed = _parse_response(raw)

    if not parsed:
        # LLM didn't follow format — surface the raw response as reasoning, no chart
        return None, None, False, raw, "table", raw

    reasoning, chart_type, sql = parsed

    try:
        result = run_query(con, sql)
        return sql, result.df, result.truncated, reasoning, chart_type, raw
    except Exception as e:
        return sql, None, False, f"{reasoning}\n\n**Query error:** {e}", chart_type, raw
