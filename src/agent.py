import xml.etree.ElementTree as ET
import re
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
        match = re.search(r"<response>.*?</response>", text, re.DOTALL | re.IGNORECASE)
        if not match:
            return None
        valid_xml = match.group(0)
        root = ET.fromstring(valid_xml)
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

Product hierarchy (3 levels):
  product → aisle → department
  Every fact_orders row contains all three levels pre-joined. Use it as the base for any hierarchy question.

Query planning — run these 3 steps before writing SQL for any hierarchy or metric question:

  Step 1 — Detect aggregation level (what the user wants to group by):
    - "products" / "items" / "what product"  → GROUP BY product_id, product_name
    - "aisles" / "categories"                → GROUP BY aisle_id, aisle
    - "departments" / "sections"             → GROUP BY department_id, department

  Step 2 — Detect metric (how it must be computed):
    - "reorder rate"         → SUM(reordered) * 1.0 / COUNT(*)   ← always recompute at target level
    - "most popular" / "top" → COUNT(*) DESC
    - "basket size"          → use order_metrics.basket_size

  Step 3 — Choose table:
    - Level needs fresh metric computation → fact_orders + GROUP BY <level>
    - Level matches a pre-aggregated table AND no further computation needed
        → use product_metrics / aisle_metrics / department_metrics directly
    - NEVER roll up a pre-aggregated metric (e.g. AVG(product_metrics.reorder_rate) for
      department-level reorder rate is WRONG — recompute from fact_orders instead)

Table selection rules:
- Tables marked DERIVED are pre-aggregated. Use them as your first choice for simple lookups.
- Tables marked RAW have tens of millions of rows. Never query them directly.
- Lookup tables (aisles, departments, products) may be joined freely.

SQL performance rules:
- Filter before joining: WHERE on the larger table before the JOIN, or use a CTE.
- Top-N: always include ORDER BY <metric_column> DESC LIMIT 10 (adjust if user specifies N).
- Never write bare SELECT * — select specific columns or aggregate. SELECT COUNT(*) is allowed.
- When result size is uncertain, add LIMIT 100.

Output your response in this EXACT XML format — no text outside the tags:
<response>
  <reasoning>
    Step 1: Aggregation level = [product|aisle|department|other]
    Step 2: Metric = [metric name and formula]
    Step 3: Table = [which table and why]
  </reasoning>
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

    messages = [{"role": "user", "content": question}]

    for attempt in range(2):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=2048,
                system=system_prompt,
                messages=messages,
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
            if attempt == 0:
                # First failure — give the LLM its own output + error, let it self-correct
                messages.extend([
                    {"role": "assistant", "content": raw},
                    {
                        "role": "user",
                        "content": (
                            f"That SQL produced an error:\n```\n{e}\n```\n"
                            "Please analyse the error, fix the SQL, and respond in the same XML format."
                        ),
                    },
                ])
            else:
                # Second failure — give up
                return sql, None, False, f"{reasoning}\n\n**Query error (after retry):** {e}", chart_type, raw

    # Unreachable — every loop iteration returns explicitly
    return None, None, False, "Unexpected error", "table", ""  # pragma: no cover
