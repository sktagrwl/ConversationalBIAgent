import re
import json
from typing import Any
import anthropic
import pandas as pd
from src.config import ANTHROPIC_API_KEY, MODEL
from src.database import get_schema, run_query, DERIVED_TABLES


client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

MAX_TOOL_ITERATIONS = 10
MAX_HISTORY_TURNS = 10  # prior conversation turns (each turn = 1 user + 1 assistant msg)

RUN_SQL_TOOL = {
    "name": "run_sql",
    "description": (
        "Execute a SQL query against the DuckDB warehouse. "
        "Use this to explore data, check counts, inspect sample values, or run "
        "intermediate analytical queries before forming your final answer."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": "The SQL query to execute. Must be a SELECT statement.",
            },
            "label": {
                "type": "string",
                "description": "A short description of what this query is checking (for the reasoning trace).",
            },
        },
        "required": ["sql"],
    },
}


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


def _parse_response(text: str) -> tuple[str, str, str, str] | None:
    """
    Parse the LLM's structured XML response using regex.
    Returns (reasoning, chart_type, sql, insight) or None on parse failure.

    Uses regex instead of an XML parser so SQL containing `<` / `>` operators
    (e.g. WHERE count < 10) does not cause a parse error.

    LLM role boundary — enforced by this parser:
      - LLM receives: schema metadata + question
      - LLM outputs: reasoning, chart_type, SQL, insight
      - LLM NEVER sees data rows (only tool_result summaries for intermediate steps)
    """
    if not re.search(r"<response>", text, re.IGNORECASE):
        return None
    reasoning_m  = re.search(r"<reasoning>(.*?)</reasoning>",   text, re.DOTALL | re.IGNORECASE)
    chart_type_m = re.search(r"<chart_type>(.*?)</chart_type>", text, re.DOTALL | re.IGNORECASE)
    sql_m        = re.search(r"<sql>(.*?)</sql>",               text, re.DOTALL | re.IGNORECASE)
    insight_m    = re.search(r"<insight>(.*?)</insight>",       text, re.DOTALL | re.IGNORECASE)
    sql = sql_m.group(1).strip() if sql_m else ""
    if not sql:
        return None
    return (
        reasoning_m.group(1).strip()          if reasoning_m  else "",
        chart_type_m.group(1).strip().lower() if chart_type_m else "table",
        sql,
        insight_m.group(1).strip()            if insight_m    else "",
    )


def answer_question(
    question: str, con, history: list[dict[str, Any]] | None = None
) -> tuple[str | None, pd.DataFrame | None, bool, str, str, str, str]:
    """
    Tool-use agent: LLM can call run_sql for intermediate exploration,
    then gives a final XML response with reasoning, chart_type, and display SQL.
    Conversation history is included for multi-turn context.

    Returns (sql, dataframe, truncated, reasoning, chart_type, insight, raw_llm_response).
    """
    schema = get_schema(con)

    if not schema:
        return None, None, False, "No CSV files found in data/. Add your CSVs and restart.", "table", "", ""

    schema_text = _schema_to_text(schema)

    system_prompt = f"""You are a SQL expert and data analyst with access to the following DuckDB tables:

{schema_text}

Product hierarchy (3 levels):
  product → aisle → department
  Every fact_orders row contains all three levels pre-joined. Use it as the base for any hierarchy question.

You have a run_sql tool. Use it to:
- Explore the data before committing to a final query
- Validate assumptions (e.g. check column values, NULL rates, row counts)
- Run intermediate analytical steps when the question requires multi-step reasoning

When you are ready to give the final answer, respond with this EXACT XML format — no text outside the tags:
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
  <insight>
    2-3 sentences interpreting the key pattern or finding in the data. Reference specific values or ranks where possible.
  </insight>
</response>

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

Chart type guide (for final <chart_type>):
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

Temporal context (no calendar dates exist):
- There are NO date/timestamp columns. Do not reference order_date, created_at, month, year, or any date column.
- Time is represented as: order_number (per-user sequence 1,2,3...) and days_since_prior_order (gap in days, NULL for first order).
- fact_orders.days_since_first_order — pre-computed cumulative days from each user's first order. Use this for timeline/trend queries — no window function needed.
- fact_orders also has order_number, order_dow, order_hour_of_day directly — no join to order_metrics needed for sequence/day/hour analysis.
- Purchase frequency → use user_metrics.avg_days_between_orders (pre-computed) or AVG(days_since_prior_order) from order_metrics.
- Trends over order sequence → GROUP BY order_number from fact_orders or order_metrics (chart_type: line).
- Day-of-week / hour patterns → use order_dow (0=Sunday) and order_hour_of_day from fact_orders or order_metrics.
- days_since_prior_order is NULL for each user's first order. Use AVG (ignores NULLs) or COALESCE(days_since_prior_order, 0).

Use DuckDB SQL syntax. All table names are exactly as listed above."""

    # Build messages: prior history (last N turns) + current question
    messages: list[dict] = []
    if history:
        start_idx = max(0, len(history) - MAX_HISTORY_TURNS * 2)
        for i in range(start_idx, len(history)):
            msg = history[i]
            if msg.get("role") in ("user", "assistant") and msg.get("content"):
                messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": question})

    intermediate_steps: list[dict] = []

    for iteration in range(MAX_TOOL_ITERATIONS):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                system=system_prompt,
                tools=[RUN_SQL_TOOL],
                messages=messages,
            )
        except anthropic.APIError as e:
            return None, None, False, f"**Anthropic API Error:** {e.message}", "table", "", ""

        if response.stop_reason == "end_turn":
            # LLM gave final text response — extract text block and parse XML
            raw = ""
            if hasattr(response, "content") and isinstance(response.content, list):
                for block in response.content:
                    if getattr(block, "type", None) == "text" and hasattr(block, "text"):
                        raw = block.text
                        break
            
            parsed = _parse_response(raw)

            if not parsed:
                error_msg = raw if raw else "The model returned an unexpected response format. Please try again."
                return None, None, False, error_msg, "table", "", raw

            reasoning, chart_type, sql, insight = parsed

            # Prepend intermediate step summary to reasoning if any steps were run
            if intermediate_steps:
                steps_text = "**Intermediate queries:**\n" + "\n".join(
                    f"- {s['label']}: {s['row_count']:,} rows" for s in intermediate_steps
                )
                reasoning = steps_text + "\n\n" + reasoning

            try:
                result = run_query(con, sql)
                return sql, result.df, result.truncated, reasoning, chart_type, insight, raw
            except Exception as e:
                if iteration < MAX_TOOL_ITERATIONS - 1:
                    # Self-correction: give LLM its error and ask it to fix
                    messages.append({"role": "assistant", "content": response.content})
                    messages.append({
                        "role": "user",
                        "content": (
                            f"That SQL produced an error:\n```\n{e}\n```\n"
                            "Please analyse the error, fix the SQL, and respond in the same XML format."
                        ),
                    })
                    continue
                return sql, None, False, f"{reasoning}\n\n**Query error (after retry):** {e}", chart_type, "", raw

        elif response.stop_reason == "tool_use":
            # LLM called run_sql — execute each tool call and collect results
            tool_results = []

            for block in response.content:
                if block.type == "tool_use" and block.name == "run_sql":
                    sql_to_run = block.input.get("sql", "")
                    label = block.input.get("label", f"query {len(intermediate_steps) + 1}")

                    try:
                        result = run_query(con, sql_to_run)
                        # Cap rows sent back to LLM to keep context size manageable
                        rows = result.df.head(100).values.tolist()
                        content = json.dumps({
                            "columns": list(result.df.columns),
                            "rows": rows,
                            "total_rows": len(result.df),
                            "truncated": result.truncated,
                        })
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": content,
                        })
                        intermediate_steps.append({"label": label, "row_count": len(result.df)})
                    except Exception as e:
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "is_error": True,
                            "content": str(e),
                        })

            messages.append({"role": "assistant", "content": response.content})
            messages.append({"role": "user", "content": tool_results})

        else:
            # Unexpected stop reason (e.g. max_tokens exceeded)
            break

    return None, None, False, "Agent reached maximum iterations without a final answer.", "table", "", ""
