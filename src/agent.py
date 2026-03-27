import re
import anthropic
import pandas as pd
from src.config import ANTHROPIC_API_KEY, MODEL
from src.database import get_schema, run_query


client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


def _schema_to_text(schema: dict) -> str:
    lines = []
    for table, cols in schema.items():
        col_str = ", ".join(f"{c['column']} ({c['type']})" for c in cols)
        lines.append(f"  {table}: {col_str}")
    return "\n".join(lines)


def _extract_sql(text: str) -> str | None:
    """Pull the first SQL block out of an LLM response."""
    match = re.search(r"```sql\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    # Fallback: look for a bare SELECT
    match = re.search(r"(SELECT\s+.+)", text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None


def answer_question(question: str, con) -> tuple[str | None, pd.DataFrame | None, str]:
    """
    Two-phase agent:
      1. Plan which tables/joins are needed.
      2. Generate + execute SQL.

    Returns (sql, dataframe, reasoning_text).
    """
    schema = get_schema(con)
    schema_text = _schema_to_text(schema)

    if not schema:
        return None, None, "No CSV files found in data/. Please add your CSV files and restart."

    system_prompt = f"""You are a SQL expert and data analyst. You have access to the following tables (loaded from CSV files):

{schema_text}

When the user asks a question:
1. Briefly explain your reasoning: which tables you'll use and why.
2. Write a single DuckDB SQL query inside a ```sql ... ``` block to answer the question.
3. Do not include any explanation after the SQL block.

Use DuckDB SQL syntax. All table names are exactly as listed above."""

    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=system_prompt,
        messages=[{"role": "user", "content": question}],
    )

    response_text = response.content[0].text
    sql = _extract_sql(response_text)

    if not sql:
        return None, None, response_text

    try:
        df = run_query(con, sql)
        return sql, df, response_text
    except Exception as e:
        return sql, None, f"{response_text}\n\n**Query error:** {e}"
