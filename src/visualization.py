import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def render_chart(df: pd.DataFrame, chart_type: str) -> go.Figure | None:
    """
    Map LLM-chosen chart_type → Plotly figure.

    The LLM decides chart_type during the planning phase (before data exists),
    so this function only maps the decision to a figure — no heuristics, no data inspection.
    Returns None → caller falls back to st.dataframe.
    """
    if df is None or df.empty:
        return None

    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    non_numeric_cols = df.select_dtypes(exclude="number").columns.tolist()

    x = non_numeric_cols[0] if non_numeric_cols else (numeric_cols[0] if len(numeric_cols) > 1 else None)
    y = numeric_cols[0] if numeric_cols else None

    if chart_type == "bar" and x and y:
        return px.bar(df, x=x, y=y)

    if chart_type == "line" and x and y:
        return px.line(df, x=x, y=y)

    if chart_type == "scatter" and len(numeric_cols) >= 2:
        return px.scatter(df, x=numeric_cols[0], y=numeric_cols[1])

    if chart_type == "pie" and x and y:
        return px.pie(df, names=x, values=y)

    # "table" or unrecognised type — caller renders st.dataframe
    return None
