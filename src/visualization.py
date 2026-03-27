import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def suggest_chart(df: pd.DataFrame, question: str) -> go.Figure | None:
    """
    Heuristically pick a chart type based on DataFrame shape and question keywords.
    Returns a Plotly figure or None if a table view is more appropriate.
    """
    if df is None or df.empty or len(df.columns) < 2:
        return None

    q = question.lower()
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    non_numeric_cols = df.select_dtypes(exclude="number").columns.tolist()

    # Time-series: line chart
    if any(k in q for k in ["trend", "over time", "monthly", "weekly", "daily", "by month", "by year"]):
        if non_numeric_cols and numeric_cols:
            return px.line(df, x=non_numeric_cols[0], y=numeric_cols[0], title=question)

    # Distribution / comparison: bar chart
    if any(k in q for k in ["top", "most", "least", "compare", "by category", "per", "breakdown", "distribution"]):
        if non_numeric_cols and numeric_cols:
            return px.bar(df, x=non_numeric_cols[0], y=numeric_cols[0], title=question)

    # Proportion: pie chart
    if any(k in q for k in ["share", "proportion", "percentage", "pie"]):
        if non_numeric_cols and numeric_cols:
            return px.pie(df, names=non_numeric_cols[0], values=numeric_cols[0], title=question)

    # Correlation: scatter
    if any(k in q for k in ["correlation", "vs", "versus", "relationship"]):
        if len(numeric_cols) >= 2:
            return px.scatter(df, x=numeric_cols[0], y=numeric_cols[1], title=question)

    # Default: bar if shape is right, else None (show as table)
    if non_numeric_cols and numeric_cols and len(df) <= 50:
        return px.bar(df, x=non_numeric_cols[0], y=numeric_cols[0], title=question)

    return None
