import streamlit as st
from src.database import get_connection, get_schema
from src.agent import answer_question
from src.visualization import render_chart
from src.config import MAX_ROWS

st.set_page_config(page_title="Conversational BI Agent", layout="wide")
st.title("Conversational BI Agent")

# Initialize DB connection once per session.
# On first run against new CSVs, get_connection() materializes them (slow once).
if "con" not in st.session_state:
    with st.status("Connecting to database...", expanded=True) as status:
        st.write("Checking for new CSV files to materialize...")
        st.session_state.con = get_connection()
        status.update(label="Database ready.", state="complete", expanded=False)

if "messages" not in st.session_state:
    st.session_state.messages = []

if "schema" not in st.session_state:
    st.session_state.schema = get_schema(st.session_state.con)

# Sidebar: show loaded tables with row counts
with st.sidebar:
    st.header("Loaded Tables")
    schema = st.session_state.schema
    if schema:
        for table, info in schema.items():
            label = f"{table} ({info['row_count']:,} rows)"
            with st.expander(label):
                for c in info["columns"]:
                    st.text(f"{c['column']}  ({c['type']})")
    else:
        st.info("No CSV files found in data/.\nAdd your CSVs and restart.")

# Chat history
for i, msg in enumerate(st.session_state.messages):
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        df = msg.get("df")
        if df is not None:
            if msg.get("truncated"):
                st.warning(
                    f"Results truncated to {MAX_ROWS:,} rows. "
                    "Add filters or aggregation to reduce the result set."
                )
            fig = render_chart(df, msg.get("chart_type", "table"))
            if fig:
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.dataframe(df, use_container_width=True)
            if msg.get("insight"):
                st.markdown(f"*{msg['insight']}*")
            st.download_button(
                label="Download CSV",
                data=df.to_csv(index=False).encode("utf-8"),
                file_name="query_results.csv",
                mime="text/csv",
                key=f"download_{i}",
            )

# Input
if prompt := st.chat_input("Ask a question about your data..."):
    # Build history BEFORE appending the current question — prevents duplicate messages in LLM context
    history = [
        {"role": m["role"], "content": m["content"]}
        for m in st.session_state.messages
    ]
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            sql, df, truncated, reasoning, chart_type, insight, _ = answer_question(
                prompt, st.session_state.con, history=history
            )

        if sql:
            with st.expander("SQL"):
                st.code(sql, language="sql")

        st.markdown(reasoning)

        if df is not None:
            if not df.empty:
                if truncated:
                    st.warning(
                        f"Results truncated to {MAX_ROWS:,} rows. "
                        "Add filters or aggregation to reduce the result set."
                    )
                fig = render_chart(df, chart_type)
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.dataframe(df, use_container_width=True)
                if insight:
                    st.markdown(f"*{insight}*")
                st.download_button(
                    label="Download CSV",
                    data=df.to_csv(index=False).encode("utf-8"),
                    file_name="query_results.csv",
                    mime="text/csv",
                )
            else:
                st.info("Query returned no results.")

        st.session_state.messages.append({
            "role": "assistant",
            "content": reasoning,
            "df": df,
            "truncated": truncated,
            "chart_type": chart_type,
            "insight": insight,
        })
