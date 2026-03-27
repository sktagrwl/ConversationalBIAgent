import streamlit as st
from src.database import get_connection
from src.agent import answer_question
from src.visualization import suggest_chart

st.set_page_config(page_title="Conversational BI Agent", layout="wide")
st.title("Conversational BI Agent")

# Initialize DB connection once per session
if "con" not in st.session_state:
    st.session_state.con = get_connection()

if "messages" not in st.session_state:
    st.session_state.messages = []

# Sidebar: show loaded tables
with st.sidebar:
    st.header("Loaded Tables")
    from src.database import get_schema
    schema = get_schema(st.session_state.con)
    if schema:
        for table, cols in schema.items():
            with st.expander(table):
                for c in cols:
                    st.text(f"{c['column']}  ({c['type']})")
    else:
        st.info("No CSV files found in data/.\nAdd your CSVs and restart.")

# Chat history
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg.get("df") is not None:
            fig = suggest_chart(msg["df"], msg["content"])
            if fig:
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.dataframe(msg["df"], use_container_width=True)

# Input
if prompt := st.chat_input("Ask a question about your data..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            sql, df, reasoning = answer_question(prompt, st.session_state.con)

        if sql:
            with st.expander("SQL"):
                st.code(sql, language="sql")

        st.markdown(reasoning.split("```")[0].strip())  # Show reasoning before the SQL block

        if df is not None and not df.empty:
            fig = suggest_chart(df, prompt)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.dataframe(df, use_container_width=True)

        st.session_state.messages.append({"role": "assistant", "content": reasoning, "df": df})
