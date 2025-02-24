# streamlit_app/app.py

import sys
import os

# Dynamically add the project root to Python's search path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

# import sys
# print("Updated Python Path:")
# for p in sys.path:
#     print(p)

import streamlit as st
import duckdb
import pandas as pd
import altair as alt
from meteora_project import config

# Connect to DuckDB
conn = duckdb.connect(config.DB_FILENAME)

# Load Tables from DuckDB
def load_table(conn, table_name):
    try:
        return conn.execute(f"SELECT * FROM {table_name}").fetchdf()
    except Exception as e:
        st.error(f"Error loading table '{table_name}': {e}")
        return pd.DataFrame()

# Load DataFrames
df_tokens = load_table(conn, "tokens")
df_pairs = load_table(conn, "pairs")
df_history = load_table(conn, "pair_history")

# Convert 'created_at' column to datetime for time-series plotting
if not df_history.empty:
    df_history['created_at'] = pd.to_datetime(df_history['created_at'])

# App Title
st.title("📈 Interactive Pair Fees Over Time (Sorted by Cumulative Fees)")

# Check if 'cumulative_fee_volume' column exists and sort the dropdown
if not df_pairs.empty:
    if 'cumulative_fee_volume' in df_pairs.columns:
        # Sort the pairs by cumulative_fee_volume descending
        df_pairs_sorted = df_pairs.sort_values(by='cumulative_fee_volume', ascending=False)
    else:
        st.warning("'cumulative_fee_volume' column not found in the 'pairs' table.")
        df_pairs_sorted = df_pairs

    # Create Dropdown with Sorted Pairs
    pair_names = df_pairs_sorted['name'].tolist()
    selected_pair_name = st.selectbox("Select a Pair", pair_names)

    # Fetch corresponding pair ID
    selected_pair_id = df_pairs_sorted[df_pairs_sorted['name'] == selected_pair_name]['id'].iloc[0]

    # Filter History Data for the Selected Pair
    df_filtered = df_history[df_history['pair_id'] == selected_pair_id]

    if df_filtered.empty:
        st.warning(f"No historical data available for {selected_pair_name}.")
    else:
        # Time-Series Graph for Fees Over Time
        st.subheader(f"💰 Fees Over Time for {selected_pair_name}")

        fee_chart = (
            alt.Chart(df_filtered)
            .mark_line(point=True)
            .encode(
                x=alt.X("created_at:T", title="Time"),
                y=alt.Y("fees:Q", title="Fees"),
                tooltip=["created_at:T", "fees:Q"]
            )
            .interactive()
            .properties(width=800, height=400)
        )

        st.altair_chart(fee_chart, use_container_width=True)

        # Show Raw Data (Optional)
        with st.expander("🔍 View Raw Data"):
            st.dataframe(df_filtered)
else:
    st.warning("No pairs found in the database.")