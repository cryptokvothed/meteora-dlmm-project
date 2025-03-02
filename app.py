import logging
import duckdb
import streamlit as st
import pandas as pd
from meteora_project import config
from ratelimit import sleep_and_retry
from tenacity import retry, wait_exponential
from st_aggrid import AgGrid, GridUpdateMode
import altair as alt

# Setup so the table is displayed in full width
st.set_page_config(layout="wide")
st.title("DLMM Opportunities")

# Database connection
# Configure logging
logging.basicConfig(level=config.LOG_LEVEL)
logging.getLogger('streamlit').setLevel(logging.WARNING)
logging.getLogger('streamlit.runtime').setLevel(logging.WARNING)
logging.getLogger('streamlit.server').setLevel(logging.WARNING)
logging.getLogger('watchdog.observers.inotify_buffer').setLevel(logging.WARNING)

# Get the number of updates made
@sleep_and_retry
@retry(wait=wait_exponential(multiplier=1.1, min=0.1, max=100))
@st.cache_data(ttl=60)
def get_update_count():
  conn = duckdb.connect(config.DB_FILENAME, read_only=True)
  query = "SELECT count(DISTINCT created_at) FROM pair_history"
  update_count = conn.execute(query).fetchone()[0]
  conn.close()
  return update_count

# Get the summary data for the specified # of minutes
@sleep_and_retry
@retry(wait=wait_exponential(multiplier=1.1, min=0.1, max=100))
@st.cache_data(ttl=60, show_spinner="Fetching data...")
def get_summary_data(num_minutes):
  conn = duckdb.connect(config.DB_FILENAME, read_only=True)
  query = f"SELECT * FROM v_pair_history where num_minutes={num_minutes}"
  summary_data = conn.execute(query).fetchdf()
  conn.close()
  return summary_data

@sleep_and_retry
@retry(wait=wait_exponential(multiplier=1.1, min=0.1, max=100))
@st.cache_data(ttl=60, show_spinner="Fetching pair details...")
def get_pair_details(pair_address, num_minutes):
  conn = duckdb.connect(config.DB_FILENAME, read_only=True)
  query = f"SELECT * FROM v_pair_history WHERE pair_address='{pair_address}' and num_minutes<={num_minutes}"
  pair_details = conn.execute(query).fetchdf()
  conn.close()
  return pair_details

def display_pair_detail_chart(pair_details):
  line_chart = alt.Chart(pair_details).mark_line().encode(
    x=alt.X('dttm:T', title='Time'),
    y=alt.Y('pct_geek_fees_liquidity_24h:Q', title='Cumulative Geek 24h Fee / TVL')
  )

  bar_chart = alt.Chart(pair_details).mark_bar().encode(
    x=alt.X('dttm:T', title='Time'),
    y=alt.Y('fees:Q', title='Fees')
  )

  combined_chart = alt.layer(line_chart, bar_chart).resolve_scale(
    y='independent'
  ).properties(
    width='container',
    height=400
  )

  st.altair_chart(combined_chart, use_container_width=True)

def display_num_minutes_selectbox(update_count):
  options=[x for x in [5, 15, 30, 60] if x <= update_count]
  options_labels = {
    5: "5 minutes", 
    15: "15 minutes", 
    30: "30 minutes", 
    60: "1 hour",
  }
  left_column, _ = st.columns([1, 4])
  with left_column:
    num_minutes = st.selectbox("Analysis Timeframe", options=options, format_func=lambda x: options_labels[x], index=0)
    return num_minutes
  
def get_selected_pair_address(selected_row):
  try:
    if selected_row == None:
      return None
  except:
    pair_address = selected_row["pair_address"].iloc[0]
    return pair_address

update_count = get_update_count()
if update_count < 5:
  st.write(f"Only {update_count} minutes of data collected.")
  st.write("Page will display when at least 5 minutes of data is collected.")
else:
  num_minutes = display_num_minutes_selectbox(update_count)
  data = get_summary_data(num_minutes)

  default_filters = {
    "filterModel": {
      "liquidity": {
        "filterType": "number",
        "type": "greaterThanOrEqual",
        "filter": 1000,
      },
      "pct_geek_fees_liquidity_24h": {
        "filterType": "number",
        "type": "greaterThanOrEqual",
        "filter": 20,
      }
    },
  }

  left_column, right_column = st.columns([1, 1])
  grid_table = None

  with left_column:
    grid_table = AgGrid(
      data, 
      update_mode=GridUpdateMode.SELECTION_CHANGED, 
      reload_data=True, 
      gridOptions={
        "suppressCellFocus": True,
        "sideBar": {
          "toolPanels": [{
            "id": "filters",
              "labelDefault": "Filters",
              "labelKey": "filters",
              "iconKey": "filter",
              "toolPanel": "agFiltersToolPanel",
            "hiddenByDefault": True,
          }],
        },
        "rowSelection": {
           "mode": "singleRow",
           "checkboxes": False,
           "enableClickSelection": True,
        },
        "initialState": {
          "filter": default_filters,
        },
        "defaultColDef": { 
          "resizable": True, 
          "sortable": True, 
          "suppressHeaderMenuButton": True, 
          "filter": True,
        },
        "autoSizeStrategy": {"type": "fitCellContents", "skipHeader": False}, 
        "columnDefs": [
          { 
            "headerName": "Pair Name", 
            "field": "name",
            "filterParams": {
              "buttons": ["apply", "reset"],
              "closeOnApply": True,
            }
          },
          { 
            "headerName": "Bin Step", 
            "field": "bin_step", 
            "type": ["numericColumn", "numberColumnFilter", "customNumericFormat"], 
            "precision": 0,
            "filterParams": {
              "defaultOption": "greaterThanOrEqual",
              "buttons": ["apply", "reset"],
              "closeOnApply": True,
            }
          },
          { 
            "headerName": "Base Fee Percentage", 
            "field": "base_fee_percentage", 
            "type": ["numericColumn", "numberColumnFilter", "customNumericFormat"], 
            "precision": 0,
            "filterParams": {
              "defaultOption": "greaterThanOrEqual",
              "buttons": ["apply", "reset"],
              "closeOnApply": True,
            }
          },
          { 
            "headerName": "Liquidity", 
            "field": "liquidity", 
            "type": ["numericColumn", "numberColumnFilter", "customNumericFormat"], 
            "precision": 2,
            "filterParams": {
              "defaultOption": "lessThan",
              "buttons": ["apply", "reset"],
              "closeOnApply": True,
            }
          },
          { 
            "headerName": "Geek 24h Fee / TVL", 
            "field": "pct_geek_fees_liquidity_24h", 
            "sort": "desc",
            "type": ["numericColumn", "numberColumnFilter", "customNumericFormat"], 
            "precision": 2,
            "filterParams": {
              "defaultOption": "greaterThanOrEqual",
              "maxNumConditions": 1,
              "buttons": ["apply", "reset"],
              "closeOnApply": True,
            }
          },        
      ] 
    })
  
  with right_column:
    pair_address = get_selected_pair_address(grid_table["selected_rows"])
    if pair_address == None:
      st.write("Select a row to view details")
    else:
      name = data[data["pair_address"] == pair_address]["name"].iloc[0]
      st.write(name)
      detail_df = get_pair_details(pair_address, num_minutes)
      display_pair_detail_chart(detail_df)

  # Show last update time
  last_update = data['dttm'].max().strftime("%Y-%m-%d %I:%M:%S %p")
  st.write(f"Collected {update_count} minutes of data, last Update: {last_update}")

