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

@sleep_and_retry
@retry(wait=wait_exponential(multiplier=1.1, min=0.1, max=100))
@st.cache_data()
def get_update_count():
  conn = duckdb.connect(config.DB_FILENAME, read_only=True)
  query = "SELECT count(DISTINCT created_at) FROM pair_history"
  update_count = conn.execute(query).fetchone()[0]
  conn.close()
  return update_count

@sleep_and_retry
@retry(wait=wait_exponential(multiplier=1.1, min=0.1, max=100))
@st.cache_data(show_spinner="Fetching data...")
def get_summary_data(num_minutes):
  conn = duckdb.connect(config.DB_FILENAME, read_only=True)
  query = f"""
    WITH updates AS (
      SELECT DISTINCT created_at
      FROM pair_history
      ORDER BY created_at DESC
      LIMIT {num_minutes}
    ), cumulative_stats AS (
      SELECT h.created_at,
        p.name,
        p.pair_address,
        p.bin_step,
        p.base_fee_percentage,
        h.price,
        h.liquidity,
        h.fees,
        count(*) OVER (
          PARTITION BY p.id
          ORDER BY created_at
        ) num_minutes,
        avg(h.price) OVER (
          PARTITION BY p.id
          ORDER BY created_at
        ) avg_price,
        sum(h.fees) OVER (
          PARTITION BY p.id
          ORDER BY created_at
        ) cumulative_fees,
        avg(h.liquidity) OVER (
          PARTITION BY p.id
          ORDER BY created_at
        ) avg_liquidity,
        round(
          stddev_samp(h.liquidity) OVER (
            PARTITION BY p.id
            ORDER BY created_at
          ),
          2
        ) liquidity_std_dev,
        round(liquidity_std_dev / avg_liquidity, 2) liquidity_volatility_ratio,
        CASE
          WHEN avg_liquidity = 0 THEN 0
          ELSE 100 * cumulative_fees / (avg_liquidity + liquidity_std_dev)
        END pct_geek_fees_liquidity,
        round(
          60 * 24 * pct_geek_fees_liquidity / num_minutes,
          2
        ) pct_geek_fees_liquidity_24h,
        count(*) FILTER (fees > 0) OVER (
          PARTITION BY p.id
          ORDER BY created_at
        ) num_minutes_with_volume,
        round(100 * num_minutes_with_volume / num_minutes) pct_minutes_with_volume,
        coalesce(
          lag(h.price) OVER (
            PARTITION BY p.id
            ORDER BY created_at
          ) < h.price,
          false
        ) tick_up,
        coalesce(
          lag(h.price) OVER (
            PARTITION BY p.id
            ORDER BY created_at
          ) > h.price,
          false
        ) tick_down,
        min(h.price) OVER (
          PARTITION BY p.id
          ORDER BY created_at
        ) min_price,
        max(h.price) OVER (
          PARTITION BY p.id
          ORDER BY created_at
        ) max_price,
        round(100 * (max_price - min_price) / min_price, 2) pct_price_range,
        ceil(100 * pct_price_range / p.bin_step) bins_range,
        ceil(bins_range / 69) num_positions_range,
        100 * (max_price - h.price) / h.price pct_below_max,
        ceil(100 * pct_below_max / p.bin_step) bins_below_max,
        bins_below_max <= 7 near_max,
        stddev_samp(h.price) OVER (
          PARTITION BY p.id
          ORDER BY created_at
        ) std_dev_price,
        std_dev_price / avg_price price_volatility_ratio
      FROM pair_history h
        JOIN pairs p ON h.pair_id = p.id
      WHERE NOT p.is_blacklisted
        AND h.created_at IN (
          SELECT created_at
          from updates
        )
    )
    SELECT created_at dttm,
      name,
      pair_address,
      bin_step,
      base_fee_percentage,
      price,
      liquidity,
      fees,
      num_minutes,
      avg_price,
      cumulative_fees,
      avg_liquidity,
      liquidity_std_dev,
      liquidity_volatility_ratio,
      pct_geek_fees_liquidity,
      pct_geek_fees_liquidity_24h,
      stddev_samp(pct_geek_fees_liquidity_24h) OVER (
        PARTITION BY pair_address
        ORDER BY created_at
      ) std_dev_pct_geek_fees_liquidity_24h,
      std_dev_pct_geek_fees_liquidity_24h / pct_geek_fees_liquidity_24h pct_geek_fees_liquidity_24h_volatility_ratio,
      num_minutes_with_volume,
      pct_minutes_with_volume,
      sum(tick_up) OVER (
        PARTITION BY pair_address
        ORDER BY created_at
      ) num_tick_up,
      sum(tick_down) OVER (
        PARTITION BY pair_address
        ORDER BY created_at
      ) num_tick_down,
      round(
        100 * num_tick_up / (num_tick_up + num_tick_down)
      ) pct_tick_up,
      min_price,
      max_price,
      pct_price_range,
      bins_range,
      num_positions_range,
      pct_below_max,
      bins_below_max,
      near_max,
      std_dev_price,
      price_volatility_ratio
    FROM cumulative_stats
    WHERE
      num_minutes = {num_minutes}
  """
  summary_data = conn.execute(query).fetchdf()
  conn.close()
  return summary_data

@sleep_and_retry
@retry(wait=wait_exponential(multiplier=1.1, min=0.1, max=100))
@st.cache_data(ttl=60, show_spinner="Fetching pair details...")
def get_pair_details(pair_address, num_minutes):
  conn = duckdb.connect(config.DB_FILENAME, read_only=True)
  query = f"""
    WITH updates AS (
      SELECT DISTINCT created_at
      FROM pair_history
      ORDER BY created_at DESC
      LIMIT {num_minutes}
    ), cumulative_stats AS (
      SELECT h.created_at,
        p.name,
        p.pair_address,
        p.bin_step,
        p.base_fee_percentage,
        h.price,
        h.liquidity,
        h.fees,
        count(*) OVER (
          PARTITION BY p.id
          ORDER BY created_at
        ) num_minutes,
        avg(h.price) OVER (
          PARTITION BY p.id
          ORDER BY created_at
        ) avg_price,
        sum(h.fees) OVER (
          PARTITION BY p.id
          ORDER BY created_at
        ) cumulative_fees,
        avg(h.liquidity) OVER (
          PARTITION BY p.id
          ORDER BY created_at
        ) avg_liquidity,
        round(
          stddev_samp(h.liquidity) OVER (
            PARTITION BY p.id
            ORDER BY created_at
          ),
          2
        ) liquidity_std_dev,
        round(liquidity_std_dev / avg_liquidity, 2) liquidity_volatility_ratio,
        CASE
          WHEN avg_liquidity = 0 THEN 0
          ELSE 100 * cumulative_fees / (avg_liquidity + liquidity_std_dev)
        END pct_geek_fees_liquidity,
        round(
          60 * 24 * pct_geek_fees_liquidity / num_minutes,
          2
        ) pct_geek_fees_liquidity_24h,
        count(*) FILTER (fees > 0) OVER (
          PARTITION BY p.id
          ORDER BY created_at
        ) num_minutes_with_volume,
        round(100 * num_minutes_with_volume / num_minutes) pct_minutes_with_volume,
        coalesce(
          lag(h.price) OVER (
            PARTITION BY p.id
            ORDER BY created_at
          ) < h.price,
          false
        ) tick_up,
        coalesce(
          lag(h.price) OVER (
            PARTITION BY p.id
            ORDER BY created_at
          ) > h.price,
          false
        ) tick_down,
        min(h.price) OVER (
          PARTITION BY p.id
          ORDER BY created_at
        ) min_price,
        max(h.price) OVER (
          PARTITION BY p.id
          ORDER BY created_at
        ) max_price,
        round(100 * (max_price - min_price) / min_price, 2) pct_price_range,
        ceil(100 * pct_price_range / p.bin_step) bins_range,
        ceil(bins_range / 69) num_positions_range,
        100 * (max_price - h.price) / h.price pct_below_max,
        ceil(100 * pct_below_max / p.bin_step) bins_below_max,
        bins_below_max <= 7 near_max,
        stddev_samp(h.price) OVER (
          PARTITION BY p.id
          ORDER BY created_at
        ) std_dev_price,
        std_dev_price / avg_price price_volatility_ratio
      FROM pair_history h
        JOIN pairs p ON h.pair_id = p.id
      WHERE NOT p.is_blacklisted
        AND h.created_at IN (
          SELECT created_at
          from updates
        )
    )
    SELECT created_at dttm,
      name,
      pair_address,
      bin_step,
      base_fee_percentage,
      price,
      liquidity,
      fees,
      num_minutes,
      avg_price,
      cumulative_fees,
      avg_liquidity,
      liquidity_std_dev,
      liquidity_volatility_ratio,
      pct_geek_fees_liquidity,
      pct_geek_fees_liquidity_24h,
      stddev_samp(pct_geek_fees_liquidity_24h) OVER (
        PARTITION BY pair_address
        ORDER BY created_at
      ) std_dev_pct_geek_fees_liquidity_24h,
      std_dev_pct_geek_fees_liquidity_24h / pct_geek_fees_liquidity_24h pct_geek_fees_liquidity_24h_volatility_ratio,
      num_minutes_with_volume,
      pct_minutes_with_volume,
      sum(tick_up) OVER (
        PARTITION BY pair_address
        ORDER BY created_at
      ) num_tick_up,
      sum(tick_down) OVER (
        PARTITION BY pair_address
        ORDER BY created_at
      ) num_tick_down,
      round(
        100 * num_tick_up / (num_tick_up + num_tick_down)
      ) pct_tick_up,
      min_price,
      max_price,
      pct_price_range,
      bins_range,
      num_positions_range,
      pct_below_max,
      bins_below_max,
      near_max,
      std_dev_price,
      price_volatility_ratio
    FROM cumulative_stats
    WHERE
      pair_address = '{pair_address}'
  """
  pair_details = conn.execute(query).fetchdf()
  conn.close()
  return pair_details

def get_token_from_list(token_address, token_list):
  try:
    return [
      token_list[x] 
      for x in range(len(token_list)) 
      if token_list[x][1] == token_address
    ][0]
  except:
    return None

@sleep_and_retry
@retry(wait=wait_exponential(multiplier=1.1, min=0.1, max=100))
@st.cache_data(show_spinner="Fetching pair details...")
def get_token(pair_address):
  conn = duckdb.connect(config.DB_FILENAME, read_only=True)
  query = f"""
    SELECT 
      t.symbol, t.mint
    FROM 
      pairs p 
      JOIN tokens t ON p.mint_x_id=t.id OR p.mint_y_id=t.id 
    WHERE 
      pair_address='{pair_address}'
  """
  results = conn.execute(query).fetchall()
  mints = [result[1] for result in results]
  conn.close()
  base_token_address = [
    mint for mint in mints 
    if mint not in [
      'So11111111111111111111111111111111111111112',
      'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
      'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
    ]
  ]
  if (len(base_token_address) == 0):
    if 'So11111111111111111111111111111111111111112' in mints:
      return get_token_from_list('So11111111111111111111111111111111111111112', results)
    if 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB' in mints:
      return get_token_from_list('Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', results)
    return results[0]
  return get_token_from_list(base_token_address[0], results)

def display_pair_detail_chart(pair_details):
    # Ensure 'dttm' is in datetime format
    pair_details['dttm'] = pd.to_datetime(pair_details['dttm'])

    # Melt the DataFrame to combine the metrics into one column
    melted_pair_details = pair_details.melt(
        id_vars=['dttm'],
        value_vars=['pct_geek_fees_liquidity_24h', 'fees'],
        var_name='Legend',
        value_name='Value'
    )

    # Map metric names to more readable labels
    metric_names = {
        'pct_geek_fees_liquidity_24h': 'Avg Geek 24h Fee / TVL',
        'fees': 'Fees'
    }
    melted_pair_details['Legend'] = melted_pair_details['Legend'].map(metric_names)  

    # Create a formatted time column in HH:MM AM/PM format
    melted_pair_details['Time'] = melted_pair_details['dttm'].dt.strftime('%I:%M %p')

    # Define the color scale to ensure consistent colors
    color_scale = alt.Scale(
        domain=['Avg Geek 24h Fee / TVL', 'Fees'],
        range=['red', 'steelblue']
    )

    # Create individual charts for each metric
    line_chart = alt.Chart(
        melted_pair_details[melted_pair_details['Legend'] == 'Avg Geek 24h Fee / TVL']
    ).mark_line(strokeWidth=3.5).encode(
        x=alt.X('dttm:T', title='Time'),
        y=alt.Y('Value:Q', title='Avg Geek 24h Fee / TVL'),
        color=alt.Color('Legend:N', scale=color_scale, legend=alt.Legend(orient='top', title=None)),
        tooltip=[
            alt.Tooltip('Time:N', title='Time'),
            alt.Tooltip('Value:Q', title='Avg Geek 24h Fee / TVL')
        ]
    )

    bar_chart = alt.Chart(
        melted_pair_details[melted_pair_details['Legend'] == 'Fees']
    ).mark_bar(opacity=0.50).encode(
        x=alt.X('dttm:T', title='Time'),
        y=alt.Y('Value:Q', title='Fees'),
        color=alt.Color('Legend:N', scale=color_scale, legend=alt.Legend(orient='top', title=None)),
        tooltip=[
            alt.Tooltip('Time:N', title='Time'),
            alt.Tooltip('Value:Q', title='Fees')
        ]
    )

    # Layer the charts and resolve the scales
    combined_chart = alt.layer(line_chart, bar_chart).resolve_scale(
        y='independent'
    ).properties(
        width='container',
        height=400,
        title='Fees and Avg Geek 24h Fee / TVL Over Time'
    )

    # Display the chart in Streamlit
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
  index = index[len(options)-1] if len(options) < 4 else 2
  with left_column:
    num_minutes = st.selectbox("Analysis Timeframe", options=options, format_func=lambda x: options_labels[x], index=index)
    return num_minutes
  
def get_selected_pair_address(selected_row):
  try:
    if selected_row == None:
      return None
  except:
    pair_address = selected_row["pair_address"].iloc[0]
    return pair_address
  
def refresh_data():
  st.cache_data.clear()
  get_update_count()
  for num_minutes in [5, 15, 30, 60]:
    get_summary_data(num_minutes)
  st.rerun()

if "data_refreshed" not in st.session_state:
    st.session_state["data_refreshed"] = True
    refresh_data()

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
      "pct_minutes_with_volume": {
        "filterType": "number",
        "type": "greaterThan",
        "filter": 50,
      },      
      "pct_geek_fees_liquidity_24h": {
        "filterType": "number",
        "type": "greaterThan",
        "filter": 0,
      },      
    },
  }

  left_column, right_column = st.columns([1, 1])
  grid_table = None

  with left_column:
    st.write("Select a row to view Geek 24h Fee / TVL Chart")
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
          "wrapHeaderText": True,
          "autoHeaderHeight": True,
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
            "maxWidth": 75,
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
            "maxWidth": 100,
            "field": "base_fee_percentage", 
            "type": ["numericColumn", "numberColumnFilter", "customNumericFormat"], 
            "precision": 2,
            "filterParams": {
              "defaultOption": "greaterThanOrEqual",
              "buttons": ["apply", "reset"],
              "closeOnApply": True,
            }
          },
          { 
            "headerName": "% Minutes w/ Volume", 
            "maxWidth": 100,
            "field": "pct_minutes_with_volume", 
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
            "maxWidth": 120,
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
    if pair_address != None:
      pair = data[data["pair_address"] == pair_address].iloc[0]
      name = pair["name"]
      bin_step = pair["bin_step"]
      base_fee_percentage = round(float(pair["base_fee_percentage"]), 2)
      token = get_token(pair_address)
      detail_df = get_pair_details(pair_address, num_minutes)
      st.markdown(f"""
          <a href="https://app.meteora.ag/dlmm/{pair_address}" target="_blank">{name} {bin_step} Bin Step, {base_fee_percentage}% Base Fee</a>
          <a href="https://gmgn.ai/sol/token/{token[1]}" target="_blank">
            <img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSsrw95DKwTe5yvTSSL0AumcHhLZsiR9SP_1jtO6-_PQltuwbWDWilFY9vv&s" alt="GMGN Link to Token" style="width:20px;height:20px;">
          </a>
        """, 
        unsafe_allow_html=True
      )
      display_pair_detail_chart(detail_df)

  # Show last update time
  last_update_time = data['dttm'].max()
  time_diff = pd.Timestamp.now() - last_update_time
  minutes_ago = int(time_diff.total_seconds() // 60)

st.write(f"Collected {update_count} minutes of data, updated {minutes_ago} minute{'s' if minutes_ago != 1 else ''} ago")
if (minutes_ago > 0):
  if st.button("Refresh data"):
    refresh_data()
