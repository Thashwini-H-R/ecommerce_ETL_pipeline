"""Simple Streamlit dashboard that queries the FastAPI endpoints and shows basic trends.

Run with:
  pip install streamlit requests pandas
  streamlit run dashboard/streamlit_app.py
"""
from __future__ import annotations

import os
import requests
import pandas as pd
import streamlit as st

API_URL = os.environ.get("API_URL", "http://localhost:8000")

st.title("E-commerce Warehouse Dashboard")

st.sidebar.header("Filters")
start = st.sidebar.date_input("Start date")
end = st.sidebar.date_input("End date")

st.header("Orders per day")
resp = requests.get(f"{API_URL}/metrics/orders_per_day", params={"start": start.isoformat(), "end": end.isoformat()})
if resp.status_code == 200:
    df = pd.DataFrame(resp.json())
    if not df.empty:
        df["day"] = pd.to_datetime(df["day"])
        st.line_chart(df.set_index("day")["orders"])
    else:
        st.write("No orders in range")
else:
    st.error(f"API error: {resp.status_code} {resp.text}")

st.header("Top customers")
resp2 = requests.get(f"{API_URL}/customers", params={"limit": 50})
if resp2.status_code == 200:
    cdf = pd.DataFrame(resp2.json())
    if not cdf.empty:
        cdf["total_lifetime_value"] = pd.to_numeric(cdf["total_lifetime_value"], errors="coerce").fillna(0)
        top = cdf.sort_values("total_lifetime_value", ascending=False).head(10)
        st.table(top[["customer_id", "email", "total_lifetime_value"]])
    else:
        st.write("No customers found")
else:
    st.error(f"API error: {resp2.status_code} {resp2.text}")
