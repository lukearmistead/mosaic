import streamlit as st
import numpy as np
from datetime import datetime, timedelta
import pandas as pd


END_DATE = datetime.now().date()
START_DATE = END_DATE - timedelta(
    days=100
)  # Fitbit doesn't like if we request more than 100 days. Meh. Fine.

FITBIT_DATA_PATH = "data/fitbit_data.csv"

st.title("My first app")

df = pd.read_csv(FITBIT_DATA_PATH)
st.write()
