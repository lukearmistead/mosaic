import streamlit as st
from os import listdir
from os.path import isfile, join
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import matplotlib.dates as mdates
import datetime
import altair as alt

PATH = '../data/fitbit/'
heart_files = [f for f in listdir(PATH) if isfile(join(PATH, f)) and 'heart' in f]

# Get DataFrame
dfs = []
for heart in heart_files:
 dfs.append(pd.read_csv(PATH + heart))
df = pd.concat(dfs)
df['dateTime'] = [datetime.datetime.strptime(date, '%Y-%m-%d') for date in df['dateTime']]


# Dashboard
st.title('Fitness Dashboard')

c = alt.Chart(df).mark_line().encode(
    x='dateTime',
    y='restingHeartRate'
)

st.altair_chart(c)


fig = plt.figure()
ax1 = plt.subplot(2,1,1)
ax2 = plt.subplot(2,1,2, sharex=ax1)
dateform = DateFormatter('%B')
ax1.plot(df['dateTime'],df['restingHeartRate'])
ax2.plot(df['dateTime'],df['Fat Burn'])
ax1.xaxis.set_major_formatter(dateform)
ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=1))

st.pyplot(fig)
