from arete.extract.plaid import extract_plaid
from arete.extract.fitbit import extract_fitbit
from arete.extract.splitwise import extract_splitwise
from arete.extract.strava import extract_strava
from arete.transform.transactions import transform_transactions
from arete.transform.aggregate_periods import aggregate_periods
import inflection
import logging as log
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from datetime import datetime


log.getLogger().setLevel(log.INFO)

# External
STRAVA_START_DATE = "2021-10-01"
# Plaid's agreement with Capital One only permits downloading the last 90 days of authorization by the user
PLAID_START_DATE = datetime(2022, 3, 16).date()
END_DATE = datetime.now().date()
START_DATE = END_DATE - pd.DateOffset(weeks=12)
LAST_WEEK = END_DATE - pd.DateOffset(weeks=2)
TRAILING_7_DAYS = END_DATE - pd.DateOffset(weeks=1)
FONTSIZE = 10


def line_chart(df, x, y):
    fig, ax = plt.subplots()
    sns.lineplot(x=x, y=y, data=df, ax=ax, color="black")
    ax.set_xlabel(x)
    ax.set_ylabel(y)
    ax.set_xticklabels(ax.get_xticks(), rotation=90)
    myFmt = mdates.DateFormatter("%Y-%m-%d")
    ax.xaxis.set_major_formatter(myFmt)
    ax.grid(which="major", axis="y", linestyle="-")
    sns.despine(left=True)
    return fig


def stacked_bar_chart(df):
    "Expects `df` to have the time series as the index"
    fig, ax = plt.subplots()
    plt.rcParams.update({"font.size": FONTSIZE})
    df.plot(kind="bar", xlabel="week", ylabel="count", stacked=True, ax=ax)
    hatches = iter(["///", "..", "x", "o.", "--", "O", "\\", "....", "+"])
    mapping = {}
    for bar in ax.patches:
        color = bar.get_facecolor()
        if color not in mapping.keys():
            mapping[color] = next(hatches)
        bar.set_hatch(mapping[color])
        bar.set_facecolor("black")
        bar.set_edgecolor("white")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.275), ncol=4, frameon=False)
    sns.despine(left=True)
    ax.grid(which="major", axis="y", linestyle="-")
    return fig


def run_etl():
    extract_strava(extract_data_since=STRAVA_START_DATE)
    extract_fitbit(
        resources=["activities/heart"],
        start_date=pd.to_datetime(STRAVA_START_DATE),
        end_date=END_DATE,
    )
    extract_plaid(start_date=PLAID_START_DATE, end_date=END_DATE)
    extract_splitwise(start_date=PLAID_START_DATE, end_date=END_DATE)
    transform_transactions()
    aggregate_periods()
    print("ran etl")


def main():
    df = pd.read_csv("data/processed/aggregate_periods.csv")
    for col in ["date", "week", "month"]:
        df[col] = pd.to_datetime(df[col]).dt.date

    if not max(df["date"]) >= END_DATE:
        run_etl()

    print(df.tail())
    st.title("🏔 Review")
    sns.set_style("white")

    adventure, heart_rate, spending = st.columns(3)
    with adventure:
        goal = 3
        run_count = df.loc[df["date"] >= TRAILING_7_DAYS, "run"].sum()
        st.metric(
            label="Runs", value=run_count, delta=run_count - goal,  # Goal
        )
    with heart_rate:
        goal = 50
        lowest_heart_rate = df.loc[
            df["date"] >= TRAILING_7_DAYS, "resting_heart_rate"
        ].min()
        st.metric(
            label="Heart Rate",
            value=lowest_heart_rate,
            delta=lowest_heart_rate - goal,  # Goal
        )
    with spending:
        goal = 750
        variable_spending = df.loc[
            df["date"] >= TRAILING_7_DAYS, "variable_spending"
        ].sum()
        st.metric(
            label="Variable Spending",
            value=variable_spending,
            delta=variable_spending - goal,  # Goal
        )
    st.write()

    adventure_chart, health_chart, spending_chart, test_chart = st.tabs(
        ["🧗 Adventure", "♥️ Health", "💸 Spending", "Test"]
    )
    within_timeframe = pd.to_datetime(df["date"]).between(
        pd.to_datetime(START_DATE), pd.to_datetime(END_DATE)
    )
    with adventure_chart:
        cols = [
            "week",
            "alpine_ski",
            "backcountry_ski",
            "hike",
            "ride",
            "rock_climbing",
            "run",
            "snowboard",
            "weight_training",
        ]
        agg = df.loc[within_timeframe, cols].groupby("week").sum()
        fig = stacked_bar_chart(agg)
        st.pyplot(fig)

    with health_chart:
        agg = df.loc[within_timeframe, ["date", "resting_heart_rate"]]
        fig = line_chart(agg, "date", "resting_heart_rate")
        st.pyplot(fig)

    with spending_chart:
        agg = (
            df.loc[within_timeframe, ["week", "variable_spending"]]
            .groupby("week", as_index=False)
            .sum()
        )
        fig = line_chart(agg, "week", "variable_spending")
        st.pyplot(fig)
    with test_chart:
        agg = df.melt(
            id_vars="date", value_vars=["resting_heart_rate", "variable_spending"]
        )
        import altair as alt

        fig = (
            alt.Chart(agg)
            .mark_line()
            .encode(
                x=alt.X("date", scale=alt.Scale()),
                y=alt.Y("value", scale=alt.Scale(zero=False)),
                row="variable",
                # color='black',
            )
            .resolve_scale(y="independent")
        )

        st.altair_chart(fig, use_container_width=True)

        # How does spending react to events?
        # How does heart rate react to events?
        # Is there a way to pull in adventure?
        pass

    st.write()


if __name__ == "__main__":
    main()
