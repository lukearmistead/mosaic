from arete.extract.plaid import extract_plaid
from arete.extract.fitbit import extract_fitbit
from arete.extract.splitwise import extract_splitwise
from arete.extract.strava import extract_strava
from arete.transform.transactions import transform_transactions
from arete.transform.aggregate_periods import aggregate_periods
import altair as alt
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
SEASON_START = datetime(2022, 11, 1).date()
SEASON_END = datetime(2023, 6, 1).date()
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


def format_thousands(value):
    return f"{value:,.0f}"


def main():
    df = pd.read_csv("data/processed/aggregate_periods.csv")
    for col in ["date", "week", "month"]:
        df[col] = pd.to_datetime(df[col]).dt.date

    if not max(df["date"]) >= END_DATE:
        run_etl()

    st.title("🏔 Review")

    goals = st.columns(2)
    with goals[0]:
        shasta = st.checkbox("Tour Shasta")
    with goals[1]:
        home = st.checkbox("Buy home")
    st.write()

    skiing, spending, health = st.tabs(["Skiing", "Spending", "Health"])

    with skiing:
        ski = pd.read_csv("data/strava/activities.csv")
        ski["date"] = pd.to_datetime(ski["start_date_local"]).dt.date
        ski["session_count"] = 1
        ski = (
            ski.loc[ski["date"].between(SEASON_START, SEASON_END),]
            .loc[ski["type"].isin(["BackcountrySki", "AlpineSki", "Snowboard"]),]
            .loc[
                :,
                [
                    "id",
                    "date",
                    "type",
                    "total_elevation_gain",
                    "session_count",
                    "max_speed",
                ],
            ]
            .sort_values("date")
        )

        metrics = st.columns(5)
        tour_count = ski.loc[ski["type"] == "BackcountrySki", "id"].count()
        alpine_vert = ski.loc[ski["type"] == "AlpineSki", "total_elevation_gain"].sum()
        alpine_count = ski.loc[ski["type"] == "AlpineSki", "id"].count()
        print(alpine_vert, alpine_count)

        with metrics[0]:
            st.metric(
                label="Tour Sessions", value=format_thousands(tour_count), delta=None
            )
        with metrics[1]:
            st.metric(
                label="Max Speed (mps)",
                value=format_thousands(ski["max_speed"].max()),
                delta=None,
            )
        with metrics[2]:
            st.metric(
                label="Session Count",
                value=format_thousands(ski["id"].count()),
                delta=None,
            )
        with metrics[3]:
            st.metric(
                label="Elevation Gain (m)",
                value=format_thousands(ski["total_elevation_gain"].sum()),
                delta=None,
            )
        with metrics[4]:
            st.metric(
                label="Elevation Gain per Alpine Session",
                value=format_thousands(alpine_vert / alpine_count),
                delta=None,
            )
        st.write()

        ski = ski.groupby("date", as_index=False).agg(
            {"total_elevation_gain": "sum", "session_count": "sum", "max_speed": "max"}
        )
        ski["season_elevation_gain"] = ski["total_elevation_gain"].cumsum().astype(int)

        base = alt.Chart(ski).encode(x="date")
        line = base.mark_line().encode(y="season_elevation_gain")
        bar = base.mark_bar(color="gray", opacity=0.7).encode(y="max_speed")
        chart = alt.layer(line, bar).resolve_scale(y="independent")
        st.altair_chart(chart, use_container_width=True)

    with spending:
        metrics = st.columns(3)
        with metrics[2]:
            variable_spending = df.loc[
                df["date"] >= TRAILING_7_DAYS, "variable_spending"
            ].sum()
            st.metric(
                label="Weekly Variable Spend",
                value="$" + format_thousands(variable_spending),
                delta=None,
            )

        agg = (
            df.loc[
                df["week"].between(START_DATE, END_DATE), ["week", "variable_spending"]
            ]
            .groupby("week", as_index=False)
            .sum()
        )
        st.altair_chart(
            alt.Chart(agg)
            .mark_bar()
            .encode(
                x=alt.X("week", scale=alt.Scale()),
                y=alt.Y("variable_spending", scale=alt.Scale(zero=False)),
            ),
            use_container_width=True,
        )
        spend = pd.read_csv("data/processed/financial_transactions.csv")
        spend["date"] = pd.to_datetime(spend["date"]).dt.date
        spend = spend.loc[
            spend["date"].between(START_DATE, END_DATE),
            ["date", "name", "category", "amount"],
        ]
        spend["amount"] = spend["amount"].astype(int)
        st.table(spend.head(50))
    with health:
        st.altair_chart(
            alt.Chart(df)
            .mark_line()
            .encode(
                x=alt.X("date", scale=alt.Scale()),
                y=alt.Y("resting_heart_rate", scale=alt.Scale(zero=False)),
            ),
            use_container_width=True,
        )


if __name__ == "__main__":
    main()
