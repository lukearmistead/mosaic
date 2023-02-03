from arete.etl import run_etl
from arete.utils import convert_string_to_date, lookup_yaml
import altair as alt
import logging as getLogger
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import datetime


getLogger.getLogger().setLevel(getLogger.INFO)


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
    plt.rcParams.update({"font.size": 10})
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


def format_thousands(value):
    return f"{value:,.0f}"


def main():
    config = lookup_yaml("etl_config.yml")
    df = pd.read_csv(config["transform"]["aggregate"]["output_path"])
    for col in ["date", "week", "month"]:
        df[col] = pd.to_datetime(df[col]).dt.date
    if not max(df["date"]) >= datetime.date.today():
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
        ski = pd.read_csv(config["transform"]["skis"]["output_path"])
        metrics = st.columns(5)
        tour_count = ski.loc[ski["type"] == "backcountry_ski", "id"].count()
        alpine_vert = ski.loc[ski["type"] == "alpine_ski", "total_elevation_gain"].sum()
        alpine_count = ski.loc[ski["type"] == "alpine_ski", "id"].count()

        with metrics[0]:
            st.metric(
                label="Tour Sessions", value=format_thousands(tour_count), delta=None
            )
        with metrics[1]:
            st.metric(
                label="Max Speed",
                value=format_thousands(ski["max_speed"].max()),
                delta=None,
            )
        with metrics[2]:
            st.metric(
                label="Days", value=format_thousands(ski["date"].nunique()), delta=None,
            )
        with metrics[3]:
            st.metric(
                label="Elevation",
                value=format_thousands(ski["total_elevation_gain"].sum()),
                delta=None,
            )
        with metrics[4]:
            st.metric(
                label="Elevation / Alpine Day",
                value=format_thousands(alpine_vert / alpine_count),
                delta=None,
            )
        st.write()

        ski = ski.groupby("date", as_index=False).agg(
            {"total_elevation_gain": "sum", "max_speed": "max"}
        )
        ski["season_elevation_gain"] = ski["total_elevation_gain"].cumsum().astype(int)

        base = alt.Chart(ski).encode(x="date")
        line = base.mark_line().encode(y="season_elevation_gain")
        bar = base.mark_bar(size=5, color="gray", opacity=0.7).encode(y="max_speed")
        chart = alt.layer(line, bar).resolve_scale(y="independent")
        st.altair_chart(chart, use_container_width=True)

    with spending:
        metrics = st.columns(3)
        seven_trailing_days = datetime.date.today() - pd.DateOffset(weeks=1)
        with metrics[2]:
            variable_spending = df.loc[
                df["date"] >= seven_trailing_days, "variable_spending"
            ].sum()
            st.metric(
                label="Weekly Variable Spend",
                value="$" + format_thousands(variable_spending),
                delta=None,
            )

        agg = df.groupby("week", as_index=False)["week", "variable_spending"].sum()
        st.altair_chart(
            alt.Chart(agg)
            .mark_bar()
            .encode(
                x=alt.X("week", scale=alt.Scale()),
                y=alt.Y("variable_spending", scale=alt.Scale(zero=False)),
            ),
            use_container_width=True,
        )
        spend = pd.read_csv(config["transform"]["transactions"]["output_path"])
        spend["date"] = pd.to_datetime(spend["date"]).dt.date
        spend = spend[["date", "name", "category", "amount"]]
        spend["amount"] = spend["amount"].astype(int)
        st.table(spend.head(50))
    with health:
        # TODO - replace aggregate with resting_heart_rate?
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
