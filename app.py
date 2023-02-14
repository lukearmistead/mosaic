from arete.etl import run_etl
from arete.utils import convert_vector_to_date, lookup_yaml
import altair as alt
import logging as getLogger
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import datetime


getLogger.getLogger().setLevel(getLogger.INFO)


def format_thousands(value):
    return f"{value:,.0f}"


def plot_dual_axis(shared_x, line_y, bar_y, df):
    # Expects wide data
    base = alt.Chart(df).encode(x=f"{shared_x}:T")
    line = base.mark_line().encode(y=f"{line_y}:Q")
    bar = base.mark_bar(size=5, color="gray", opacity=0.7).encode(y=f"{bar_y}:Q")
    chart = alt.layer(line, bar).resolve_scale(y="independent")
    return st.altair_chart(chart, use_container_width=True)


def main():
    config = lookup_yaml("etl_config.yml")
    # TODO - Think of a clever way to gate this
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
        tour_count = ski.loc[ski["type"] == "backcountry_ski", "date"].nunique()
        alpine_vert = ski.loc[ski["type"] == "alpine_ski", "total_elevation_gain"].sum()
        alpine_count = ski.loc[ski["type"] == "alpine_ski", "id"].count()

        with metrics[0]:
            st.metric(label="Tour Days", value=format_thousands(tour_count), delta=None)
        with metrics[1]:
            st.metric(
                label="Max Speed",
                value=format_thousands(ski["max_speed"].max()),
                delta=None,
            )
        with metrics[2]:
            st.metric(
                label="Ski Days",
                value=format_thousands(ski["date"].nunique()),
                delta=None,
            )
        with metrics[3]:
            st.metric(
                label="Elevation",
                value=format_thousands(ski["total_elevation_gain"].sum()),
                delta=None,
            )
        with metrics[4]:
            st.metric(
                label="Elevation / Alpine",
                value=format_thousands(alpine_vert / alpine_count),
                delta=None,
            )
        st.write()

        ski = ski.groupby("date", as_index=False).agg(
            {"total_elevation_gain": "sum", "max_speed": "max"}
        )
        ski["season_elevation_gain"] = ski["total_elevation_gain"].cumsum().astype(int)
        plot_dual_axis(
            shared_x="date", line_y="season_elevation_gain", bar_y="max_speed", df=ski
        )

    with spending:
        metrics = st.columns(3)
        spend = pd.read_csv(config["transform"]["transactions"]["output_path"])
        trailing_week = datetime.date.today() - pd.DateOffset(weeks=1)
        trailing_quarter = datetime.date.today() - pd.DateOffset(weeks=12)
        spend["date"] = convert_vector_to_date(spend["date"])
        with metrics[0]:
            weekly_restaurant_spend = (
                spend.loc[spend["date"] >= trailing_week]
                .loc[spend["category"] == "restaurants", "amount"]
                .sum()
            )
            quarterly_restaurant_spend = (
                spend.loc[spend["date"] >= trailing_quarter,]
                .loc[spend["category"] == "restaurants", "amount"]
                .sum()
            )
            st.metric(
                label="Weekly Restaurant Spend",
                value="$" + format_thousands(weekly_restaurant_spend),
                delta=format_thousands(
                    weekly_restaurant_spend - quarterly_restaurant_spend / 12
                ),
                delta_color="inverse",
            )

        with metrics[1]:
            weekly_variable_spend = (
                spend.loc[spend["date"] >= trailing_week]
                .loc[spend["is_variable"], "amount"]
                .sum()
            )
            quarterly_variable_spend = (
                spend.loc[spend["date"] >= trailing_quarter,]
                .loc[spend["is_variable"], "amount"]
                .sum()
            )
            st.metric(
                label="Weekly Variable Spend",
                value="$" + format_thousands(weekly_variable_spend),
                delta=format_thousands(
                    weekly_variable_spend - quarterly_variable_spend / 12
                ),
                delta_color="inverse",
            )
        with metrics[2]:
            pass

        # df.query('is_variable').groupby('date').sum()['amount'].rolling('30D').sum()
        st.altair_chart(
            alt.Chart(spend.loc[spend["is_variable"],])
            .mark_bar()
            .encode(
                x=alt.X("week:T", scale=alt.Scale()),
                y=alt.Y("amount", scale=alt.Scale(zero=False)),
                color="category",
            ),
            use_container_width=True,
        )
        spend = spend[["date", "name", "account", "category", "amount"]]
        spend["amount"] = spend["amount"].astype(int)
        with st.expander("Transactions"):
            st.table(spend.sort_values("date", ascending=False).head(50))
    with health:
        metrics = st.columns(5)
        vitals = pd.read_csv(config["transform"]["vitals"]["output_path"])
        vitals["date"] = convert_vector_to_date(vitals["date"])
        with metrics[0]:
            last_heart_rate = (
                vitals.sort_values("date")
                .loc[vitals["type"] == "resting_heart_rate", "value"]
                .iloc[0]
            )
            st.metric(
                label="Resting Heart Rate", value=last_heart_rate,
            )
        with metrics[1]:
            last_heart_rate = (
                vitals.sort_values("date")
                .loc[vitals["type"] == "sleep_hours", "value"]
                .iloc[0]
            )
            st.metric(
                label="Sleep Hours", value=last_heart_rate,
            )
        vitals = vitals.pivot_table(
            values="value", index="date", columns="type"
        ).reset_index()
        plot_dual_axis(
            shared_x="date", line_y="resting_heart_rate", bar_y="sleep_hours", df=vitals
        )


if __name__ == "__main__":
    main()
