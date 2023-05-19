from arete.etl import run_etl
from arete.utils import convert_vector_to_date, lookup_yaml
import altair as alt
import datetime
import inflection
import logging as getLogger
import pandas as pd
import streamlit as st


getLogger.getLogger().setLevel(getLogger.INFO)


def format_thousands(value):
    return f"{value:,.0f}"


def most_recent_value(
    long_df, category, category_col="type", value_col="value", date_col="date"
):
    # Expects long data
    last_value = (
        long_df.sort_values(date_col)
        .loc[long_df[category_col] == category, value_col]
        .dropna()
        .iloc[-1]
    )
    return last_value


def plot_dual_axis(shared_x, line_y, bar_y, df):
    # Expects wide data
    base = alt.Chart(df).encode(x=f"{shared_x}:T")
    line = base.mark_line().encode(y=alt.Y(f"{line_y}:Q", scale=alt.Scale(zero=False)))
    bar = base.mark_bar(size=5, color="gray", opacity=0.5).encode(y=f"{bar_y}:Q")
    chart = alt.layer(line, bar).resolve_scale(y="independent")
    return st.altair_chart(chart, use_container_width=True)


def write_goal_checklist(goals: list):
    goal_cols = st.columns(len(goals))
    for i, goal in enumerate(goals):
        with goal_cols[i]:
            st.checkbox(goal)
    st.write()


def main():
    config = lookup_yaml("etl_config.yml")
    # TODO - Think of a clever way to gate ETL run so it doesn't hit the request limit
    run_etl()
    st.title("🏔 Review")
    write_goal_checklist(["Tour Shasta", "Climb Serengeti", "Buy home"])
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
        trailing_week = pd.to_datetime(
            datetime.date.today() - pd.DateOffset(weeks=1)
        ).date()
        trailing_quarter = pd.to_datetime(
            datetime.date.today() - pd.DateOffset(weeks=12)
        ).date()
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

        weekly_variable_spend = (
            spend.loc[spend["is_variable"], ["week", "category", "amount"]]
            .groupby(["week", "category"], as_index=False)
            .sum()
        )
        st.altair_chart(
            alt.Chart(weekly_variable_spend)
            .mark_bar()
            .encode(
                x=alt.X("week:T", scale=alt.Scale()),
                y=alt.Y("amount:Q", scale=alt.Scale(zero=False)),
                color="category:N",
            ),
            use_container_width=True,
        )
        spend = spend[["date", "name", "account", "category", "amount"]]
        spend["amount"] = spend["amount"].astype(int)
        with st.expander("Transactions"):
            st.table(spend.sort_values("date", ascending=False).head(50))
    with health:
        metric_values = ["resting_heart_rate", "sleep_hours", "weight", "bmi"]
        metrics = st.columns(len(metric_values))
        vitals = pd.read_csv(config["transform"]["vitals"]["output_path"])
        vitals["date"] = convert_vector_to_date(vitals["date"])
        for i, value in enumerate(metric_values):
            with metrics[i]:
                last_value = f"{most_recent_value(vitals,value):.1f}"
                st.metric(label=inflection.titleize(value), value=last_value)
        vitals = vitals.pivot_table(
            values="value", index="date", columns="type"
        ).reset_index()
        plot_dual_axis(
            shared_x="date", line_y="resting_heart_rate", bar_y="sleep_hours", df=vitals
        )


if __name__ == "__main__":
    main()
