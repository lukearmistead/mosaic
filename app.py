from arete.extract_plaid import extract_plaid
from arete.extract_fitbit import extract_fitbit
from arete.extract_splitwise import extract_splitwise
from arete.extract_strava import extract_strava
from arete.process_expenses import process_expenses
import logging as log
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from datetime import datetime


log.getLogger().setLevel(log.INFO)


# External
SEASON_STARTS_ON = "2021-10-01"
# Plaid's agreement with Capital One only permits downloading the last 90 days of authorization by the user
START_DATE = datetime(2022, 3, 16).date()
END_DATE = datetime.now().date()
FONTSIZE = 10


def main():
    st.title("Luke Dashboard")
    sns.set_style("white")

    # Adventure
    st.subheader("Adventure")
    extract_strava(extract_data_since=SEASON_STARTS_ON)
    df = pd.read_csv("data/strava/activities.csv")
    df["month"] = pd.to_datetime(df["start_date_local"]).dt.to_period("M")
    plt.rcParams.update({"font.size": FONTSIZE})
    fig, ax = plt.subplots()
    df.pivot_table(values="id", index="month", columns="type", aggfunc="count").plot(
        kind="bar", xlabel="Month", ylabel="Count", stacked=True, ax=ax
    )
    hatches = iter(["///", "..", "x", "o.", "--", "O", "\\", "....", "+"])
    mapping = {}
    for bar in ax.patches:
        color = bar.get_facecolor()
        if color not in mapping.keys():
            mapping[color] = next(hatches)
        bar.set_hatch(mapping[color])
        bar.set_facecolor("black")
        bar.set_edgecolor("white")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.2), ncol=4, frameon=False)
    sns.despine(left=True)
    ax.grid(which="major", axis="y", linestyle="-")
    st.pyplot(fig)
    st.write()

    # Fitbit
    st.subheader("Heart Rate")
    extract_fitbit(start_date=pd.to_datetime(SEASON_STARTS_ON), end_date=END_DATE)
    plt.rcParams.update({"font.size": FONTSIZE})
    df = pd.read_csv("data/fitbit/heart.csv")
    df["dateTime"] = pd.to_datetime(df["dateTime"])
    fig, ax = plt.subplots()
    sns.lineplot(
        x="dateTime", y="restingHeartRate", data=df, ax=ax, color="black",
    )
    ax.set_xlabel("Month")
    ax.set_ylabel("Beats per Minute")
    ax.set_xticklabels(ax.get_xticks(), rotation=90)
    ax.grid(which="major", axis="y", linestyle="-")
    month_format = mdates.DateFormatter("%Y-%m")
    ax.xaxis.set_major_formatter(month_format)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    sns.despine(left=True)
    st.pyplot(fig)
    st.write()

    # Spending
    st.subheader("Spending")
    plt.rcParams.update({"font.size": FONTSIZE})
    extract_splitwise(start_date=START_DATE, end_date=END_DATE)
    extract_plaid(start_date=START_DATE, end_date=END_DATE)
    process_expenses()
    df = pd.read_csv("data/processed/financial_transactions.csv")
    df["date"] = pd.to_datetime(df["date"])
    df["month"] = df["date"].dt.to_period("M")
    agg = (
        df.groupby(["month", "category"], as_index=False)
        .sum()[["month", "category", "amount"]]
        .sort_values("month", ascending=True)
    )
    category_filter = ~agg["category"].isin(["income", "transfer",])
    fig, ax = plt.subplots()
    ax.set_xticklabels(ax.get_xticks(), rotation=90)
    barplot = sns.barplot(
        x="month",
        y="amount",
        hue="category",
        data=agg.loc[category_filter,],
        estimator=sum,
        ax=ax,
    )
    hatches = iter(["///", "..", "x", "o.", "--", "O", "\\", "....", "+"])
    mapping = {}
    for bar in ax.patches:
        color = bar.get_facecolor()
        if color not in mapping.keys():
            mapping[color] = next(hatches)
        bar.set_hatch(mapping[color])
        bar.set_facecolor("black")
        bar.set_edgecolor("white")

    # https://stackoverflow.com/questions/4700614/how-to-put-the-legend-outside-the-plot
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.2), ncol=3, frameon=False)
    ax.grid(which="major", axis="y", linestyle="-")
    ax.set_xlabel("Month")
    ax.set_ylabel("$ Amount")
    sns.despine(left=True)
    st.pyplot(fig)
    st.write()

    # Spending Table
    # df["date"] = df["date"].dt.date
    # df["amount"] = df["amount"].astype(int)
    # st.table(
    #     df[["name", "date", "category", "account", "amount"]] \
    #         .sort_values("date", ascending=False) \
    #         .head(100)
    # )
    # st.write()


if __name__ == "__main__":
    main()
