import logging
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns


logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.FileHandler("debug.log"), logging.StreamHandler()],
)


# External
START_DATE = None
END_DATE = None


FONTSIZE = 10


def main():
    st.title("Luke Dashboard")
    sns.set_style("white")


    # Adventure
    st.subheader("Adventure")
    df = pd.read_csv('data/strava/activities.csv')
    df["month"] = pd.to_datetime(df['start_date_local']).dt.to_period("M")
    plt.rcParams.update({'font.size': FONTSIZE})
    fig, ax = plt.subplots()
    df \
        .pivot_table(values='id', index='month', columns='type', aggfunc='count') \
        .plot(kind='bar', xlabel='Month', ylabel='Count', stacked=True, ax=ax)
    hatches = iter(['///', '..', 'x', 'o.', '--', 'O', '\\', '....', '+'])
    mapping = {}
    for bar in ax.patches:
        color = bar.get_facecolor()
        if color not in mapping.keys():
            mapping[color] = next(hatches)
        bar.set_hatch(mapping[color])
        bar.set_facecolor('black')
        bar.set_edgecolor('white')
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.2), ncol=4, frameon=False)
    sns.despine(left=True)
    ax.grid(which='major', axis='y', linestyle='-')
    st.pyplot(fig)
    st.write()


    # Fitbit
    st.subheader("Heart Rate")
    plt.rcParams.update({'font.size': FONTSIZE})
    df = pd.read_csv("data/fitbit/heart.csv")
    df["dateTime"] = pd.to_datetime(df["dateTime"])
    fig, ax = plt.subplots()
    sns.lineplot(
        x='dateTime',
        y='restingHeartRate',
        data=df,
        ax=ax,
        color='black',
        )
    ax.set_xlabel("Month")
    ax.set_ylabel("Beats per Minute")
    ax.set_xticklabels(ax.get_xticks(), rotation=90)
    ax.grid(which='major', axis='y', linestyle='-')
    month_format = mdates.DateFormatter('%Y-%m')
    ax.xaxis.set_major_formatter(month_format)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    sns.despine(left=True)
    st.pyplot(fig)
    st.write()


    # Spending
    st.subheader("Spending")
    plt.rcParams.update({'font.size': FONTSIZE})
    df = pd.read_csv("data/processed/financial_transactions.csv")
    df["date"] = pd.to_datetime(df["date"])
    df["month"] = df["date"].dt.to_period("M")
    agg = (
        df
        .groupby(["month", "category"], as_index=False)
        .sum()[["month", "category", "amount"]]
        .sort_values("month", ascending=True)
    )
    category_filter = ~agg["category"].isin(
        [
            "Payroll",
            "Rent",
            "Transfer",
            "Government",
            "Credit Card Payment",
            "Loans and Mortgages",
        ]
    )
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
    hatches = iter(['///', '..', 'x', 'o.', '--', 'O', '\\', '....', '+'])
    mapping = {}
    for bar in ax.patches:
        color = bar.get_facecolor()
        if color not in mapping.keys():
            mapping[color] = next(hatches)
        bar.set_hatch(mapping[color])
        bar.set_facecolor('black')
        bar.set_edgecolor('white')


    # https://stackoverflow.com/questions/4700614/how-to-put-the-legend-outside-the-plot
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.2), ncol=3, frameon=False)
    ax.grid(which='major', axis='y', linestyle='-')
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

if __name__ == '__main__':
    main()
