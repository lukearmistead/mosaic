import logging
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.FileHandler("debug.log"), logging.StreamHandler()],
)

st.set_page_config(layout="wide")
st.title("Luke Stuff")
sns.set(font="Proxima Nova")
sns.set_style("whitegrid")


df = pd.read_csv("data/processed/financial_transactions.csv")
df["date"] = pd.to_datetime(df["date"])
df["month"] = df["date"].dt.to_period("M")
# df['month'] = df['date'].dt.strftime('%b') # Formats month as short string

# This is how you do columns
# col1, col2 = st.columns(2)
# with col1:
#     st.subheader('Foo')
# with col2:
#     st.subheader('Bar')

agg = (
    df.groupby(["month", "category"], as_index=False)
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


fig, ax = plt.subplots(nrows=1, ncols=1, sharex="col", figsize=(10, 5))
st.subheader("Spending")
barplot = sns.barplot(
    x="month",
    y="amount",
    hue="category",
    data=agg.loc[category_filter,],
    estimator=sum,
    palette=["black"],
    ax=ax,
)
hatches = ["///", "..", "x", "o.", "-", "O", "\\", "....", "+"]
x_count = agg["month"].unique().size
i, j = 0, 0
for bar in barplot.patches:
    # https://stackoverflow.com/questions/35467188/is-it-possible-to-add-hatches-to-each-individual-bar-in-seaborn-barplot
    bar.set_hatch(hatches[j])
    i += 1
    if i == x_count:
        i = 0
        j += 1

ax.legend(loc="center left", bbox_to_anchor=(1, 0.5))
ax.set_xlabel("Month")
ax.set_ylabel("$ Amount")

st.pyplot(fig)
st.write()

df["date"] = df["date"].dt.date
df["amount"] = df["amount"].astype(int)
st.table(
    df[["name", "date", "category", "account", "amount"]].sort_values(
        "date", ascending=False
    )
)
st.write()
