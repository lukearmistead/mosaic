from datetime import datetime
import inflection
import pandas as pd
import pandasql

transform_dir = "arete/transform/"
output_path = "data/processed/aggregate_periods.csv"
END_DATE = datetime.now().date()
START_DATE = datetime(2022, 1, 1)

def read_query(path):
    with open(path, "r") as f:
        return f.read()

def date_dimension_table(start, end):
    df = pd.DataFrame({"date": pd.date_range(start, end, periods=None, freq="d")})
    df["week"] = df["date"].dt.to_period("W").dt.to_timestamp()
    df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()
    df["quarter"] = df["date"].dt.to_period("Q").dt.to_timestamp()
    return df


def convert_to_date(col):
    return pd.to_datetime(col).dt.date.astype("datetime64[ns]")


def snakecase_format(string):
    return inflection.underscore(string)


def aggregate_periods():
    df = date_dimension_table(start=START_DATE, end=END_DATE)
    strava = pd.read_csv("data/strava/activities.csv")
    activity_counts = (
        strava.pivot_table(
            values="id", index="start_date_local", columns="type", aggfunc="count"
        )
        .reset_index()
        .rename(columns={"start_date_local": "date"})
    )
    activity_counts.columns = [snakecase_format(col) for col in activity_counts.columns]
    activity_counts["date"] = convert_to_date(activity_counts["date"])
    df = df.merge(activity_counts, how="left", on="date")

    transactions = pd.read_csv("data/processed/financial_transactions.csv")
    category_filter = ~transactions["category"].isin(["income", "transfer", "housing"])
    variable_spending = (
        transactions.loc[category_filter,]
        .pivot_table(values="amount", index="date", aggfunc="sum")
        .rename(columns={"amount": "variable_spending"})
        .reset_index()
    )
    variable_spending["date"] = convert_to_date(variable_spending["date"])
    df = df.merge(variable_spending, how="left", on="date")
    hearts = pd.read_csv("data/fitbit/heart.csv")
    hearts = hearts[["dateTime", "restingHeartRate"]].rename(
        columns={"dateTime": "date"}
    )
    hearts["date"] = convert_to_date(hearts["date"])
    hearts.columns = [snakecase_format(col) for col in hearts.columns]
    df = df.merge(hearts, how="left", on="date")
    df.to_csv(output_path, index=False)


if __name__ == "__main__":
    aggregate_periods()
