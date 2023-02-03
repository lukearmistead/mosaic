from arete.utils import convert_vector_to_date, snakecase_format
from datetime import datetime
import inflection
import pandas as pd
import pandasql


def read_query(path):
    with open(path, "r") as f:
        return f.read()


def date_period_vector(v, period):
    return convert_vector_to_date(v.dt.to_period(period).dt.to_timestamp().astype(str))


def date_dimension_table(start, end):
    df = pd.DataFrame({"date": pd.date_range(start, end, periods=None, freq="d")})
    df["week"] = date_period_vector(df["date"], "W")
    df["month"] = date_period_vector(df["date"], "M")
    df["quarter"] = date_period_vector(df["date"], "Q")
    df["date"] = convert_vector_to_date(df["date"].astype(str))
    return df


def transform_aggregate(start_date, end_date, transform_transactions_path, extract_strava_path, transform_resting_heart_rates_path, output_path):
    df = date_dimension_table(start=start_date, end=end_date)
    strava = pd.read_csv(extract_strava_path)
    activity_counts = (
        strava.pivot_table(
            values="id", index="start_date_local", columns="type", aggfunc="count"
        )
        .reset_index()
        .rename(columns={"start_date_local": "date"})
    )
    activity_counts.columns = [snakecase_format(col) for col in activity_counts.columns]
    activity_counts["date"] = convert_vector_to_date(activity_counts["date"])
    df = df.merge(activity_counts, how="left", on="date")

    ski_verts = (
        strava.loc[strava["type"].isin(["AlpineSki", "BackcountrySki"]),]
        .pivot_table(
            values="total_elevation_gain",
            index="start_date_local",
            columns="type",
            aggfunc="sum",
        )
        .reset_index()
        .rename(
            columns={
                "start_date_local": "date",
                "AlpineSki": "alpine_ski_vert",
                "BackcountrySki": "backcountry_ski_vert",
            }
        )
    )
    ski_verts["ski_vert"] = ski_verts["alpine_ski_vert"].fillna(0) + ski_verts[
        "backcountry_ski_vert"
    ].fillna(0)
    ski_verts["date"] = convert_vector_to_date(ski_verts["date"])
    df = df.merge(ski_verts, how="left", on="date")
    print(df.head())

    transactions = pd.read_csv(transform_transactions_path)
    category_filter = ~transactions["category"].isin(["income", "transfer", "housing"])
    variable_spending = (
        transactions.loc[category_filter,]
        .pivot_table(values="amount", index="date", aggfunc="sum")
        .rename(columns={"amount": "variable_spending"})
        .reset_index()
    )
    variable_spending["date"] = convert_vector_to_date(variable_spending["date"])
    df = df.merge(variable_spending, how="left", on="date")
    hearts = pd.read_csv(transform_resting_heart_rates_path)
    df = df.merge(hearts, how="left", on="date")
    df = df.loc[df["date"].between(start_date, end_date),]
    df.to_csv(output_path, index=False)
