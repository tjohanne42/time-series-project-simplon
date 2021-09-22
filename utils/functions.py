import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime


def generate_df_by_time_section(time_section="hour", save_path=None):
    """
    This function returns the dataframe or energy usage
    with all the values grouped by the time section wanted
    (hour, day, week, month, year).

    Parameters
    ----------
    - time_section: str = "hour"

        What time section the original DataFrame should be
        grouped by. If "original", returns the original unprocessed
        DataFrame.
        Possible values:
            "original",
            "hour",
            "day",
            "week",
            "month",
            "year".

    - save_path : str | pathlib.Path | None = None

        Add a path to a csv file if you want to save the
        DataFrame.

    Returns
    -------
    - df_result: pd.DataFrame

        The DataFrame resulting from the groupby.
    """

    assert time_section in ["original", "hour", "day", "week", "month", "year"], \
        f"{time_section} is not a valid value for time_section."

    path_original_csv = Path(__file__).resolve().parent.parent / "data" / "original.csv"

    # path = 'https://drive.google.com/uc?export=download&id=14rWJ6OKc_aZbAcb6h220vQP0xfitLgHM'
    # df_original = pd.read_csv(path, sep=";", parse_dates=[" timestamp"])
    df_original = pd.read_csv(path_original_csv, parse_dates=["timestamp"])

    # remove space from column names
    # df_original = df_original.rename(columns=lambda col: col[1:])
    # return df_original

    if time_section == "original":
        df_result = df_original

    else:

        # drop duplicates
        df_result = df_original.drop_duplicates().reset_index(drop=True)

        # delete minutes and seconds
        def del_seconds_minutes(timestamp):
            return timestamp.replace(second=0, minute=0)
        df_result["timestamp"] = df_result["timestamp"].map(del_seconds_minutes)

        # get the mean values or every hour
        df_result = df_result.groupby(["timestamp"], as_index=False).mean()

        # add null values to fill missing hours
        df_result["timestamp_day"] = df_result["timestamp"].map(lambda timestamp: datetime(timestamp.year, timestamp.month, timestamp.day))

        data = {"timestamp": []}
        dates = df_result["timestamp_day"].unique()

        for date in dates:
            date = pd.to_datetime(date)
            df_tmp = df_result.loc[df_result["timestamp_day"] == pd.to_datetime(date)]
            for hour in range(24):
                if not np.any(df_tmp["timestamp"].dt.hour == hour):
                    data["timestamp"].append(datetime(date.year, date.month, date.day, hour, 0, 0))

        df_missing_hours = pd.DataFrame.from_dict(data)
        df_result = df_result.append(df_missing_hours).sort_values(by="timestamp").reset_index(drop=True)

        # interpolation to replace null values
        df_result = df_result.drop(columns="timestamp_day").interpolate()

        # multiply by 12 to get the actual values by hour
        # (the original values were for every 5 minutes)
        for col in df_result.columns[1:]:
            df_result[col] = df_result[col] * 12

        if time_section == "hour":
            # return df_result as is
            pass

        elif time_section == "day":

            def del_hours(timestamp):
                return timestamp.replace(hour=0)
            df_result["timestamp"] = df_result["timestamp"].map(del_hours)

            # get the sum of every hours for every days
            df_result = df_result.groupby(["timestamp"], as_index=False).sum()

        elif time_section == "week":

            def del_hours(timestamp):
                return timestamp.replace(hour=0)
            df_result["timestamp"] = df_result["timestamp"].map(del_hours)

            # get the sum of every hours for every days
            df_result = df_result.groupby(["timestamp"], as_index=False).sum()

            # add the column "week"
            df_result["week"] = df_result["timestamp"].map(lambda timestamp: timestamp.week)

            # add the column "year"

            def associate_week_with_year(row):
                # if it's the 52nd week in january, then
                # it's the 52nd week of the previous year
                if row["week"] == 52 and row["timestamp"].month == 1:
                    year = row["timestamp"].year - 1
                # if it's the 1st week in deecember, then
                # it's the 1st week of the next year
                elif row["week"] == 1 and row["timestamp"].month == 12:
                    year = row["timestamp"].year + 1
                else:
                    year = row["timestamp"].year
                return year

            df_result["year"] = df_result.apply(associate_week_with_year, axis=1)

            # get the sum of every days for every weeks
            df_tmp_1 = df_result.groupby(by=["year","week"], as_index=False).sum()

            # retrieve the timestamp for the mondays of every week
            df_tmp_2 = df_result[["year", "week", "timestamp"]].groupby(by=["year", "week"], as_index=False).min()

            df_tmp_1["timestamp"] = df_tmp_2["timestamp"]

            df_result = df_tmp_1[["timestamp", "year", "week", "coal", "nuclear", "wind", "hydro", "solar"]]

        elif time_section == "month":

            def del_hours_days(timestamp):
                return timestamp.replace(hour=0, day=1)
            df_result["timestamp"] = df_result["timestamp"].map(del_hours_days)

            # get the sum of every hours for every months
            df_result = df_result.groupby(["timestamp"], as_index=False).sum()

        elif time_section == "year":

            def del_hours_days_month(timestamp):
                return timestamp.replace(hour=0, day=1, month=1)
            df_result["timestamp"] = df_result["timestamp"].map(del_hours_days_month)

            # get the sum of every hours for every years
            df_result = df_result.groupby(["timestamp"], as_index=False).sum()

    if save_path:
        df_result.to_csv(path_or_buf=save_path, index=False)

    return df_result


if __name__ == "__main__":
    pass
