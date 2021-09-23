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

    path_original_csv = Path(__file__).resolve().parent.parent / "data" / "energy_use_in_the_UK_original.csv"

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

        # set the timestamps minutes and seconds to 0
        df_result["timestamp"] = df_result["timestamp"].map(lambda timestamp: timestamp.replace(second=0, minute=0))

        # get the mean values or every hour
        df_result = df_result.groupby(["timestamp"], as_index=False).mean()

        # add null values to fill missing hours

        # add a column "timestamp_day"
        df_result["timestamp_day"] = df_result["timestamp"].map(lambda timestamp: datetime(timestamp.year, timestamp.month, timestamp.day))
        # dictionary to store the missing hours
        missing_hours = {"timestamp": []}
        # place in "days" the list of all the unique recording days
        days = df_result["timestamp_day"].unique()
        # for each unique recording day ...
        for day in days:
            day = pd.to_datetime(day)
            # place in df_tmp all recording hours of that day
            df_tmp = df_result.loc[df_result["timestamp_day"] == day]
            for hour in range(24):
                # if any hour of that day is missing ...
                if not np.any(df_tmp["timestamp"].dt.hour == hour):
                    # add the missing hour to "missing hours"
                    missing_hours["timestamp"].append(datetime(day.year, day.month, day.day, hour, 0, 0))
        # create a DataFrame to hold the missing hours
        df_missing_hours = pd.DataFrame.from_dict(missing_hours)
        # add the missing hours to df_result, sort and reset the index
        df_result = df_result.append(df_missing_hours).sort_values(by="timestamp").reset_index(drop=True)

        # interpolation to fill the null values of the missing hours
        df_result = df_result.drop(columns="timestamp_day").interpolate()

        # multiply by 12 to get the actual values by hour
        # (the original values were for every 5 minutes)
        for col in df_result.columns[1:]:
            df_result[col] = df_result[col] * 12

        if time_section == "hour":
            # return df_result as is
            pass

        elif time_section == "day":

            # set the timestamps hours to 0
            df_result["timestamp"] = df_result["timestamp"].map(lambda timestamp: timestamp.replace(hour=0))

            # get the sum of every hours for every days
            df_result = df_result.groupby(["timestamp"], as_index=False).sum()

        elif time_section == "week":

            # set the timestamps hours to 0
            df_result["timestamp"] = df_result["timestamp"].map(lambda timestamp: timestamp.replace(hour=0))

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
                # if it's the 1st week in december, then
                # it's the 1st week of the next year
                elif row["week"] == 1 and row["timestamp"].month == 12:
                    year = row["timestamp"].year + 1
                else:
                    year = row["timestamp"].year
                return year
            df_result["year"] = df_result.apply(associate_week_with_year, axis=1)

            # get the sum of every days for every weeks
            df_tmp_1 = df_result.groupby(by=["year","week"], as_index=False).sum()

            # retrieve the timestamps for the mondays of every week
            df_tmp_2 = df_result[["year", "week", "timestamp"]].groupby(by=["year", "week"], as_index=False).min()

            # add the timestamps to df_tmp_1
            df_tmp_1["timestamp"] = df_tmp_2["timestamp"]

            # reorganise the columns
            df_result = df_tmp_1[["timestamp", "year", "week", "coal", "nuclear", "wind", "hydro", "solar"]]

        elif time_section == "month":

            # set the timestamps hours to 0 and days to 1
            df_result["timestamp"] = df_result["timestamp"].map(lambda timestamp: timestamp.replace(hour=0, day=1))

            # get the sum of every hours for every months
            df_result = df_result.groupby(["timestamp"], as_index=False).sum()

        elif time_section == "year":

            # set the timestamps hours to 0, days to 1 and months to 1
            df_result["timestamp"] = df_result["timestamp"].map(lambda timestamp: timestamp.replace(hour=0, day=1, month=1))

            # get the sum of every hours for every years
            df_result = df_result.groupby(["timestamp"], as_index=False).sum()

    # save the csv to save_path if provided
    if save_path:
        df_result.to_csv(path_or_buf=save_path, index=False)

    return df_result


if __name__ == "__main__":
    pass
