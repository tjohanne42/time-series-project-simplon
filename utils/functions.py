import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from tqdm.auto import tqdm
tqdm.pandas()


def generate_df_by_time_section(time_section="hour", save_path=None, verbose=False):
    """
    This function returns the dataframe of energy usage
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

    - verbose: bool = False

        Set to True for some feedback.

    Returns
    -------
    - df_result: pd.DataFrame

        The DataFrame resulting from the groupby.
    """

    assert_err_msg = f"{time_section} is not a valid value for time_section."
    assert time_section in ["original", "hour", "day", "week", "month", "year"], assert_err_msg

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

        if time_section == "hour":

            # set the timestamps minutes and seconds to 0
            if verbose:
                print("set seconds and minutes to 0 ...")
                df_result["timestamp"] = df_result["timestamp"].progress_map(lambda timestamp: timestamp.replace(second=0, minute=0))
            else:
                df_result["timestamp"] = df_result["timestamp"].map(lambda timestamp: timestamp.replace(second=0, minute=0))

            # get the meant values or every hour
            print("get the mean values for every hour ...") if verbose else ...
            df_result = df_result.groupby(["timestamp"], as_index=False).mean()

            # add null values to fill missing hours

            # add a column "timestamp_day"
            if verbose:
                print("add a column timestamp_day")
                df_result["timestamp_day"] = df_result["timestamp"].progress_map(lambda timestamp: datetime(timestamp.year, timestamp.month, timestamp.day))
            else:
                df_result["timestamp_day"] = df_result["timestamp"].map(lambda timestamp: datetime(timestamp.year, timestamp.month, timestamp.day))
            # dictionary to store the missing hours
            missing_hours = {"timestamp": []}
            # place in "days" the list of all the unique recording days
            # (no recording day is missing from the first to the last one)
            days = df_result["timestamp_day"].unique()
            if verbose:
                print("look for missing recording hours ...")
                days = tqdm(days)
            # for each unique recording day ...
            for day in days:
                day = pd.to_datetime(day)
                # place in df_tmp all recording hours of that day
                df_tmp = df_result.loc[df_result["timestamp_day"] == day]
                # if there is less than 24 hours in that day ...
                if len(df_tmp) != 24:
                    # for every possible hours ...
                    for hour in range(24):
                        # if that hour is missing ...
                        if not np.any(df_tmp["timestamp"].dt.hour == hour):
                            # add the missing hour to "missing hours"
                            missing_hours["timestamp"].append(datetime(day.year, day.month, day.day, hour, 0, 0))
            # create a DataFrame to hold the missing hours
            df_missing_hours = pd.DataFrame.from_dict(missing_hours)
            # add the missing hours to df_result, sort by timestamp and reset the index
            print("add the missing hours to df_result, sort by timestamp and reset index") if verbose else ...
            df_result = df_result.append(df_missing_hours).sort_values(by="timestamp").reset_index(drop=True)

            # drop the column "timestamp_day"
            print("drop the column timestamp_day") if verbose else ...
            df_result = df_result.drop(columns="timestamp_day")

            # interpolation on the energy columns to fill
            # the null values of the missing hours
            print("linear interpolation to fill the missing values ...") if verbose else ...
            df_result.iloc[:, 1:] = df_result.iloc[:, 1:].interpolate(method="linear", axis="index")

            # multiply by 12 to get the actual values by hour
            # (the original values were for every 5 minutes)
            print("multiply all values by 12 to get the actual values by hour ...") if verbose else ...
            df_result.iloc[:, 1:] = df_result.iloc[:, 1:] * 12

        elif time_section == "day":

            # set the timestamps seconds, minutes and hours to 0
            if verbose:
                print("set seconds, minutes and hours to 0 ...")
                df_result["timestamp"] = df_result["timestamp"].progress_map(lambda timestamp: timestamp.replace(second=0, minute=0, hour=0))
            else:
                df_result["timestamp"] = df_result["timestamp"].map(lambda timestamp: timestamp.replace(second=0, minute=0, hour=0))

            # get the mean values of every day
            print("get the mean values for every day ...") if verbose else ...
            df_result = df_result.groupby(["timestamp"], as_index=False).mean()

            # multiply by 12 * 24 to get the actual values by day
            print("multiply all values by 12 * 24 to get the actual values by day ...") if verbose else ...
            df_result.iloc[:, 1:] = df_result.iloc[:, 1:] * 12 * 24

        elif time_section == "week":

            # set the timestamps seconds, minutes and hours to 0
            if verbose:
                print("set the timestamp seconds, minutes and hours to 0 ...")
                df_result["timestamp"] = df_result["timestamp"].progress_map(lambda timestamp: timestamp.replace(second=0, minute=0, hour=0))
            else:
                df_result["timestamp"] = df_result["timestamp"].map(lambda timestamp: timestamp.replace(second=0, minute=0, hour=0))

            # get the mean of every hours for every days
            print("get the mean of every hours for every days ...") if verbose else ...
            df_result = df_result.groupby(["timestamp"], as_index=False).mean()

            # add the column "week"
            print("add a column week") if verbose else ...
            df_result["week"] = df_result["timestamp"].map(lambda timestamp: timestamp.week)

            # add the column "year"
            def associate_week_with_year(row):
                # if it's the 52nd week in january, then it's the 52nd week of the previous year
                if row["week"] == 52 and row["timestamp"].month == 1:
                    year = row["timestamp"].year - 1
                # if it's the 1st week in december, then it's the 1st week of the next year
                elif row["week"] == 1 and row["timestamp"].month == 12:
                    year = row["timestamp"].year + 1
                else:
                    year = row["timestamp"].year
                return year
            if verbose:
                print("add a column year ...")
                df_result["year"] = df_result.progress_apply(associate_week_with_year, axis=1)
            else:
                df_result["year"] = df_result.apply(associate_week_with_year, axis=1)

            # get the mean of every day for every week
            print("get the mean of every day for every week ...") if verbose else ...
            df_tmp_1 = df_result.groupby(by=["year","week"], as_index=False).mean()

            # retrieve the timestamps for the mondays of every week
            print("retrive the timestamp of the first day of each week ...") if verbose else ...
            df_tmp_2 = df_result[["year", "week", "timestamp"]].groupby(by=["year", "week"], as_index=False).min()

            # add the timestamps to df_tmp_1
            df_tmp_1["timestamp"] = df_tmp_2["timestamp"]

            # reorganise the columns
            df_result = df_tmp_1[["timestamp", "year", "week", "coal", "nuclear", "wind", "hydro", "solar"]]

            # multiply by 12 * 24 * 7 to get the actual values by week
            print("multiply all values by 12 * 24 * 7 to get the actual values by week ...") if verbose else ...
            df_result.iloc[:, 3:] = df_result.iloc[:, 3:] * 12 * 24 * 7

        elif time_section == "month":

            # set the timestamps seconds, minutes and hours to 0 and days to 1
            if verbose:
                print("set the timestamp seconds, minutes and hours to 0 and days to 1 ...")
                df_result["timestamp"] = df_result["timestamp"].progress_map(lambda timestamp: timestamp.replace(second=0, minute=0, hour=0, day=1))
            else:
                df_result["timestamp"] = df_result["timestamp"].map(lambda timestamp: timestamp.replace(second=0, minute=0, hour=0, day=1))

            # get the mean of every hours for every months
            print("get the mean of every hour for every month ...") if verbose else ...
            df_result = df_result.groupby(["timestamp"], as_index=False).mean()

            # multiply by 12 * 24 to get the actual values by day
            print("multiply all values by 12 * 24 to get the actual values by day ...") if verbose else ...
            df_result.iloc[:, 1:] = df_result.iloc[:, 1:] * 12 * 24

            print("/!\\ THE DATAFRAME CONTAINS THE MEAN VALUES PER DAY IN THE MONTH /!\\")

        elif time_section == "year":

            # set the timestamps seconds, minutes and hours to 0, days to 1 and months to 1
            if verbose:
                print("set the timestamp seconds, minutes and hours to 0, days to 1 and months to 1 ...")
                df_result["timestamp"] = df_result["timestamp"].progress_map(lambda timestamp: timestamp.replace(second=0, minute=0, hour=0, day=1, month=1))
            else:
                df_result["timestamp"] = df_result["timestamp"].map(lambda timestamp: timestamp.replace(second=0, minute=0, hour=0, day=1, month=1))

            # get the mean of every hours for every months
            print("get the mean of every hour for every year ...") if verbose else ...
            df_result = df_result.groupby(["timestamp"], as_index=False).mean()

            # multiply by 12 * 24 to get the actual values by day
            print("multiply all values by 12 * 24 to get the actual values by day ...") if verbose else ...
            df_result.iloc[:, 1:] = df_result.iloc[:, 1:] * 12 * 24

            print("/!\\ THE DATAFRAME CONTAINS THE MEAN VALUES PER DAY IN THE YEAR /!\\")

    # save the csv to save_path if provided
    if save_path:
        print(f"saving file to {str(save_path)}") if verbose else ...
        df_result.to_csv(path_or_buf=save_path, index=False)

    print("done") if verbose else ...

    return df_result
