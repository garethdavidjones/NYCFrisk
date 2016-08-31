import pandas as pd

pd.options.display.max_columns = 120

def SAF_data_cleaner(file_path):
    """This function is used for cleaning the data coming from the stop and
    frisk data sets and returning a cleaned pandas dataframe
    
    Args: file_path, the path to the file. Must be a csv

    Returns: year, the year of the data collection
             df, the parsed pandas dataframe
    
    """
    col_types = {"datestop": str, "timestop": str}
    
    df = pd.read_csv(file_path, encoding="latin1", low_memory=False, converters=col_types)

    year = df["year"][0]

    if year >= 2010:
        df["timestop"] = df["timestop"].str.zfill(4)  # Left pad with zeros
    if year <= 2004:
        df["timestop"] = df["timestop"].str.replace(":", "")
    if year == 2006:
        df["datestop"] = pd.to_datetime(df["datestop"], format="%Y-%m-%d", coerce=True)
        # Should try to see if this works for every year
    else:
        df["datestop"] = pd.to_datetime(df["datestop"], format="%m%d%Y", coerce=True)

    df["timestop"] = pd.to_datetime(df["timestop"], format="%H%M", coerce=True)
    df["hour"] = df["timestop"].dt.hour
    df["minute"] = df["timestop"].dt.minute
    df["dayofyear"] = df["datestop"].dt.dayofyear
    df["month"] = df["datestop"].dt.month
    df["dayofweek"] = df["datestop"].dt.dayofweek
    df["weekofyear"] = df["datestop"].dt.weekofyear
    df["day"] = df["datestop"].dt.day    

    return year, df



def count_aggregator(file_regex, file_path=""):
    """Come back to this when you can copy the code in

    """
    pass
    for filename in glob.glob("[0-9]*.csv"):
        year, df = SAF_data_cleaner(["dayofweek"]).size()

        day_of_week_counts = df.groupby(["dayofweek"]).size()
        hourly_counts = df.groupby(["hour"]).size()
        daily_counts = df.groupby(["datofyear"]).size()


    return holder
