# SAFutils.py
# ETL protocl for NYCFrisk data
# Author: Gareth Jones

import pandas as pd
import numpy as np

pd.options.display.max_columns = 120

class dataContainer(object):
    """Used for holding the year data
    Contains components 
    """
    def __init__(self, years, path="", datetime=True, lazyEval=False, inChunks=True):
        
        self.years = sorted(years) 
        self.container = []
        self.path = path
        self.datetime = datetime

        if inChunks == False:
            for year in self.years:
                newData = yearData(year, datetime, lazyEval)
                self.container.append(newData)
        else:
            self.yearsToLoad = sorted(years) 
            self.chunkMode()


    def chunkMode(self):
        """Load the data and evaluate it an iterative way

        """
        self.currentData = None
        self.yearsLoaded = []
        print "Call chunkNext to load the first year of data"

    def chunkNext(self):
        """

        """
        self.chunkLoad()

    def chunkLoad(self):
        """
        Used to preform the incremental evaluation

        """
        # Make sure there are yeears left
        try:
            year = self.yearsToLoad.pop(0)

        except:
            print "No years left to load. The following were loaded"
            print yearsLoaded
            return

        print "Year to be loaded: {}".format(year)
        print "Loading..."
        self.currentData = yearData(year, self.path, self.datetime).data
        self.yearsLoaded.append(year)
        print "Load Succesful"



class yearData(object):
    """The class is a wrapper for a pandas dataframe that contains information
    
    Consider dropping lazyEval component

    """
    def __init__(self, year, path="", datetime=True, lazyEval=False):

        self.year = year
        self.fullPath = path + str(year) + ".csv"
        self.datetime = datetime

        if lazyEval == False:
            self.loadData()
        else:
            print "Call loadData to load the data into a dataframe"
            return


    def loadData(self):

        col_types = {"datestop": str, "timestop": str}
        try:
            self.data = pd.read_csv(self.fullPath, encoding="latin1", 
                                    low_memory=False, converters=col_types)

            if self.datetime:
                self.parseDatetime()
            self.prepData()

        except:
            print "Load Failed"

    def parseDatetime(self):
        """
        Optionally preform date time parsing on loaded data

        """
        df = self.data

        # Format Time
        if self.year >= 2010:
            df["timestop"] = df["timestop"].str.zfill(4)  # Left pad with zeros
        if self.year <= 2004:
            df["timestop"] = df["timestop"].str.replace(":", "")

        # Convert Dates
        if self.year == 2006:
            df["datestop"] = pd.to_datetime(df["datestop"], format="%Y-%m-%d", errors='coerce')
        else:
            df["datestop"] = pd.to_datetime(df["datestop"], format="%m%d%Y", errors='coerce')

        # Parse Time
        df["timestop"] = pd.to_datetime(df["timestop"], format="%H%M", errors='coerce')
C
        # Seperate time features into unique columns
        df["hour"] = df["timestop"].dt.hour
        df["minute"] = df["timestop"].dt.minute
        df["dayofyear"] = df["datestop"].dt.dayofyear
        df["month"] = df["datestop"].dt.month
        df["dayofweek"] = df["datestop"].dt.dayofweek
        df["weekofyear"] = df["datestop"].dt.weekofyear
        df["day"] = df["datestop"].dt.day


    def prepData(self):
        """
        Preform other data cleaning to create aggregate columns
        """
        df = self.data

        df["used_force"] = np.where((df["pf_hands"] == "Y") | (df["pf_wall"] == "Y") | 
                                   (df["pf_grnd"] == "Y") | (df["pf_drwep"] == "Y") |
                                   (df["pf_ptwep"] == "Y") | (df["pf_baton"] == "Y") | 
                                   (df["pf_hcuff"] == "Y") | (df["pf_pepsp"] == "Y") | 
                                   (df["pf_other"] == "Y"), 1, 0)

        df["arstmade"] = np.where(df["arstmade"] == "Y", 1, 0)

    def textParsing(self):

        """
        Parse text components of the dataframe

        """

        df = self.data



def SAF_data_cleaner(file_path):
    """This function is used for cleaning the data coming from the stop and
    frisk data sets and returning a cleaned pandas dataframe
    
    Args: file_path, the path to the file. Must be a csv

    Returns: year, the year of the data collection
             df, the parsed pandas dataframe
    
    """

    print "This function is depreciated"

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

    df["used_force"] = np.where((df["pf_hands"] == "Y") | (df["pf_wall"] == "Y") | (df["pf_grnd"] == "Y") |
                                (df["pf_drwep"] == "Y") | (df["pf_ptwep"] == "Y") | (df["pf_baton"] == "Y") | 
                                (df["pf_hcuff"] == "Y") | (df["pf_pepsp"] == "Y") | (df["pf_other"] == "Y"), 1, 0)
    df["arstmade"] = np.where(df["arstmade"] == "Y", 1, 0)

    return year, df
