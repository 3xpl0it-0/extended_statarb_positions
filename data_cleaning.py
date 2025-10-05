"""checking data for cleanliness"""

# notification of entry

print(">>> entering data_cleaning.py <<<")

# importing packages

import pandas as pd

# importing data

from funding_data import funding_data
from tohlcv_data import price_data

# defining constants

TIMEPERIOD = int(86400000/6)

# function for data checking

def time_gaps(
        dataframe : pd.DataFrame | None = None,
        time_period : int | None = None,
        ):
    """check for missing entries for specified time interval"""

    _dataframe = dataframe if dataframe is not None else price_data
    _time_period = time_period or TIMEPERIOD

    timeall = {}
    temptime = []

    for market in _dataframe["asset"].unique():
        timedf = _dataframe[_dataframe["asset"] == market]
        oldtime = timedf["timestamp"].iloc[0]

        for time_ in timedf["timestamp"][1:]:
            newtime = time_

            if oldtime != 0:
                temptime.append([newtime, oldtime, newtime - oldtime])

            oldtime = newtime

        timeall[market] = temptime
        temptime = []

    print("beginning missing data checks")

    for market, times in timeall.items():
        for t in times:
            if not _time_period - 900000 <= t[2] <= _time_period + 900000:
                print(f"{market} {t}")
        if not all(( _time_period - 900000 <= t[2] <= _time_period + 900000) for t in times):
            print(f"{market}: not all equal to {_time_period}")

    print("finished missing data checks")

# checking tohlcv data

# checking for 0 values
print("checking price_data for 0's")
cleantemp = price_data[(price_data == 0).any(axis=1)]
print(cleantemp)
print(len(cleantemp))
# note many 0 values for volume

print("checking price_data for 0's excluding volume column")
cleandf = price_data.drop(columns=["volume"])
cleantemp = cleandf[(cleandf == 0).any(axis=1)]
print(cleantemp)
print(len(cleantemp))
print("finished checking price_data for 0's")
# none without volume

# checking for missing data
time_gaps()

# checking funding data

# checking for 0 values
print("checking funding_data for 0's")
cleantemp = funding_data[(funding_data == 0).any(axis=1)]
print(cleantemp)
print(len(cleantemp))
# note some funding payments are 0 but this may not be a problem
print("finished checking funding_data for 0's")

# checking for missing data
time_gaps(funding_data, int(86400000/24))
