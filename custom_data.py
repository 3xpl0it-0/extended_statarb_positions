"""creating custom df for purpose of extracting signals"""

# importing packages

import pandas as pd

# importing data

from funding_data import funding_data
from tohlcv_data import price_data
import data_cleaning

# creating custom daily prices

all_rows = []

for ticker in price_data["asset"].unique():
    count = 0
    tempdata = price_data[price_data["asset"] == ticker]

    for index in tempdata.index:
        dt = pd.to_datetime(tempdata["timestamp"].loc[index], unit="ms", utc=True)

        if int(dt.hour) == 20:  # 8pm
            count = 1

        if count != 0: # start creating from first candle in period (candles identified from open time)
            if int(dt.hour) == 20:  # 8pm
                time_ = tempdata["timestamp"].loc[index]
                high_ = float(tempdata["high"].loc[index])
                low_ = float(tempdata["low"].loc[index])
                open_ = float(tempdata["open"].loc[index])
                volume_ = float(tempdata["volume"].loc[index])

            if float(tempdata["high"].loc[index]) > high_:
                high_ = float(tempdata["high"].loc[index])

            if float(tempdata["low"].loc[index]) < low_:
                low_ = float(tempdata["low"].loc[index])

            if int(dt.hour) == 16:  # 4pm
                all_rows.append(
                    {
                        "timestamp": time_,
                        "asset": ticker,
                        "open": open_,
                        "high": high_,
                        "low": low_,
                        "close": float(tempdata["close"].loc[index]),
                        "volume": volume_ + float(tempdata["volume"].loc[index]),
                    }
                )

            volume_ = volume_ + float(tempdata["volume"].loc[index])

    print(f"custom price done for {ticker}")

df = pd.DataFrame(all_rows)
print("custom time data put into df")
print(df.head(10))

# adding funding into 24 hour periods

df["funding"] = None
df["funding inaccurate"] = None

for ticker in df["asset"].unique():
    tempdf = df[df["asset"] == ticker]
    tempdf = tempdf.reset_index()

    for index in tempdf.index:
        oldtime = tempdf["timestamp"].iloc[index]
        newtime = oldtime + 86400000

        tempfundingdf = funding_data[
            (funding_data["asset"] == ticker)
            & (
                funding_data["timestamp"] > int(oldtime + 3600000 * 0.33)
            )  # adding 20 min to make sure its not included
            & (funding_data["timestamp"] <= int(newtime + 3600000 * 0.33))
        ]  # adding 20 min to make sure its included

        df.loc[tempdf["index"].loc[index], "funding"] = (
            tempfundingdf["funding"].astype(float).sum()
        )

        df.loc[tempdf["index"].loc[index], "funding inaccurate"] = len(tempfundingdf)

        oldtime = newtime

    print(f"funding done for {ticker}")

print("funding data put into df")
print(df.head(10))

df.to_csv("trade_data.csv", index = False)
