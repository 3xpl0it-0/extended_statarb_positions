"""getting open time, open, high, low, close and volume for specified time interval"""

# notification of entry

print(">>> entering tohlcv_data.py <<<")

# importing data and packages

from asset_listing import listingsdf

import pandas as pd
import requests

import time

# defining constants

CUTOFF = int(1754956800 * 1000)  # 12 august 2025 - when migration started
EXTENDED_OLDBASEURL = "https://api.extended.exchange/"
EXTENDED_NEWBASEURL = "https://api.starknet.extended.exchange/"
EXTENDED_CANDLEURL = "api/v1/info/candles/{urlsymbol}/trades"

# candle times and range for getting data
TIME_INTERVAL = "PT4H"
RANGE = int(60 * 6) # 60 days of 4 hour candles

# function for getting data

def get_x_prices(
        ticker_info: tuple | None = None,
        baseurl: str | None = None,
        time_interval: str | None = None,
        candleurl: str | None = None,
        range: int | None = None,
        ) -> list[dict]:
    """get candlestick prices for specified time"""

    if ticker_info:
        _ticker_info = ticker_info
    else:
        raise ValueError("tohlcv_data.py: ticker info is required in get_x_prices")
    _baseurl = baseurl or EXTENDED_NEWBASEURL # default to new base url
    _time_interval = time_interval or TIME_INTERVAL
    _candleurl = candleurl or EXTENDED_CANDLEURL
    _range = range or RANGE

    candleslist = []

    try:
        url_request = f"{_baseurl}{_candleurl.format(urlsymbol=_ticker_info[0])}"
        params = {"interval": _time_interval, "limit": (_range)}
        response = requests.get(url_request, params=params, timeout = 10)
        response.raise_for_status()

        candles = response.json()["data"]
        candleslist = candles[::-1]
        candleslist = [d for d in candleslist]

    except requests.exceptions.RequestException as e:
        print(f"tohlcv_data.py: Warning: API request failed for {_ticker_info[0]}: {e}")

    if not candleslist:
        print(f"tohlcv_data.py: problem with {_ticker_info[0]} wrt getting price data")
        return []

    _candleslist = []
    for row in candleslist:
        _candleslist.append({
        "timestamp": int(row["T"]),
        "asset":  _ticker_info[0],
        "open": float(row["o"]),
        "high": float(row["h"]),
        "low": float(row["l"]),
        "close": float(row["c"]),
        "volume": float(row["v"]),
        })

    return _candleslist

olddata = []
newdata = []

for index in listingsdf.index:
    if listingsdf["migration"].iloc[index] == "before":

        time.sleep(.1)
        market_data = get_x_prices(tuple(listingsdf.iloc[index]), EXTENDED_OLDBASEURL)
        olddata.extend(market_data)

        market_data = get_x_prices(tuple(listingsdf.iloc[index]))
        newdata.extend(market_data)

    elif listingsdf["migration"].iloc[index] == "after":
        time.sleep(.1)
        market_data = get_x_prices(tuple(listingsdf.iloc[index]))
        newdata.extend(market_data)
try:
    olddata = [d for d in olddata if d["timestamp"] <= CUTOFF]
    newdata = [d for d in newdata if d["timestamp"] > CUTOFF]

except Exception as e:
    print(f"tohlcv_data.py: problem with data: {e}")

price_data = pd.DataFrame(olddata + newdata)

price_data = price_data.sort_values(
by=["asset", "timestamp"],
ascending=True,
).reset_index(drop=True)

price_data.to_csv("price_data.csv", index = False)
