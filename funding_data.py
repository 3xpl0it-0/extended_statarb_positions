"""getting funding data for each ticker"""

# notification of entry

print(">>> entering funding_data.py <<<")

# importing packages

import time

import pandas as pd
import requests

# importing data

from asset_listing import listingsdf

# defining constants

CUTOFF = int(1754956800 * 1000)  # 12 august 2025 - when migration started
EXTENDED_OLDBASEURL = "https://api.extended.exchange/"
EXTENDED_NEWBASEURL = "https://api.starknet.extended.exchange/"
EXTENDED_FUNDINGURL = "api/v1/info/{urlsymbol}/funding"
ENDTIME = int(time.time() * 1000)

# function for getting data

def get_funding_rates(
        ticker_info: tuple | None = None,
        baseurl: str | None = None,
        end_time : int | None = None,
        fundingurl: str | None = None,
        ) -> list[dict]:
    """get funding paid"""

    if ticker_info:
        _ticker_info = ticker_info
    else:
        raise ValueError("funding_data.py: ticker info is required in get_funding_rates")
    _baseurl = baseurl or EXTENDED_NEWBASEURL
    _end_time = end_time or ENDTIME
    _fundingurl = fundingurl or EXTENDED_FUNDINGURL

    all_data = []

    try:
        url_request = f"{_baseurl}{_fundingurl.format(urlsymbol=_ticker_info[0])}"
        cursor = None
        limit = 5000

        while True:
            params = {
                "startTime": _ticker_info[2],
                "endTime": _end_time,
                "limit": limit,
            }

            if cursor:
                params["cursor"] = cursor

            response = requests.get(url_request, params=params, timeout = 10)
            response.raise_for_status()
            data = response.json()

            batch = data.get("data", [])
            all_data.extend(batch)

            pagination = data.get("pagination", {})
            cursor = pagination.get("cursor")
            count = pagination.get("count", 0)

            if count < limit:  # last page
                break

    except requests.exceptions.RequestException as e:
        print(f"funding_data.py: Warning: API request failed for funding with {_ticker_info[0]}: {e}")

    if not all_data:
        print(f"funding_data.py: problem with {_ticker_info[0]} wrt getting funding data")
        return []

    _all_data = []
    for row in all_data:
        _all_data.append({
        "timestamp": int(row["T"]),
        "asset":  row["m"],
        "funding": float(row["f"]),
        })

    return _all_data

olddata = []
newdata = []

for index in listingsdf.index:
    if listingsdf["migration"].iloc[index] == "before":

        time.sleep(.1)
        market_data = get_funding_rates(tuple(listingsdf.iloc[index]), EXTENDED_OLDBASEURL)
        olddata.extend(market_data)

        market_data = get_funding_rates(tuple(listingsdf.iloc[index]))
        newdata.extend(market_data)

    elif listingsdf["migration"].iloc[index] == "after":
        time.sleep(.1)
        market_data = get_funding_rates(tuple(listingsdf.iloc[index]))
        newdata.extend(market_data)
try:
    olddata = [d for d in olddata if d["timestamp"] <= CUTOFF]
    newdata = [d for d in newdata if d["timestamp"] > CUTOFF]
except Exception as e:
    print(f"funding_data.py: problem with data: {e}")

funding_data = pd.DataFrame(olddata + newdata)
funding_data = funding_data.sort_values(
by=["asset", "timestamp"],
ascending=True,
).reset_index(drop=True)

funding_data.to_csv("funding_data.csv", index = False)
