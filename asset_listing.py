"""getting symbol names, listing time and if listing was before the migration"""

# notification of entry

print(">>> entering asset_listing.py <<<")

# importing packages

import pandas as pd
import requests

import time

# defining constants

CUTOFF = 1754956800 * 1000  # 12 august 2025 - when migration started
EXTENDED_OLDBASEURL = "https://api.extended.exchange/"
EXTENDED_NEWBASEURL = "https://api.starknet.extended.exchange/"
EXTENDED_MARKETURL = "api/v1/info/markets"
EXTENDED_CANDLEURL = "api/v1/info/candles/{urlsymbol}/trades"
TIME_INTERVAL = "PT24H"
DATA_BEGIN = int(time.time() * 1000 - 86400000 * 60)

# functions to pull data

def get_all_markets(
        newbaseurl: str | None = None,
        marketurl: str | None = None,
) -> dict[str, str]:
    """get all actively traded non-tradfi symbol names"""

    _newbaseurl = newbaseurl or EXTENDED_NEWBASEURL
    _marketurl = marketurl or EXTENDED_MARKETURL

    newurl = f"{_newbaseurl}{_marketurl}"
    response = requests.get(newurl, timeout=10)
    response.raise_for_status()
    data = response.json()
    assets = [market for market in data["data"] if market["status"] == "ACTIVE"]

    sorting = {asset["name"]: asset["category"] for asset in assets}
    sorting = {
        asset["name"]: asset["category"]
        for asset in assets
        if asset["category"] != "TradFi"
    }

    assets = sorting

    return assets

def get_listing_date(
        ticker: str | None = None,
        tickertype: str | None = None,
        time_interval: str | None = None,
        oldbaseurl: str | None = None,
        newbaseurl: str | None = None,
        candleurl: str | None = None,
        cutoff: int | None = None,
        begin: int | None = None,
        ) -> tuple[str, str, int, str]:
    """get earliest recorded timestamp of asset"""

    if ticker:
        _ticker = ticker
    else:
        raise ValueError("asset_listing.py: ticker is required in get_listing_date")
    if tickertype:
        _tickertype = tickertype
    else:
        raise ValueError("asset_listing.py: ticker type is required in get_listing_date")
    _time_interval = time_interval or TIME_INTERVAL
    _oldbaseurl = oldbaseurl or EXTENDED_OLDBASEURL
    _newbaseurl = newbaseurl or EXTENDED_NEWBASEURL
    _candleurl = candleurl or EXTENDED_CANDLEURL
    _cutoff = cutoff or CUTOFF
    _begin = begin or DATA_BEGIN

    timelist = []
    count = 0

    while count < 2:

        try:
            if count == 0:
                url_request = f"{_oldbaseurl}{_candleurl.format(urlsymbol=_ticker)}"
            elif count == 1:
                url_request = f"{_newbaseurl}{_candleurl.format(urlsymbol=_ticker)}"

            params = {"interval": _time_interval, "limit": 10000}
            response = requests.get(url_request, params=params, timeout = 10)
            response.raise_for_status()

            candles = response.json()["data"]
            candles = candles[::-1]

            if candles:
                c_time = candles[0]["T"]
            else:
                c_time = "None"

            timelist.append(c_time)

            count = count + 1

        except requests.exceptions.RequestException as e:
            if count == 0:
                print(f"asset_listing.py: Warning: Old API request failed for {_ticker}: {e}")
                timelist.append("None")
            elif count == 1:
                print(f"asset_listing.py: Warning: New API request failed for {_ticker}: {e}")
                timelist.append("None")

            count = count + 1

    if timelist[0] != "None" and timelist[1] != "None":
        _time = int(min(timelist[0], timelist[1]))
    elif timelist[0]  != "None" and timelist[1] == "None":
        _time = int(timelist[0])
    elif timelist[1]  != "None" and timelist[0] == "None":
        _time = int(timelist[1])
    else:
        print(f"asset_listing.py: no listing time recorded for {_ticker}")
        _time = None
        return (_ticker, _tickertype, _time, "data_problem")

    before_after = None

    if _time > _cutoff:
        before_after = "after"
    else:
        before_after = "before"

    return (_ticker, _tickertype, max(_begin, _time), before_after)

all_markets = get_all_markets()

listings = []

for market, market_type in all_markets.items():

    time.sleep(.1)
    listing = get_listing_date(market, market_type)
    listings.append(listing)

listingsdf = pd.DataFrame(listings, columns = ["ticker", "ticker_type", "begin", "migration"])

print(listingsdf.head(20))
