"""get signals from data and turn to position sizes for trading"""

# import packages and data

from custom_data import df

import pandas as pd
import math
import numpy as np

#df = pd.read_csv("trade_data.csv")

# define constants
RETURNS_LOOKBACK = 30
ANNUAL_VOLATILITY = 0.5
TARGET_VOLATILITY = ANNUAL_VOLATILITY / math.sqrt(365)

# making time readable
df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

# getting returns and return and funding volatility

# asset retuns
df["returns"] = df.groupby("asset")["close"].transform(lambda x: x.pct_change())

# asset returns inclusive of funding
df["returns_funding"] = df["returns"] - df["funding"]

# asset returns and funding volatility
df["rolling_returns_funding_vol"] = (
    df.groupby("asset")["returns_funding"]
    .rolling(window=RETURNS_LOOKBACK)
    .std()
    .reset_index(level=0, drop=True)
)

# create dollar volume
df["dollar_volume"] = df["volume"] * df["close"]

# rolling median volume
df["rolling_volume"] = (
    df.groupby("asset")["dollar_volume"]
    .rolling(window=RETURNS_LOOKBACK)
    .median()
    .reset_index(level=0, drop=True)
)

# creating funding signals

for i in [3, 5, 10, 15, 25, 40, 55]:
    df[f"funding_sig_1v{i}"] = (
        df.groupby("asset")["funding"]
        .rolling(window=i)
        .mean()
        .reset_index(level=0, drop=True)
    )

    df[f"funding_sig_2v{i}"] = df["funding"] - df[f"funding_sig_1v{i}"]

df = df.drop(columns=["funding_sig_1v40", "funding_sig_1v55"])

# scaling returns and funding by volatility
df["returns_funding_volscaled"] = df["returns_funding"] * (
    TARGET_VOLATILITY / df["rolling_returns_funding_vol"]
)

# editing to remove final day and values that wont have complete data
df = df[df.groupby("asset").cumcount() > 55].copy()
df = df.groupby("asset").head(-1).copy()

# check if missing values remaining
nan_rows = df.isna().any(axis=1)
rows_with_nan = df[nan_rows]
print(f"rows with nan: {rows_with_nan}")

corr_signals = [
    "funding_sig_1v15",
    "funding_sig_1v10",
    "funding_sig_2v40",
    "funding_sig_2v25",
    "funding_sig_2v55",
    "funding_sig_2v40",
]

df["avg_signal"] = 0

df["avg_signal"] = df[corr_signals].mean(axis=1)

# create deciles
df["avgsignal_decile"] = 0

# per day deciles across all assets
df["avgsignal_decile"] = df.groupby("timestamp")["avg_signal"].transform(
    lambda x: pd.qcut(x.rank(method="first"), 10, labels=False)
)

df["volume_decile"] = 0

# per day deciles across all assets
df["volume_decile"] = df.groupby("timestamp")["rolling_volume"].transform(
    lambda x: pd.qcut(x.rank(method="first"), 3, labels=False)
)

df = df[df["volume_decile"] != 2].copy()

trade_df = df.loc[df.groupby("asset")["timestamp"].idxmax()]
last_day = trade_df["timestamp"].dt.normalize().max()
trade_df = trade_df[trade_df["timestamp"].dt.normalize() == last_day].copy()

pos_list = []

for ticker in trade_df["asset"].unique():
    if trade_df[trade_df["asset"] == ticker]["avgsignal_decile"].iloc[0] <= 3:
        pos_list.append(
            [
                trade_df[trade_df["asset"] == ticker][
                    "timestamp"
                ].iloc[0],
                ticker,
                "long",
                TARGET_VOLATILITY
                / trade_df[trade_df["asset"] == ticker][
                    "rolling_returns_funding_vol"
                ].iloc[0],
            ]
        )

    elif (5 <= trade_df[trade_df["asset"] == ticker]["avgsignal_decile"].iloc[0] <= 8):
        pos_list.append(
            [
                trade_df[trade_df["asset"] == ticker][
                    "timestamp"
                ].iloc[0],
                ticker,
                "short",
                TARGET_VOLATILITY
                / trade_df[trade_df["asset"] == ticker][
                    "rolling_returns_funding_vol"
                ].iloc[0],
            ]
        )

pos_df = pd.DataFrame(pos_list, columns=["timestamp", "asset", "trade_direction", "allocation"])
print(pos_df)
total_allocation = len(pos_df["asset"])

for index in pos_df["asset"].index:
    print(
        f"ticker: {pos_df['asset'].iloc[index]} | trade: {pos_df['trade_direction'].iloc[index]} | size: {pos_df['allocation'].iloc[index]} | adjusted size: {pos_df['allocation'].iloc[index] * 1 / total_allocation}"
    )

