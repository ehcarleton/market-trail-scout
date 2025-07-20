from sklearn.linear_model import LinearRegression
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def fit_trendlines_for_symbol(df: pd.DataFrame, symbol: str, plot=False) -> dict:
    df = df[df["symbol"] == symbol].copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df["Ordinal"] = df["Date"].map(pd.Timestamp.toordinal)

    highs = df[df["Swing_Type"] == 1]
    lows = df[df["Swing_Type"] == -1]

    result = {"symbol": symbol}

    if len(highs) >= 2:
        X_high = highs["Ordinal"].values.reshape(-1, 1)
        y_high = highs["Close"].values
        high_model = LinearRegression().fit(X_high, y_high)
        result["resistance_slope"] = high_model.coef_[0]
        result["resistance_intercept"] = high_model.intercept_
    else:
        result["resistance_slope"] = None

    if len(lows) >= 2:
        X_low = lows["Ordinal"].values.reshape(-1, 1)
        y_low = lows["Close"].values
        low_model = LinearRegression().fit(X_low, y_low)
        result["support_slope"] = low_model.coef_[0]
        result["support_intercept"] = low_model.intercept_
    else:
        result["support_slope"] = None

    if plot:
        plt.figure(figsize=(10, 6))
        plt.plot(df["Date"], df["Close"], "o", label="Swing Points")
        if len(highs) >= 2:
            xfit = np.linspace(df["Ordinal"].min(), df["Ordinal"].max(), 100).reshape(-1, 1)
            yfit_high = high_model.predict(xfit)
            plt.plot([pd.Timestamp.fromordinal(int(x)) for x in xfit.flatten()], yfit_high, label="Resistance", linestyle="--")
        if len(lows) >= 2:
            yfit_low = low_model.predict(xfit)
            plt.plot([pd.Timestamp.fromordinal(int(x)) for x in xfit.flatten()], yfit_low, label="Support", linestyle="--")
        plt.title(f"{symbol} Swing Trendlines")
        plt.legend()
        plt.grid(True)
        plt.show()

    return result
