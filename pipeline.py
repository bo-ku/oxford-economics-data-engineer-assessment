"""
Oxford Economics - Data Engineer Technical Assessment
AAPL stock price analysis using Python + pandas.
"""

import sqlite3
from pathlib import Path

import pandas as pd

from generate_report import generate_report


# ---------------------------------------------------------------------------
# Data Quality: In a production pipeline I'd build programmatic DQ checks
# before any transformation, things like asserting no nulls in critical
# columns, no zero-volume days (bad feed), no duplicate dates, and that
# High >= Low for every row. For a dataset this size I validated manually,
# but at scale these would gate the pipeline and alert on failure.
#
# Performance Tracking: Similarly, in a cloud environment (Lambda, Prefect
# tasks, etc.) I'd wrap pipeline steps in a lightweight decorator that logs
# wall-clock time and memory usage. Helps catch regressions.
# ---------------------------------------------------------------------------


def load_data(csv_path: Path) -> pd.DataFrame:
    """Load and prep the CSV."""
    df = pd.read_csv(csv_path, parse_dates=["Date"])
    df = df.sort_values("Date").reset_index(drop=True)
    df["Change"] = df["Close"] - df["Open"]
    return df


def load_to_sqlite(df: pd.DataFrame, db_path: Path) -> None:
    """Load into SQLite with idempotent upsert (INSERT OR REPLACE on PK)."""
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS aapl_stock_prices (
                trade_date  TEXT PRIMARY KEY,
                open_price  REAL NOT NULL,
                close_price REAL NOT NULL,
                high_price  REAL NOT NULL,
                low_price   REAL NOT NULL,
                volume      INTEGER NOT NULL
            )
        """)
        # Originally wrote this as a for loop with iterrows() + execute() per row.
        # Refactored to executemany + itertuples -- iterrows builds a full Series
        # object per row which is slow, itertuples gives lightweight namedtuples.
        # executemany also lets SQLite batch the inserts in a single call instead
        # of round-tripping per row. Doesn't matter at 89 rows, but it's the kind
        # of thing that bites you when the dataset grows.
        conn.executemany(
            "INSERT OR REPLACE INTO aapl_stock_prices VALUES (?,?,?,?,?,?)",
            [(row.Date.strftime("%Y-%m-%d"), round(row.Open, 2),
              round(row.Close, 2), round(row.High, 2),
              round(row.Low, 2), int(row.Volume))
             for row in df.itertuples()],
        )


def best_single_trade(df: pd.DataFrame) -> dict:
    """Find optimal buy/sell dates for max profit (single trade).

    Classic running-minimum approach -- track the lowest close seen so far,
    and at each day check if selling now beats our best profit.
    """
    # Same iterrows -> itertuples refactor as load_to_sqlite (see note there).
    best = {"profit": 0.0}
    first = df.iloc[0]
    min_price, min_date = first["Close"], first["Date"]

    for row in df.itertuples():
        if row.Close < min_price:
            min_price, min_date = row.Close, row.Date
        profit = row.Close - min_price
        if profit > best["profit"]:
            best = {"buy_date": min_date, "sell_date": row.Date,
                    "buy_price": round(min_price, 2),
                    "sell_price": round(row.Close, 2),
                    "profit": round(profit, 2)}
    return best


def greedy_trades(df: pd.DataFrame) -> list[dict]:
    """Greedy daily strategy: buy before every increase, sell before every drop.

    All positions must close. We look one day ahead -- if tomorrow's close
    is higher, we want to be holding. On state transitions we buy/sell.
    """
    trades = []
    holding = False
    buy_date = buy_price = None
    closes = df["Close"].tolist()
    dates = df["Date"].tolist()

    for i in range(len(closes) - 1):
        if not holding and closes[i + 1] > closes[i]:
            holding, buy_date, buy_price = True, dates[i], closes[i]
        elif holding and closes[i + 1] <= closes[i]:
            trades.append({"buy_date": buy_date, "sell_date": dates[i],
                           "buy_price": round(buy_price, 2),
                           "sell_price": round(closes[i], 2),
                           "return": round(closes[i] - buy_price, 2)})
            holding = False

    # Close any open position on the last day
    if holding:
        trades.append({"buy_date": buy_date, "sell_date": dates[-1],
                       "buy_price": round(buy_price, 2),
                       "sell_price": round(closes[-1], 2),
                       "return": round(closes[-1] - buy_price, 2)})
    return trades


def main() -> None:
    base = Path(__file__).parent
    csv_path = base / "data" / "aapl_stock_prices.csv"
    db_path = base / "data" / "aapl.db"
    report_path = base / "output" / "report.html"

    # Ingest
    df = load_data(csv_path)
    print(f"Loaded {len(df)} rows")

    # Load to SQLite (idempotent)
    load_to_sqlite(df, db_path)

    # Q1 & Q2: Largest increase / decrease
    inc = df.loc[df["Change"].idxmax()]
    dec = df.loc[df["Change"].idxmin()]
    print(f"\nQ1 - Largest increase: {inc['Date'].strftime('%Y-%m-%d')} "
          f"(Open ${inc['Open']:.2f} -> Close ${inc['Close']:.2f}, +${inc['Change']:.2f})")
    print(f"Q2 - Largest decrease: {dec['Date'].strftime('%Y-%m-%d')} "
          f"(Open ${dec['Open']:.2f} -> Close ${dec['Close']:.2f}, ${dec['Change']:.2f})")

    # Bonus Q1: Best single buy/sell
    bst = best_single_trade(df)
    print(f"\nBonus Q1 - Buy {bst['buy_date'].strftime('%Y-%m-%d')}, "
          f"Sell {bst['sell_date'].strftime('%Y-%m-%d')}, Profit +${bst['profit']:.2f}")

    # Bonus Q2: Greedy strategy
    trades = greedy_trades(df)
    total = sum(t["return"] for t in trades)
    print(f"Bonus Q2 - {len(trades)} trades, Total Return +${total:.2f}\n")
    print(f"{'Buy Date':<12} {'':>6} {'Price':>8}   {'Sell Date':<12} {'':>6} {'Price':>8}   {'Return':>8}")
    print("-" * 74)
    for t in trades:
        print(f"{t['buy_date'].strftime('%Y-%m-%d'):<12} {'BUY':>6} ${t['buy_price']:>7.2f}   "
              f"{t['sell_date'].strftime('%Y-%m-%d'):<12} {'SELL':>6} ${t['sell_price']:>7.2f}   "
              f"${t['return']:>7.2f}")

    # HTML report
    generate_report(df, inc, dec, bst, trades, report_path)
    print(f"\nReport: {report_path}")


if __name__ == "__main__":
    main()
