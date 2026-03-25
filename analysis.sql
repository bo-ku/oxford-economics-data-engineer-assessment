-- Oxford Economics - Data Engineer Technical Assessment
-- AAPL Stock Price Analysis
--
-- Uses the CSV column names directly (Date, Open, Close, High, Low, Volume)
-- so this can be pasted into SQLite Online after importing the CSV, or run
-- against the pipeline's database after the CREATE TABLE below.


-- DDL
-- PK on Date prevents duplicates. CHECK constraints catch bad data at the
-- database layer so we're not relying solely on upstream validation.
-- IF NOT EXISTS makes this safe to run whether or not the table already exists.

CREATE TABLE IF NOT EXISTS aapl_stock_prices (
    Date   TEXT    PRIMARY KEY,
    Open   REAL    NOT NULL CHECK(Open > 0),
    Close  REAL    NOT NULL CHECK(Close > 0),
    High   REAL    NOT NULL CHECK(High > 0),
    Low    REAL    NOT NULL CHECK(Low > 0),
    Volume INTEGER NOT NULL CHECK(Volume > 0)
);


-- Idempotent load pattern
-- INSERT OR REPLACE deduplicates on the PK, so re-running won't create
-- duplicate rows. In Snowflake this would be a MERGE statement. The actual
-- CSV -> SQLite load happens in pipeline.py; this is the pattern it uses:

-- INSERT OR REPLACE INTO aapl_stock_prices (Date, Open, Close, High, Low, Volume)
-- VALUES ('2025-08-15', 233.77, 231.37, 234.05, 229.12, 56038700);


-- Q1: Largest price increase from Open to Close

SELECT
    Date,
    Open,
    Close,
    ROUND(Close - Open, 2) AS price_change
FROM aapl_stock_prices
ORDER BY price_change DESC
LIMIT 1;


-- Q2: Largest price decrease from Open to Close

SELECT
    Date,
    Open,
    Close,
    ROUND(Close - Open, 2) AS price_change
FROM aapl_stock_prices
ORDER BY price_change ASC
LIMIT 1;


-- Bonus Q1: Best single buy/sell pair for max return
--
-- Running-minimum approach: for each day, track the lowest close seen so far
-- via a window function, then compute the profit of selling at today's close.
-- The correlated subquery finds which date actually had that min price.

WITH running_min AS (
    SELECT
        Date,
        Close,
        MIN(Close) OVER (
            ORDER BY Date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS min_price_so_far
    FROM aapl_stock_prices
),

profit_calc AS (
    SELECT
        Date       AS sell_date,
        Close      AS sell_price,
        min_price_so_far AS buy_price,
        ROUND(Close - min_price_so_far, 2) AS profit
    FROM running_min
)

SELECT
    (SELECT Date FROM aapl_stock_prices
     WHERE Close = pc.buy_price AND Date <= pc.sell_date
     ORDER BY Date LIMIT 1) AS buy_date,
    pc.sell_date,
    ROUND(pc.buy_price, 2)  AS buy_price,
    ROUND(pc.sell_price, 2) AS sell_price,
    pc.profit
FROM profit_calc pc
ORDER BY pc.profit DESC
LIMIT 1;


-- Bonus Q2: Greedy daily trading
--
-- Buy before every price increase, sell before every decrease. All positions
-- must close. The idea:
--   1. Use LEAD() to peek at tomorrow's close
--   2. If tomorrow > today -> we want to be holding (WANT_HOLD)
--   3. Use LAG() to detect state transitions -> those are our BUY/SELL events
--   4. Number buys and sells separately, then join to pair them into trades

WITH daily_signals AS (
    SELECT
        Date,
        Close,
        CASE
            WHEN LEAD(Close) OVER (ORDER BY Date) > Close
                THEN 'WANT_HOLD'
            ELSE 'WANT_FLAT'
        END AS signal
    FROM aapl_stock_prices
),

with_prev_signal AS (
    SELECT
        Date,
        Close,
        signal,
        LAG(signal, 1, 'WANT_FLAT') OVER (ORDER BY Date) AS prev_signal
    FROM daily_signals
),

actions AS (
    -- BUY on WANT_FLAT -> WANT_HOLD transitions
    -- SELL on WANT_HOLD -> WANT_FLAT transitions
    -- Force-close on the last day if still holding
    SELECT
        Date,
        Close,
        CASE
            WHEN prev_signal = 'WANT_FLAT' AND signal = 'WANT_HOLD' THEN 'BUY'
            WHEN prev_signal = 'WANT_HOLD' AND signal = 'WANT_FLAT' THEN 'SELL'
            WHEN prev_signal = 'WANT_HOLD' AND signal = 'WANT_HOLD'
                 AND Date = (SELECT MAX(Date) FROM aapl_stock_prices)
                THEN 'SELL'
            ELSE NULL
        END AS action
    FROM with_prev_signal
),

trade_actions AS (
    SELECT
        Date,
        Close,
        action,
        SUM(CASE WHEN action = 'BUY'  THEN 1 ELSE 0 END)
            OVER (ORDER BY Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS buy_num,
        SUM(CASE WHEN action = 'SELL' THEN 1 ELSE 0 END)
            OVER (ORDER BY Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sell_num
    FROM actions
    WHERE action IS NOT NULL
),

buys AS (
    SELECT Date AS buy_date, Close AS buy_price, buy_num AS trade_num
    FROM trade_actions WHERE action = 'BUY'
),

sells AS (
    SELECT Date AS sell_date, Close AS sell_price, sell_num AS trade_num
    FROM trade_actions WHERE action = 'SELL'
)

SELECT
    b.buy_date,
    'BUY'  AS buy_action,
    ROUND(b.buy_price, 2) AS buy_price,
    s.sell_date,
    'SELL' AS sell_action,
    ROUND(s.sell_price, 2) AS sell_price,
    ROUND(s.sell_price - b.buy_price, 2) AS trade_return
FROM buys b
JOIN sells s ON b.trade_num = s.trade_num
ORDER BY b.buy_date;
