{{ config(
    materialized='table',
    alias='etf_ma_indicators',
    unique_key=['ts_code', 'trade_date']  
        ) }}

WITH daily_with_ma AS (
SELECT
ts_code,
trade_date,
close,
-- 使用窗口函数计算5日移动平均线
AVG(close) OVER (
PARTITION BY ts_code
ORDER BY trade_date ASC
ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
) AS ma_5_day,
-- 使用窗口函数计算10日移动平均线
AVG(close) OVER (
PARTITION BY ts_code
ORDER BY trade_date ASC
ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
) AS ma_10_day,
-- 使用窗口函数计算20日移动平均线
AVG(close) OVER (
PARTITION BY ts_code
ORDER BY trade_date ASC
ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
) AS ma_20_day,
-- 使用窗口函数计算60日移动平均线
AVG(close) OVER (
PARTITION BY ts_code
ORDER BY trade_date ASC
ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
) AS ma_60_day
FROM {{ source('main', 'etf_daily') }}
WHERE close IS NOT NULL 
)

SELECT
ts_code,
trade_date,
close,
ma_5_day,
ma_10_day,
ma_20_day,
ma_60_day
FROM daily_with_ma
ORDER BY
ts_code,
trade_date
