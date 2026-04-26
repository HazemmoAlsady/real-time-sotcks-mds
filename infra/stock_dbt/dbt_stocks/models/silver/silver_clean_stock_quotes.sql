select 
    symbol,
    current_price, 
    ROUND(day_high, 2) AS day_high,
    ROUND(day_low, 2) AS day_low,
    ROUND(day_open, 2) AS day_open,
    ROUND(prev_close, 2) AS prev_close,
    ROUND(change_amount, 2) AS change_amount,
    ROUND(change_percent, 4) AS change_percent,
    market_timestamp,
    fetched_at
from {{ ref('bronze_stg_stock_quotes') }}
where current_price is not null