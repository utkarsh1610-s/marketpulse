{{ config(materialized='table') }}

select
    ticker,
    count(*) as total_anomalies_7d,
    max(detected_at) as last_seen,
    round(avg(confidence), 4) as avg_confidence,
    approx_top_count(signal_type, 1)[offset(0)].value as dominant_signal
from {{ source('marketpulse_gold', 'anomalies') }}
where detected_at >= timestamp_sub(current_timestamp(), interval 7 day)
group by ticker