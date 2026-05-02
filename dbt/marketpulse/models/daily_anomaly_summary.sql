{{ config(materialized='table') }}

select
    date(detected_at) as anomaly_date,
    ticker,
    signal_type,
    count(*) as anomaly_count,
    round(avg(confidence), 4) as avg_confidence,
    round(max(confidence), 4) as max_confidence
from {{ source('marketpulse_gold', 'anomalies') }}
where ticker is not null and signal_type is not null
group by 1, 2, 3
order by 1 desc, 4 desc