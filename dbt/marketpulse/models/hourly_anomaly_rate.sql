{{ config(materialized='table') }}

select
    date(detected_at) as anomaly_date,
    extract(hour from detected_at) as hour,
    ticker,
    signal_type,
    count(*) as anomaly_count,
    round(avg(confidence), 4) as avg_confidence
from {{ source('marketpulse_gold', 'anomalies') }}
where ticker is not null and signal_type is not null
group by 1, 2, 3, 4
order by 1 desc, 2, 5 desc