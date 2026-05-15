{% snapshot fuel_prices_snapshot %}

{{
    config(
        target_schema   = 'DBT',
        target_database = 'USER_DB_FERRET',
        unique_key      = ['WEEK_DATE', 'REGION'],
        strategy        = 'timestamp',
        updated_at      = 'LOAD_TS',
        invalidate_hard_deletes = True,
    )
}}

SELECT
    WEEK_DATE,
    REGION,
    REGULAR_GASOLINE_PRICE,
    MIDGRADE_GASOLINE_PRICE,
    PREMIUM_GASOLINE_PRICE,
    DIESEL_PRICE,
    PRICE_UNIT,
    SOURCE,
    LOAD_TS
FROM {{ source('raw', 'fuel_prices') }}

{% endsnapshot %}
