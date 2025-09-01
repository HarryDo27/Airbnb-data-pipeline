{{
    config(
        unique_key='property_id',
        alias='g_dim_property'
    )
}}

SELECT *
FROM {{ ref('property_snapshot') }}
