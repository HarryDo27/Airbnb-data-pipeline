{{
    config(
        unique_key='host_hash',
        alias='g_dim_host'
    )
}}


SELECT * FROM {{ ref('host_snapshot') }}