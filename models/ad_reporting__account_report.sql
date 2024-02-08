{% set enabled_packages = get_enabled_packages() %}
{{ config(enabled=is_enabled(enabled_packages),
    unique_key = ['source_relation','platform','date_day','account_id'],
    partition_by={
      "field": "date_day",
      "data_type": "date",
      "granularity": "day"
    }
) }}

with base as (

    select *
    from {{ ref('int_ad_reporting__account_report') }}
),

aggregated as (
    
    select
        source_relation,
        date_day,
        platform,
        account_id,
        account_name,
        sum(clicks) as clicks,
        sum(impressions) as impressions,
        sum(spend) as spend 

        {{ fivetran_utils.persist_pass_through_columns(pass_through_variable='ad_reporting__account_passthrough_metrics', transform = 'sum') }}

    from base
    {{ dbt_utils.group_by(5) }}
),
-- Video data integration
    
all_data as(
select *
from aggregated

union all


select
'' as source_relation,
date(airing_data_aired_at_et) AS date_day,
'video' as  platform ,
CAST(brand_data_id as string) as account_id,
brand_data_name as account_name,
sum(CAST(0 as INT64)) as clicks,
sum(CAST(audience_data_impressions as INT64)) as impressions,
sum(CAST(airing_data_spend_estimated as FLOAT64)) as spend,
sum(CAST(0 as FLOAT64)) as conversions
from {{ ref('int_ispot_airings_joined') }}
group by 1,2,3,4,5

union all

SELECT 
source_relation
,date_day
,platform
,account_id
,account_name
,clicks
,impressions
,spend
,conversions    
from {{ ref('ttd_ads__account_report') }}
    
)
select *
from all_data
