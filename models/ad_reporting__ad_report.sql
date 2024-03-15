{% set enabled_packages = get_enabled_packages() %}
{{ config(enabled=is_enabled(enabled_packages),
    unique_key = ['source_relation','platform','date_day','ad_id','ad_group_id','campaign_id','account_id'],
    partition_by={
      "field": "date_day",
      "data_type": "date",
      "granularity": "day"
    }
    ) }}

with base as (

    select *
    from {{ ref('int_ad_reporting__ad_report') }}
),

aggregated as (
    
    select
        source_relation,
        date_day,
        platform,
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        ad_group_id,
        ad_group_name,
        ad_id,
        ad_name,
        sum(clicks) as clicks,
        sum(impressions) as impressions,
        sum(spend) as spend 
        
        {{ fivetran_utils.persist_pass_through_columns(pass_through_variable='ad_reporting__ad_passthrough_metrics', transform = 'sum') }}

    from base
    {{ dbt_utils.group_by(11) }}
)
,
-- video data integration 
    
all_data as(
select *
from aggregated

union all

select
'' as source_relation,
date(airing_data_aired_at_et) AS date_day,
'video' as platform,
cast(brand_data_id as string) as account_id,
brand_data_name as account_name,
'' as campaign_id,
'' as campaign_name,
cast(creative_data_spot_id as string) as ad_group_id,
spot_data_title_short as ad_group_name,
cast(spot_data_id as string) as ad_id,
spot_data_title_short as ad_name,
sum(CAST(0 as INT64)) as clicks,
sum(CAST(audience_data_impressions as INT64)) as impressions,
sum(CAST(airing_data_spend_estimated as FLOAT64)) as spend,
sum(CAST(0 as FLOAT64)) as conversions
from {{ ref('int_ispot_airings_joined') }}
group by 1,2,3,4,5,6,7,8,9,10,11

union all

SELECT 
source_relation
,date_day
,platform
,account_id
,account_name
,campaign_id
,campaign_name
,ad_group_id
,ad_group_name
,ad_id
,ad_name
,clicks
,impressions
,spend
,conversions    
from {{ ref('ttd_ads__ad_report') }}  

union all
select
source_relation,
date_day,
platform,
cast(account_id as string) as account_id,
account_name,
cast(campaign_id as string) as campaign_id,
campaign_name,
cast(ad_group_id as string) as ad_group_id,
ad_group_name,
cast(ad_id as string) as ad_id,
ad_name,
clicks,
impressions,
spend,
cast(conversions as FLOAT64) as  conversions
from {{ ref('nextdoor__ad_report') }}  
)

select *
from all_data

