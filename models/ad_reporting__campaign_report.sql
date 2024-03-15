{% set enabled_packages = get_enabled_packages() %}
{{ config(enabled=is_enabled(enabled_packages),
    unique_key = ['source_relation','platform','date_day','campaign_id','account_id'],
    partition_by={
      "field": "date_day",
      "data_type": "date",
      "granularity": "day"
    }
    ) }}

with base as (

    select *
    from {{ ref('int_ad_reporting__campaign_report') }}
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
        sum(clicks) as clicks,
        sum(impressions) as impressions,
        sum(spend) as spend 

        {{ fivetran_utils.persist_pass_through_columns(pass_through_variable='ad_reporting__campaign_passthrough_metrics', transform = 'sum') }}

    from base
    {{ dbt_utils.group_by(7) }}
),
    
all_data as(
select *
from aggregated

union all

SELECT 
source_relation
,date_day
,platform
,account_id
,account_name
,campaign_id
,campaign_name
,clicks
,impressions
,spend
,conversions    
from {{ ref('ttd_ads__campaign_report') }}   

union all
select
source_relation,
date_day,
platform,
cast(account_id as string) as account_id,
account_name,
cast(campaign_id as string) as campaign_id,
campaign_name,
clicks,
impressions,
spend,
cast(conversions as FLOAT64) as  conversions
from {{ ref('nextdoor__campaign_report') }}   
    
)

Select * from all_data
