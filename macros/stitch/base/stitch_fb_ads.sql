{% macro stitch_fb_ads() %}

    {{ adapter.dispatch('stitch_fb_ads', 'facebook_ads')() }}

{% endmacro %}

--there are multiple records for different statuses, but we don't need them for our purposes yet.
--if this table needs to be expended to include multiple rows per id, this will break downstream queries that depend
--on uniqueness on this id when joins are done.

{% macro athena__stitch_fb_ads() %}

with ads_table as (
    
    select * from {{ var('ads_table') }}
    
),

latest_fb_ads as (

    select 
    
        id,

        max(_sdc_sequence) AS sequence,
        max(_sdc_batched_at) as batched_at

    from ads_table
    group by 1

),

fb_ads as (
    select distinct

        nullif(ads_table.id,'') as ad_id,
        nullif(account_id,'') as account_id,
        nullif(adset_id,'') as adset_id,
        nullif(campaign_id,'') as campaign_id,
        nullif(name,'') as name,
        nullif({{ facebook_ads.nested_field('creative',['id']) }}, '') as creative_id,
        case
            when typeof(created_time) = 'varchar' then cast(from_iso8601_timestamp(created_time) as timestamp)
            else cast(created_time as timestamp)
        end as created_at,
        case
            when typeof(updated_time) = 'varchar' then cast(from_iso8601_timestamp(updated_time) as timestamp)
            else cast(updated_time as timestamp)
        end as updated_at

    from ads_table
    inner join latest_fb_ads

        on ads_table.id = latest_fb_ads.id
        and ads_table._sdc_sequence = latest_fb_ads.sequence
        and ads_table._sdc_batched_at = latest_fb_ads.batched_at

)

select * from fb_ads

{% endmacro %}
