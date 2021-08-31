{% macro stitch_fb_ad_insights() %}

    {{ adapter.dispatch('stitch_fb_ad_insights', 'facebook_ads')() }}

{% endmacro %}


{% macro athena__stitch_fb_ad_insights() %}

with ads_insights_table as (
    
    select * from {{ var('ads_insights_table') }}
    
),

latest_fb_ads_insights as (

    select 
    
        date_start,
        ad_id,

        max(_sdc_sequence) AS sequence,
        max(_sdc_batched_at) as batched_at

    from ads_insights_table
    group by 1,2

),

fb_ads_insights as (

    select

        cast(from_iso8601_timestamp(ads_insights_table.date_start) as date) as date_day,
        nullif(account_id,'') as account_id,
        nullif(account_name,'') as account_name,
        nullif(ads_insights_table.ad_id,'') as ad_id,
        nullif(adset_id,'') as adset_id,
        nullif(campaign_id,'') as campaign_id,
        impressions,
        clicks,
        unique_clicks,
        spend,
        frequency,
        reach,
        nullif(objective,'') as objective,
        canvas_avg_view_percent,
        canvas_avg_view_time,
        inline_link_clicks,
        inline_post_engagement,
        unique_inline_link_clicks

    from ads_insights_table
    inner join latest_fb_ads_insights

        on ads_insights_table.date_start = latest_fb_ads_insights.date_start
        and ads_insights_table.ad_id = latest_fb_ads_insights.ad_id
        and ads_insights_table._sdc_sequence = latest_fb_ads_insights.sequence
        and ads_insights_table._sdc_batched_at = latest_fb_ads_insights.batched_at

)

select * from fb_ads_insights


{% endmacro %}
