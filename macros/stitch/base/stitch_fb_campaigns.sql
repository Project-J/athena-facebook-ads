{% macro stitch_fb_ads_campaigns() %}

    {{ adapter.dispatch('stitch_fb_ads_campaigns', 'facebook_ads')() }}

{% endmacro %}

{% macro athena__stitch_fb_ads_campaigns() %}

with campaigns_table as (
    
    select * from {{ var('campaigns_table') }}
    
),

latest_campaigns as (

    select 

        id,

        max(_sdc_sequence) AS sequence,
        max(_sdc_batched_at) as batched_at

    from campaigns_table
    group by 1

),

fb_campaigns as (

    select

    nullif(campaigns_table.id,'') as campaign_id,
    nullif(name,'') as name

    from campaigns_table
    inner join latest_campaigns

        on campaigns_table.id = latest_campaigns.id
        and campaigns_table._sdc_sequence = latest_campaigns.sequence
        and campaigns_table._sdc_batched_at = latest_campaigns.batched_at

)

select * from fb_campaigns

{% endmacro %}
