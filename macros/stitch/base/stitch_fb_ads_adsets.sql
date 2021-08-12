{% macro stitch_fb_ads_adsets() %}

    {{ adapter.dispatch('stitch_fb_ads_adsets','facebook_ads')() }}

{% endmacro %}


{% macro athena__stitch_fb_ads_adsets() %}

select

    id as adset_id,
    nullif(name,'') as name,
    nullif(account_id,'') as account_id,
    nullif(campaign_id,'') as campaign_id,
    case
        when typeof(created_time) = 'varchar' then cast(from_iso8601_timestamp(created_time) as timestamp) 
        else cast(created_time as timestamp)
    end as created_time,
    nullif(effective_status,'') as effective_status
    
from {{ var('adsets_table') }}

{% endmacro %}