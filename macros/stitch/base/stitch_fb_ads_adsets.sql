{% macro stitch_fb_ads_adsets() %}

    {{ adapter.dispatch('stitch_fb_ads_adsets','facebook_ads')() }}

{% endmacro %}


{% macro athena__stitch_fb_ads_adsets() %}

with adsets_table as (
    
    select * from {{ var('adsets_table') }}
    
),

latest_adsets as (

    select 

        id,

        max(_sdc_sequence) AS sequence,
        max(_sdc_batched_at) as batched_at

    from adsets_table
    group by 1

),

fb_adsets as (

    select

    adsets_table.id as adset_id,
    nullif(name,'') as name,
    nullif(account_id,'') as account_id,
    nullif(campaign_id,'') as campaign_id,
    case
        when typeof(created_time) = 'varchar' then cast(from_iso8601_timestamp(created_time) as timestamp) 
        else cast(created_time as timestamp)
    end as created_time,
    nullif(effective_status,'') as effective_status

    from adsets_table
    inner join latest_adsets

        on adsets_table.id = latest_adsets.id
        and adsets_table._sdc_sequence = latest_adsets.sequence
        and adsets_table._sdc_batched_at = latest_adsets.batched_at

)

select * from fb_adsets

{% endmacro %}