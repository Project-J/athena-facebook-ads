-- There are cases where ads can have multiple links attached to them.
-- This model was created to extract all the child links and return the first
-- instance where a URL contains utm parameters.


{% macro stitch_fb_ad_creatives__child_links() %}

    {{ adapter.dispatch('stitch_fb_ad_creatives__child_links', 'facebook_ads')() }}

{% endmacro %}

{% macro default__stitch_fb_ad_creatives__child_links() %}


with base as (

    select * from {{ var('ad_creatives__child_links_table') }}

),

child_attachment_links as (

    select

        id as creative_id,
        _sdc_batched_at,
        link as child_link

    from base
    where lower(cast(link as varchar)) like '%utm%'

),

aggregated as (

    select distinct

        creative_id,
        first_value(child_link) over (
            partition by creative_id
            order by _sdc_batched_at
            rows between unbounded preceding and unbounded following
        ) as child_link

    from child_attachment_links

)

select * from aggregated

{% endmacro %}