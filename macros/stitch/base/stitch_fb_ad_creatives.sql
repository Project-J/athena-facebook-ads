{% macro stitch_fb_ad_creatives() %}

    {{ adapter.dispatch('stitch_fb_ad_creatives', 'facebook_ads')() }}

{% endmacro %}


{% macro athena__stitch_fb_ad_creatives() %}

with base as (

    select * from {{ var('ad_creatives_table') }}

),

child_links as (

    select * from {{ ref('fb_ad_creatives__child_links') }}

),

links_joined as (

    select

        id as creative_id,

        lower(coalesce(
          nullif(child_link, ''),
          nullif({{ facebook_ads.nested_field('base.object_story_spec', ['link_data', 'call_to_action', 'value', 'link']) }}, ''),
          nullif({{ facebook_ads.nested_field('base.object_story_spec', ['video_data', 'call_to_action', 'value', 'link']) }}, ''),
          nullif({{ facebook_ads.nested_field('base.object_story_spec', ['link_data', 'link']) }}, '')
        )) as url,

        url_tags

    from base
    left join child_links
        on base.id = child_links.creative_id

),

links_tags_joined as (

    select 

        creative_id,

        url,

        lower(coalesce(
            nullif(url_tags, {{ dbt_utils.split_part('url', "'?'", 2) }}), '')
        ) as url_tags

    from links_joined

),

parsed as (

    select

        links_tags_joined.*,
        {{ dbt_utils.split_part('url', "'?'", 1) }} as base_url,
        {{ dbt_utils.get_url_host('url') }} as url_host,
        {{ dbt_utils.concat(["'/'", dbt_utils.get_url_path('url')]) }} as url_path,
        {{ facebook_ads.get_url_parameter() }}

    from links_tags_joined

)

select * from parsed

{% endmacro %}
