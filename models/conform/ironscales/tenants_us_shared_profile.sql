with profiles_profile as (
    select * from {{ ref('stg_ironscales_profiles_profile_table')}}
    where
    date(_RIVERY_LAST_UPDATE) = current_date()
),

profiles_profile_tags as (
    select * from {{ ref('stg_ironscales_profiles_profile_tags_table')}}
    where
    date(_RIVERY_LAST_UPDATE) = current_date()
),

profiles_tag as (
    select * from {{ ref('stg_ironscales_profiles_tag_table')}}
    where
    date(_RIVERY_LAST_UPDATE) = current_date()
)


select
    p.tenant_global_id,
    count(p.tenant_global_id) as shared_profiles
from
    profiles_profile p
    left join profiles_profile_tags t on p.id = t.profile_id
    left join profiles_tag g on t.tag_id = g.id
where
    g.auto_tag_type_name = 'SHARED_MAILBOX_TAG'
    and p.active = true
group by
    p.tenant_global_id