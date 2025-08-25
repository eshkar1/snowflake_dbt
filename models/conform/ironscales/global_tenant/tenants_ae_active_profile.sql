with profiles_profile as (
    select * from {{ ref('stg_ironscales_ae_profiles_profile_table')}}
    where
    date(_RIVERY_LAST_UPDATE) = current_date()
),

campaigns_companylicense as (
    select * from {{ ref('stg_ironscales_ae_campaigns_companylicense_table')}}
    where
    date(_RIVERY_LAST_UPDATE) = current_date()
)


select
    concat('AE-', p.company_id) as tenant_global_id,
    count(p.company_id) as active_profiles
from
    profiles_profile p
    left join campaigns_companylicense c on p.company_id = c.id
where
    p.active = true
group by
    p.company_id