with campaigns_brand as (
    select * from {{ ref('stg_ironscales_campaigns_brand_table')}}
    where
    date(_RIVERY_LAST_UPDATE) = current_date()
),

campaigns_company as (
    select * from {{ ref('stg_ironscales_campaigns_company_table')}}
    where
    date(_RIVERY_LAST_UPDATE) = current_date()
),

higher_tree as (
                with recursive tree as (
                                    select
                                    '' as indent,
                                    id as tenant_id,
                                    null as parent_id,
                                    null as parent_name,
                                    name as tenant_name,
                                    domain as domain,
                                    last_action_date as last_action,
                                    registration_date as registration_date,
                                    approved as approved,
                                        -- ironscales_us_db.rr_prod_sch.tkey_fn(id) as tree_key
                                    SUBSTRING('XXXXXXX' || id::VARCHAR, -7) || ' ' as tree_key 
                                    from campaigns_company
                                    where
                                    brand_id is null
                                    union all
                                    select
                                    indent || '-> ',
                                    c.id,
                                    b.owner_id,
                                    b.name,
                                    c.name,
                                    c.domain,
                                    c.last_action_date,
                                    c.registration_date,
                                    c.approved,
                                    tree_key || SUBSTRING('XXXXXXX' || c.id::VARCHAR, -7) || ' '
                                    from
                                    campaigns_brand b
                                    join campaigns_company c on b.id = c.brand_id
                                    join tree on b.owner_id = tree.tenant_id
                                    )
                select
                indent || tenant_name as indented_name,
                tenant_id,
                parent_id,
                parent_name,
                tenant_name,
                domain,
                last_action,
                registration_date,
                approved,
                tree_key
            from
                tree
            )

select
concat(
    'US-',
    split_part(split_part(tree_key, ' ', 1), 'X', -1)
) as root,
(regexp_count(tree_key, ' ') -1) as depth,
iff(
    depth > 0,
    concat(
        'US-',
        split_part(split_part(tree_key, ' ', -3), 'X', -1)
    ),
    null
) as parent_global_id,
concat(
    'US-',
    split_part(split_part(tree_key, ' ', -2), 'X', -1)
) as tenant_global_id,
indented_name,
tenant_id,
parent_id,
parent_name,
tenant_name,
domain,
last_action,
registration_date,
approved,
tree_key
from higher_tree