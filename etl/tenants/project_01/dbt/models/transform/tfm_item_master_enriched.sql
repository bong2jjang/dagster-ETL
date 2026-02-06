-- transform: 아이템 마스터 Enriched
-- 우선순위 분류, 생산 여부 등 비즈니스 로직 추가

with staged as (
    select * from {{ ref('stg_cfg_item_master') }}
),

enriched as (
    select
        *,
        case
            when item_priority >= 8 then 'HIGH'
            when item_priority >= 4 then 'MEDIUM'
            else 'LOW'
        end as priority_category,
        case
            when prod_type is not null and prod_type != '' then true
            else false
        end as is_manufactured,
        current_timestamp as dbt_loaded_at
    from staged
)

select * from enriched
