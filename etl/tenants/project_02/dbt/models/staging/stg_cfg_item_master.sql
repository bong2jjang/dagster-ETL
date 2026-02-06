-- staging: cfg_item_master 정제
-- 소스 테이블에서 기본 클렌징 수행

with source as (
    select * from {{ source('source_db', 'cfg_item_master') }}
),

cleaned as (
    select
        project_id,
        item_id,
        coalesce(item_type, 'UNKNOWN') as item_type,
        coalesce(item_name, '') as item_name,
        item_group_id,
        description,
        coalesce(item_priority, 0) as item_priority,
        procurement_type,
        prod_type,
        item_size_type,
        item_spec,
        create_datetime,
        update_datetime
    from source
)

select * from cleaned
