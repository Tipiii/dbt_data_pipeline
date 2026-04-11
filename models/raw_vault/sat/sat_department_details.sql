{{ build_sat(
    source_models=['stg_departments'],
    parent_hk_column='hk_department',
    parent_key_columns=['department_id'],
    hashdiff_columns_list=['department'],
    payload_columns=['department']
) }}
