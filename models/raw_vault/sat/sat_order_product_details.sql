{{ build_sat(
    source_models=['stg_order_products_prior', 'stg_order_products_train'],
    parent_hk_column='hk_link_order_product',
    parent_key_columns=['order_id', 'product_id'],
    hashdiff_columns_list=['add_to_cart_order', 'reordered'],
    payload_columns=['add_to_cart_order', 'reordered']
) }}
