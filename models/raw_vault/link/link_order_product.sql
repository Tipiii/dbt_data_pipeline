{{ build_link(
    source_models=['stg_order_products_prior', 'stg_order_products_train'],
    link_hk_column='hk_link_order_product',
    link_key_columns=['order_id', 'product_id'],
    hub_key_map={
        'hk_order': ['order_id'],
        'hk_product': ['product_id']
    }
) }}
