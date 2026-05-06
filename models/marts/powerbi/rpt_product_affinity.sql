with order_lines as (
    select distinct
        order_id,
        user_id,
        product_id,
        product_name,
        aisle_id,
        aisle,
        department_id,
        department
    from {{ ref('fct_order_products') }}
    where eval_set in ('prior', 'train')
),

order_totals as (
    select count(distinct order_id) as total_order_count
    from order_lines
),

product_stats as (
    select
        product_id,
        min(product_name) as product_name,
        count(distinct order_id) as product_order_count,
        count(distinct user_id) as product_user_count
    from order_lines
    group by 1
),

product_pairs as (
    select
        concat(cast(a.product_id as string), '|', cast(b.product_id as string)) as product_pair_key,
        a.product_id as product_id_a,
        min(a.product_name) as product_name_a,
        a.aisle_id as aisle_id_a,
        min(a.aisle) as aisle_a,
        a.department_id as department_id_a,
        min(a.department) as department_a,
        b.product_id as product_id_b,
        min(b.product_name) as product_name_b,
        b.aisle_id as aisle_id_b,
        min(b.aisle) as aisle_b,
        b.department_id as department_id_b,
        min(b.department) as department_b,
        count(distinct a.order_id) as pair_order_count,
        count(distinct a.user_id) as pair_user_count
    from order_lines as a
    inner join order_lines as b
        on a.order_id = b.order_id
       and a.product_id < b.product_id
    group by 1, 2, 4, 6, 8, 10, 12
)

select
    pairs.product_pair_key,
    pairs.product_id_a,
    pairs.product_name_a,
    pairs.aisle_id_a,
    pairs.aisle_a,
    pairs.department_id_a,
    pairs.department_a,
    pairs.product_id_b,
    pairs.product_name_b,
    pairs.aisle_id_b,
    pairs.aisle_b,
    pairs.department_id_b,
    pairs.department_b,
    pairs.pair_order_count,
    pairs.pair_user_count,
    safe_divide(pairs.pair_order_count, totals.total_order_count) as support_rate,
    safe_divide(pairs.pair_order_count, stats_a.product_order_count) as confidence_a_to_b,
    safe_divide(pairs.pair_order_count, stats_b.product_order_count) as confidence_b_to_a,
    safe_divide(
        pairs.pair_order_count * totals.total_order_count,
        stats_a.product_order_count * stats_b.product_order_count
    ) as lift
from product_pairs as pairs
cross join order_totals as totals
inner join product_stats as stats_a
    on pairs.product_id_a = stats_a.product_id
inner join product_stats as stats_b
    on pairs.product_id_b = stats_b.product_id
