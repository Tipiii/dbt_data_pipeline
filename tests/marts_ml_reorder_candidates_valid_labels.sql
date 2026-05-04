select *
from {{ ref('ml_reorder_candidates') }}
where
    (eval_set = 'train' and label not in (0, 1))
    or (eval_set = 'test' and label is not null)
