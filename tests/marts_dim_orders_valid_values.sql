select *
from {{ ref('dim_orders') }}
where order_number < 1
   or order_dow not between 0 and 6
   or order_hour_of_day not between 0 and 23
   or days_since_prior_order < 0
   or order_dow_name != case order_dow
        when 0 then 'Sunday'
        when 1 then 'Monday'
        when 2 then 'Tuesday'
        when 3 then 'Wednesday'
        when 4 then 'Thursday'
        when 5 then 'Friday'
        when 6 then 'Saturday'
    end
   or is_first_order != (order_number = 1)
