select *
from schedulerule 
where resource @@ 'availableTime.#:($.daysOfWeek @> ["sat"])'::jsquery
