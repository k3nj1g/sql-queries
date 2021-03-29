update organization 
set resource = resource || jsonb_build_object('activityPeriod', jsonb_build_object('start','2020-01-01','end','2020-04-01'))
where id='58c52a8c-10b0-4500-bd7d-e61f95950ebb'

update organization 
set resource = resource || '{"activityPeriod": {"start":"2020-01-01", "end":"2020-03-01"}}'
where id='58c52a8c-10b0-4500-bd7d-e61f95950ebb'

select resource
from organization
where id='58c52a8c-10b0-4500-bd7d-e61f95950ebb'