SELECT org.resource ->> 'name',
       count(sch.*)
FROM schedulerule sch
  JOIN organization org ON org.id = sch.resource #>> '{mainOrganization,id}'
WHERE sch.resource @@ 'availableTime.#.channel.# = "web-referral"'::jsquery
    AND (immutable_ts(COALESCE((sch.resource #>> '{planningHorizon,end}'::text[]), 'infinity'::text))) > current_timestamp::timestamp          
GROUP BY org.resource ->> 'name'