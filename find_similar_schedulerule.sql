SELECT "similar".*
FROM schedulerule sch
JOIN schedulerule "similar" ON "similar".resource @@ concat('mainOrganization.id="', sch.resource #>> '{mainOrganization,id}', '" and actor.#.id="',  sch.resource #>> '{actor,0,id}', '"')::jsquery
WHERE sch.id = 'fcb87f97-3b37-48f1-8c30-11d2ead8f5af'