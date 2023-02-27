SELECT 
    -- sch.id
    schedule_range_free_slot(sch.id, '2023-02-20', '2023-03-06', 'web')
    , to_jsonb(sch.*) AS sch,
    location.id,
    jsonb_path_query_first(
        hcs.resource,
        '$.type.coding ? (@.system=="urn:CodeSystem:service").code'
    )#>>'{}' AS service
FROM schedulerule sch
    INNER JOIN location location ON location.id = sch.resource#>>'{location,id}'
    INNER JOIN healthcareservice hcs ON (
        hcs.id = sch.resource#>>'{healthcareService,0,id}'
        AND jsonb_path_query_first(
            hcs.resource,
            '$.type.coding ? (@.system=="urn:CodeSystem:service").code'
        )#>>'{}' <> '153'
    )
WHERE (
        immutable_ts(
            coalesce(
                sch.resource#>>'{planningHorizon,end}',
                'infinity'
            )
        ) >= '2023-02-20'
        AND sch.resource @@ 'availableTime.#.channel.#($="web")'::jsquery
    )
    and "location".id = 'e363bb50-cd89-48ca-b81e-6bb5c041dea3'
