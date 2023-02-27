SELECT TO_JSONB(sch.*) AS sch,
    TO_JSONB(prr.*) AS prr,
    TO_JSONB(pr.*) AS pr,
    TO_JSONB(hcs.*) AS hcs,
    TO_JSONB(l.*) AS loc,
    (
        JSONB_PATH_QUERY_FIRST(
            hcs.resource,
            '$.type.coding ? (@.system=="urn:CodeSystem:service").code'
        )#>>'{}'
    ) AS service
FROM location AS l
    INNER JOIN schedulerule AS sch ON ((sch.resource#>>'{location,id}') = l.id)
    AND (
        IMMUTABLE_TS(
            COALESCE(
                (sch.resource#>>'{planningHorizon,end}'),
                'infinity'
            )
        ) >= CAST('2023-02-03' AS timestamp)
    )
    AND JSONB_PATH_EXISTS(
        sch.resource,
        '$.availableTime.channel ? (@=="web")'
    )
    INNER JOIN practitionerrole AS prr ON (
        prr.id = (
            JSONB_PATH_QUERY_FIRST(
                sch.resource,
                '$.actor ? (@.resourceType=="PractitionerRole").id'
            )#>>'{}'
        )
    )
    AND (
        COALESCE((prr.resource#>>'{active}'), 'true') = 'true'
    )
    INNER JOIN practitioner AS pr ON (
        pr.resource @@ LOGIC_INCLUDE(prr.resource, 'practitioner')
    )
    INNER JOIN healthcareservice AS hcs ON hcs.id = (sch.resource#>>'{healthcareService,0,id}')
WHERE l.id = 'b347131b-f2d7-4042-855d-72fda740bc9b'
