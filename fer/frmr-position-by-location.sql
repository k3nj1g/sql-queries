WITH pre AS (
    SELECT (
            JSONB_PATH_QUERY_FIRST(
                prr.resource,
                '$.code.coding ? (@.system=="urn:CodeSystem:frmr.position").code'
            )#>>'{}'
        ) AS frmr_position,
        TO_JSONB(sch.*) AS sch,
        (
            JSONB_PATH_QUERY_FIRST(
                hcs.resource,
                '$.type.coding ? (@.system=="urn:CodeSystem:service").code'
            )#>>'{}'
        ) AS service
        , sch.id
    FROM schedulerule AS sch
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
        INNER JOIN healthcareservice AS hcs ON hcs.id = (sch.resource#>>'{healthcareService,0,id}')
    WHERE (
            IMMUTABLE_TS(
                COALESCE(
                    (sch.resource#>>'{planningHorizon,end}'),
                    'infinity'
                )
            ) >= CAST('2023-01-25' AS timestamp)
        )
        AND (
            sch.resource @@ 'availableTime.#.channel.#="web" and location.id="3adf1b1a-9b61-4bf4-bf56-ff69645ce9fd"'::jsquery
        )
)
SELECT pre.*
    , TO_JSONB(concept.*) AS concept
FROM pre
    INNER JOIN concept ON (
        (resource#>>'{system}') = 'urn:CodeSystem:frmr.position'
    )
    AND ((resource#>>'{code}') = frmr_position)