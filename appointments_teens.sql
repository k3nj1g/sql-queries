SELECT DATE_PART('year', AGE(CURRENT_DATE, CAST(p.resource ->> 'birthDate' AS TIMESTAMP))) p_age,
       p.resource ->> 'birthDate'
FROM patient p
WHERE DATE_PART('year', AGE(CURRENT_DATE, CAST(p.resource ->> 'birthDate' AS TIMESTAMP))) < 18

CREATE OR REPLACE FUNCTION patient_age (birth_date text) RETURNS INT LANGUAGE plpgsql
AS
$$ 
BEGIN 
    RETURN DATE_PART('year', AGE(CURRENT_DATE, CAST(birth_date AS TIMESTAMP)));
EXCEPTION
    WHEN invalid_datetime_format THEN
        RETURN 0;
END;
$$

SELECT COALESCE (o.resource #>> '{alias,0}', o.resource #>> '{name}') AS org
       , count(*) FILTER (WHERE a.resource ->> 'from' = 'web') AS epgu
       , count(*) FILTER (WHERE a.resource ->> 'from' = 'reg') AS reg
       , count(*) FILTER (WHERE a.resource ->> 'from' = 'kc') AS "call-center"
       , count(*) AS total
FROM appointment a
JOIN organization o ON o.id = a.resource #>> '{mainOrganization,id}'
JOIN patient p ON p.id = jsonb_path_query_first(a.resource, '$.participant ? (@.actor.resourceType == "Patient").actor.id') #>> '{}'
    AND patient_age(p.resource ->> 'birthDate') < 18
WHERE a.resource ->> 'start' BETWEEN '2021-01-01' AND '2021-09-23'
    AND a.resource @@ 'serviceType.#.coding.#(not code = "153")'::jsquery
GROUP BY 1
