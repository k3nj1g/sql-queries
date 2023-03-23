WITH grouped AS (
  SELECT cond.resource#>>'{encounter,identifier,value}' encounter,
    jsonb_path_query_first(
      cond.resource,
      '$.code.coding ? (@.system == "urn:CodeSystem:icd-10")'
    )->>'code' icd,
    jsonb_agg(cond.*) conditions,
    count(cond.*)
  FROM patient
    JOIN "condition" cond ON cond.resource @@ logic_revinclude("patient"."resource", "patient"."id", 'subject')
  WHERE patient.id = '01fa38f5-c5c0-43e6-bc6e-00d4a2a4c845'
  GROUP BY 1,
    2
),
to_delete_aggs(ids) AS (
  SELECT (
      WITH rows(v) AS (
        SELECT *
        FROM jsonb_array_elements(conditions)
      ),
      aggregated AS (
        SELECT v->>'id' id
        FROM rows
        EXCEPT (
            SELECT v->>'id' id
            FROM rows
            ORDER BY v#>>'{resource,recordedDate}' DESC
            LIMIT 1
          )
      )
      SELECT array_agg(id)
      FROM aggregated
    ) cond
  FROM grouped
),
to_delete AS (
  SELECT unnest(ids) id
  FROM to_delete_aggs
)
DELETE FROM "condition" cond
USING to_delete
WHERE cond.id = to_delete.id
RETURNING cond.id;
