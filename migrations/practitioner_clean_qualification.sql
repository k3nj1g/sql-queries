WITH txid AS (
  SELECT nextval('transaction_id_seq') txid)
, to_update AS (
  SELECT id
    , jsonb_set_lax(
      resource
      , '{qualification}'
      , (
        SELECT jsonb_agg(qual)
        FROM jsonb_array_elements(resource->'qualification') q(qual)
        WHERE qual @@ 'identifier.#.system="urn:identity:frmr2:medical-certificate"'::jsquery
      )
      , FALSE
      , 'delete_key') resource
  FROM practitioner
  WHERE resource @@ 'qualification=*'::jsquery)
UPDATE practitioner pr
SET ts = current_timestamp
  , txid = (SELECT txid FROM txid)
  , status = 'updated'
  , resource = tu.resource
FROM to_update tu
WHERE pr.id = tu.id
RETURNING *;
