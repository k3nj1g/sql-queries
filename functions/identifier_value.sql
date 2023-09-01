CREATE INDEX IF NOT EXISTS episodeofcare_patient_ref__btree
  ON episodeofcare ((reference_value(resource, 'patient')));   
 
CREATE OR REPLACE FUNCTION public.reference_value(resource jsonb, path text)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE
AS $function$
SELECT CASE WHEN resource->PATH->'identifier' IS NOT NULL THEN concat(resource->path#>>'{identifier,system}', '_', resource->path#>>'{identifier,value}')
            ELSE resource->path->>'id'
       END
$function$;

CREATE OR REPLACE FUNCTION public.identifier_reference_value(resource jsonb)
 RETURNS text[]
 LANGUAGE sql
 IMMUTABLE
AS $function$
SELECT (SELECT array_agg(concat(idf->>'system', '_', idf->>'value'))  FROM jsonb_array_elements(resource->'identifier') idfs(idf) WHERE idf#>>'{period,end}' IS NULL) 
$function$;

CREATE OR REPLACE FUNCTION public.identifier_reference_value(id TEXT, resource jsonb)
 RETURNS text[]
 LANGUAGE sql
 IMMUTABLE
AS $function$
SELECT array_append((SELECT array_agg(concat(idf->>'system', '_', idf->>'value'))  FROM jsonb_array_elements(resource->'identifier') idfs(idf) WHERE idf#>>'{period,end}' IS NULL), id)
$function$;
