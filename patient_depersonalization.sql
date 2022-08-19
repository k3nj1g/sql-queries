CREATE SCHEMA IF NOT EXISTS depersonalization;

CREATE TABLE IF NOT EXISTS depersonalization.patient (LIKE public.patient INCLUDING ALL);

DO
  $$BEGIN
    EXECUTE (
      SELECT 'DROP INDEX ' || string_agg(indexrelid::regclass::text, ', ')
      FROM pg_index  i
      LEFT JOIN pg_depend d ON d.objid = i.indexrelid
        AND d.deptype = 'i'
      WHERE i.indrelid = 'depersonalization.patient'::regclass
        AND i.indisunique = false
        AND d.objid IS NULL
);
END$$;

INSERT INTO depersonalization.patient (id, txid, cts, ts, resource_type, status, resource)
(
  SELECT id, txid, cts, ts, resource_type, status, resource ||
    CASE WHEN resource ?? 'name'
      THEN jsonb_build_object(
             'name', (SELECT jsonb_agg(value || CASE WHEN value ?? 'given'
                                                  THEN jsonb_build_object('given', (SELECT jsonb_agg(to_jsonb(concat(substring(value #>> '{}' FOR 3), regexp_replace(substring(value #>> '{}' FROM 4),'.','*','g')))) 
                                                                                    FROM jsonb_array_elements(value #> '{given}')))
                                                ELSE '{}'::jsonb
                                                END
                                             || CASE WHEN value ?? 'family'
                                                  THEN jsonb_build_object('family', to_jsonb(concat (substring(value #>> '{family}' FOR 3), regexp_replace(substring(value #>> '{family}' FROM 4),'.','*','g'))))
                                                ELSE '{}'::jsonb
                                                END)
                      FROM jsonb_array_elements(resource #> '{name}')))
    ELSE '{}'::jsonb
    END ||
    CASE WHEN jsonb_array_length(resource -> 'telecom') > 0
      THEN jsonb_build_object('telecom', (SELECT jsonb_agg(value || concat ('{','"value": ',to_jsonb(concat (substring(value #>> '{value}' FOR 3),regexp_replace(substring(value #>> '{value}' FROM 4),'.','*','g'))),'}')::jsonb) 
                                          FROM jsonb_array_elements(resource #> '{telecom}')))
    ELSE '{}'::jsonb
    END AS resource
  FROM patient
);

CREATE INDEX lookup_patient_g_1 ON depersonalization.patient  USING gin (regexp_replace(aidbox_text_search(knife_extract_text(resource, '[["name", "family"]]'::jsonb)), '[ -.,";:'']+'::text, ' '::text, 'g'::text) gin_trgm_ops);
CREATE INDEX lookup_patient_g_2 ON depersonalization.patient  USING gin (regexp_replace(aidbox_text_search(knife_extract_text(resource, '[["name", "family"], ["name", "given", 0]]'::jsonb)), '[ -.,";:'']+'::text, ' '::text, 'g'::text) gin_trgm_ops);
CREATE INDEX lookup_patient_g_3 ON depersonalization.patient  USING gin (regexp_replace(aidbox_text_search(knife_extract_text(resource, '[["name", "family"], ["name", "given", 0], ["name", "given", 1]]'::jsonb)), '[ -.,";:'']+'::text, ' '::text, 'g'::text) gin_trgm_ops);
CREATE INDEX lookup_patient_g_4 ON depersonalization.patient  USING gin (regexp_replace(aidbox_text_search(knife_extract_text(resource, '[["name", "family"], ["name", "given", 0], ["name", "given", 1], ["birthDate"]]'::jsonb)), '[ -.,";:'']+'::text, ' '::text, 'g'::text) gin_trgm_ops);
CREATE INDEX lookup_patient_g_5 ON depersonalization.patient  USING gin (regexp_replace(aidbox_text_search(knife_extract_text(resource, '[["name", "family"], ["name", "given", 0], ["name", "given", 1], ["birthDate"], ["identifier", "value"]]'::jsonb)), '[ -.,";:'']+'::text, ' '::text, 'g'::text) gin_trgm_ops);
CREATE INDEX lookup_patient_g_6 ON depersonalization.patient  USING gin (regexp_replace(aidbox_text_search(knife_extract_text(resource, '[["name", "family"], ["name", "given", 0], ["name", "given", 1], ["birthDate"], ["identifier", "value"], ["telecom", "value"]]'::jsonb)), '[ -.,";:'']+'::text, ' '::text, 'g'::text) gin_trgm_ops);
CREATE INDEX lookup_patient_g_7 ON depersonalization.patient  USING gin (regexp_replace(aidbox_text_search(knife_extract_text(resource, '[["name", "family"], ["name", "given", 0], ["name", "given", 1], ["birthDate"], ["identifier", "value"], ["telecom", "value"], ["extension", "extension", "valueHumanName", "given"], ["extension", "extension", "valueHumanName", "family"], ["extension", "extension", "valueString"], ["extension", "extension", "valueDate"]]'::jsonb)), '[ -.,";:'']+'::text, ' '::text, 'g'::text) gin_trgm_ops);
CREATE INDEX patient_enp__with_jsonb_path ON depersonalization.patient  USING btree (jsonb_path_query_first(resource, '$."identifier"[*]?(@."system" == "urn:identity:enp:Patient")."value"'::jsonpath));
CREATE INDEX patient_insurance_gov ON depersonalization.patient  USING btree (identifier_value(resource, 'urn:identity:insurance-gov:Patient'::text));
CREATE INDEX patient_resource__birthdate_btree_index ON depersonalization.patient  USING btree (((resource #>> '{birthDate}'::text[])));
CREATE INDEX patient_resource__gin_jsquer ON depersonalization.patient  USING gin (resource jsonb_path_value_ops);
CREATE INDEX patient_resource__rmis_search ON depersonalization.patient  USING gin (rmis_patient_index_string(resource) gin_trgm_ops);
CREATE INDEX patient_resource_deceased_datetime_and_active__gin ON depersonalization.patient  USING gin (resource jsonb_path_value_ops) WHERE ((COALESCE((resource ->> 'active'::text), 'true'::text) = 'true'::text) AND ((resource #>> '{deceased,dateTime}'::text[]) IS NULL));
CREATE INDEX patient_resource_idf_value_knife__gin ON depersonalization.patient  USING gin (knife_extract_text(resource, '[["identifier", "value"]]'::jsonb));
CREATE INDEX patient_snils ON depersonalization.patient  USING btree (identifier_value(resource, 'urn:identity:snils:Patient'::text));
CREATE INDEX patient_snils__with_jsonb_path ON depersonalization.patient  USING btree (jsonb_path_query_first(resource, '$."identifier"[*]?(@."system" == "urn:identity:snils:Patient")."value"'::jsonpath));
CREATE INDEX patient_snils_value ON depersonalization.patient  USING btree (jsonb_path_query_first(resource, '$."identifier"[*]?(@."system" == "urn:identity:snils:Patient")."value"'::jsonpath));
CREATE INDEX patient_tfoms_id ON depersonalization.patient  USING btree (identifier_value(resource, 'urn:source:tfoms:Patient'::text));
CREATE INDEX patient_ts___btree ON depersonalization.patient  USING btree (ts);
CREATE INDEX patient_txid__btree ON depersonalization.patient  USING btree (txid);

BEGIN;
  DROP TABLE public.patient CASCADE;
  ALTER TABLE depersonalization.patient SET SCHEMA public;  
  DROP SCHEMA depersonalization CASCADE;
COMMIT;

ANALYZE patient;

--ALTER DEFAULT PRIVILEGES FOR ROLE reader IN SCHEMA public GRANT SELECT ON TABLES TO reader;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO reader;