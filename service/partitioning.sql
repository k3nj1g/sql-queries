CREATE TABLE observation2 (LIKE observation INCLUDING ALL except )
PARTITION BY RANGE(cts)

-- public.observation definition

-- Drop table

-- DROP TABLE public.observation2;

CREATE TABLE public.observation2 (
    id text NOT NULL,
    txid int8 NOT NULL,
    cts timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ts timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
    resource_type text NULL DEFAULT 'Observation'::text,
    status public."resource_status" NOT NULL,
    resource jsonb NOT NULL,
    CONSTRAINT observation2_pkey PRIMARY KEY (id,cts)
)
PARTITION BY RANGE(cts);

CREATE INDEX IF NOT EXISTS observation2_resource__gin_jsquery ON public.observation2 USING gin (resource jsonb_path_value_ops);
CREATE INDEX IF NOT EXISTS observation2_resource_effective_datetime ON public.observation2 USING btree (((resource #>> '{effective,dateTime}'::text[])));
CREATE INDEX IF NOT EXISTS observation2_resource_eoc__pregnant ON public.observation2 USING gin (((resource -> 'episodeOfCare'::text)) jsonb_path_value_ops) WHERE ((resource -> 'category'::text) @@ '#."coding".#("system" = "urn:CodeSystem:pregnancy" AND "code" = "current-pregnancy")'::jsquery);
CREATE INDEX IF NOT EXISTS observation2_resource_identifier__gin ON public.observation2 USING gin (knife_extract_text(resource, '[["identifier", "value"]]'::jsonb));
CREATE INDEX IF NOT EXISTS observation2_resource_identifier_rmis_system__gin ON public.observation2 USING gin (knife_extract_text(resource, '[["identifier", {"system": "urn:source:rmis:Observation"}, "value"]]'::jsonb));
CREATE INDEX IF NOT EXISTS observation2_resource_period_patient_condition__gist ON public.observation2 USING gist (immutable_tsrange((resource #>> '{effective,Period,start}'::text[]), (resource #>> '{effective,Period,end}'::text[]), '[]'::text)) WHERE (resource @@ '("category".#."coding".#("system" = "urn:CodeSystem:observation-category" AND "code" = "patient-condition") AND "value"."CodeableConcept"."coding".#("system" = "urn:CodeSystem:1.2.643.5.1.13.13.11.1006" AND "code" IN ("3", "4", "6")))'::jsquery);
CREATE INDEX IF NOT EXISTS observation2_resource_subject_ref_valid ON public.observation2 USING btree (enp_valid((resource #>> '{subject,identifier,value}'::text[]))) WHERE (resource @@ '"subject"."identifier"."system" = "urn:identity:insurance-gov:Patient"'::jsquery);
CREATE INDEX IF NOT EXISTS observation2_ts__btree ON public.observation2 USING btree (ts);
CREATE INDEX IF NOT EXISTS observation2_txid__btree ON public.observation2 USING btree (txid);

SELECT *
FROM pg_indexes
WHERE tablename = 'observation'

SELECT EXTRACT(YEAR FROM ts),count (*)
FROM observation o 
GROUP BY 1 

CREATE TABLE observation2_2020 PARTITION OF observation2
    FOR VALUES FROM ('2000-01-01') TO ('2021-01-01');
    
CREATE TABLE observation2_2021_1 PARTITION OF observation2
    FOR VALUES FROM ('2021-01-01') TO ('2021-04-01');
    
CREATE TABLE observation2_2021_2 PARTITION OF observation2
    FOR VALUES FROM ('2021-04-01') TO ('2021-07-01');
    
CREATE TABLE observation2_2021_3 PARTITION OF observation2
    FOR VALUES FROM ('2021-07-01') TO ('2021-10-01');
    
CREATE TABLE observation2_2021_4 PARTITION OF observation2
    FOR VALUES FROM ('2021-10-01') TO ('2022-01-01');
    
CREATE TABLE observation2_2022_1 PARTITION OF observation2
    FOR VALUES FROM ('2022-01-01') TO ('2022-04-01');
    
CREATE TABLE observation2_2022_2 PARTITION OF observation2
    FOR VALUES FROM ('2022-04-01') TO ('2022-07-01');
    
CREATE TABLE observation2_2022_3 PARTITION OF observation2
    FOR VALUES FROM ('2022-07-01') TO ('2022-10-01');
    
CREATE TABLE observation2_2022_4 PARTITION OF observation2
    FOR VALUES FROM ('2022-10-01') TO ('2023-01-01');

CREATE TABLE observation2_default PARTITION OF observation2
    DEFAULT;

INSERT INTO observation2
SELECT * FROM observation WHERE cts<'2021-01-01'

CREATE INDEX observation_cts__btree ON public.observation USING btree (cts);

DO
$do$
BEGIN
   FOR i IN 0..30 LOOP
   --    raise notice  '%d', '2020-01-01'::date + ((i*3)::text || ' months')::INTERVAL;
     INSERT INTO observation2
     SELECT * 
     FROM observation
     WHERE cts >= '2020-01-01'::date + (i::text || ' months')::INTERVAL
       AND cts < '2020-01-01'::date + ((i+1)::text || ' months')::INTERVAL;
   END LOOP;
END
$do$;


