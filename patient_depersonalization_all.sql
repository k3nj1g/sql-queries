DO
$$
--DECLARE tabs text[][] := ARRAY[['servicerequest','subject'],['observation','subject'],['flag','subject'],['documentreference','subject'],['condition','subject'],['diagnosticreport','subject'],['procedure','subject'],['riskassessment','subject'],['specimen','subject']];
DECLARE tabs text[][] := ARRAY[[['episodeofcare','{patient,display}'],['episodeofcare','{contained,0,subject,display}'],['task','{for,display}'],['appointment','{participant,1,actor,display}'],['encounter','{contained,0,subject,display}'],['encounter','{subject,display}']]];
DECLARE t TEXT[];
BEGIN
   FOREACH t SLICE 1 IN ARRAY tabs
   LOOP
     RAISE NOTICE 'Updating table % with path %', t[1], t[2];
     EXECUTE 'WITH variables("path") AS
      (
        VALUES ( ' || quote_literal(t[2]) || '::text[] )
      )
      UPDATE ' || quote_ident(t[1]) || '
      SET resource = jsonb_set(resource, v."path", to_jsonb((SELECT array_to_string(ARRAY[(SELECT string_agg(concat (substring(subs FOR 3),regexp_replace(substring(subs FROM 4),''.'',''*'',''g'')),'' '') 
                                                       FROM regexp_split_to_table(display[1],'' '') AS subs)
                                     , CASE 
                                         WHEN array_length(display,1) > 1 THEN display[2] 
                                         ELSE '''' 
                                       END] ,'','')
                FROM regexp_split_to_array(resource #>>"path",'','') AS display)))
      FROM variables v
      RETURNING *
      '
     USING t[2], t[1];
   END LOOP;
END
$$
LANGUAGE plpgsql;

UPDATE task
SET resource = jsonb_set(resource, '{input}',
                 (SELECT jsonb_agg(elements.el)
                 FROM (
                    SELECT jsonb_set(array_object
                                    , '{value,Reference,display}'
                                    , to_jsonb((SELECT array_to_string(ARRAY[(SELECT string_agg(concat (substring(subs FOR 3),regexp_replace(substring(subs FROM 4),'.','*','g')),' ') 
                                                                                FROM regexp_split_to_table(display[1],' ') AS subs)
                                                                        , CASE 
                                                                            WHEN array_length(display,1) > 1 THEN display[2] 
                                                                            ELSE '' 
                                                                          END] ,',')
                                                FROM regexp_split_to_array(array_object #>>'{value,Reference,display}',',') AS display))) el
                    FROM jsonb_array_elements(resource -> 'input') array_object
                    WHERE array_object @@ 'type.coding.#.code=patient'::jsquery
                    UNION
                    SELECT array_object el
                    FROM jsonb_array_elements(resource -> 'input') array_object
                    WHERE NOT array_object @@ 'type.coding.#.code=patient'::jsquery
                    ) elements))
WHERE resource ?? 'input'
RETURNING *

UPDATE patient
SET resource = jsonb_set(resource
                    , '{extension}',
                    (SELECT jsonb_agg(elements.el)
                     FROM (
                       SELECT jsonb_set(array_object, '{extension}', (SELECT jsonb_agg(elements.el)
                                FROM (
                                   SELECT jsonb_set(array_object
                                                   , '{valueReference,display}'
                                                   , to_jsonb((SELECT array_to_string(ARRAY[(SELECT string_agg(concat (substring(subs FOR 3),regexp_replace(substring(subs FROM 4),'.','*','g')),' ') 
                                                                                               FROM regexp_split_to_table(display[1],' ') AS subs)
                                                                                       , CASE 
                                                                                           WHEN array_length(display,1) > 1 THEN display[2] 
                                                                                           ELSE '' 
                                                                                         END] ,',')
                                                               FROM regexp_split_to_array(array_object #>>'{valueReference,display}',',') AS display))) el
                                   FROM jsonb_array_elements(array_object -> 'extension') array_object
                                   WHERE array_object @@ 'url = "reference"'::jsquery
                                   UNION
                                   SELECT jsonb_set(array_object
                                                    , '{valueHumanName}'
                                                    , (SELECT array_object -> 'valueHumanName' || 
                                                               CASE WHEN (array_object -> 'valueHumanName') ? 'given'
                                                                 THEN jsonb_build_object('given'
                                                                                        , (SELECT jsonb_agg(to_jsonb(concat(substring(value #>> '{}' FOR 3), regexp_replace(substring(value #>> '{}' FROM 4),'.','*','g')))) 
                                                                                           FROM jsonb_array_elements(array_object #> '{valueHumanName,given}')))
                                                                ELSE '{}'::jsonb
                                                                END || 
                                                                CASE WHEN (array_object -> 'valueHumanName') ? 'family'
                                                                  THEN jsonb_build_object('family', to_jsonb(concat (substring(array_object #>> '{valueHumanName,family}' FOR 3), regexp_replace(substring(array_object #>> '{valueHumanName,family}' FROM 4),'.','*','g'))))
                                                                ELSE '{}'::jsonb
                                                                END))
                                   FROM jsonb_array_elements(array_object -> 'extension') array_object
                                   WHERE array_object @@ 'url = "name"'::jsquery
                                   UNION
                                   SELECT array_object el
                                   FROM jsonb_array_elements(array_object -> 'extension') array_object
                                   WHERE NOT array_object @@ 'url in ("reference","name")'::jsquery) elements)) el
                       FROM jsonb_array_elements(resource -> 'extension') array_object
                       WHERE array_object @@ 'url = "urn:extension:mother"'::jsquery
                       UNION
                       SELECT array_object el
                       FROM jsonb_array_elements(resource -> 'extension') array_object
                       WHERE NOT array_object @@ 'url = "urn:extension:mother"'::jsquery
                       ) elements))
WHERE resource @@ 'extension.#.url="urn:extension:mother"'::jsquery
RETURNING *

UPDATE patient
SET resource = jsonb_set(resource, '{contact}', (resource #> '{contact,0}' ||
    CASE WHEN resource #> '{contact,0}' ?? 'name'
      THEN jsonb_build_object(
             'name', CASE WHEN resource #> '{contact,0,name}' ?? 'given'
                       THEN jsonb_build_object('given', (SELECT jsonb_agg(to_jsonb(concat(substring(value #>> '{}' FOR 3), regexp_replace(substring(value #>> '{}' FROM 4),'.','*','g')))) 
                                                         FROM jsonb_array_elements(resource #> '{contact,0,name}' #> '{given}')))
                       ELSE '{}'::jsonb
                     END
                     || 
                     CASE WHEN resource #> '{contact,0,name}' ?? 'family'
                       THEN jsonb_build_object('family', to_jsonb(concat (substring(resource #> '{contact,0,name}' #>> '{family}' FOR 3), regexp_replace(substring(resource #> '{contact,0,name}' #>> '{family}' FROM 4),'.','*','g'))))
                       ELSE '{}'::jsonb
                     END)
    ELSE '{}'::jsonb
    END ||
    CASE WHEN jsonb_array_length(resource #> '{contact,0}' -> 'telecom') > 0
      THEN jsonb_build_object('telecom', (SELECT jsonb_agg(value || concat ('{','"value": ',to_jsonb(concat (substring(value #>> '{value}' FOR 3),regexp_replace(substring(value #>> '{value}' FROM 4),'.','*','g'))),'}')::jsonb) 
                                          FROM jsonb_array_elements(resource #> '{contact,0}' #> '{telecom}')))
    ELSE '{}'::jsonb
    END))
WHERE resource @@ 'contact.@# > 0'::jsquery  
RETURNING *

UPDATE vaccinationinfo 
SET resource = jsonb_set(resource, '{data,dsReg,fio}', 
                 to_jsonb((SELECT array_to_string(ARRAY[(SELECT string_agg(concat (substring(subs FOR 3),regexp_replace(substring(subs FROM 4),'.','*','g')),' ') 
                                                         FROM regexp_split_to_table(display[1],' ') AS subs)
                                                       , CASE WHEN array_length(display,1) > 1 
                                                           THEN display[2]
                                                         END] ,',')
                FROM regexp_split_to_array(resource #>>'{data,dsReg,fio}',',') AS display)))
WHERE resource @@ 'data.dsReg.fio=*'::jsquery

--- fix redundant comma
UPDATE vaccinationinfo 
SET resource = COALESCE((jsonb_set(resource, '{data,dsReg,fio}', to_jsonb(regexp_replace(resource #>> '{data,dsReg,fio}',',','','g')))), resource)
WHERE resource @@ 'data.dsReg.fio=*'::jsquery

