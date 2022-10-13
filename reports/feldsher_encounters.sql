--EXPLAIN ANALYZE 
SELECT 
       (max((l.resource ->> 'name'))) AS fap_name,
       (jsonb_build_object('male',count(enc.*) FILTER (WHERE (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_1.1")'::jsquery) AND (((p.resource ->> 'gender') = 'male') AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) > 18))),'female',count(enc.*) FILTER (WHERE (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_1.1")'::jsquery) AND (((p.resource ->> 'gender') = 'female') AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) > 18))),'children',count(enc.*) FILTER (WHERE (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_1.1")'::jsquery) AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) < 18)))) AS emergency_all,
       (jsonb_build_object('male',count(enc.*) FILTER (WHERE (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_1.1")'::jsquery) AND (((p.resource ->> 'gender') = 'male') AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) > 18)) AND (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:mz.place-of-medical-care" and code="1")'::jsquery)),'female',count(enc.*) FILTER (WHERE (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_1.1")'::jsquery) AND (((p.resource ->> 'gender') = 'female') AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) > 18)) AND (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:mz.place-of-medical-care" and code="1")'::jsquery)),'children',count(enc.*) FILTER (WHERE (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_1.1")'::jsquery) AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) < 18) AND (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:mz.place-of-medical-care" and code="1")'::jsquery)))) AS emergency_clinic,
       (jsonb_build_object('male',count(enc.*) FILTER (WHERE (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_1.1")'::jsquery) AND (((p.resource ->> 'gender') = 'male') AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) > 18)) AND (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:mz.place-of-medical-care" and code="2")'::jsquery)),'female',count(enc.*) FILTER (WHERE (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_1.1")'::jsquery) AND (((p.resource ->> 'gender') = 'female') AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) > 18)) AND (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:mz.place-of-medical-care" and code="2")'::jsquery)),'children',count(enc.*) FILTER (WHERE (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_1.1")'::jsquery) AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) < 18) AND (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:mz.place-of-medical-care" and code="2")'::jsquery)))) AS emergency_home,
       (jsonb_build_object('male',count(enc.*) FILTER (WHERE (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_1.1")'::jsquery) AND (((p.resource ->> 'gender') = 'male') AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) > 18)) AND (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:mz.place-of-medical-care" and code="9")'::jsquery)),'female',count(enc.*) FILTER (WHERE (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_1.1")'::jsquery) AND (((p.resource ->> 'gender') = 'female') AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) > 18)) AND (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:mz.place-of-medical-care" and code="9")'::jsquery)),'children',count(enc.*) FILTER (WHERE (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_1.1")'::jsquery) AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) < 18) AND (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:mz.place-of-medical-care" and code="9")'::jsquery)))) AS emergency_other,
       ((jsonb_build_object('male',count(enc.*) FILTER (WHERE ((enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_2.2")'::jsquery) OR (c.resource @@ 'severity.coding.#(system="urn:CodeSystem:nature-disease" and code in ("1","2"))'::jsquery)) AND (((p.resource ->> 'gender') = 'male') AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) > 18))),'female',count(enc.*) FILTER (WHERE ((enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_2.2")'::jsquery) OR (c.resource @@ 'severity.coding.#(system="urn:CodeSystem:nature-disease" and code in ("1","2"))'::jsquery)) AND (((p.resource ->> 'gender') = 'female') AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) > 18))),'children',count(enc.*) FILTER (WHERE ((enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_2.2")'::jsquery) OR (c.resource @@ 'severity.coding.#(system="urn:CodeSystem:nature-disease" and code in ("1","2"))'::jsquery)) AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) < 18))))) AS chronic_all,
       (jsonb_build_object('male',count(enc.*) FILTER (WHERE ((enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_2.2")'::jsquery) OR (c.resource @@ 'severity.coding.#(system="urn:CodeSystem:nature-disease" and code in ("1","2"))'::jsquery)) AND (((p.resource ->> 'gender') = 'male') AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) > 18)) AND (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:mz.place-of-medical-care" and code="1")'::jsquery)),'female',count(enc.*) FILTER (WHERE ((enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_2.2")'::jsquery) OR (c.resource @@ 'severity.coding.#(system="urn:CodeSystem:nature-disease" and code in ("1","2"))'::jsquery)) AND (((p.resource ->> 'gender') = 'female') AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) > 18)) AND (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:mz.place-of-medical-care" and code="1")'::jsquery)),'children',count(enc.*) FILTER (WHERE ((enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_2.2")'::jsquery) OR (c.resource @@ 'severity.coding.#(system="urn:CodeSystem:nature-disease" and code in ("1","2"))'::jsquery)) AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) < 18) AND (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:mz.place-of-medical-care" and code="1")'::jsquery)))) AS chronic_clinic,
       (jsonb_build_object('male',count(enc.*) FILTER (WHERE ((enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_2.2")'::jsquery) OR (c.resource @@ 'severity.coding.#(system="urn:CodeSystem:nature-disease" and code in ("1","2"))'::jsquery)) AND (((p.resource ->> 'gender') = 'male') AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) > 18)) AND (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:mz.place-of-medical-care" and code="2")'::jsquery)),'female',count(enc.*) FILTER (WHERE ((enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_2.2")'::jsquery) OR (c.resource @@ 'severity.coding.#(system="urn:CodeSystem:nature-disease" and code in ("1","2"))'::jsquery)) AND (((p.resource ->> 'gender') = 'female') AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) > 18)) AND (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:mz.place-of-medical-care" and code="2")'::jsquery)),'children',count(enc.*) FILTER (WHERE ((enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_2.2")'::jsquery) OR (c.resource @@ 'severity.coding.#(system="urn:CodeSystem:nature-disease" and code in ("1","2"))'::jsquery)) AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) < 18) AND (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:mz.place-of-medical-care" and code="2")'::jsquery)))) AS chronic_home,
       (jsonb_build_object('male',count(enc.*) FILTER (WHERE ((enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_2.2")'::jsquery) OR (c.resource @@ 'severity.coding.#(system="urn:CodeSystem:nature-disease" and code in ("1","2"))'::jsquery)) AND (((p.resource ->> 'gender') = 'male') AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) > 18)) AND (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:mz.place-of-medical-care" and code="9")'::jsquery)),'female',count(enc.*) FILTER (WHERE ((enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_2.2")'::jsquery) OR (c.resource @@ 'severity.coding.#(system="urn:CodeSystem:nature-disease" and code in ("1","2"))'::jsquery)) AND (((p.resource ->> 'gender') = 'female') AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) > 18)) AND (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:mz.place-of-medical-care" and code="9")'::jsquery)),'children',count(enc.*) FILTER (WHERE ((enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_2.2")'::jsquery) OR (c.resource @@ 'severity.coding.#(system="urn:CodeSystem:nature-disease" and code in ("1","2"))'::jsquery)) AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) < 18) AND (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:mz.place-of-medical-care" and code="9")'::jsquery)))) AS chronic_other,
       (jsonb_build_object('male',count(enc.*) FILTER (WHERE (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_3.1")'::jsquery) AND (((p.resource ->> 'gender') = 'male') AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) > 18))),'female',count(enc.*) FILTER (WHERE (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_3.1")'::jsquery) AND (((p.resource ->> 'gender') = 'female') AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) > 18))),'children',count(enc.*) FILTER (WHERE (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:encounter-goal" and code="3_3.1")'::jsquery) AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) < 18)))) AS vaccination,
       (jsonb_build_object('children',count(enc.*) FILTER (WHERE (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:mz.place-of-medical-care" and code="1")'::jsquery) AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) < 18)))) AS children_clinic,
       (jsonb_build_object('children',count(enc.*) FILTER (WHERE (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:mz.place-of-medical-care" and code="2")'::jsquery) AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) < 18)))) AS children_home,
       (jsonb_build_object('children',count(enc.*) FILTER (WHERE (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:mz.place-of-medical-care" and code="2.1")'::jsquery) AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) < 18)))) AS children_home_active,
       (jsonb_build_object('children',count(enc.*) FILTER (WHERE (enc.resource @@ 'type.#.coding.#(system="urn:CodeSystem:mz.place-of-medical-care" and code="9")'::jsquery) AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) < 18)))) AS children_other,
       (jsonb_build_object('female',count(enc.*) FILTER (WHERE enc.frmr_position IN ('150','334') AND (((p.resource ->> 'gender') = 'female') AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) >= 18)))
                           , 'male',count(enc.*) FILTER (WHERE enc.frmr_position IN ('150','334') AND (((p.resource ->> 'gender') = 'male')))    
                           ,'children',count(enc.*) FILTER (WHERE (enc.frmr_position IN ('150','334')) AND (((p.resource ->> 'gender') = 'female') AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) < 18))))) AS obstetrician,
       (jsonb_build_object('male',count(enc.*) FILTER (WHERE ((p.resource ->> 'gender') = 'male') AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) > 18))
                           ,'female',count(enc.*) FILTER (WHERE ((p.resource ->> 'gender') = 'female') AND (extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) > 18))
                           ,'children',count(enc.*) FILTER (WHERE extract(YEAR FROM age(cast(now() AS timestamp),cast((p.resource ->> 'birthDate') AS timestamp))) < 18))) AS total
FROM practitionerrole AS prr
  INNER JOIN organization AS org ON (org.resource @@ LOGIC_INCLUDE (prr.resource,'organization'))
  INNER JOIN "location" AS l
          ON (IDENTIFIER_VALUE (l.resource,'urn:identity:oid:Location') = (jsonb_path_query_first (org.resource,'$.identifier ? (@.system=="urn:identity:oid:Organization").value') #>> '{}'))
         AND ( (l.resource -> 'type') @@ '#.coding.#(system="urn:CodeSystem:feldsher-office-type")'::jsquery)
  INNER JOIN personbinding AS pb ON (pb.resource @@ LOGIC_REVINCLUDE (l.resource,l.id,'location'))
  INNER JOIN patient AS p ON IDENTIFIER_VALUE (p.resource,'urn:source:tfoms:Patient') = REF_IDENTIFIER_VALUE (pb.resource,'urn:source:tfoms:Patient','subject')
  LEFT JOIN LATERAL 
    (SELECT enc.*, 
            jsonb_path_query_first(prr_enc.resource, '$.code.coding ? (@.system=="urn:CodeSystem:frmr.position").code') #>> '{}' frmr_position
     FROM encounter AS enc
     JOIN practitionerrole prr_enc
       ON ((prr_enc.resource @@ LOGIC_INCLUDE(enc.resource, 'participant.individual'))
            OR (prr_enc.id = ANY(ARRAY((SELECT (JSONB_PATH_QUERY(enc.resource, '$.participant.individual.id') #>> '{}'))))))
         AND prr_enc.resource @@ 'code.#.coding.#(system="urn:CodeSystem:frmr.position" and code in ("145", "150", "195", "334"))'::jsquery
     WHERE ((enc.resource -> 'subject') @@ LOGIC_REVINCLUDE (p.resource,p.id))
       AND ((enc.resource -> 'class') @@ 'code="AMB" and system="http://terminology.hl7.org/CodeSystem/v3-ActCode"'::jsquery)
       AND (tsrange ('2022-01-01','2022-05-31','[]') @> cast ( (enc.resource #>> '{period,end}') AS timestamp))
     ORDER BY enc.ts
    ) enc ON TRUE
  LEFT JOIN "condition" AS c ON (c.resource @@ LOGIC_REVINCLUDE (enc.resource,enc.id,'encounter'))
WHERE prr.id = 'e27c16f7-f9cd-4a5f-92b8-68c11576ec31'
--  AND prr.resource @@ 'code.#.coding.#(system="urn:CodeSystem:frmr.position" and code in ("145", "150", "195", "334"))'::jsquery;

  
SELECT *
FROM pg_indexes
WHERE tablename = 'encounter'