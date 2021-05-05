SELECT id, obs.resource #>> '{subject,identifier,value}'
FROM observation obs
WHERE (obs.resource @@ 'category.#.coding.#(system="urn:CodeSystem:observation-category" and code="patient-condition") and value.CodeableConcept.coding.#(system="urn:CodeSystem:1.2.643.5.1.13.13.11.1006" and code in ("3","4","6"))'::jsquery
	AND daterange(cast(obs.resource #>> '{effective,Period,start}' AS date),cast(obs.resource #>> '{effective,Period,end}' AS date),'[]') @> current_date);
	
UPDATE observation 
SET resource = resource #- '{subject,identifier,value}'
WHERE id = '99cc44f3-22ec-460e-a9cf-0813974cd5ef';

