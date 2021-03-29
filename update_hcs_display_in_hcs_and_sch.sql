SELECT *
FROM healthcareservice h 
WHERE resource @@ 'type.#.coding.#(system = "urn:CodeSystem:service" and code = "4000")'::jsquery

--- обновление дисплея в услуге ---
UPDATE healthcareservice 
SET resource = jsonb_set(resource, '{type}', jsonb_build_array(jsonb_build_object('coding', jsonb_build_array(jsonb_build_object('code', '4000', 'system', 'urn:CodeSystem:service', 'display', 'Вакцинация от COVID-19')))))
WHERE resource @@ 'type.#.coding.#(system = "urn:CodeSystem:service" and code = "4000")'::jsquery
RETURNING id
---

SELECT jsonb_build_array(jsonb_build_object('coding', jsonb_build_array(jsonb_build_object('code', '4000', 'system', 'urn:CodeSystem:service', 'display', 'Вакцинация от COVID-19'))))

SELECT *
FROM schedulerule s 
WHERE resource @@ 'healthcareService.#.id = "901bdc43-430d-4726-b3d7-0dac62c8eea9"'::jsquery

SELECT sr.resource -> 'healthcareService', jsonb_set(sr.resource, '{healthcareService}', (
		SELECT hcss.hcs
	    FROM (
	 		SELECT jsonb_set(sr.resource -> 'healthcareService', '{0,display}', '"Вакцинация от COVID-19"') hcs
 		) hcss)) -> 'healthcareService'
FROM healthcareservice h 
JOIN schedulerule sr ON sr.resource @@ logic_revinclude(h.resource, h.id, 'healthcareService.#')
WHERE h.id IN ('ff168efc-febf-448d-8af4-b2b83e9f9b31', '901bdc43-430d-4726-b3d7-0dac62c8eea9', 'd825f8a8-1a04-4739-ab39-e90a56d3d461', '9fb8d242-bbaf-43b8-a1e8-651898abdbd7', 'fa5476b2-eeba-48eb-8183-ec5a17065a59', '89d1b496-e896-4b1a-8ddc-a4c2116e7e70', 'e652fafc-c7ed-4cee-b68d-41a0b6f63a1e', 'd6cded8a-4e85-4ce0-8937-303151e107de', '5531c948-cc87-4d49-a207-3cd943898a15', '1bf4d60a-ee6a-4664-91db-65ecd6305449', 'b71c9b58-907f-4e0a-9133-4083793cd206', 'fce7fa03-d007-4d87-ae98-2271910348a0', '36d5fd0b-9126-4136-9f04-9b0437c9ae6f', '14e3e854-bbde-4278-a996-274306b7b831', 'bcc6154a-b20a-4e97-9639-cce3ffa3b6d3', 'f2fdc044-8053-4351-a001-547b025bf6f1', 'e4a22e41-5e2f-4546-b953-a5bc5023a870', 'd5ed6da0-f6e4-4ecd-80c1-8b00c4197183', '4e549a21-2364-4a3a-a63d-ae83b7fdbfe9', '6aef8efd-474f-4aba-b680-764e319add9f', 'd59a850e-b689-4a25-bb34-66517c9ab845', '8ccb970f-0daa-4754-b21c-caa1acbc1064', '46a8a1c6-733d-488f-a34d-891ad4be1de4', 'a9b2d8d1-a0ce-4b29-afa4-8379708bb3de', '7cd7bde5-c580-46b2-9c8c-83a1e7a71879', '6918d819-5673-4ec7-a971-f0312a8433e0', '0aa4f3d8-b972-4bf0-a329-9ee11a561ed9')

--- обновление дисплея услуги в расписании ---
WITH sch_4000 AS (
	SELECT sr.id
	FROM healthcareservice h 
	JOIN schedulerule sr ON sr.resource @@ logic_revinclude(h.resource, h.id, 'healthcareService.#')
	WHERE h.id IN ('ff168efc-febf-448d-8af4-b2b83e9f9b31', '901bdc43-430d-4726-b3d7-0dac62c8eea9', 'd825f8a8-1a04-4739-ab39-e90a56d3d461', '9fb8d242-bbaf-43b8-a1e8-651898abdbd7', 'fa5476b2-eeba-48eb-8183-ec5a17065a59', '89d1b496-e896-4b1a-8ddc-a4c2116e7e70', 'e652fafc-c7ed-4cee-b68d-41a0b6f63a1e', 'd6cded8a-4e85-4ce0-8937-303151e107de', '5531c948-cc87-4d49-a207-3cd943898a15', '1bf4d60a-ee6a-4664-91db-65ecd6305449', 'b71c9b58-907f-4e0a-9133-4083793cd206', 'fce7fa03-d007-4d87-ae98-2271910348a0', '36d5fd0b-9126-4136-9f04-9b0437c9ae6f', '14e3e854-bbde-4278-a996-274306b7b831', 'bcc6154a-b20a-4e97-9639-cce3ffa3b6d3', 'f2fdc044-8053-4351-a001-547b025bf6f1', 'e4a22e41-5e2f-4546-b953-a5bc5023a870', 'd5ed6da0-f6e4-4ecd-80c1-8b00c4197183', '4e549a21-2364-4a3a-a63d-ae83b7fdbfe9', '6aef8efd-474f-4aba-b680-764e319add9f', 'd59a850e-b689-4a25-bb34-66517c9ab845', '8ccb970f-0daa-4754-b21c-caa1acbc1064', '46a8a1c6-733d-488f-a34d-891ad4be1de4', 'a9b2d8d1-a0ce-4b29-afa4-8379708bb3de', '7cd7bde5-c580-46b2-9c8c-83a1e7a71879', '6918d819-5673-4ec7-a971-f0312a8433e0', '0aa4f3d8-b972-4bf0-a329-9ee11a561ed9')
)
UPDATE schedulerule sr
SET resource = jsonb_set(sr.resource, '{healthcareService}', (
		SELECT hcss.hcs
	    FROM (
	 		SELECT jsonb_set(sr.resource -> 'healthcareService', '{0,display}', '"Вакцинация от COVID-19"') hcs
 		) hcss))
FROM sch_4000
WHERE sr.id = sch_4000.id
RETURNING sr.id
---
--- обновление дисплея услуги в визитах ---
UPDATE appointment
SET resource = jsonb_set(resource, '{serviceType}', jsonb_build_array(jsonb_build_object('coding', jsonb_build_array(jsonb_build_object('code', '4000', 'system', 'urn:CodeSystem:service', 'display', 'Вакцинация от COVID-19')))))
WHERE resource @@ 'serviceType.#.coding.#(system = "urn:CodeSystem:service" and code = "4000")'::jsquery
RETURNING id
