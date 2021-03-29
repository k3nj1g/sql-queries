select jsonb_agg(jsonb_build_object('id', pt.id, 'name', pt.resource ->> 'name', 'identifier', pt.resource ->> 'identifier')) as pts
from patient pt
where pt.resource @@ 'identifier.#(system = "urn:source:rmis:Patient")'::jsquery and pt.resource @@ 'extension.#(url = "urn:extension:patient-type" and valueCode = "newborn")'::jsquery
group by (knife_extract_text(resource,'[["identifier",{"system":"urn:source:rmis:Patient"},"value"]]'))[1]
having count(pt.*) = 1

select jsonb_agg(pt.*) as pts
from patient pt
where pt.resource @@ 'identifier.#(system = "urn:source:rmis:Patient")'::jsquery and pt.resource @@ 'extension.#(url = "urn:extension:patient-type" and valueCode = "newborn")'::jsquery
group by (knife_extract_text(resource,'[["identifier",{"system":"urn:source:rmis:Patient"},"value"]]'))[1]
having count(pt.*) = 1

select pt.resource as pts
from patient pt
join organization org on org.id = 'bf3658a1-585e-4dbb-901c-7d24cd0db7ba'
where pt.resource @@ 'identifier.#(system = "urn:source:rmis:Patient")'::jsquery 
	and pt.resource @@ 'extension.#(url = "urn:extension:patient-type" and valueCode = "newborn")'::jsquery
	and pt.resource @@ logic_revinclude(org.resource, org.id, 'managingOrganization')
group by (knife_extract_text(pt.resource,'[["identifier",{"system":"urn:source:rmis:Patient"},"value"]]'))[1], pt.resource 
having count(pt.*) = 1

