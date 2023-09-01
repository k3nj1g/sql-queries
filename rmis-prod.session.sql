select resource
from practitionerrole
where resource @@ 'code.#(coding.#.system="urn:CodeSystem:frmr.position" and text=*)'::jsquery
limit 100;