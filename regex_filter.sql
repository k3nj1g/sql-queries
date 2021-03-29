with regx as (select id, resource#>>'{links,practitionerRole,id}' prr_id, (regexp_matches(resource#>>'{links,practitionerRole,display}', '^\s\(\S+\)'))[1] from role)
select *
from regx