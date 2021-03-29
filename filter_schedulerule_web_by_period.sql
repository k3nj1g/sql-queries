-- кол-во расписаний без канала госуслуг на прием к врачу в периоде май
select count(sch.*) from schedulerule sch
join organization org on sch.resource @@ logic_revinclude(org.resource, org.id, 'mainOrganization')
join organizationinfo org_info on org.id = org_info.id
where sch.resource @@ 'not availableTime.#.channel.# = "web" and actor.#.resourceType = "PractitionerRole"'::jsquery
  and (sch.resource #>> '{planningHorizon,start}')::date < '2020-05-31'::date
    and ((sch.resource #>> '{planningHorizon,end}') is null or (sch.resource #>> '{planningHorizon,end}')::date > '2020-05-01'::date)
    and not jsonb_path_exists(org_info.resource, '$.identifier[*] ? (@.system == "urn:identity:reg-id:OrganizationInfo" && @.value starts with "24")')

-- кол-во расписаний без канала госуслуг на прием к врачу в периоде май с отображением информации об организации
select org.resource ->> 'name', count(sch.*) from schedulerule sch
join organization org on sch.resource @@ logic_revinclude(org.resource, org.id, 'mainOrganization')
join organizationinfo org_info on org.id = org_info.id
where sch.resource @@ 'availableTime.#.channel.# = "web" and actor.#.resourceType = "PractitionerRole"'::jsquery
  and (sch.resource #>> '{planningHorizon,start}')::date < '2020-05-31'::date
    and ((sch.resource #>> '{planningHorizon,end}') is null or (sch.resource #>> '{planningHorizon,end}')::date > '2020-05-01'::date)
    and not jsonb_path_exists(org_info.resource, '$.identifier[*] ? (@.system == "urn:identity:reg-id:OrganizationInfo" && @.value starts with "24")')
group by org.resource ->> 'name'

-- кол-во расписаний всего, с каналом госуслуг, без канала госуслуг на прием к врачу в периоде май
with all_sch as 
	(select org.id org_id, count(sch.*) c_sch from schedulerule sch
	join organization org on sch.resource @@ logic_revinclude(org.resource, org.id, 'mainOrganization')
	join organizationinfo org_info on org.id = org_info.id
	where sch.resource @@ 'actor.#.resourceType = "PractitionerRole"'::jsquery
		and (sch.resource #>> '{planningHorizon,start}')::date < '2020-05-31'::date
	    and ((sch.resource #>> '{planningHorizon,end}') is null or (sch.resource #>> '{planningHorizon,end}')::date > '2020-05-01'::date)
	    and not jsonb_path_exists(org_info.resource, '$.identifier[*] ? (@.system == "urn:identity:reg-id:OrganizationInfo" && @.value starts with "24")')
	group by org.id),
	web as 
	(select org.id org_id, count(sch.*) c_sch from schedulerule sch
	join organization org on sch.resource @@ logic_revinclude(org.resource, org.id, 'mainOrganization')
	join organizationinfo org_info on org.id = org_info.id
	where sch.resource @@ 'availableTime.#.channel.# = "web" and actor.#.resourceType = "PractitionerRole"'::jsquery
	  and (sch.resource #>> '{planningHorizon,start}')::date < '2020-05-31'::date
	    and ((sch.resource #>> '{planningHorizon,end}') is null or (sch.resource #>> '{planningHorizon,end}')::date > '2020-05-01'::date)
	    and not jsonb_path_exists(org_info.resource, '$.identifier[*] ? (@.system == "urn:identity:reg-id:OrganizationInfo" && @.value starts with "24")')
	group by org.id),
	no_web as 
	(select org.id org_id, count(sch.*) c_sch from schedulerule sch
	join organization org on sch.resource @@ logic_revinclude(org.resource, org.id, 'mainOrganization')
	join organizationinfo org_info on org.id = org_info.id
	where sch.resource @@ 'not availableTime.#.channel.# = "web" and actor.#.resourceType = "PractitionerRole"'::jsquery
	  and (sch.resource #>> '{planningHorizon,start}')::date < '2020-05-31'::date
	    and ((sch.resource #>> '{planningHorizon,end}') is null or (sch.resource #>> '{planningHorizon,end}')::date > '2020-05-01'::date)
	    and not jsonb_path_exists(org_info.resource, '$.identifier[*] ? (@.system == "urn:identity:reg-id:OrganizationInfo" && @.value starts with "24")')
	group by org.id)
select org.resource ->> 'name', all_sch.c_sch "all", web.c_sch with_web, no_web.c_sch without_web
from all_sch all_sch
left join web on all_sch.org_id = web.org_id
left join no_web on all_sch.org_id = no_web.org_id
join organization org on org.id = web.org_id

-- кол-во расписаний по конкретной организации
select sch.id, sch.resource from schedulerule sch
join organization org on sch.resource @@ logic_revinclude(org.resource, org.id, 'mainOrganization')
join organizationinfo org_info on org.id = org_info.id
where sch.resource ?? 'availableTime'
  	and (sch.resource #>> '{planningHorizon,start}')::date < '2020-05-31'::date
    	and ((sch.resource #>> '{planningHorizon,end}') is null or (sch.resource #>> '{planningHorizon,end}')::date > '2020-05-01'::date)
	and not jsonb_path_exists(org_info.resource, '$.identifier[*] ? (@.system == "urn:identity:reg-id:OrganizationInfo" && @.value starts with "24")')
  	and org.id = '0d891717-c282-4b66-b9d6-d8d52fd1079b'




