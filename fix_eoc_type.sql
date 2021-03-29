select * from episodeofcare eoc 
where eoc.resource @@ 'type.#.coding.#.code = "PregnantCard_EKO"'::jsquery

update episodeofcare 
set resource = jsonb_set(resource, '{type}', jsonb_build_array(
												jsonb_build_object('coding', jsonb_build_array(jsonb_build_object('code', 'PregnantCard', 'system', 'urn:CodeSystem:episodeofcare-type', 'display', '����� ����������') ) ) ,
												jsonb_build_object('coding', jsonb_build_array(jsonb_build_object('code', 'PregnantCard_EKO', 'system', 'urn:CodeSystem:PregnantCard-subtype', 'display', '����� ���������� __ ������ ������������ ��������������') ) ) ) )
where resource @@ 'type.#.coding.#.code = "PregnantCard_EKO"'::jsquery												
returning id 												