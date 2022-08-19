WITH grouped AS (
  SELECT prr.resource #>> '{derived,moshort}' org_name
    , prr.resource #>> '{code,0,text}' "role"
    , concat(pr.resource#>>'{name,0,family}', ' ', pr.resource#>>'{name,0,given,0}', ' ', pr.resource#>>'{name,0,given,1}') doctor 
    ,jsonb_path_query_first(pr.resource, '$. identifier ? (@.system=="urn:identity:snils:Practitioner"). value') #>> '{}' snils
    ,org.resource->>'name' sp
    , jsonb_build_object((current_date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb(current_date))) > 0 THEN '��' ELSE '���' END
                        ,((current_date + INTERVAL '1 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '1 day')::date))) > 0 THEN '��' ELSE '���' END
                        ,((current_date + INTERVAL '2 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '2 day')::date))) > 0 THEN '��' ELSE '���' END 
                        ,((current_date + INTERVAL '3 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '3 day')::date))) > 0 THEN '��' ELSE '���' END 
                        ,((current_date + INTERVAL '4 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '4 day')::date))) > 0 THEN '��' ELSE '���' END 
                        ,((current_date + INTERVAL '5 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '5 day')::date))) > 0 THEN '��' ELSE '���' END 
                        ,((current_date + INTERVAL '6 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '6 day')::date))) > 0 THEN '��' ELSE '���' END 
                        ,((current_date + INTERVAL '7 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '7 day')::date))) > 0 THEN '��' ELSE '���' END 
                        ,((current_date + INTERVAL '8 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '8 day')::date))) > 0 THEN '��' ELSE '���' END 
                        ,((current_date + INTERVAL '9 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '9 day')::date))) > 0 THEN '��' ELSE '���' END 
                        ,((current_date + INTERVAL '10 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '10 day')::date))) > 0 THEN '��' ELSE '���' END 
                        ,((current_date + INTERVAL '11 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '11 day')::date))) > 0 THEN '��' ELSE '���' END 
                        ,((current_date + INTERVAL '12 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '12 day')::date))) > 0 THEN '��' ELSE '���' END 
                        ,((current_date + INTERVAL '13 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '13 day')::date))) > 0 THEN '��' ELSE '���' END                        
                        ,((current_date + INTERVAL '14 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '14 day')::date))) > 0 THEN '��' ELSE '���' END
                        ,((current_date + INTERVAL '15 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '15 day')::date))) > 0 THEN '��' ELSE '���' END
                        ,((current_date + INTERVAL '16 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '16 day')::date))) > 0 THEN '��' ELSE '���' END
                        ,((current_date + INTERVAL '17 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '17 day')::date))) > 0 THEN '��' ELSE '���' END
                        ,((current_date + INTERVAL '18 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '18 day')::date))) > 0 THEN '��' ELSE '���' END
                        ,((current_date + INTERVAL '19 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '19 day')::date))) > 0 THEN '��' ELSE '���' END
                        ,((current_date + INTERVAL '20 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '20 day')::date))) > 0 THEN '��' ELSE '���' END
                        ,((current_date + INTERVAL '21 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '21 day')::date))) > 0 THEN '��' ELSE '���' END
                        ,((current_date + INTERVAL '22 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '22 day')::date))) > 0 THEN '��' ELSE '���' END
                        ,((current_date + INTERVAL '23 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '23 day')::date))) > 0 THEN '��' ELSE '���' END
                        ,((current_date + INTERVAL '24 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '24 day')::date))) > 0 THEN '��' ELSE '���' END
                        ,((current_date + INTERVAL '25 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '25 day')::date))) > 0 THEN '��' ELSE '���' END
                        ,((current_date + INTERVAL '26 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '26 day')::date))) > 0 THEN '��' ELSE '���' END
                        ,((current_date + INTERVAL '27 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '27 day')::date))) > 0 THEN '��' ELSE '���' END
                        ,((current_date + INTERVAL '28 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '28 day')::date))) > 0 THEN '��' ELSE '���' END
                        ,((current_date + INTERVAL '29 day')::date::text), CASE WHEN (count(s.id) FILTER (WHERE jsonb_path_query_array(schedule_days(s.id), '$.day') @> to_jsonb((current_date + INTERVAL '29 day')::date))) > 0 THEN '��' ELSE '���' END
                        ) activity
  FROM healthcareservice h 
  JOIN schedulerule s ON s.resource #>> '{healthcareService,0,id}' = h.id
    AND s.resource -> 'availableTime' @@ '#.channel.#($="web")'::jsquery
    AND immutable_ts(COALESCE((s.resource #>> '{planningHorizon,end}'::TEXT[]),'infinity'::TEXT)) > CURRENT_TIMESTAMP
  JOIN practitionerrole prr ON prr.id = jsonb_path_query_first(s.resource, '$.actor ? (@.resourceType=="PractitionerRole")') #>> '{id}'
    AND COALESCE(prr.resource->'active', 'true') = 'true'
  JOIN practitioner pr ON pr.resource @@ logic_include(prr.resource, 'practitioner')  
    AND replace(replace(jsonb_path_query_first(pr.resource, '$. identifier ? (@.system=="urn:identity:snils:Practitioner"). value') #>> '{}', '-', ''), ' ', '') not in ('13949855626','14647369796','15452338760','14956003675','15176430355','16492132065','15228780773','15973021985','12652627454','16499706527','15246732562','15976640422','17211202708','15977020397','12559803476','10448294851','10789980716','14573953897','15016854140','10789980716','17740977613','14107424618','15768129404','12801386942','14840806569','12801391834','13596802390','17400382133','15435080043','15560779389','15443246956','12542429639','07727429091','15004065905','11245619125','11431017994','14086153144','19362633995','12764355369','11983371885','14459611274','15205810426','11853279973','13285632056','10921768252','07434493474','16546185386','12465368059','15857836223','09198680423','15453946687','07434493474','10921768252','14036716940','13061941326','15782091791','14544743974','14567839408','15579845326','11796712084','12459346972','13742605756','14961353986','12420087002','15016092722','16042651132','15044994164','11643566956','15846099810','15352114933','13293868384','10636568153','12334391933','16084291061','15976042100','15440176643','10558418755','12505955752','14552974686','16225924865','15498125797','17260716259','16465140263','12830396756','15687287927','14803116333','11983372079','15213035406','15049576373','15076720559','15021093402','14127887266','15442787379','15027234727','12812258540','14884801603','15407865071','12097884481','11631804324','17309026351','15548986725','16118506849','18821995825','10950213725','15232181626','14006625010','05188243969','17153208242','08008891670','15789780142','15955646922','15094971687','15499400394','14575411669','11909709676','14987032503','10303431880','11291407019','14090618441','12953781187','11681169657','15406218538','15878514016','16782368816','15391896100','15987132616','11439914559','15419418769','05218212317','11001306753','16504305639','14565516273','17532724877','15073078446','16552705473','16373374681','15694749122','11655416347','15910865077','08261792986','14003580605','15781125070','16657522800','16550819882','15110058598','11385407343','10512542803','12855122655','17108976585','15446396586','14690940993','15014451815','16434191459','15520569253','13872602775','16719982726','14170208619','14407402124','15981994337','12457011830','09568383831','10528098846','12835925883','16477856120','15058699390','11959523086','15443293359','15440937766','14749364197','15436586989','16706480580','07545062768','13748264988','12272239328','13228245836','14560720449','12747694899','05047229030','13617722356','16985022102','18004198544','15102235800','14210874627','11853763069','13604961050','14546480877','16548550595','14137116625','16010991128','15852977822','00973147857','16535949000','15959569243','17375089195','17334149057','17304434341','15504997686','11093402005','15110917115','15157660870','15440176643','15081575658','07797176837','12830394954','16767004084','11392891466','15440176643','14687255805','02501272594','15420154924','13934715781','12942883790','14166820455','13019168223','15213035608','14246859477','14910234034','14910234034','15284476685','15237974181','01279750253','06013784836','14201005070','14086144547','15441308235','15927588522','16996547351','16219988097','12931176653','13938091082','15016845745','15065511539','12899266016','16485740400','15927569013','15531622949','15384726992','17747367823','00911583528','15446424262','08008891670','12193635346','11056248517','12195093851','15749134895','12659025663','15012624709','07646705291','01296193547','16468484714','12252568436','15208117831','13805707863','15208117831','15529540880','14486345282','16044812544','15166449269','15166449269','17480248076','14127542737','15995935650','08235565472','12729566181','11675521659','14860743482','15910605758','16442197972','15675995534','15889014916','12051157901','14493826693','12704649855','13623917963','11085257026','10912584443','15986326119','16846824415','12411730808','10921768555','15396659514','07877895359')
  JOIN organization org ON org.resource @@ logic_include(prr.resource, 'organization', 'and type.#.coding.#(system="urn:CodeSystem:frmo.structure-type" and code="1")')  
  WHERE h.resource @@ 'type.#.coding.#(system="urn:CodeSystem:service" and code="195")'::jsquery
  GROUP BY 1,2,3,4,5)
SELECT g.org_name "�������� ��", '����������� ���������������' "������", g.sp "�������� �������������", g."role" "���������", g.snils "�����", g.doctor "����/�������", g.activity "����������"
FROM grouped g
ORDER BY 1,4;