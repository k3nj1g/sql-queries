SELECT *
FROM outboundqueue o 
--WHERE resource @@ 'status=pending'::jsquery
--WHERE resource @@ 'payload.servicerequest-id="35d49ede-c929-4a34-9d4a-79857f4b9919"'::jsquery
ORDER BY cts DESC 
LIMIT 100;

SELECT *
FROM outboundqueue o 
WHERE resource @@ 'queueType="send-lmi-payment"'::jsquery
ORDER BY cts DESC 
LIMIT 100;

SELECT *
FROM servicerequest s 
JOIN diagnosticreport d ON d.resource @@ logic_revinclude(s.resource, s.id, 'basedOn.#') 
WHERE s.id = '35d49ede-c929-4a34-9d4a-79857f4b9919'

SELECT *
FROM flag f 
WHERE resource #>> '{subject,id}' = '00b5b736-bc5e-4f7f-8101-4e862110059d'
  AND resource @@ 'code.coding.#.code="R01.1"'::jsquery
  
SELECT *
FROM careteam
WHERE resource @@ 'subject.identifier.value = "2155720829000167"'::jsquery
ORDER BY cts DESC 

SELECT *
FROM documentreference
WHERE resource @@ 'subject.identifier.value = "2155720829000167"'::jsquery
ORDER BY cts DESC 

LIMIT 1
SELECT *
FROM encounter e 
WHERE resource @@ 'identifier.#.value = "db22b7ca-3364-4944-b645-80f4a3850929"'::jsquery

SELECT *
FROM outboundqueue_history oh 
WHERE id = '277a9990-8974-4412-b245-7134990dc82b'

SELECT *
FROM outboundqueue o 
JOIN encounter e ON e.id = o.resource #>> '{payload,encounter}'
  AND e.resource #>> '{subject,identifier,value}' = '2155720829000167'
ORDER BY o.cts DESC 
LIMIT 100;

SELECT *
FROM outboundqueue o 
WHERE id = '277a9990-8974-4412-b245-7134990dc82b'


SELECT *
FROM schedulerule s 
WHERE resource #>> '{planningHorizon,start}' >= resource #>> '{planningHorizon,end}'