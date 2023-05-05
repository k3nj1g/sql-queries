SELECT *
FROM integrationqueue
WHERE ((resource ->> 'status') = 'pending')
AND   ((resource -> 'payload') @@ 'resourceType in ("Patient","PersonBinding","Sector")'::jsquery)
AND   (id < 'x2317')
AND   (id > 'x2314')
AND   ((resource ->> 'clientId') = 'tfoms')
ORDER BY ts ASC LIMIT 5000

SELECT *
FROM integrationqueue
WHERE ((resource ->> 'status') = 'pending')
AND   ((resource -> 'payload') @@ 'resourceType in ("Patient","PersonBinding","Sector")'::jsquery)
AND   (id < 'x2317')
AND   (id > 'x2314')
AND   ((resource ->> 'clientId') = 'rmis')
ORDER BY ts ASC LIMIT 5000

SELECT *
FROM integrationqueue
WHERE ((resource ->> 'status') = 'pending')
AND   ((resource -> 'payload') @@ 'resourceType in ("EpisodeOfCare","Condition","Procedure","Observation","Task","RiskAssessment")'::jsquery)
AND   (id < 'x2317')
AND   (id > 'x2314')
ORDER BY ts ASC LIMIT 5000

SELECT *
FROM integrationqueue
WHERE ((resource ->> 'status') = 'pending')
AND   ((resource -> 'payload') @@ 'resourceType="DiagnosticReport"'::jsquery)
AND   (id < 'x2317')
AND   (id > 'x2314')
AND   NOT ((resource ->> 'clientId') IN ('lis','lis-galen','archimed'))
ORDER BY ts ASC LIMIT 5000

SELECT *
FROM integrationqueue
WHERE ((resource ->> 'status') = 'pending')
AND   ((resource -> 'payload') @@ 'resourceType="DiagnosticReport"'::jsquery)
AND   (id < 'x2317')
AND   (id > 'x2314')
AND   ((resource ->> 'clientId') = 'lis')
ORDER BY ts ASC LIMIT 5000

SELECT *
FROM integrationqueue
WHERE ((resource ->> 'status') = 'pending')
AND   ((resource -> 'payload') @@ 'resourceType="DiagnosticReport"'::jsquery)
AND   (id < 'x2317')
AND   (id > 'x2314')
AND   ((resource ->> 'clientId') IN ('lis-galen','archimed'))
ORDER BY ts ASC LIMIT 5000

SELECT *
FROM integrationqueue
WHERE ((resource ->> 'status') = 'pending')
AND   ((resource -> 'payload') @@ '((resourceType="ServiceRequest" and not priority in ("urgent","asap","stat")) or (resourceType in ("Specimen","Encounter")))'::jsquery)
AND   (id < 'x2317')
AND   (id > 'x2314')
ORDER BY ts ASC LIMIT 5000

SELECT *
FROM integrationqueue
WHERE ((resource ->> 'status') = 'pending')
AND   ((resource -> 'payload') @@ 'resourceType="ServiceRequest" and priority in ("urgent","asap","stat")'::jsquery)
AND   (id < 'x2317')
AND   (id > 'x2314')
ORDER BY ts ASC LIMIT 5000
