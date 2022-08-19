SELECT *
FROM integrationqueue
WHERE resource @@ 'payload.identifier.#.system = "urn:source:mco:DiagnosticReport"'::jsquery
ORDER BY ts DESC 
LIMIT 10


SELECT *
FROM diagnosticreport d 
WHERE resource @@ 'result.#.identifier = *'::jsquery

SELECT count(*)
FROM integrationqueue i 
WHERE resource @@ 'status = pending'::jsquery
    AND cts < '2021-10-13T14:38'

SELECT *
FROM integrationqueue
ORDER BY ts DESC 
LIMIT 10

SELECT *
FROM diagnosticreport d 
WHERE resource @@ 'identifier.#.value = "c287d01e-6ad2-43ac-ad47-980d50968ac1"'::jsquery