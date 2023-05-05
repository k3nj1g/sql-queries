SELECT *, date_trunc('minute',cts) AS time
FROM (SELECT resource ->> 'queueType' "Очередь",
             count(*)
      FROM outboundqueue
      WHERE resource @@ 'status in ("pending","errored","unprocessable")'::jsquery
      GROUP BY 1,2) outboundqueue
WHERE $__timeFilter(ts)
ORDER BY date_trunc('minute',cts)