BEGIN;

ALTER TABLE outboundqueue DROP CONSTRAINT outboundqueue2_pkey;
ALTER TABLE outboundqueue ADD CONSTRAINT outboundqueue_pkey PRIMARY KEY (id);

CREATE TABLE outboundqueue2 (LIKE outboundqueue INCLUDING ALL)
PARTITION BY RANGE(cts);

CREATE TABLE outboundqueue2_2022_1 PARTITION OF outboundqueue2
    FOR VALUES FROM ('2022-01-01') TO ('2022-04-01');
    
CREATE TABLE outboundqueue2_2022_2 PARTITION OF outboundqueue2
    FOR VALUES FROM ('2022-04-01') TO ('2022-07-01');
    
CREATE TABLE outboundqueue2_2022_3 PARTITION OF outboundqueue2
    FOR VALUES FROM ('2022-07-01') TO ('2022-10-01');
    
CREATE TABLE outboundqueue2_2022_4 PARTITION OF outboundqueue2
    FOR VALUES FROM ('2022-10-01') TO ('2023-01-01');

CREATE TABLE outboundqueue2_2023_1 PARTITION OF outboundqueue2
    FOR VALUES FROM ('2023-01-01') TO ('2023-04-01');
    
CREATE TABLE outboundqueue2_2023_2 PARTITION OF outboundqueue2
    FOR VALUES FROM ('2023-04-01') TO ('2023-07-01');
    
CREATE TABLE outboundqueue2_2023_3 PARTITION OF outboundqueue2
    FOR VALUES FROM ('2023-07-01') TO ('2023-10-01');
    
CREATE TABLE outboundqueue2_2023_4 PARTITION OF outboundqueue2
    FOR VALUES FROM ('2023-10-01') TO ('2024-01-01');

CREATE TABLE outboundqueue2_default PARTITION OF outboundqueue2
    DEFAULT;

INSERT INTO outboundqueue2
SELECT * FROM outboundqueue;

DROP TABLE public.outboundqueue;
ALTER TABLE public.outboundqueue2 RENAME TO outboundqueue;

COMMIT;