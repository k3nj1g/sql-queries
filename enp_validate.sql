CREATE OR REPLACE FUNCTION enp_valid(enp text) RETURNS boolean 
AS $$
BEGIN
	IF enp IS NULL THEN RETURN FALSE;
	ELSIF char_length(enp) < 16 THEN RETURN FALSE;
	ELSIF enp !~ '^[0-9]+$' THEN RETURN FALSE;
	ELSIF enp = '0000000000000000' THEN RETURN FALSE;
	ELSE 
		RETURN (WITH splited AS (
		   	SELECT regexp_split_to_table(substring(enp FROM 1 FOR 15),'') c
		)
		, splited_with_row AS (
		 	SELECT *, ROW_NUMBER() OVER () row_num
			FROM splited  
		)
		, odd AS (
			SELECT *
			FROM splited_with_row 
			WHERE row_num % 2 = 1
			ORDER BY row_num DESC
		)
		, even AS (
			SELECT *
			FROM splited_with_row 
			WHERE row_num % 2 = 0
			ORDER BY row_num DESC
		)
		, action_a AS (
			SELECT int4(string_agg(c, '')) * 2 a
			FROM odd
		)
		, action_b AS (
			SELECT string_agg(c, '') b
			FROM even, action_a
		)
		, action_c AS (
			SELECT concat(b, text(a)) c
			FROM action_a, action_b 
		)
		, action_d AS (
			SELECT regexp_split_to_table(c,'') d
			FROM action_c
		)
		, action_e AS (
			SELECT MOD(sum(int4(d)), 10) e
			FROM action_d
		)
		, action_f AS (
			SELECT CASE WHEN e = 0 THEN 0 ELSE 10 - e END f
			FROM action_e
		)
		SELECT int4(substring(enp FROM 16 FOR 17)) = f
		FROM action_f);
	END IF;
END;
$$ LANGUAGE plpgsql
IMMUTABLE;

SELECT enp_validate('2187599739000091');
SELECT enp_validate('0000000000000000');

