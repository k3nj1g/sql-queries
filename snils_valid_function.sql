CREATE OR REPLACE FUNCTION snils_valid(snils text) 
RETURNS boolean 
AS $$
DECLARE 
    snils_num VARCHAR(14);
    checksum INT;
    digit_sum INT;
    outcome boolean;
BEGIN
    IF snils IS NULL OR snils !~ '^[0-9]{3}\-[0-9]{3}\-[0-9]{3}\ [0-9]{2}$' OR snils = '000-000-000 00' 
    THEN 
        outcome = FALSE;
    ELSE 
        snils_num = REPLACE((REPLACE(snils,' ','')),'-','');
        digit_sum = CAST(SUBSTRING(snils_num,1,1) AS INT) * 9 
                    + CAST(SUBSTRING(snils_num,2,1) as INT) * 8 
                    + CAST(SUBSTRING(snils_num,3,1) as INT) * 7
                    + CAST(SUBSTRING(snils_num,4,1) AS INT) * 6
                    + CAST(SUBSTRING(snils_num,5,1) AS INT) * 5
                    + CAST(SUBSTRING(snils_num,6,1) AS INT) * 4 
                    + CAST(SUBSTRING(snils_num,7,1) AS INT) * 3
                    + CAST(SUBSTRING(snils_num,8,1) AS INT) * 2 
                    + CAST(SUBSTRING(snils_num,9,1) AS INT) * 1;
        checksum = CASE WHEN digit_sum < 100 THEN digit_sum
                        WHEN digit_sum = 100 OR digit_sum % 100 = 1 THEN 0
                        WHEN digit_sum > 101 THEN digit_sum % 101
                   END;
        IF CAST(REVERSE(SUBSTRING(REVERSE(snils),1,2)) AS INT) = checksum 
        THEN 
            outcome = TRUE;
        ELSE
            outcome = FALSE;
        END IF;
    END IF;
RETURN outcome;
END;
$$ LANGUAGE plpgsql
IMMUTABLE;

SELECT snils_valid('998-485-029 03');


