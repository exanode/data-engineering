CREATE TABLE DE_T1 (
    col1 NUMBER
);

INSERT ALL
    INTO DE_T1 (col1) VALUES (2)
    INTO DE_T1 (col1) VALUES (3)
    INTO DE_T1 (col1) VALUES (11)
    INTO DE_T1 (col1) VALUES (12)
    INTO DE_T1 (col1) VALUES (13)
    INTO DE_T1 (col1) VALUES (27)
    INTO DE_T1 (col1) VALUES (33)
    INTO DE_T1 (col1) VALUES (34)
    INTO DE_T1 (col1) VALUES (35)
    INTO DE_T1 (col1) VALUES (42)
SELECT * FROM dual;

select * from de_t1;


-- Conventional Solution -- scales in quadratic time 
with base as (
    SELECT col1,
        (SELECT MIN(B.col1)
        FROM DE_T1  B
        WHERE B.col1 >= A.col1
        -- is this row the last in its group?
        AND NOT EXISTS
            (SELECT *
            FROM DE_T1  C
            WHERE C.col1 = B.col1 + 1)) AS grp
    FROM DE_T1  A
)
select min(col1) startval, max(col1) endval
from base group by grp;

-- Window function -- scales almost linearly
WITH base AS (
SELECT col1, 
    ROW_NUMBER() OVER(ORDER BY col1) - col1 AS grp
from DE_T1
)
SELECT 
    MIN(col1) startnum, 
    MAX(col1) endnum
FROM base
GROUP BY grp;


CREATE TABLE DE_T2 (
    keycol NUMBER,
    col1 VARCHAR2(10)
);

INSERT ALL
    INTO DE_T2 (keycol, col1) VALUES (2, 'A')
    INTO DE_T2 (keycol, col1) VALUES (3, 'A')
    INTO DE_T2 (keycol, col1) VALUES (5, 'B')
    INTO DE_T2 (keycol, col1) VALUES (7, 'B')
    INTO DE_T2 (keycol, col1) VALUES (11, 'B')
    INTO DE_T2 (keycol, col1) VALUES (13, 'C')
    INTO DE_T2 (keycol, col1) VALUES (17, 'C')
    INTO DE_T2 (keycol, col1) VALUES (19, 'C')
    INTO DE_T2 (keycol, col1) VALUES (23, 'C')
SELECT * FROM dual;

select * from DE_T2;

SELECT keycol, col1,
COUNT(*) OVER(ORDER BY col1
ROWS BETWEEN UNBOUNDED PRECEDING
AND CURRENT ROW) AS cnt
FROM DE_T2;