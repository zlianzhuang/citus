CREATE SCHEMA intermediate_result_pruning;
SET search_path TO intermediate_result_pruning;


SET citus.shard_count TO 4;

CREATE TABLE table_1 (key int, value text);
SELECT create_distributed_table('table_1', 'key');

CREATE TABLE table_2 (key int, value text);
SELECT create_distributed_table('table_2', 'key');


CREATE TABLE table_3 (key int, value text);
SELECT create_distributed_table('table_3', 'key');

CREATE TABLE ref_table (key int, value text);
SELECT create_reference_table('ref_table');


-- load some data
INSERT INTO table_1    VALUES (1, '1'), (2, '2'), (3, '3'), (4, '4'); 
INSERT INTO table_2    VALUES (3, '3'), (4, '4'), (5, '5'), (6, '6'); 
INSERT INTO table_3    VALUES (3, '3'), (4, '4'), (5, '5'), (6, '6'); 
INSERT INTO ref_table  VALUES (1, '1'), (2, '2'), (3, '3'), (4, '4'), (5, '5'), (6, '6'); 


-- a very basic case, where the intermediate result
-- should go to both workers
WITH some_values AS 
	(SELECT key FROM table_1 WHERE value IN ('3', '4'))
SELECT 
	count(*) 
FROM 
	some_values JOIN table_2 USING (key);


-- a very basic case, where the intermediate result
-- should only go to one worker because the final query is a router
-- we use random() to prevent postgres inline the CTE(s)
WITH some_values AS 
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4'))
SELECT 
	count(*) 
FROM 
	some_values JOIN table_2 USING (key) WHERE table_2.key = 1;

-- a similar query as above, but this time use the CTE inside
-- another CTE
WITH some_values AS 
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
	some_value_2 AS 
	(SELECT key, random() FROM some_values)
SELECT 
	count(*) 
FROM 
	some_value_2 JOIN table_2 USING (key) WHERE table_2.key = 1;

-- the second CTE does a join with a distributed table
-- and the final query is a router query
WITH some_values AS 
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
	some_value_2 AS 
	(SELECT key, random() FROM some_values JOIN table_2 USING (key))
SELECT 
	count(*) 
FROM 
	some_value_2 JOIN table_2 USING (key) WHERE table_2.key = 3;

-- the first CTE is used both within second CTE and the final query
-- the second CTE does a join with a distributed table
-- and the final query is a router query
WITH some_values AS 
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
	some_value_2 AS 
	(SELECT key, random() FROM some_values JOIN table_2 USING (key))
SELECT 
	count(*) 
FROM 
	(some_value_2 JOIN table_2 USING (key)) JOIN some_values  USING (key) WHERE table_2.key = 3;

-- the same query with the above, but the final query is hitting all shards
WITH some_values AS 
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
	some_value_2 AS 
	(SELECT key, random() FROM some_values JOIN table_2 USING (key))
SELECT 
	count(*) 
FROM 
	(some_value_2 JOIN table_2 USING (key)) JOIN some_values  USING (key) WHERE table_2.key != 3;


-- join with a reference table, intermediate result should
-- only hit one worker
WITH some_values AS 
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4'))
SELECT 
	count(*) 
FROM
	some_values JOIN ref_table USING (key);


-- similar query as the above, but this time the final query
-- hits all shards because of the join with a distributed table
WITH some_values AS 
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4'))
SELECT 
	count(*) 
FROM
	(some_values JOIN ref_table USING (key)) JOIN table_2 USING (key);


-- now, the second CTE has a single shard join with a distributed table
-- so the first CTE should only be broadcasted to that node
-- since the final query doesn't have a join, it should simply be broadcasted
-- to one node
WITH some_values AS 
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
	some_value_2 AS 
	(SELECT key, random() FROM some_values JOIN table_2 USING (key) WHERE key = 1)
SELECT 
	count(*)
FROM
	some_value_2;


-- the sama query inlined inside a CTE, and the final query has a 
-- join with a distributed table
WITH top_cte as (
		WITH some_values AS 
		(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
		some_value_2 AS 
		(SELECT key, random() FROM some_values JOIN table_2 USING (key) WHERE key = 1)
	SELECT 
		DISTINCT key 
	FROM
		some_value_2
)
SELECT 
	count(*) 
FROM
	top_cte JOIN table_2 USING (key);

-- some_values is first used by a single shard-query, and than with a multi-shard
-- query on the top query
WITH some_values AS 
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
	some_value_2 AS 
	(SELECT key, random() FROM some_values JOIN table_2 USING (key) WHERE key = 1)
SELECT 
	count(*) 
FROM 
	(some_value_2 JOIN table_2 USING (key)) JOIN some_values USING (key);

-- some_values is first used by a single shard-query, and than with a multi-shard
-- CTE, finally a cartesian product join
WITH some_values AS 
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
	some_value_2 AS 
	(SELECT key, random() FROM some_values JOIN table_2 USING (key) WHERE key = 1),
	some_value_3 AS 
	(SELECT key FROM (some_value_2 JOIN table_2 USING (key)) JOIN some_values USING (key))
SELECT * FROM some_value_3 JOIN ref_table ON (true);



WITH some_values AS 
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
	some_value_2 AS 
	(SELECT key, random() FROM some_values JOIN table_2 USING (key))
SELECT 
	count(*) 
FROM 
	some_value_2 JOIN table_1 USING (key) WHERE table_1.key = 3;




WITH some_values AS 
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
	some_value_2 AS 
	(SELECT key, random() FROM table_2 WHERE value IN ('3', '4')),
	some_value_3 AS 
	(SELECT * FROM some_value_2 JOIN some_values USING (key))
	
SELECT 
	count(*) 
FROM 
	some_value_3;



WITH some_values AS 
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
	some_value_2 AS 
	(SELECT key, random() FROM some_values),
	some_value_3 AS 
	(SELECT key, random() FROM some_values)
SELECT 
	count(*) 
FROM 
	some_value_3;

WITH select_data AS (
	SELECT * FROM table_1
),
raw_data AS (
	DELETE FROM table_2 WHERE key >= (SELECT min(key) FROM select_data WHERE key > 10) RETURNING *
)
SELECT * FROM raw_data;


DROP SCHEMA intermediate_result_pruning CASCADE;
