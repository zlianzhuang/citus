CREATE SCHEMA mx_add_coordinator;
SET search_path TO mx_add_coordinator,public;
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 8;
SET citus.next_shard_id TO 7000000;
SET citus.next_placement_id TO 7000000;
SET citus.replication_model TO streaming;
SET client_min_messages TO WARNING;

SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);

-- test that coordinator pg_dist_node entry is synced to the workers
SELECT wait_until_metadata_sync();

SELECT verify_metadata('localhost', :worker_1_port),
       verify_metadata('localhost', :worker_2_port);

CREATE TABLE ref(a int);
SELECT create_reference_table('ref');

-- test that changes from a metadata node is reflected in the coordinator placement
\c - - - :worker_1_port
SET search_path TO mx_add_coordinator,public;
INSERT INTO ref VALUES (1), (2), (3);
UPDATE ref SET a = a + 1;
DELETE FROM ref WHERE a > 3;

\c - - - :master_port
SET search_path TO mx_add_coordinator,public;
SELECT * FROM ref ORDER BY a;

SELECT master_remove_node('localhost', :master_port);

-- test that coordinator pg_dist_node entry was removed from the workers
SELECT wait_until_metadata_sync();
SELECT verify_metadata('localhost', :worker_1_port),
       verify_metadata('localhost', :worker_2_port);

DROP SCHEMA mx_add_coordinator CASCADE;
SET search_path TO DEFAULT;
RESET client_min_messages;
