--
-- REPLICATE_REF_TABLES_ON_COORDINATOR
--

CREATE SCHEMA replicate_ref_to_coordinator;
SET search_path TO 'replicate_ref_to_coordinator';
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 4;
SET citus.next_shard_id TO 8000000;
SET citus.next_placement_id TO 8000000;

--- enable logging to see which tasks are executed locally
SET client_min_messages TO LOG;
SET citus.log_local_commands TO ON;

CREATE TABLE squares(a int, b int);
SELECT create_reference_table('squares');
INSERT INTO squares SELECT i, i * i FROM generate_series(1, 10) i;

SELECT groupid FROM pg_dist_placement 
WHERE shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid='squares'::regclass::oid)
ORDER BY groupid;

-- should be executed on a worker
SELECT count(*) FROM squares;

-- should error out
SELECT replicate_reference_table_to_coordinator('squares');

INSERT INTO pg_dist_node(nodeid, groupid, nodename, nodeport, noderack, 
                         hasmetadata, isactive, noderole, nodecluster, metadatasynced)
    VALUES (0, 0, 'localhost', :master_port, 'default', true, true, 'primary', 'default', true);

-- try again
SELECT replicate_reference_table_to_coordinator('squares');

SELECT groupid FROM pg_dist_placement 
WHERE shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid='squares'::regclass::oid)
ORDER BY groupid;

-- should be executed locally
SELECT count(*) FROM squares;

DROP TABLE squares;

DROP SCHEMA replicate_ref_to_coordinator CASCADE;
SET search_path TO DEFAULT;
