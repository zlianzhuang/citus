/* citus--7.0-1--7.0-2.sql */

-- set schema to pg_catalog?

-- rename the placementid sequence
  -- to do this, need to touch many tests as well...

ALTER SEQUENCE pg_catalog.pg_dist_shard_placement_placementid_seq
  RENAME TO pg_dist_placement_placementid_seq;

ALTER TABLE pg_catalog.pg_dist_shard_placement
  ALTER COLUMN placementid SET DEFAULT nextval('pg_catalog.pg_dist_placement_placementid_seq');

-- also do something about pg_dist_shard_placement_placementid_index

--CREATE TABLE pg_dist_placement (
--  placementid BIGINT NOT NULL default nextval('pg_dist_placement_placementid_seq'::regclass),
--  shardid BIGINT NOT NULL,
--  shardstate INT NOT NULL,
--  shardlength BIGINT NOT NULL,
--  groupid INT NOT NULL
--);

-- fill placement with the old contents

--DROP TABLE pg_dist_shard_placement;
--CREATE VIEW pg_dist_shard_placement AS
--SELECT shardid, shardstate, shardlength, placmentid, nodename, nodeport
-- assumes there's only one node per group
--FROM pg_dist_placement placement INNER JOIN pg_dist_node node ON (
--  placement.groupid = node.groupid
--);
