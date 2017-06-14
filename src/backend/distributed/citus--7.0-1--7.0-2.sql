/* citus--7.0-1--7.0-2.sql */

ALTER SEQUENCE pg_catalog.pg_dist_shard_placement_placementid_seq
  RENAME TO pg_dist_placement_placementid_seq;

ALTER TABLE pg_catalog.pg_dist_shard_placement
  ALTER COLUMN placementid SET DEFAULT nextval('pg_catalog.pg_dist_placement_placementid_seq');

-- also do something about pg_dist_shard_placement_placementid_index

CREATE TABLE citus.pg_dist_placement (
  placementid BIGINT NOT NULL default nextval('pg_dist_placement_placementid_seq'::regclass),
  shardid BIGINT NOT NULL,
  shardstate INT NOT NULL,
  shardlength BIGINT NOT NULL,
  groupid INT NOT NULL
);
ALTER TABLE citus.pg_dist_placement SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.pg_dist_placement TO public;

INSERT INTO pg_catalog.pg_dist_placement
SELECT placementid, shardid, shardstate, shardlength, node.groupid
FROM pg_dist_shard_placement placement LEFT JOIN pg_dist_node node ON (
  -- use a LEFT JOIN so if the node is missing for some reason we error out
  placement.nodename = node.nodename AND placement.nodeport = node.nodeport
);

--DROP TABLE pg_dist_shard_placement;
--CREATE VIEW pg_dist_shard_placement AS
--SELECT shardid, shardstate, shardlength, placmentid, nodename, nodeport
-- assumes there's only one node per group
--FROM pg_dist_placement placement INNER JOIN pg_dist_node node ON (
--  placement.groupid = node.groupid
--);
