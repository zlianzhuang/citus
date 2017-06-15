--
-- MULTI_COLOCATED_SHARD_TRANSFER
--

-- These tables are created in multi_colocation_utils test

-- test repair
-- manually set shardstate as inactive
UPDATE pg_dist_placement SET shardstate = 3
  WHERE shardid IN (1300000, 1300004, 1300016, 1300020)
  AND groupid = (SELECT groupid FROM pg_dist_node WHERE nodeport = :worker_2_port);


-- test repairing colocated shards
-- status before shard repair
SELECT s.shardid, s.logicalrelid::regclass, sp.nodeport, p.colocationid, sp.shardstate
FROM 
    pg_dist_partition p, pg_dist_shard s, pg_dist_shard_placement sp
WHERE
    p.logicalrelid = s.logicalrelid AND
    s.shardid = sp.shardid AND
    colocationid = (SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'table1_group1'::regclass)
ORDER BY s.shardid, sp.nodeport;

-- repair colocated shards
SELECT master_copy_shard_placement(1300000, 'localhost', :worker_1_port, 'localhost', :worker_2_port);

-- status after shard repair
SELECT s.shardid, s.logicalrelid::regclass, sp.nodeport, p.colocationid, sp.shardstate
FROM 
    pg_dist_partition p, pg_dist_shard s, pg_dist_shard_placement sp
WHERE
    p.logicalrelid = s.logicalrelid AND
    s.shardid = sp.shardid AND
    colocationid = (SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'table1_group1'::regclass)
ORDER BY s.shardid, sp.nodeport;


-- test repairing NOT colocated shard
-- status before shard repair
SELECT s.shardid, s.logicalrelid::regclass, sp.nodeport, p.colocationid, sp.shardstate
FROM 
    pg_dist_partition p, pg_dist_shard s, pg_dist_shard_placement sp
WHERE
    p.logicalrelid = s.logicalrelid AND
    s.shardid = sp.shardid AND
    p.logicalrelid = 'table5_groupX'::regclass
ORDER BY s.shardid, sp.nodeport;

-- repair NOT colocated shard
SELECT master_copy_shard_placement(1300016, 'localhost', :worker_1_port, 'localhost', :worker_2_port);

-- status after shard repair
SELECT s.shardid, s.logicalrelid::regclass, sp.nodeport, p.colocationid, sp.shardstate
FROM 
    pg_dist_partition p, pg_dist_shard s, pg_dist_shard_placement sp
WHERE
    p.logicalrelid = s.logicalrelid AND
    s.shardid = sp.shardid AND
    p.logicalrelid = 'table5_groupX'::regclass
ORDER BY s.shardid, sp.nodeport;


-- test repairing shard in append distributed table
-- status before shard repair
SELECT s.shardid, s.logicalrelid::regclass, sp.nodeport, p.colocationid, sp.shardstate
FROM 
    pg_dist_partition p, pg_dist_shard s, pg_dist_shard_placement sp
WHERE
    p.logicalrelid = s.logicalrelid AND
    s.shardid = sp.shardid AND
    p.logicalrelid = 'table6_append'::regclass
ORDER BY s.shardid, sp.nodeport;

-- repair  shard in append distributed table
SELECT master_copy_shard_placement(1300020, 'localhost', :worker_1_port, 'localhost', :worker_2_port);

-- status after shard repair
SELECT s.shardid, s.logicalrelid::regclass, sp.nodeport, p.colocationid, sp.shardstate
FROM 
    pg_dist_partition p, pg_dist_shard s, pg_dist_shard_placement sp
WHERE
    p.logicalrelid = s.logicalrelid AND
    s.shardid = sp.shardid AND
    p.logicalrelid = 'table6_append'::regclass
ORDER BY s.shardid, sp.nodeport;


-- test repair while all placements of one shard in colocation group is unhealthy
-- manually set shardstate as inactive
UPDATE pg_dist_placement SET shardstate = 3 WHERE shardid = 1300000;

-- status before shard repair
SELECT s.shardid, s.logicalrelid::regclass, sp.nodeport, p.colocationid, sp.shardstate
FROM 
    pg_dist_partition p, pg_dist_shard s, pg_dist_shard_placement sp
WHERE
    p.logicalrelid = s.logicalrelid AND
    s.shardid = sp.shardid AND
    colocationid = (SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'table1_group1'::regclass)
ORDER BY s.shardid, sp.nodeport;

-- repair while all placements of one shard in colocation group is unhealthy
SELECT master_copy_shard_placement(1300000, 'localhost', :worker_1_port, 'localhost', :worker_2_port);

-- status after shard repair
SELECT s.shardid, s.logicalrelid::regclass, sp.nodeport, p.colocationid, sp.shardstate
FROM 
    pg_dist_partition p, pg_dist_shard s, pg_dist_shard_placement sp
WHERE
    p.logicalrelid = s.logicalrelid AND
    s.shardid = sp.shardid AND
    colocationid = (SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'table1_group1'::regclass)
ORDER BY s.shardid, sp.nodeport;
