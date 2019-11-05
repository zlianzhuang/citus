setup
{
  SELECT citus_internal.replace_isolation_tester_func();
  SELECT citus_internal.refresh_isolation_tester_prepared_statement();

  SET citus.next_shard_id TO 8000000;
  SET citus.next_placement_id TO 8000000;

  SELECT master_add_node('localhost', 57636);

  CREATE TABLE ref_table(a int primary key);
  SELECT create_reference_table('ref_table');
  INSERT INTO ref_table VALUES (1), (2), (3), (4);

  CREATE TABLE dist_table(a int, b int);
  SELECT create_distributed_table('dist_table', 'a');
}

teardown
{
  SELECT master_remove_node('localhost', 57636);
  DROP TABLE ref_table, dist_table;
  SELECT citus_internal.restore_isolation_tester_func();
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-end"
{
    END;
}

step "s1-update-dist-table"
{
    update dist_table set b = 2 where a = 1;
}

step "s1-lock-ref-table-placement-on-coordinator"
{
    SELECT * FROM ref_table_8000000 FOR UPDATE;
}

session "s2"

step "s2-begin"
{
    BEGIN;
}

step "s2-end"
{
    END;
}

step "s2-update-dist-table"
{
    update dist_table set b = 2 where a = 1;
}

step "s2-lock-ref-table-placement-on-coordinator"
{
    SELECT * FROM ref_table_8000000 FOR UPDATE;
}

# we disable the daemon during the regression tests in order to get consistent results
# thus we manually issue the deadlock detection 
session "deadlock-checker"

# we issue the checker not only when there are deadlocks to ensure that we never cancel
# backend inappropriately
step "deadlock-checker-call"
{
  SELECT check_distributed_deadlocks();
}

# verify that locks on the placement of the reference table on the coordinator is
# taken into account when looking for distributed deadlocks
permutation "s1-begin" "s2-begin" "s1-update-dist-table" "s2-lock-ref-table-placement-on-coordinator" "s1-lock-ref-table-placement-on-coordinator" "s2-update-dist-table" "deadlock-checker-call" "s1-end" "s2-end"
