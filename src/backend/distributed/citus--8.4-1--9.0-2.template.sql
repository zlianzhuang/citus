/* citus--8.4-1--9.0-2 */

SET search_path = 'pg_catalog';

CREATE TYPE isdatanode AS ENUM('true', 'marked for removal', 'false');
ALTER TABLE pg_dist_node ADD isdatanode isdatanode NOT NULL DEFAULT 'true';

@include udfs/master_add_node/9.0-2.sql
@include udfs/master_activate_node/9.0-2.sql
@include udfs/master_add_secondary_node/9.0-2.sql
@include udfs/master_add_inactive_node/9.0-2.sql

RESET search_path;
