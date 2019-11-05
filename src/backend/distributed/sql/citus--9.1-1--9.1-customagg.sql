SET search_path = 'citus';

CREATE FUNCTION worker_partial_agg_sfunc(internal, oid, anyelement)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION worker_partial_agg_ffunc(internal)
RETURNS cstring
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION coord_combine_agg_sfunc(internal, oid, cstring, anyelement)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION coord_combine_agg_ffunc(internal, oid, cstring, anyelement)
RETURNS anyelement
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

-- select worker_partial_agg(agg, ...)
-- equivalent to
-- select to_cstring(agg_without_ffunc(...))
CREATE AGGREGATE worker_partial_agg(oid, anyelement) (
	STYPE = internal,
	SFUNC = worker_partial_agg_sfunc,
	FINALFUNC = worker_partial_agg_ffunc
);

-- select coord_combine_agg(agg, col)
-- equivalent to
-- select agg_ffunc(agg_combine(from_cstring(col)))
CREATE AGGREGATE coord_combine_agg(oid, cstring, anyelement) (
	STYPE = internal,
	SFUNC = coord_combine_agg_sfunc,
	FINALFUNC = coord_combine_agg_ffunc,
	FINALFUNC_EXTRA
);

RESET search_path;
