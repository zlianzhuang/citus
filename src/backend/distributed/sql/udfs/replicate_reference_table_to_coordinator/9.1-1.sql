CREATE FUNCTION pg_catalog.replicate_reference_table_to_coordinator(rel regclass)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$replicate_reference_table_to_coordinator$$;
