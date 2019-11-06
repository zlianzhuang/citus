--
-- AGGREGATE SUPPORT
--
-- Tests support for user defined aggregates

create schema aggregate_support;
set search_path to aggregate_support;

-- We test with & without STRICT as our code is responsible for managing these NULL checks
create function sum2_sfunc_strict(state int, x int)
returns int immutable strict language plpgsql as $$
begin return state + x;
end;
$$;

create function sum2_finalfunc_strict(state int)
returns int immutable strict language plpgsql as $$
begin return state * 2;
end;
$$;

create function sum2_sfunc(state int, x int)
returns int immutable language plpgsql as $$
begin return state + x;
end;
$$;

create function sum2_finalfunc(state int)
returns int immutable language plpgsql as $$
begin return state * 2;
end;
$$;

create aggregate sum2 (int) (
    sfunc = sum2_sfunc,
    stype = int,
    finalfunc = sum2_finalfunc,
    combinefunc = sum2_sfunc,
    initcond = '0'
);

create aggregate sum2_strict (int) (
    sfunc = sum2_sfunc_strict,
    stype = int,
    finalfunc = sum2_finalfunc_strict,
    combinefunc = sum2_sfunc_strict
);

select create_distributed_function('sum2_sfunc(int,int)');
select create_distributed_function('sum2_finalfunc(int)');
select create_distributed_function('sum2_sfunc_strict(int,int)');
select create_distributed_function('sum2_finalfunc_strict(int)');

-- This can use create_distributed_function once support for aggregates is merged
select run_command_on_workers($$
    set search_path to aggregate_support;
    create aggregate sum2 (int) (
        sfunc = sum2_sfunc,
        stype = int,
        finalfunc = sum2_finalfunc,
        combinefunc = sum2_sfunc,
        initcond = '0'
    );

    create aggregate sum2_strict (int) (
        sfunc = sum2_sfunc_strict,
        stype = int,
        finalfunc = sum2_finalfunc_strict,
        combinefunc = sum2_sfunc_strict
    );
$$);

create table aggdata (id int, key int, val int, valf float8);
select create_distributed_table('aggdata', 'id');
insert into aggdata (id, key, val, valf) values (1, 1, 2, 11.2), (2, 1, NULL, 2.1), (3, 2, 2, 3.22), (4, 2, 3, 4.23), (5, 2, 5, 5.25), (6, 3, 4, 63.4), (7, 5, NULL, 75), (8, 6, NULL, NULL), (9, 6, NULL, 96), (10, 7, 8, 1078), (11, 9, 0, 1.19);
select key, sum2(val), sum2_strict(val), stddev(valf) from aggdata group by key order by key;

set client_min_messages to error;
drop schema aggregate_support cascade;
