
--
-- test parser
--

create type textrange as range (subtype=text, collation="C");

-- negative tests; should fail
select ''::textrange;
select '-[a,z)'::textrange;
select '[a,z) - '::textrange;
select '(",a)'::textrange;
select '(,,a)'::textrange;
select '(),a)'::textrange;
select '(a,))'::textrange;
select '(],a)'::textrange;
select '(a,])'::textrange;

-- should succeed
select '  empty  '::textrange;
select ' ( empty, empty )  '::textrange;
select ' ( " a " " a ", " z " " z " )  '::textrange;
select '(,z)'::textrange;
select '(a,)'::textrange;
select '[,z]'::textrange;
select '[a,]'::textrange;
select '( , )'::textrange;
select '("","")'::textrange;
select '["",""]'::textrange;
select '(",",",")'::textrange;
select '("\\","\\")'::textrange
select '(\\,a)'::textrange;
select '((,z)'::textrange;
select '([,z)'::textrange;
select '(!,()'::textrange;
select '(!,[)'::textrange;

drop type textrange;

--
-- create some test data and test the operators
--

CREATE TABLE numrange_test (nr NUMRANGE);
create index numrange_test_btree on numrange_test(nr);
SET enable_seqscan = f;

INSERT INTO numrange_test VALUES('[,)');
INSERT INTO numrange_test VALUES('[3,]');
INSERT INTO numrange_test VALUES('[, 5)');
INSERT INTO numrange_test VALUES(numrange(1.1, 2.2));
INSERT INTO numrange_test VALUES('empty');
INSERT INTO numrange_test VALUES(numrange(1.7));

SELECT isempty(nr) FROM numrange_test;
SELECT lower_inc(nr), lower(nr), upper(nr), upper_inc(nr) FROM numrange_test
  WHERE NOT isempty(nr) AND NOT lower_inf(nr) AND NOT upper_inf(nr);

SELECT * FROM numrange_test WHERE contains(nr, numrange(1.9,1.91));
SELECT * FROM numrange_test WHERE nr @> numrange(1.0,10000.1);
SELECT * FROM numrange_test WHERE contained_by(numrange(-1e7,-10000.1), nr);
SELECT * FROM numrange_test WHERE 1.9 <@ nr;
SELECT * FROM numrange_test WHERE nr = 'empty';
SELECT * FROM numrange_test WHERE range_eq(nr, '(1.1, 2.2)');
SELECT * FROM numrange_test WHERE nr = '[1.1, 2.2)';

select numrange(2.0, 1.0);

select numrange(2.0, 3.0) -|- numrange(3.0, 4.0);
select adjacent(numrange(2.0, 3.0), numrange(3.1, 4.0));
select numrange(2.0, 3.0, '[]') -|- numrange(3.0, 4.0, '()');
select numrange(1.0, 2.0) -|- numrange(2.0, 3.0,'[]');
select adjacent(numrange(2.0, 3.0, '(]'), numrange(1.0, 2.0, '(]'));

select numrange(1.1, 3.3) <@ numrange(0.1,10.1);
select numrange(0.1, 10.1) <@ numrange(1.1,3.3);

select numrange(1.1, 2.2) - numrange(2.0, 3.0);
select numrange(1.1, 2.2) - numrange(2.2, 3.0);
select numrange(1.1, 2.2,'[]') - numrange(2.0, 3.0);
select minus(numrange(10.1,12.2,'[]'), numrange(110.0,120.2,'(]'));
select minus(numrange(10.1,12.2,'[]'), numrange(0.0,120.2,'(]'));

select numrange(4.5, 5.5, '[]') && numrange(5.5, 6.5);
select numrange(1.0, 2.0) << numrange(3.0, 4.0);
select numrange(1.0, 2.0) >> numrange(3.0, 4.0);
select numrange(3.0, 70.0) &< numrange(6.6, 100.0);

select numrange(1.1, 2.2) < numrange(1.0, 200.2);
select numrange(1.1, 2.2) < numrange(1.1, 1.2);

select numrange(1.0, 2.0) + numrange(2.0, 3.0);
select numrange(1.0, 2.0) + numrange(1.5, 3.0);
select numrange(1.0, 2.0) + numrange(2.5, 3.0);

select numrange(1.0, 2.0) * numrange(2.0, 3.0);
select numrange(1.0, 2.0) * numrange(1.5, 3.0);
select numrange(1.0, 2.0) * numrange(2.5, 3.0);

select * from numrange_test where nr < numrange(-1000.0, -1000.0,'[]');
select * from numrange_test where nr < numrange(0.0, 1.0,'[]');
select * from numrange_test where nr < numrange(1000.0, 1001.0,'[]');
select * from numrange_test where nr > numrange(-1001.0, -1000.0,'[]');
select * from numrange_test where nr > numrange(0.0, 1.0,'[]');
select * from numrange_test where nr > numrange(1000.0, 1000.0,'[]');

create table numrange_test2(nr numrange);
create index numrange_test2_hash_idx on numrange_test2 (nr);
INSERT INTO numrange_test2 VALUES('[, 5)');
INSERT INTO numrange_test2 VALUES(numrange(1.1, 2.2));
INSERT INTO numrange_test2 VALUES(numrange(1.1, 2.2));
INSERT INTO numrange_test2 VALUES(numrange(1.1, 2.2,'()'));
INSERT INTO numrange_test2 VALUES('empty');

select * from numrange_test2 where nr = 'empty'::numrange;
select * from numrange_test2 where nr = numrange(1.1, 2.2);
select * from numrange_test2 where nr = numrange(1.1, 2.3);

set enable_nestloop=t;
set enable_hashjoin=f;
set enable_mergejoin=f;
select * from numrange_test natural join numrange_test2 order by nr;
set enable_nestloop=f;
set enable_hashjoin=t;
set enable_mergejoin=f;
select * from numrange_test natural join numrange_test2 order by nr;
set enable_nestloop=f;
set enable_hashjoin=f;
set enable_mergejoin=t;
select * from numrange_test natural join numrange_test2 order by nr;

set enable_nestloop to default;
set enable_hashjoin to default;
set enable_mergejoin to default;
SET enable_seqscan TO DEFAULT;
DROP TABLE numrange_test;
DROP TABLE numrange_test2;

-- test canonical form for int4range
select int4range(1,10,'[]');
select int4range(1,10,'[)');
select int4range(1,10,'(]');
select int4range(1,10,'[]');

-- test canonical form for daterange
select daterange('2000-01-10'::date, '2000-01-20'::date,'[]');
select daterange('2000-01-10'::date, '2000-01-20'::date,'[)');
select daterange('2000-01-10'::date, '2000-01-20'::date,'(]');
select daterange('2000-01-10'::date, '2000-01-20'::date,'[]');

create table test_range_gist(ir int4range);
create index test_range_gist_idx on test_range_gist using gist (ir);

insert into test_range_gist select int4range(g, g+10) from generate_series(1,2000) g;
insert into test_range_gist select 'empty'::int4range from generate_series(1,500) g;
insert into test_range_gist select int4range(g, g+10000) from generate_series(1,1000) g;
insert into test_range_gist select 'empty'::int4range from generate_series(1,500) g;
insert into test_range_gist select int4range(NULL,g*10,'(]') from generate_series(1,100) g;
insert into test_range_gist select int4range(g*10,NULL,'(]') from generate_series(1,100) g;
insert into test_range_gist select int4range(g, g+10) from generate_series(1,2000) g;

BEGIN;
SET LOCAL enable_seqscan    = t;
SET LOCAL enable_bitmapscan = f;
SET LOCAL enable_indexscan  = f;

select count(*) from test_range_gist where ir @> 'empty'::int4range;
select count(*) from test_range_gist where ir = int4range(10,20);
select count(*) from test_range_gist where ir @> 10;
select count(*) from test_range_gist where ir @> int4range(10,20);
select count(*) from test_range_gist where ir && int4range(10,20);
select count(*) from test_range_gist where ir <@ int4range(10,50);
select count(*) from (select * from test_range_gist where not isempty(ir)) s where ir << int4range(100,500);
select count(*) from (select * from test_range_gist where not isempty(ir)) s where ir >> int4range(100,500);
select count(*) from (select * from test_range_gist where not isempty(ir)) s where ir &< int4range(100,500);
select count(*) from (select * from test_range_gist where not isempty(ir)) s where ir &> int4range(100,500);
select count(*) from (select * from test_range_gist where not isempty(ir)) s where ir -|- int4range(100,500);
COMMIT;

BEGIN;
SET LOCAL enable_seqscan    = f;
SET LOCAL enable_bitmapscan = f;
SET LOCAL enable_indexscan  = t;

select count(*) from test_range_gist where ir @> 'empty'::int4range;
select count(*) from test_range_gist where ir = int4range(10,20);
select count(*) from test_range_gist where ir @> 10;
select count(*) from test_range_gist where ir @> int4range(10,20);
select count(*) from test_range_gist where ir && int4range(10,20);
select count(*) from test_range_gist where ir <@ int4range(10,50);
select count(*) from test_range_gist where ir << int4range(100,500);
select count(*) from test_range_gist where ir >> int4range(100,500);
select count(*) from test_range_gist where ir &< int4range(100,500);
select count(*) from test_range_gist where ir &> int4range(100,500);
select count(*) from test_range_gist where ir -|- int4range(100,500);
COMMIT;

drop index test_range_gist_idx;
create index test_range_gist_idx on test_range_gist using gist (ir);

BEGIN;
SET LOCAL enable_seqscan    = f;
SET LOCAL enable_bitmapscan = f;
SET LOCAL enable_indexscan  = t;

select count(*) from test_range_gist where ir @> 'empty'::int4range;
select count(*) from test_range_gist where ir = int4range(10,20);
select count(*) from test_range_gist where ir @> 10;
select count(*) from test_range_gist where ir @> int4range(10,20);
select count(*) from test_range_gist where ir && int4range(10,20);
select count(*) from test_range_gist where ir <@ int4range(10,50);
select count(*) from test_range_gist where ir << int4range(100,500);
select count(*) from test_range_gist where ir >> int4range(100,500);
select count(*) from test_range_gist where ir &< int4range(100,500);
select count(*) from test_range_gist where ir &> int4range(100,500);
select count(*) from test_range_gist where ir -|- int4range(100,500);
COMMIT;

drop table test_range_gist;

--
-- Btree_gist is not included by default, so to test exclusion
-- constraints with range types, use singleton int ranges for the "="
-- portion of the constraint.
--

create table test_range_excl(
  room int4range,
  speaker int4range,
  during tsrange,
  exclude using gist (room with =, during with &&),
  exclude using gist (speaker with =, during with &&)
);

insert into test_range_excl
  values(int4range(123), int4range(1), '[2010-01-02 10:00, 2010-01-02 11:00)');
insert into test_range_excl
  values(int4range(123), int4range(2), '[2010-01-02 11:00, 2010-01-02 12:00)');
insert into test_range_excl
  values(int4range(123), int4range(3), '[2010-01-02 10:10, 2010-01-02 11:10)');
insert into test_range_excl
  values(int4range(124), int4range(3), '[2010-01-02 10:10, 2010-01-02 11:10)');
insert into test_range_excl
  values(int4range(125), int4range(1), '[2010-01-02 10:10, 2010-01-02 11:10)');

drop table test_range_excl;

-- test bigint ranges
select int8range(10000000000::int8, 20000000000::int8,'(]');
-- test tstz ranges
set timezone to '-08';
select '[2010-01-01 01:00:00 -05, 2010-01-01 02:00:00 -08)'::tstzrange;
-- should fail
select '[2010-01-01 01:00:00 -08, 2010-01-01 02:00:00 -05)'::tstzrange;
set timezone to default;

--
-- Test user-defined range of floats
--

--should fail
create type float8range as range (subtype=float8, subtype_diff=float4mi);

--should succeed
create type float8range as range (subtype=float8, subtype_diff=float8mi);
select '[123.001, 5.e9)'::float8range @> 888.882::float8;
create table float8range_test(f8r float8range, i int);
insert into float8range_test values(float8range(-100.00007, '1.111113e9'));
select * from float8range_test;
drop table float8range_test;
drop type float8range;

--
-- Test range types over domains
--

create domain mydomain as int4;
create type mydomainrange as range(subtype=mydomain);
select '[4,50)'::mydomainrange @> 7::mydomain;
drop type mydomainrange;
drop domain mydomain;

--
-- Test domains over range types
--

create domain restrictedrange  as int4range check (upper(value) < 10);
select '[4,5)'::restrictedrange @> 7;
select '[4,50)'::restrictedrange @> 7; -- should fail
drop domain restrictedrange;

--
-- Test multiple range types over the same subtype
--

create type textrange1 as range(subtype=text, collation="C");
create type textrange2 as range(subtype=text, collation="C");

select textrange1('a','Z') @> 'b'::text;
select textrange2('a','z') @> 'b'::text;

drop type textrange1;
drop type textrange2;

--
-- Test out polymorphic type system
--

create function anyarray_anyrange_func(a anyarray, r anyrange)
  returns anyelement as 'select $1[1] + lower($2);' language sql;

select anyarray_anyrange_func(ARRAY[1,2], int4range(10,20));

-- should fail
select anyarray_anyrange_func(ARRAY[1,2], numrange(10,20));

drop function anyarray_anyrange_func(anyarray, anyrange);

-- should fail
create function bogus_func(anyelement)
  returns anyrange as 'select int4range(1,10)' language sql;

-- should fail
create function bogus_func(int)
  returns anyrange as 'select int4range(1,10)' language sql;

create function range_add_bounds(anyrange)
  returns anyelement as 'select lower($1) + upper($1)' language sql;

select range_add_bounds(numrange(1.0001, 123.123));

--
-- Arrays of ranges
--

select ARRAY[numrange(1.1), numrange(12.3,155.5)];

--
-- Ranges of arrays
--

create type arrayrange as range (subtype=int4[]);

select arrayrange(ARRAY[1,2], ARRAY[2,1]);

drop type arrayrange;

--
-- OUT/INOUT/TABLE functions
--

create function outparam_succeed(i anyrange, out r anyrange, out t text)
  as $$ select $1, 'foo' $$ language sql;

create function inoutparam_succeed(out i anyelement, inout r anyrange)
  as $$ select $1, $2 $$ language sql;

create function table_succeed(i anyelement, r anyrange) returns table(i anyelement, r anyrange)
  as $$ select $1, $2 $$ language sql;

-- should fail
create function outparam_fail(i anyelement, out r anyrange, out t text)
  as $$ select '[1,10]', 'foo' $$ language sql;

--should fail
create function inoutparam_fail(inout i anyelement, out r anyrange)
  as $$ select $1, '[1,10]' $$ language sql;

--should fail
create function table_succeed(i anyelement) returns table(i anyelement, r anyrange)
  as $$ select $1, '[1,10]' $$ language sql;
