--
-- nested calls
--

CREATE FUNCTION nested_call_one(a text) RETURNS text
	AS
'q = "SELECT nested_call_two(''%s'')" % a
r = plpy.execute(q)
return r[0]'
	LANGUAGE plpythonu ;

CREATE FUNCTION nested_call_two(a text) RETURNS text
	AS
'q = "SELECT nested_call_three(''%s'')" % a
r = plpy.execute(q)
return r[0]'
	LANGUAGE plpythonu ;

CREATE FUNCTION nested_call_three(a text) RETURNS text
	AS
'return a'
	LANGUAGE plpythonu ;

-- some spi stuff

CREATE FUNCTION spi_prepared_plan_test_one(a text) RETURNS text
	AS
'if "myplan" not in SD:
	q = "SELECT count(*) FROM users WHERE lname = $1"
	SD["myplan"] = plpy.prepare(q, [ "text" ])
try:
	rv = plpy.execute(SD["myplan"], [a])
	return "there are " + str(rv[0]["count"]) + " " + str(a) + "s"
except Exception, ex:
	plpy.error(str(ex))
return None
'
	LANGUAGE plpythonu;

CREATE FUNCTION spi_prepared_plan_test_nested(a text) RETURNS text
	AS
'if "myplan" not in SD:
	q = "SELECT spi_prepared_plan_test_one(''%s'') as count" % a
	SD["myplan"] = plpy.prepare(q)
try:
	rv = plpy.execute(SD["myplan"])
	if len(rv):
		return rv[0]["count"]
except Exception, ex:
	plpy.error(str(ex))
return None
'
	LANGUAGE plpythonu;




CREATE FUNCTION join_sequences(s sequences) RETURNS text
	AS
'if not s["multipart"]:
	return s["sequence"]
q = "SELECT sequence FROM xsequences WHERE pid = ''%s''" % s["pid"]
rv = plpy.execute(q)
seq = s["sequence"]
for r in rv:
	seq = seq + r["sequence"]
return seq
'
	LANGUAGE plpythonu;





-- spi and nested calls
--
select nested_call_one('pass this along');
select spi_prepared_plan_test_one('doe');
select spi_prepared_plan_test_one('smith');
select spi_prepared_plan_test_nested('smith');




SELECT join_sequences(sequences) FROM sequences;
SELECT join_sequences(sequences) FROM sequences
	WHERE join_sequences(sequences) ~* '^A';
SELECT join_sequences(sequences) FROM sequences
	WHERE join_sequences(sequences) ~* '^B';


--
-- plan and result objects
--

CREATE FUNCTION result_nrows_test() RETURNS int
AS $$
plan = plpy.prepare("SELECT 1 AS foo, '11'::text AS bar UNION SELECT 2, '22'")
plpy.info(plan.status()) # not really documented or useful
result = plpy.execute(plan)
if result.status() > 0:
   plpy.info(result.colnames())
   plpy.info(result.coltypes())
   plpy.info(result.coltypmods())
   return result.nrows()
else:
   return None
$$ LANGUAGE plpythonu;

SELECT result_nrows_test();


-- cursor objects

CREATE FUNCTION simple_cursor_test() RETURNS int AS $$
res = plpy.cursor("select fname, lname from users")
does = 0
for row in res:
    if row['lname'] == 'doe':
        does += 1
return does
$$ LANGUAGE plpythonu;

CREATE FUNCTION double_cursor_close() RETURNS int AS $$
res = plpy.cursor("select fname, lname from users")
res.close()
res.close()
$$ LANGUAGE plpythonu;

CREATE FUNCTION cursor_fetch() RETURNS int AS $$
res = plpy.cursor("select fname, lname from users")
assert len(res.fetch(3)) == 3
assert len(res.fetch(3)) == 1
assert len(res.fetch(3)) == 0
assert len(res.fetch(3)) == 0
try:
    # use next() or __next__(), the method name changed in
    # http://www.python.org/dev/peps/pep-3114/
    try:
        res.next()
    except AttributeError:
        res.__next__()
except StopIteration:
    pass
else:
    assert False, "StopIteration not raised"
$$ LANGUAGE plpythonu;

CREATE FUNCTION cursor_mix_next_and_fetch() RETURNS int AS $$
res = plpy.cursor("select fname, lname from users order by fname")
assert len(res.fetch(2)) == 2

item = None
try:
    item = res.next()
except AttributeError:
    item = res.__next__()
assert item['fname'] == 'rick'

assert len(res.fetch(2)) == 1
$$ LANGUAGE plpythonu;

CREATE FUNCTION fetch_after_close() RETURNS int AS $$
res = plpy.cursor("select fname, lname from users")
res.close()
try:
    res.fetch(1)
except ValueError:
    pass
else:
    assert False, "ValueError not raised"
$$ LANGUAGE plpythonu;

CREATE FUNCTION next_after_close() RETURNS int AS $$
res = plpy.cursor("select fname, lname from users")
res.close()
try:
    try:
        res.next()
    except AttributeError:
        res.__next__()
except ValueError:
    pass
else:
    assert False, "ValueError not raised"
$$ LANGUAGE plpythonu;

CREATE FUNCTION cursor_fetch_next_empty() RETURNS int AS $$
res = plpy.cursor("select fname, lname from users where false")
assert len(res.fetch(1)) == 0
try:
    try:
        res.next()
    except AttributeError:
        res.__next__()
except StopIteration:
    pass
else:
    assert False, "StopIteration not raised"
$$ LANGUAGE plpythonu;

CREATE FUNCTION cursor_plan() RETURNS SETOF text AS $$
plan = plpy.prepare(
    "select fname, lname from users where fname like $1 || '%' order by fname",
    ["text"])
for row in plpy.cursor(plan, ["w"]):
    yield row['fname']
for row in plpy.cursor(plan, ["j"]):
    yield row['fname']
$$ LANGUAGE plpythonu;

CREATE FUNCTION cursor_plan_wrong_args() RETURNS SETOF text AS $$
plan = plpy.prepare("select fname, lname from users where fname like $1 || '%'",
                    ["text"])
c = plpy.cursor(plan, ["a", "b"])
$$ LANGUAGE plpythonu;

SELECT simple_cursor_test();
SELECT double_cursor_close();
SELECT cursor_fetch();
SELECT cursor_mix_next_and_fetch();
SELECT fetch_after_close();
SELECT next_after_close();
SELECT cursor_fetch_next_empty();
SELECT cursor_plan();
SELECT cursor_plan_wrong_args();
