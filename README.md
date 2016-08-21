# Lua Foreign Data Wrapper for PostgreSQL

Based on the Blackhole Foreign Data Wrapper by Andrew Dunstan:
https://bitbucket.org/adunstan/blackhole_fdw

Write PostgreSQL foreign data wrappers in Lua. Currently read-only.

## Hello World

```lua
-- hello_world.lua

function ScanStart ()
  done = false
end

function ScanIterate ()
  if not done then
    done = true
    return { data = "hello world" }
  end
end

function ScanEnd ()
  -- noop
end
```

```SQL
CREATE EXTENSION lua_fdw;
CREATE SERVER lua_srv FOREIGN DATA WRAPPER lua_fdw;
CREATE FOREIGN TABLE lua_test (data text) SERVER lua_srv OPTIONS (script '/path/to/hello_world.lua');
SELECT * FROM lua_test;
```

```
    data
-------------
 hello world
(1 row)
```

## Lua API

The FDW looks for named Lua callback functions to be handle each stage of query execution. Missing callbacks are skipped.

| Lua callback function | Return | Stage | Description |
| --- | --- | --- | --- | --- |
| `EstimateRowCount()` | Integer | Planning | Approximate row count |
| `EstimateRowWidth()` | Integer (bytes) | Planning | Average row width |
| `EstimateStartupCost()` | Double | Planning | See EXPLAIN |
| `EstimateTotalCost()` | Double | Planning | See EXPLAIN |
| `ScanStart()` | N/A | Table Scan | Prepare for a table scan, open any resources, files, connections etc, but don't return any data yet |
| `ScanIterate()` | Table (row) | Table Scan | Return the next available row, keys = column names, values = anything scalar. Missing columns are assumed to be NULL |
| `ScanRestart()` | N/A | Table Scan | Restart the current table scan from the beginning |
| `ScanEnd()` | N/A | Table Scan | Close/free any resources used for the current table scan |

A global table called `fdw` exposes information about the table and query. Some fields:

| Element | Lua Type | Description |
| --- | --- | --- |
| `fdw.table` | string | Foreign table name |
| `fdw.columns` | table | { [column] = 'type', ... } |
| `fdw.clauses` | table | List of simple WHERE clauses: *"column" (operator) 'constant'* |
| `fdw.ereport()` | function | PostgreSQL error messages, eg `fdw.ereport(fdw.WARNING, "some text")` |
| `fdw.WARNING` | number | PostgreSQL error level. Also DEBUG5, DEBUG4, DEBUG3, DEBUG2, DEBUG1, INFO, NOTICE, ERROR, LOG, FATAL, and PANIC |

## Table OPTIONS

```
FOREIGN TABLE ... OPTIONS (
  script '/path/to/hello_world.lua'
  inject '... lua code ...'
);
```
| Option | Description |
| --- | --- |
| script | Path to the Lua script |
| inject | Fragment of Lua code to execute after the script is loaded. Useful for setting globals. May be replaced with a constructor callback. |

## Scan Clauses (condition pushdown)

To allow pushing filter conditions to the foreign data service, `fdw.clauses` lists any simple top-level WHERE clauses of the form *"column" (operator) 'constant'*, eg:

```
"column" (operator) number
"column" (operator) 'string'
"column" (operator) timestamp
```

Each clause is split into a table:

```lua
{
  column = "email",
  operator = "eq",
  type = "text",
  constant = "me@example.com",
}
```
