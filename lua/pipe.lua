---------------------------------------------------------------------------
--
-- Pipe Foreign Data Wrapper for PostgreSQL
--
-- Copyright (c) 2016 Sean Pringle (lua_fdw)
--
-- This software is released under the PostgreSQL Licence
--
-- Author: Sean Pringle <sean.pringle@gmail.com> (lua_fdw)
--
---------------------------------------------------------------------------
--
--CREATE FOREIGN TABLE test as (
--  line text
--)
--SERVER lua_srv OPTIONS (
--  script '/home/900/stp900/zcat.lua',
--  inject $$
--    input = "zcat /local/stp900/jam9_2017_1.gz"
--  $$
--);

input = 'cat /dev/null'

function ScanStart (is_explain)
  pipe = io.popen(input.." 2>/dev/null")
end

function ScanIterate ()
  line = pipe:read("*l")
  return line and { line = line } or nil
end

function ScanStop ()
  pipe:close()
end
