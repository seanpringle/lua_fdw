---------------------------------------------------------------------------
--
-- Lua Foreign Data Wrapper for PostgreSQL
--
-- Copyright (c) 2016 Sean Pringle (lua_fdw)
--
-- This software is released under the PostgreSQL Licence
--
-- Author: Sean Pringle <sean.pringle@gmail.com> (lua_fdw)
--
---------------------------------------------------------------------------
--
-- Reads a text file, one record per line. Table column name is irrelevant.
--
-- CREATE FOREIGN TABLE a_table SERVER lua_fdw OPTIONS (
--   script '/path/to/cat.lua'
--   inject 'path = [[/path/to/file.txt]]'
-- );

function EstimateRowCount ()
  return 0
end

function EstimateRowWidth ()
  return 80
end

function EstimateStartupCost ()
  return 1.0
end

function EstimateTotalCost ()
  return EstimateRowCount()
end

function ScanStart (cols)
  line = 0
  file, msg = io.open(path)

  if file == nil then
    fdw.ereport(fdw.ERROR, msg)
  end
end

function ScanIterate ()
  local row = nil
  local line = file:read()
  if type(line) == "string" then
    row = { }
    for field, data_type in ipairs(fdw.columns) do
      row[field] = line
    end
  end
  return row
end

function ScanRestart ()
  file:seek("set", 0)
end

function ScanEnd ()
  file:close()
end

function ScanExplain ()
  return "scanning "..path
end
