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
-- Reads a CSV file, one record per line. Table column names must be a
-- subset of CSV headers on the first line.
--
-- CREATE FOREIGN TABLE a_table SERVER lua_fdw OPTIONS (
--   script '/path/to/csv.lua'
--   inject 'path = [[/path/to/file.csv]]'
-- );

-- From: http://lua-users.org/wiki/CsvUtils
-- Convert from CSV string to table (converts a single line of a CSV file)
function fromCSV (s)
  s = s .. ','        -- ending comma
  local t = {}        -- table to collect fields
  local fieldstart = 1
  repeat
    -- next field is quoted? (start with `"'?)
    if string.find(s, '^"', fieldstart) then
      local a, c
      local i  = fieldstart
      repeat
        -- find closing quote
        a, i, c = string.find(s, '"("?)', i+1)
      until c ~= '"'    -- quote not followed by quote?
      if not i then error('unmatched "') end
      local f = string.sub(s, fieldstart+1, i-1)
      table.insert(t, (string.gsub(f, '""', '"')))
      fieldstart = string.find(s, ',', i) + 1
    else                -- unquoted; find next comma
      local nexti = string.find(s, ',', fieldstart)
      table.insert(t, string.sub(s, fieldstart, nexti-1))
      fieldstart = nexti + 1
    end
  until fieldstart > string.len(s)
  return t
end

function getCSV (csv, headers, field)
  for i = 1,#headers do
    if field == headers[i] then
      return csv[i]
    end
  end
end

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
  fields = cols
  ereport(WARNING, field)
  file, msg = io.open(path)

  if file == nil then
    ereport(ERROR, msg)
  end

  headers = fromCSV(file:read())
end

function ScanIterate ()
  local row = nil
  local line = file:read()
  if type(line) == "string" then
    row = { }
    local csv = fromCSV(line)
    for _, field in ipairs(fields) do
      row[field] = getCSV(csv, headers, field)
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
