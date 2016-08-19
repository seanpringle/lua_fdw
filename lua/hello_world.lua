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

function EstimateRowCount ()
  return 1
end

function EstimateRowWidth ()
  return string.len("hello world")
end

function EstimateStartupCost ()
  return 1.0
end

function EstimateTotalCost ()
  return EstimateRowCount()
end

function ScanStart (cols)
  rows = 0
  field = cols[1]
end

function ScanIterate ()
  if rows == 0 then
    rows = rows + 1
    return { [field] = "hello world" }
  end
end

function ScanRestart ()

end

function ScanEnd ()

end

function ScanExplain ()
  return "hello world"
end
