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

function ScanStart ()
  done = false
end

function ScanIterate ()
  if not done then
    done = true
    local row = { }
    for column, data_type in pairs(fdw.columns) do
      row[column] = data_type == "text" and "hello world" or nil
    end
    return row
  end
end

function ScanRestart ()

end

function ScanEnd ()

end

function ScanExplain ()
  return "hello world"
end
