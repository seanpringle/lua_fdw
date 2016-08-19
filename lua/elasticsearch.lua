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

json = require('json')
elasticsearch = require("elasticsearch")

proto = "http"
host = "ne4-stp900a"
port = 9200
index = "rsyslog"

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

  client = elasticsearch.client({
    hosts = {
      {
        protocol = proto,
        host = host,
        port = port
      }
    },
  })

  data, err = client:search({
    index = index,
    search_type = "scan",
    scroll = "1m",
    body = {
      query = {
        bool = {
          filter = {
            { range = { ["@timestamp"] = { gte = "2016-08-01" }}},
            { match = { stream = "sshd" }},
            { match = { host = "qt6" }},
            { match = { content = "stp900" }},
          }
        }
      }
    }
  })

  scroll_id = data["_scroll_id"]
  data = { }
end

function ScanIterate ()

  if #data == 0 then

    local chunk, err = client:scroll({
      scroll_id = scroll_id,
      scroll = "1m",
    })

    if chunk and #chunk["hits"]["hits"] > 0 then
      data = chunk["hits"]["hits"]
    end
  end

  if #data > 0 then
    rows = rows + 1
    return { [field] = json.encode(table.remove(data, #data)) }
  end
end

function ScanRestart ()
  ScanStart({ field })
end

function ScanEnd ()

end
