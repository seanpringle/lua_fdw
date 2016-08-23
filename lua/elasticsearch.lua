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
-- https://luarocks.org/#quick-start
-- https://github.com/DhavalKapil/elasticsearch-lua
--
-- Example from an ELK stack ingesting Apache combined access logs:
--
-- CREATE FOREIGN TABLE elastic_apache_access (
--   stamp timestamp,
--   agent text,
--   bytes integer,
--   clientip text,
--   domain text,
--   httpversion double precision,
--   referrer text,
--   request text,
--   response integer,
--   verb text
-- ) SERVER lua_srv OPTIONS (
--   script '/path/to/this/script/elasticsearch.lua',
--   inject 'index = "apache_access" remap["stamp"] = "@timestamp"'
-- );

json = require('cjson')
elasticsearch = require("elasticsearch")

-- Connection. Override via 'inject' option on each foreign table
hosts = {
  { protocol = "http", host = "localhost", port = 9200 },
}

-- Re-map Postgres columns to Elasticsearch fields, eg: stamp = "@timestamp"
remap = { }

function ScanStart (is_explain)

  client = elasticsearch.client({
    hosts = hosts
  })

  filters = { }

  -- lua_fdw exposes simple top-level WHERE clauses of the form:
  -- "column" (eq/ne/lt/gt/lte/gte/like) "constant". Try to
  -- convert them to Elasticsearch query filters. False-positive
  -- results are fine (opposite situation is not!)

  for i, clause in ipairs(fdw.clauses) do

    local field = remap[clause.column] or clause.column
    local value = clause.constant

    if clause.operator == "like" then
      if value:match("^[^%%]+%%$") then
        -- convert "abc%" to prefix
        table.insert(filters, { prefix = { [field] = value:gsub("%%", "") }})
      else
        -- other patterns to wildcard
        value = clause.constant:gsub("_", "?")
        value = clause.constant:gsub("%%", "*")
        for word in value:gmatch("%S+") do
          table.insert(filters, { wildcard = { [field] = word }})
        end
      end
    end

    if fdw.columns[clause.column]:match("timestamp") then
      value = clause.constant:gsub(" ", "T")
    end

    if clause.operator == "eq" then
      -- full text. intuitive if { index: not_analyzed } ES fields
      table.insert(filters, { match = { [field] = value }})
    end
    if clause.operator == "lt" then
      table.insert(filters, { range = { [field] = { lt = value }}})
    end
    if clause.operator == "gt" then
      table.insert(filters, { range = { [field] = { gt = value }}})
    end
    if clause.operator == "lte" then
      table.insert(filters, { range = { [field] = { lte = value }}})
    end
    if clause.operator == "gte" then
      table.insert(filters, { range = { [field] = { gte = value }}})
    end
  end

  if is_explain then

    data = nil
    scroll_id = nil

  else

    data, err = client:search({
      index = index,
      search_type = "scan",
      scroll = "1m",
      body = {
        size = 1000,
        query = {
          bool = {
            filter = filters,
          }
        }
      }
    })

    if data == nil then
      fdw.ereport(fdw.ERROR, err)
      scroll_id = nil
    else
      scroll_id = data["_scroll_id"]
      data = { }
    end
  end
end

function ScanIterate ()

  if scroll_id then

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
      local cell = table.remove(data, #data)
      local row = { }
      for column, data_type in pairs(fdw.columns) do
        local field = remap[column] or column
        row[column] = cell["_source"][field]
      end
      return row
    end
  end
end

function ScanEnd ()
  if scroll_id then
    client:clearScroll({
      scroll_id = scroll_id
    })
  end
end

function ScanRestart ()
  ScanEnd()
  ScanStart()
end

function ScanExplain ()
  return json.encode(filters)
end
