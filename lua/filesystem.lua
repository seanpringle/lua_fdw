---------------------------------------------------------------------------
--
-- Filesystem Foreign Data Wrapper for PostgreSQL
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
-- https://github.com/luaposix/luaposix
-- https://github.com/mpx/lua-cjson
--
-- CREATE FOREIGN TABLE filesystem (
--   path text,
--   attributes json,
--   content text
-- ) SERVER lua_srv OPTIONS (
--   script '/path/to/this/script/filesystem.lua',
--   inject 'root = [[/some/path]]'
-- );
--
-- The "content" field is probably going to be very expesive
-- unless careful equality/prefix-matching is used to control
-- the file-system recursive scan. May be safest to have two
-- tables, one with "content", one without. Query the former
-- for big scans and the latter for direct look-ups.
-- ... Or just use file_fdw.

posix = require('posix')
json = require('cjson')

-- Probably want to override this in "inject" option
-- Postgres user will need read access
root = "/"

paths = { }
recurse = true
prefix = false

function ScanStart (is_explain)

  for i, clause in pairs(fdw.clauses) do

    if clause.operator == "like" and clause.constant:match("^[^%%]+%%$") then
      root = clause.constant:sub(1, -2)
      prefix = true
    end

    if clause.operator == "eq" then
      root = clause.constant
      recurse = false
    end
  end

  if prefix and not root:match("/$") then
    root = root:gsub("/[^/]+$", "/")
    fdw.ereport(fdw.WARNING, root)
  end

  if root:len() > 1 then
    root = root:gsub("[/]+$", "")
    fdw.ereport(fdw.WARNING, root)
  end

  paths = { root }
  fdw.ereport(fdw.WARNING, root)
end

function get_content(path)
  local content = nil
  local file = io.open(path)
  if file then
    content = file:read("*a")
    file:close()
  end
  return content
end

function ScanIterate ()

  local row = { }
  local attr = nil
  local path = nil

  while #paths > 0 and attr == nil do
    path = table.remove(paths)
    attr = posix.stat(path)
  end

  if attr then

    for column,data_type in pairs(fdw.columns) do

      if column == "path" then
        row.path = path
      end

      if column == "attributes" then
        row.attributes = json.encode(attr)
      end

      if column == "content" then
        local ok, content = pcall(get_content, path)
        if ok then row.content = content end
      end
    end

    if recurse and attr and attr.type == "directory" and not path:match("[.]$") then
      for fi,file in ipairs(posix.dir(path) or { }) do
        local fpath = (path.."/"..file):gsub("//", "/")
        table.insert(paths, fpath)
      end
    end

    return row
  end
end

function ScanStop ()
  paths = { }
end
