/*-------------------------------------------------------------------------
 *
 *                foreign-data wrapper  lua
 *
 * Copyright (c) 2013, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author:  Andrew Dunstan <andrew@dunslane.net>
 *
 * IDENTIFICATION
 *                lua_fdw/=sql/lua_fdw.sql
 *
 *-------------------------------------------------------------------------
 */

CREATE FUNCTION lua_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION lua_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER lua_fdw
  HANDLER lua_fdw_handler
  VALIDATOR lua_fdw_validator;
