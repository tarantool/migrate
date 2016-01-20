#include <assert.h>
#include <ctype.h>
#include <stdio.h>

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "xlog.h"

#include <tarantool/module.h>
#include <tarantool/tnt.h>
#include <tarantool/tnt_log.h>
#include <tarantool/tnt_xlog.h>
#include <tarantool/tnt_snapshot.h>

static const char *parser_lib_name = "xlog.parser_v11";
static const char *parser_xlog_name = "xlog.parser_v11.xlog";
static const char *parser_snap_name = "xlog.parser_v11.snap";
static const char *parser_iter_name = "xlog.parser_v11.iter";

/* HELPERS */

/**
 * A helper to register a single type metatable.
 */
void
luaL_register_type(struct lua_State *L, const char *type_name,
		   const struct luaL_Reg *methods)
{
	luaL_newmetatable(L, type_name);
	/*
	 * Conventionally, make the metatable point to itself
	 * in __index. If 'methods' contain a field for __index,
	 * this is a no-op.
	 */
	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");
	lua_pushstring(L, type_name);
	lua_setfield(L, -2, "__metatable");
	luaL_register(L, NULL, methods);
	lua_pop(L, 1);
}

void
lua_pushstream(struct lua_State *L, struct tnt_stream *s,
	       enum tnt_log_type type)
{
	const char *name = (type == TNT_LOG_SNAPSHOT ? parser_snap_name
						     : parser_xlog_name);
	struct tnt_stream **ps = (struct tnt_stream **)lua_newuserdata(L,
			sizeof(struct tnt_stream *));
	*ps = s;
	luaL_getmetatable(L, name);
	lua_setmetatable(L, -2);
}

struct tnt_stream **
lua_checkstream(struct lua_State *L, int narg, const char *src,
		enum tnt_log_type type)
{
	const char *name = (type == TNT_LOG_SNAPSHOT ? parser_snap_name
						     : parser_xlog_name);
	int top = lua_gettop(L);
	if (narg > top || top + narg < 0)
		luaL_error(L, "usage: %s", src);
	void *log = luaL_checkudata(L, narg, name);
	if (log == NULL)
		luaL_error(L, "usage: %s", src);
	return (struct tnt_stream **)log;
}

void
lua_pushiter(struct lua_State *L, struct tnt_iter *i)
{
	struct tnt_iter **pi = (struct tnt_iter **)lua_newuserdata(L,
			sizeof(struct tnt_iter *));
	*pi = i;
	luaL_getmetatable(L, parser_iter_name);
	lua_setmetatable(L, -2);
}

struct tnt_iter **
lua_checkiter(struct lua_State *L, int narg, const char *src)
{
	int top = lua_gettop(L);
	if (narg > top || top + narg < 0)
		luaL_error(L, "usage: %s", src);
	void *log = luaL_checkudata(L, narg, parser_iter_name);
	if (log == NULL)
		luaL_error(L, "usage: %s", src);
	return (struct tnt_iter **)log;
}

/* Internal methods */

const char *parse_xlog_update [] = {
	/* TNT_UPDATE_ASSIGN */	"=",
	/* TNT_UPDATE_ADD */	"+",
	/* TNT_UPDATE_AND */	"&",
	/* TNT_UPDATE_XOR */	"^",
	/* TNT_UPDATE_OR */	"|",
	/* TNT_UPDATE_SPLICE */	":",
	/* TNT_UPDATE_DELETE */	"#",
	/* TNT_UPDATE_INSERT */	"!",
	NULL,
};

static int
parser_xlog_iter_op_insert(struct lua_State *L, struct tnt_request *r)
{
	lua_pushstring(L, "op");
	lua_pushstring(L, "insert");
	lua_settable(L, -3); /* op */
	lua_pushstring(L, "flags");
	lua_pushnumber(L, r->r.insert.h.flags);
	lua_settable(L, -3); /* op */
	lua_pushstring(L, "space");
	lua_pushnumber(L, r->r.insert.h.ns);
	lua_settable(L, -3); /* space */
	lua_pushstring(L, "tuple");
	lua_newtable(L);
	struct tnt_iter ifl;
	tnt_iter(&ifl, &r->r.insert.t);
	while (tnt_next(&ifl)) {
		int idx = TNT_IFIELD_IDX(&ifl);
		char *data = TNT_IFIELD_DATA(&ifl);
		uint32_t size = TNT_IFIELD_SIZE(&ifl);
		lua_pushinteger(L, idx + 1);
		lua_pushlstring(L, data, size);
		lua_settable(L, -3); /* tuple field */
	}
	if (ifl.status == TNT_ITER_FAIL)
		luaL_error(L, "<parsing error>");
	tnt_iter_free(&ifl);
	lua_settable(L, -3); /* tuple */
	return 1;
}

static int
parser_xlog_iter_op_delete(struct lua_State *L, struct tnt_request *r)
{
	lua_pushstring(L, "op");
	lua_pushstring(L, "delete");
	lua_settable(L, -3); /* op */
	lua_pushstring(L, "space");
	lua_pushnumber(L, r->r.del.h.ns);
	lua_settable(L, -3); /* space */
	lua_pushstring(L, "key");
	lua_newtable(L);
	struct tnt_iter ifl;
	tnt_iter(&ifl, &r->r.del.t);
	while (tnt_next(&ifl)) {
		int idx = TNT_IFIELD_IDX(&ifl);
		char *data = TNT_IFIELD_DATA(&ifl);
		uint32_t size = TNT_IFIELD_SIZE(&ifl);
		lua_pushinteger(L, idx + 1);
		lua_pushlstring(L, data, size);
		lua_settable(L, -3); /* tuple field */
	}
	if (ifl.status == TNT_ITER_FAIL)
		luaL_error(L, "<parsing error>");
	tnt_iter_free(&ifl);
	lua_settable(L, -3); /* tuple */
	return 1;
}

static int
parser_xlog_iter_op_update(struct lua_State *L, struct tnt_request *r)
{
	lua_pushstring(L, "op");
	lua_pushstring(L, "update");
	lua_settable(L, -3); /* op */
	lua_pushstring(L, "space");
	lua_pushnumber(L, r->r.update.h.ns);
	lua_settable(L, -3); /* space */
	lua_pushstring(L, "key");
	lua_newtable(L);
	struct tnt_iter ifl;
	tnt_iter(&ifl, &r->r.del.t);
	while (tnt_next(&ifl)) {
		int idx = TNT_IFIELD_IDX(&ifl);
		char *data = TNT_IFIELD_DATA(&ifl);
		uint32_t size = TNT_IFIELD_SIZE(&ifl);
		lua_pushinteger(L, idx + 1);
		lua_pushlstring(L, data, size);
		lua_settable(L, -3); /* tuple field */
	}
	if (ifl.status == TNT_ITER_FAIL)
		luaL_error(L, "<parsing error>");
	tnt_iter_free(&ifl);
	lua_settable(L, -3); /* tuple */
	lua_pushstring(L, "ops");
	lua_newtable(L);
	/* lua_newtable(L); */
	uint32_t i = 0;
	for (i = 0; i < r->r.update.opc; ++i) {
		lua_pushinteger(L, i + 1);
		lua_newtable(L);
		lua_pushnumber(L, 1);
		if (r->r.update.opv[i].op >= TNT_UPDATE_MAX)
			luaL_error(L, "undefined update operation");
		lua_pushstring(L, parse_xlog_update[r->r.update.opv[i].op]);
		lua_settable(L, -3); /* operation */
		lua_pushnumber(L, 2);
		lua_pushnumber(L, r->r.update.opv[i].field);
		lua_settable(L, -3); /* field */
		lua_pushnumber(L, 3);
		char *data = r->r.update.opv[i].data;
		uint32_t size = r->r.update.opv[i].size;
		switch (r->r.update.opv[i].op) {
		case TNT_UPDATE_ADD:
		case TNT_UPDATE_AND:
		case TNT_UPDATE_XOR:
		case TNT_UPDATE_OR: {
			switch (size) {
			case 4:
				lua_pushnumber(L, *((uint32_t*)data));
				break;
			case 8:
				lua_pushnumber(L, *((uint64_t*)data));
				break;
			default:
				luaL_error(L, "bad value for arithm op");
			}
			break;
		}
		case TNT_UPDATE_INSERT:
		case TNT_UPDATE_ASSIGN:
			lua_pushlstring(L, data, size);
			break;
		case TNT_UPDATE_SPLICE: {
			size_t pos = 1;
			lua_pushnumber(L, *(int32_t *)(data + pos));
			lua_settable(L, -3); /* offset */
			lua_pushnumber(L, 4);
			pos += 5;
			lua_pushnumber(L, *(int32_t *)(data + pos));
			lua_settable(L, -3); /* count */
			lua_pushnumber(L, 5);
			pos += 4 + r->r.update.opv[i].size_enc_len;
			lua_pushlstring(L, data, size - pos);
			break;
		}
		case TNT_UPDATE_DELETE:
			lua_pushnumber(L, 1);
			break;
		}
		lua_settable(L, -3); /* value */
		lua_settable(L, -3); /* op */
	}
	lua_settable(L, -3); /* ops */
	return 1;
}

static int
parser_xlog_iterate_int(struct lua_State *L)
{
	struct tnt_iter **pi = lua_checkiter(L, 1, "pairs");
	int n = luaL_checkinteger(L, 2);

	lua_pushinteger(L, n + 1);
	if (tnt_next(*pi)) {
		struct tnt_request *r = TNT_IREQUEST_PTR(*pi);
		struct tnt_log_row *row =
			&(TNT_SXLOG_CAST(TNT_IREQUEST_STREAM(*pi))->log.current);
		lua_newtable(L);
		lua_pushstring(L, "lsn");
		lua_pushnumber(L, row->hdr.lsn);
		lua_settable(L, -3); /* lsn */
		lua_pushstring(L, "time");
		lua_pushnumber(L, row->hdr.tm);
		lua_settable(L, -3); /* time */
		switch (r->h.type) {
		case TNT_OP_INSERT:
			parser_xlog_iter_op_insert(L, r);
			break;
		case TNT_OP_DELETE:
		case TNT_OP_DELETE_1_3:
			parser_xlog_iter_op_delete(L, r);
			break;
		case TNT_OP_UPDATE:
			parser_xlog_iter_op_update(L, r);
			break;
		default:
			luaL_error(L, "unknown op");
		}
		/* PARSING */
		return 2;
	}
	if ((*pi)->status == TNT_ITER_FAIL) {
		char *errstr = tnt_xlog_strerror(TNT_IREQUEST_STREAM(*pi));
		luaL_error(L, "parsing failed: %s", errstr);
	}
	return 0;
}

static int
parser_snap_iterate_int(struct lua_State *L)
{
	struct tnt_iter **pi = lua_checkiter(L, 1, "pairs");
	int n = luaL_checkinteger(L, 2);
	lua_pushinteger(L, n + 1);
	lua_newtable(L);

	if (tnt_next(*pi)) {
		struct tnt_tuple *tu = TNT_ISTORAGE_TUPLE(*pi);
		struct tnt_log_row *row =
			&(TNT_SSNAPSHOT_CAST(TNT_ISTORAGE_STREAM(*pi))->log.current);
		uint32_t space = row->row_snap.space;
		lua_pushstring(L, "space");
		lua_pushinteger(L, space);
		lua_settable(L, -3); /* space */
		lua_pushstring(L, "tuple");
		lua_newtable(L);
		struct tnt_iter ifl;
		tnt_iter(&ifl, tu);
		while (tnt_next(&ifl)) {
			int idx = TNT_IFIELD_IDX(&ifl);
			char *data = TNT_IFIELD_DATA(&ifl);
			uint32_t size = TNT_IFIELD_SIZE(&ifl);
			lua_pushinteger(L, idx + 1);
			lua_pushlstring(L, data, size);
			lua_settable(L, -3); /* tuple field */
		}
		if (ifl.status == TNT_ITER_FAIL)
			luaL_error(L, "<parsing error>");
		tnt_iter_free(&ifl);
		lua_settable(L, -3); /* tuple */
		return 2;
	}
	if ((*pi)->status == TNT_ITER_FAIL) {
		char *errstr = tnt_snapshot_strerror(TNT_ISTORAGE_STREAM(*pi));
		luaL_error(L, "parsing failed: %s", errstr);
	}
	return 0;
}

/* Methods, Functions and Tables */

static int
parser_iter_gc(struct lua_State *L)
{
	struct tnt_iter **i = lua_checkiter(L, 1, "__gc");
	tnt_iter_free(*i);
	return 0;
}

static const struct luaL_reg parser_iter_meth [] = {
	{"__gc",	parser_iter_gc},
	{NULL, NULL}
};

static int
parser_xlog_gc(struct lua_State *L)
{
	struct tnt_stream **s = lua_checkstream(L, 1, "__gc", TNT_LOG_XLOG);
	tnt_stream_free(*s);
	return 0;
}

static int
parser_xlog_iterate(struct lua_State *L)
{
	int args_n = lua_gettop(L);
	struct tnt_stream **s = lua_checkstream(L, 1, "pairs", TNT_LOG_XLOG);
	if (args_n != 1)
		luaL_error(L, "Usage: parser:pairs()");
	struct tnt_iter *i = tnt_iter_request(NULL, *s);
	lua_pushcclosure(L, &parser_xlog_iterate_int, 1);
	lua_pushiter(L, i);
	lua_pushinteger(L, 0);
	return 3;
}

static const struct luaL_reg parser_xlog_meth [] = {
	{"__gc",	parser_xlog_gc},
	{"pairs",	parser_xlog_iterate},
	{NULL, NULL}
};

static int
parser_snap_gc(struct lua_State *L)
{
	struct tnt_stream **s = lua_checkstream(L, 1, "__gc", TNT_LOG_SNAPSHOT);
	tnt_stream_free(*s);
	return 0;
}

static int
parser_snap_iterate(struct lua_State *L)
{
	int args_n = lua_gettop(L);
	struct tnt_stream **s = lua_checkstream(L, 1, "pairs", TNT_LOG_SNAPSHOT);
	if (args_n != 1)
		luaL_error(L, "Usage: parser:pairs()");
	struct tnt_iter *i = tnt_iter_storage(NULL, *s);
	lua_pushcclosure(L, &parser_snap_iterate_int, 1);
	lua_pushiter(L, i);
	lua_pushinteger(L, 0);
	return 3;
}

static const struct luaL_reg parser_snap_meth [] = {
	{"__gc",	parser_snap_gc},
	{"pairs",	parser_snap_iterate},
	{NULL, NULL}
};

static int
lua_parser_open(struct lua_State *L)
{
	int top = lua_gettop(L);
	if (top == 0)
		luaL_error(L, "not enough args");
	const char *filename = luaL_checkstring(L, -1);
	enum tnt_log_type type = tnt_log_guess((char *)filename);
	struct tnt_stream *s = NULL;
	switch (type) {
	case TNT_LOG_SNAPSHOT:
		s = tnt_snapshot(NULL);
		if (s == NULL)
			luaL_error(L, "Failed to allocate memory for snapshot");
		if (tnt_snapshot_open(s, (char *)filename) == -1) {
			char *errstr = tnt_snapshot_strerror(s);
			luaL_error(L, "Cannot open snapshot '%s': %s",
				   filename, errstr);
			tnt_stream_free(s);
		}
		break;
	case TNT_LOG_XLOG:
		s = tnt_xlog(NULL);
		if (s == NULL)
			luaL_error(L, "Failed to allocate memory for xlog");
		if (tnt_xlog_open(s, (char *)filename) == -1) {
			char *errstr = tnt_xlog_strerror(s);
			luaL_error(L, "Cannot open xlog '%s': %s",
				   filename, errstr);
			tnt_stream_free(s);
		}
		break;
	default:
		luaL_error(L, "can't detect filetype");
	}
	lua_pushstream(L, s, type);
	return 1;
}

static const struct luaL_reg parser_lib_func [] = {
	{"open",	lua_parser_open},
	{NULL, NULL}
};

int
luaopen_migrate_xlog(struct lua_State *L)
{
	luaL_register_type(L, parser_xlog_name, parser_xlog_meth);
	luaL_register_type(L, parser_snap_name, parser_snap_meth);
	luaL_register_type(L, parser_iter_name, parser_iter_meth);
	luaL_register(L, parser_lib_name, parser_lib_func);
	return 1;
}
