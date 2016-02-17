#include "table.h"

#include <module.h>

#include <assert.h>
#include <stdint.h>
#include <inttypes.h>

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include <tarantool/tnt.h>

#include "xlog.h"

static void
lua_field_encode(struct lua_State *L, const char *data, size_t size,
		 enum field_t tp, bool throws)
{
	if (tp == F_FLD_NUM && size == 4) {
		lua_pushnumber(L, *((uint32_t*)data));
	} else if (tp == F_FLD_NUM && size == 8) {
		luaL_pushuint64(L, *((uint64_t*)data));
	} else {
		if (tp == F_FLD_NUM && throws)
			luaL_error(L, "Cannot convert field '%.*s' to type NUM,"
				      " exptected len 4 or 8, got '%zd'",
				      size, data, size);
		lua_pushlstring(L, data, size);
	}
}

void
luata_tuple_fields(struct lua_State *L, struct tnt_tuple *t,
		   struct space_def *def)
{
	lua_newtable(L);
	struct tnt_iter ifl;
	tnt_iter(&ifl, t);
	while (tnt_next(&ifl)) {
		int idx = TNT_IFIELD_IDX(&ifl);
		char *data = TNT_IFIELD_DATA(&ifl);
		uint32_t size = TNT_IFIELD_SIZE(&ifl);
		enum field_t tp = F_FLD_STR;
		int throws = true;
		if (def) {
			tp = def->defaults;
			if (def->convert && idx < def->schema_len)
				tp = def->schema[idx];
		}
		lua_pushinteger(L, idx + 1);
		lua_field_encode(L, data, size, tp, throws);
		lua_settable(L, -3); /* tuple field */
	}
	if (ifl.status == TNT_ITER_FAIL) {
		luaL_error(L, "failed to parse tuple");
	}
	tnt_iter_free(&ifl);
}

void
luata_key_fields(struct lua_State *L, struct tnt_tuple *t,
		 struct space_def *def)
{
	lua_newtable(L);
	struct tnt_iter ifl;
	tnt_iter(&ifl, t);
	while (tnt_next(&ifl)) {
		int idx = TNT_IFIELD_IDX(&ifl);
		char *data = TNT_IFIELD_DATA(&ifl);
		uint32_t size = TNT_IFIELD_SIZE(&ifl);
		enum field_t tp = F_FLD_STR;
		int throws = true;
		if (def) {
			tp = def->defaults;
			if (def->convert && idx < def->ischema_len)
				tp = def->ischema[idx];
		}
		lua_pushinteger(L, idx + 1);
		lua_field_encode(L, data, size, tp, throws);
		lua_settable(L, -3); /* tuple field */
	}
	if (ifl.status == TNT_ITER_FAIL) {
		luaL_error(L, "failed to parse tuple");
	}
	tnt_iter_free(&ifl);
}

static inline void
lua_settable_nn(struct lua_State *L, lua_Number n1, lua_Number n2, int idx)
{
	lua_pushnumber(L, n1);
	lua_pushnumber(L, n2);
	if (idx < 0) idx -= 2;
	lua_settable(L, idx);
}

static inline void
lua_settable_ns(struct lua_State *L, lua_Number n, const char *s, int idx)
{
	lua_pushnumber(L, n);
	lua_pushstring(L, s);
	if (idx < 0) idx -= 2;
	lua_settable(L, idx);
}

static inline void
lua_settable_nsl(struct lua_State *L, lua_Number n, const char *s,
		 size_t l, int idx)
{
	lua_pushnumber(L, n);
	lua_pushlstring(L, s, l);
	if (idx < 0) idx -= 2;
	lua_settable(L, idx);
}

void
luata_ops_fields(struct lua_State *L, struct tnt_request_update *req,
		 struct space_def *def)
{
	lua_newtable(L);
	uint32_t i = 0;
	for (i = 0; i < req->opc; ++i) {
		struct tnt_request_update_op *op = &req->opv[i];
		lua_pushinteger(L, i + 1);
		lua_newtable(L);
		if (op->op >= TNT_UPDATE_MAX)
			luaL_error(L, "undefined update operation");
		lua_settable_ns(L, 1, update_op_records[op->op].operation, -1);
		lua_settable_nn(L, 2, op->field + 1, -1);
		char *data = op->data;
		uint32_t size = op->size;
		switch (op->op) {
		case TNT_UPDATE_ADD:
		case TNT_UPDATE_AND:
		case TNT_UPDATE_XOR:
		case TNT_UPDATE_OR:
			lua_pushnumber(L, 3);
			lua_field_encode(L, data, size, F_FLD_NUM, true);
			lua_settable(L, -3); /* value */
			break;
		case TNT_UPDATE_INSERT:
		case TNT_UPDATE_ASSIGN:
			lua_pushnumber(L, 3);
			enum field_t tp = F_FLD_STR;
			int throws = true;
			if (def) {
				tp = def->defaults;
				if (def->convert && op->field < def->schema_len)
					tp = def->schema[op->field];
			}
			lua_field_encode(L, data, size, tp, throws);
			lua_settable(L, -3); /* value */
			break;
		case TNT_UPDATE_SPLICE: {
			size_t pos = 1;
			lua_settable_nn(L, 3, *(int32_t *)(data + pos), -1);
			pos += 5;
			lua_settable_nn(L, 4, *(int32_t *)(data + pos), -1);
			pos += 4 + op->size_enc_len;
			lua_settable_nsl(L, 5, data, size - pos, -1);
			break;
		}
		case TNT_UPDATE_DELETE:
			lua_settable_nn(L, 3, 1, -1);
			break;
		}
		lua_settable(L, -3); /* op */
	}
}
