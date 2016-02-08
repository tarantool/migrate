#include "tuple.h"

#include <module.h>

#include <assert.h>
#include <stdint.h>
#include <inttypes.h>

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include <small/ibuf.h>
#include <msgpuck/mpstream.h>

#include <tarantool/tnt.h>

#include "xlog.h"

extern uint32_t CTID_CONST_STRUCT_TUPLE_REF;
extern struct ibuf xlog_ibuf;
extern box_tuple_format_t *tuple_format;

static void
mpstream_tarantool_err(void *error_ctx, const char *err, size_t errlen)
{
	say_error("%.*s", (int )errlen, err);
	struct lua_State *L = (struct lua_State *) error_ctx;
	luaL_error(L, err);
}

struct tuple *
lua_istuple(struct lua_State *L, int narg)
{
	assert(CTID_CONST_STRUCT_TUPLE_REF != 0);
	uint32_t ctypeid;
	void *data;

	data = luaL_checkcdata(L, narg, &ctypeid);
	if (ctypeid != CTID_CONST_STRUCT_TUPLE_REF)
		return NULL;

	return *(struct tuple **) data;
}

static struct tuple *
lua_checktuple(struct lua_State *L, int narg)
{
	struct tuple *tuple = lua_istuple(L, narg);
	if (tuple == NULL)  {
		luaL_error(L, "Invalid argument #%d (box.tuple expected, got %s)",
		   narg, lua_typename(L, lua_type(L, narg)));
	}

	return tuple;
}

static int
lua_tuple_gc(struct lua_State *L)
{
	struct tuple *tuple = lua_checktuple(L, 1);
	box_tuple_unref(tuple);
	return 0;
}

static void
lua_pushtuple(struct lua_State *L, struct tuple *tuple)
{
	assert(CTID_CONST_STRUCT_TUPLE_REF != 0);
	struct tuple **ptr = (struct tuple **)
		luaL_pushcdata(L, CTID_CONST_STRUCT_TUPLE_REF);
	*ptr = tuple;
	/* The order is important - first reference tuple, next set gc */
	if (box_tuple_ref(tuple) != 0) {
		box_error_t *err = box_error_last();
		luaL_error(L, box_error_message(err));
		return;
	}
	lua_pushcfunction(L, lua_tuple_gc);
	luaL_setcdatagc(L, -2);
}

static void
lua_field_encode(struct lua_State *L, struct mpstream *stream, const char *data,
		 size_t size, enum field_t tp, bool throws)
{
	if (tp == F_FLD_NUM && size == 4) {
		mpstream_encode_uint(stream, *((uint32_t *)data));
	} else if (tp == F_FLD_NUM && size == 8) {
		mpstream_encode_uint(stream, *((uint64_t *)data));
	} else {
		if (tp == F_FLD_NUM && throws)
			luaL_error(L, "Cannot convert field '%.*s' to type NUM,"
				      " exptected len 4 or 8, got '%zd'",
				      data, size);
		mpstream_encode_str(stream, data, size);
	}
}

void
luatu_tuple_fields(struct lua_State *L, struct tnt_tuple *t,
		   struct space_def *def)
{
	struct ibuf *buf = &xlog_ibuf;

	ibuf_reset(buf);
	struct mpstream stream;
	mpstream_init(&stream, buf, ibuf_reserve_cb, ibuf_alloc_cb,
		      mpstream_tarantool_err, L);

	mpstream_encode_array(&stream, t->cardinality);
	struct tnt_iter ifl;
	tnt_iter(&ifl, t);
	while (tnt_next(&ifl)) {
		int idx = TNT_IFIELD_IDX(&ifl);
		assert(idx < t->cardinality);
		char *data = TNT_IFIELD_DATA(&ifl);
		uint32_t size = TNT_IFIELD_SIZE(&ifl);
		enum field_t tp = F_FLD_STR;
		int throws = true;
		if (def) {
			tp = def->defaults;
			if (idx < def->schema_len)
				tp = def->schema[idx];
		}
		lua_field_encode(L, &stream, data, size, tp, throws);
	}
	if (ifl.status == TNT_ITER_FAIL)
		luaL_error(L, "failed to parse tuple");
	tnt_iter_free(&ifl);

	box_tuple_t *tuple = box_tuple_new(tuple_format, buf->buf,
					   buf->buf + ibuf_used(buf));
	if (tuple == NULL)
		luaL_error(L, "%s: out of memory (box_tuple_new)", __func__);
	lua_pushtuple(L, tuple);
	return;
}

void
luatu_key_fields(struct lua_State *L, struct tnt_tuple *t,
		 struct space_def *def)
{
	struct ibuf *buf = &xlog_ibuf;

	ibuf_reset(buf);
	struct mpstream stream;
	mpstream_init(&stream, buf, ibuf_reserve_cb, ibuf_alloc_cb,
		      mpstream_tarantool_err, L);

	mpstream_encode_array(&stream, t->cardinality);
	struct tnt_iter ifl;
	tnt_iter(&ifl, t);
	while (tnt_next(&ifl)) {
		int idx = TNT_IFIELD_IDX(&ifl);
		assert(idx < t->cardinality);
		char *data = TNT_IFIELD_DATA(&ifl);
		uint32_t size = TNT_IFIELD_SIZE(&ifl);
		enum field_t tp = F_FLD_STR;
		int throws = true;
		if (def) {
			tp = def->defaults;
			if (idx < def->ischema_len)
				tp = def->ischema[idx];
		}
		lua_field_encode(L, &stream, data, size, tp, throws);
	}
	if (ifl.status == TNT_ITER_FAIL)
		luaL_error(L, "failed to parse tuple");
	tnt_iter_free(&ifl);

	box_tuple_t *tuple = box_tuple_new(tuple_format, buf->buf,
					   buf->buf + ibuf_used(buf));
	if (tuple == NULL)
		luaL_error(L, "%s: out of memory (box_tuple_new)", __func__);
	lua_pushtuple(L, tuple);
	return;
}

void
luatu_ops_fields(struct lua_State *L, struct tnt_request_update *req,
		 struct space_def *def)
{
	struct ibuf *buf = &xlog_ibuf;

	ibuf_reset(buf);
	struct mpstream stream;
	mpstream_init(&stream, buf, ibuf_reserve_cb, ibuf_alloc_cb,
		      mpstream_tarantool_err, L);

	uint32_t i = 0;
	mpstream_encode_array(&stream, req->opc);
	for (i = 0; i < req->opc; ++i) {
		struct tnt_request_update_op *op = &req->opv[i];
		if (op->op >= TNT_UPDATE_MAX)
			luaL_error(L, "Undefined update operation: 0x%02x",
				   op->op);
		char *data = op->data;
		uint32_t size = op->size;
		mpstream_encode_array(&stream, update_op_records[op->op].args_count);
		mpstream_encode_str(&stream, update_op_records[op->op].operation, 1);
		mpstream_encode_uint(&stream, op->field);
		switch (op->op) {
		case TNT_UPDATE_ADD:
		case TNT_UPDATE_AND:
		case TNT_UPDATE_XOR:
		case TNT_UPDATE_OR: {
			lua_field_encode(L, &stream, data, size, F_FLD_NUM, true);
			break;
		}
		case TNT_UPDATE_INSERT:
		case TNT_UPDATE_ASSIGN: {
			enum field_t tp = F_FLD_STR;
			int throws = true;
			if (def) {
				tp = def->defaults;
				if (op->field < def->schema_len)
					tp = def->schema[op->field];
			}
			lua_field_encode(L, &stream, data, size, tp, throws);
			break;
		}
		case TNT_UPDATE_SPLICE: {
			size_t pos = 1;
			mpstream_encode_uint(&stream, *(int32_t *)(data + pos));
			pos += 5;
			mpstream_encode_uint(&stream, *(int32_t *)(data + pos));
			pos += 4 + op->size_enc_len;
			mpstream_encode_str(&stream, data, size - pos);
			break;
		}
		case TNT_UPDATE_DELETE:
			mpstream_encode_uint(&stream, 1);
			break;
		}
	}

	box_tuple_t *tuple = box_tuple_new(tuple_format, buf->buf,
					   buf->buf + ibuf_used(buf));
	if (tuple == NULL)
		luaL_error(L, "%s: out of memory (box_tuple_new)", __func__);
	lua_pushtuple(L, tuple);
	return;
}
