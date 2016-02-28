#include <assert.h>
#include <ctype.h>
#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <tarantool/lua.h>
#include <tarantool/lauxlib.h>
#include <tarantool/lualib.h>

#include "xlog.h"

#include <tarantool/module.h>

#include <small/ibuf.h>

#include <tarantool/tnt.h>

#include <tarantool/tnt_opt.h>
#include <tarantool/tnt_net.h>
#include <tarantool/tnt_log.h>
#include <tarantool/tnt_rpl.h>

#include <tarantool/tnt_xlog.h>
#include <tarantool/tnt_snapshot.h>

#include "tuple.h"
#include "table.h"

struct ibuf xlog_ibuf;

static const char *parser_lib_name = "xlog.parser_v11";

uint32_t CTID_STRUCT_ITER_HELPER_REF;
box_tuple_format_t *tuple_format;

/* Internal methods */

struct space_def *
search_space(struct iter_helper *hlp, int space_no) {
	struct space_def *def = hlp->spaces;
	while (def != NULL) {
		if (space_no == def->space_no)
			return def;
		def = def->next;
	}
	return NULL;
}

static void
lual_pushtuple(struct lua_State *L, struct tnt_tuple *t,
	       struct space_def *def, int ret)
{
	if (ret == F_RET_TUPLE)
		return luatu_tuple_fields(L, t, def);
	return luata_tuple_fields(L, t, def);
}

static void
lual_pushkey(struct lua_State *L, struct tnt_tuple *t,
	     struct space_def *def, int ret)
{
	(void )ret;
//	if (ret == F_RET_TUPLE)
//		return luatu_key_fields(L, t, def);
	return luata_key_fields(L, t, def);
}

static void
lual_pushops(struct lua_State *L, struct tnt_request_update *req,
	     struct space_def *def, int ret)
{
	(void )ret;
//	if (ret == F_RET_TUPLE)
//		return luatu_ops_fields(L, req, def);
	return luata_ops_fields(L, req, def);
}

static int
parser_xlog_iter_op_insert(struct lua_State *L, struct tnt_request *r,
			   struct iter_helper *hlp)
{
	struct tnt_request_insert *req = &(r->r.insert);
	struct space_def *def = search_space(hlp, req->h.ns);
	if (!def && hlp->spaces)
		return 0;
	lua_pushstring(L, "op");
	lua_pushstring(L, "insert");
	lua_settable(L, -3); /* op */
	lua_pushstring(L, "flags");
	lua_pushnumber(L, req->h.flags);
	lua_settable(L, -3); /* flags */
	lua_pushstring(L, "space");
	lua_pushnumber(L, req->h.ns);
	lua_settable(L, -3); /* space */
	lua_pushstring(L, "tuple");
	lual_pushtuple(L, &(req->t), def, hlp->return_type);
	lua_settable(L, -3); /* tuple */
	return 1;
}

static int
parser_xlog_iter_op_delete(struct lua_State *L, struct tnt_request *r,
			   struct iter_helper *hlp)
{
	struct tnt_request_delete *req = &(r->r.del);
	struct space_def *def = search_space(hlp, req->h.ns);
	if (!def && hlp->spaces)
		return 0;
	lua_pushstring(L, "op");
	lua_pushstring(L, "delete");
	lua_settable(L, -3); /* op */
	lua_pushstring(L, "space");
	lua_pushnumber(L, req->h.ns);
	lua_settable(L, -3); /* space */
	lua_pushstring(L, "key");
	lual_pushkey(L, &(req->t), def, hlp->return_type);
	lua_settable(L, -3); /* tuple */
	return 1;
}

static int
parser_xlog_iter_op_update(struct lua_State *L, struct tnt_request *r,
			   struct iter_helper *hlp)
{
	struct tnt_request_update *req = &(r->r.update);
	struct space_def *def = search_space(hlp, req->h.ns);
	if (!def && hlp->spaces)
		return 0;
	lua_pushstring(L, "op");
	lua_pushstring(L, "update");
	lua_settable(L, -3); /* op */
	lua_pushstring(L, "space");
	lua_pushnumber(L, req->h.ns);
	lua_settable(L, -3); /* space */
	lua_pushstring(L, "key");
	lual_pushkey(L, &(req->t), def, hlp->return_type);
	lua_settable(L, -3); /* tuple */
	lua_pushstring(L, "ops");
	lual_pushops(L, req, def, hlp->return_type);
	lua_settable(L, -3); /* ops */
	return 1;
}

static int
lua_xlog_pairs(struct lua_State *L)
{
	lua_pushinteger(L, 2);
	lua_gettable(L, 1);
	uint32_t cdata;
	struct iter_helper *hlp = luaL_checkcdata(L, -1, &cdata);
	assert(cdata == CTID_STRUCT_ITER_HELPER_REF);

	int n = luaL_checkinteger(L, 2);
	lua_pushinteger(L, n + 1);

	struct tnt_iter *pi = hlp->iter;
	int batch_count = 0;

	lua_newtable(L);
	while (batch_count < hlp->batch_count && tnt_next(pi)) {
		lua_pushinteger(L, batch_count + 1);
		struct tnt_request *r = TNT_IREQUEST_PTR(pi);
		struct tnt_log_row *row =
			&(TNT_SXLOG_CAST(TNT_IREQUEST_STREAM(pi))->log.current);
		if (row->hdr.lsn < hlp->lsn_from ||
		    row->hdr.lsn > hlp->lsn_to) {
			lua_pop(L, 1);
			continue;
		}

		lua_newtable(L);
		lua_pushstring(L, "lsn");
		lua_pushnumber(L, row->hdr.lsn);
		lua_settable(L, -3); /* lsn */
		lua_pushstring(L, "time");
		lua_pushnumber(L, row->hdr.tm);
		lua_settable(L, -3); /* time */
		int rv = 0;
		switch (r->h.type) {
		case TNT_OP_INSERT:
			rv = parser_xlog_iter_op_insert(L, r, hlp);
			break;
		case TNT_OP_DELETE:
		case TNT_OP_DELETE_1_3:
			rv = parser_xlog_iter_op_delete(L, r, hlp);
			break;
		case TNT_OP_UPDATE:
			rv = parser_xlog_iter_op_update(L, r, hlp);
			break;
		default:
			luaL_error(L, "Unknown operation");
		}
		if (rv == 0) {
			lua_pop(L, 2);
			continue;
		}

		lua_settable(L, -3);
		batch_count += 1; /* operation */
	}

	if (pi->status == TNT_ITER_FAIL) {
		char *errstr = tnt_xlog_strerror(TNT_IREQUEST_STREAM(pi));
		luaL_error(L, "parsing failed: %s", errstr);
	}

	if (batch_count == 0)
		return 0;

	return 2;
}

static int
lua_snap_pairs(struct lua_State *L)
{
	lua_pushinteger(L, 2);
	lua_gettable(L, 1);
	uint32_t cdata;
	struct iter_helper *hlp = luaL_checkcdata(L, -1, &cdata);
	assert(cdata == CTID_STRUCT_ITER_HELPER_REF);

	int n = luaL_checkinteger(L, 2);
	lua_pushinteger(L, n + 1);

	struct tnt_iter *pi = hlp->iter;
	int batch_count = 0;
	struct space_def *def = NULL;

	lua_newtable(L);
	while (batch_count < hlp->batch_count && tnt_next(pi)) {
		lua_pushinteger(L, batch_count + 1);
		lua_newtable(L);

		struct tnt_log_row *row =
			&(TNT_SSNAPSHOT_CAST(TNT_ISTORAGE_STREAM(pi))->log.current);
		uint32_t space = row->row_snap.space;
		if (!def || (int )space != def->space_no)
			def = search_space(hlp, space);
		if (!def && hlp->spaces) {
			lua_pop(L, 2);
			continue;
		}
		lua_pushstring(L, "space");
		lua_pushinteger(L, space);
		lua_settable(L, -3); /* space */
		lua_pushstring(L, "tuple");
		lual_pushtuple(L, TNT_ISTORAGE_TUPLE(pi), def, hlp->return_type);
		lua_settable(L, -3); /* tuple */

		lua_settable(L, -3);
		batch_count += 1; /* operation */
	}

	if (pi->status == TNT_ITER_FAIL) {
		char *errstr = tnt_snapshot_strerror(TNT_ISTORAGE_STREAM(pi));
		luaL_error(L, "parsing failed: %s", errstr);
	}

	if (batch_count == 0)
		return 0;

	return 2;
}


enum tnt_error
iowait_cb(int fd, int *event, double tm) {
	assert((*event & ~(IO_READ | IO_WRITE)) == 0);
	int ev = 0;
	ev |= (*event & IO_READ ? COIO_READ : 0);
	ev |= (*event & IO_WRITE ? COIO_WRITE : 0);
	ev = coio_wait(fd, ev, tm);
	if (ev == 0)
		return TNT_ETMOUT;
	*event = 0;
	*event |= (ev & IO_READ ? COIO_READ : 0);
	*event |= (ev & IO_WRITE ? COIO_WRITE : 0);
	return TNT_EOK;
}

enum tnt_error
gaiwait_cb(const char *host, const char *port,
	   const struct addrinfo *hints,
	   struct addrinfo **res, double tm)
{
	int rv = coio_getaddrinfo(host, port, hints, res, tm);
	if (rv != 0) {
		freeaddrinfo(*res);
		*res = NULL;
		return TNT_ERESOLVE;
	}
	return TNT_EOK;
}

static char *opname(uint32_t type) {
	switch (type) {
	case TNT_OP_PING:   return "Ping";
	case TNT_OP_INSERT: return "Insert";
	case TNT_OP_DELETE: return "Delete";
	case TNT_OP_UPDATE: return "Update";
	case TNT_OP_SELECT: return "Select";
	case TNT_OP_CALL:   return "Call";
	}
	return "Unknown";
}

static int
lua_rpl_prepair(struct lua_State *L)
{
	if (lua_gettop(L) != 3)
		luaL_error(L, "Usage: rpl_prepare(host, port, lsn)");
	const char *host = luaL_checkstring(L, 1);
	int port = luaL_checkinteger(L, 2);
	long lsn = luaL_checklong(L, 3);
	/* Prepare network stream */
	struct tnt_stream *net = NULL;
	net = tnt_net(NULL);
	tnt_set(net, TNT_OPT_HOSTNAME, host);
	tnt_set(net, TNT_OPT_PORT, port);
	tnt_set(net, TNT_OPT_IOWAIT_CB, iowait_cb);
	tnt_set(net, TNT_OPT_GAIWAIT_CB, gaiwait_cb);
	/* Prepare replication stream */
	struct tnt_stream *rpl = NULL;
	rpl = tnt_rpl(NULL);
	tnt_rpl_attach(rpl, net);
	tnt_rpl_open(rpl, lsn);
	if (tnt_error(net) != TNT_EOK)
		luaL_error(L, "tnt_rpl_open failed: %s", tnt_strerror(net));
	/* do something */
	struct tnt_iter iter; tnt_iter_request(&iter, rpl);
	while(tnt_next(&iter)) {
		struct tnt_stream_rpl *sr = TNT_RPL_CAST(rpl);
		say_info("%s lsn: %"PRIu64", time: %f, len: %d\n",
			 opname(sr->row.op), sr->hdr.lsn, sr->hdr.tm,
			 sr->hdr.len);
	}
	if (iter.status == TNT_ITER_FAIL)
		luaL_error(L, "parsing failed: %s", tnt_strerror(net));
	/* close replication and network stream */
	tnt_rpl_close(rpl);
	tnt_close(net);
	return 0;
}

static const struct luaL_reg
parser_lib_func [] = {
	{ "snap_pairs",		lua_snap_pairs		 },
	{ "xlog_pairs",		lua_xlog_pairs		 },
	{ "rpl_prepare",	lua_rpl_prepair		 },
	{ NULL,			NULL			 }
};

int
luaopen_migrate_xlog_internal(struct lua_State *L)
{
	ibuf_create(&xlog_ibuf, cord_slab_cache(), 16000);;
	CTID_STRUCT_ITER_HELPER_REF = luaL_ctypeid(L, "struct iter_helper [1]");
	luaL_register(L, parser_lib_name, parser_lib_func);
	tuple_format = box_tuple_format_default();
	/* assert(tuple_format); */
	return 1;
}
