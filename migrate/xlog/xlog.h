#ifndef   __LUA_XLOG_H__
#define   __LUA_XLOG_H__

#include <sys/queue.h>
#include <stdint.h>

enum field_t {
	F_FLD_STR = 0,
	F_FLD_NUM,
	F_FLD_MAX
};

enum ret_t {
	F_RET_TUPLE = 0,
	F_RET_TABLE,
	F_RET_MAX
};

struct space_def {
	int space_no;
	int *schema;
	int *ischema;
	uint32_t schema_len;
	uint32_t ischema_len;
	int defaults;
	int throws;
	int convert;
	struct space_def *next;
};

struct iter_helper {
	struct tnt_iter *iter;
	struct space_def *spaces;
	int batch_count;
	int convert;
	int return_type;
	uint64_t lsn_from;
	uint64_t lsn_to;
};

int luaopen_xlog(struct lua_State *L);

struct update_op_record {
	const char *operation;
	uint8_t args_count;
};

static struct update_op_record
update_op_records [] = {
	/* TNT_UPDATE_ASSIGN */	{"=", 3 },
	/* TNT_UPDATE_ADD */	{"+", 3 },
	/* TNT_UPDATE_AND */	{"&", 3 },
	/* TNT_UPDATE_XOR */	{"^", 3 },
	/* TNT_UPDATE_OR */	{"|", 3 },
	/* TNT_UPDATE_SPLICE */	{":", 5 },
	/* TNT_UPDATE_DELETE */	{"#", 3 },
	/* TNT_UPDATE_INSERT */	{"!", 3 },
	{ NULL, 0 }
};

#endif /* __LUA_XLOG_H__ */
