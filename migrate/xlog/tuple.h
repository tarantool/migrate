#ifndef   _XLOG_TUPLE_H_
#define   _XLOG_TUPLE_H_

struct lua_State;
struct tnt_tuple;
struct tnt_request_update;
struct space_def;

void
luatu_tuple_fields(struct lua_State *L, struct tnt_tuple *t,
		   struct space_def *def);

void
luatu_key_fields(struct lua_State *L, struct tnt_tuple *t,
		 struct space_def *def);

void
luatu_ops_fields(struct lua_State *L, struct tnt_request_update *req,
		 struct space_def *def);

#endif /* _XLOG_TUPLE_H_ */
