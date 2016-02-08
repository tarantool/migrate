#ifndef   _XLOG_TABLE_H_
#define   _XLOG_TABLE_H_

struct lua_State;
struct tnt_tuple;
struct tnt_request_update;
struct space_def;

void
luata_tuple_fields(struct lua_State *L, struct tnt_tuple *t,
		   struct space_def *def);

void
luata_key_fields(struct lua_State *L, struct tnt_tuple *t,
		 struct space_def *def);

void
luata_ops_fields(struct lua_State *L, struct tnt_request_update *req,
		 struct space_def *def);

#endif /* _XLOG_TABLE_H_ */
