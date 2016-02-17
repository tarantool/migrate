local ffi = require('ffi')
local fun = require('fun')
local json = require('json')

local utils = require('migrate.utils')
local ct = require('migrate.utils.checktype')
local log = require('log')

local wrap = fun.wrap
local iter = fun.iter
local error = utils.error
local checkt = ct.checkt
local checkt_xc = ct.checkt_xc
local checkt_table_xc = ct.checktable_table_xc

local UINT64_MAX = 18446744073709551615ULL

-- ffi.load(package.searchpath('migrate.xlog.libtarantool', package.cpath), true)
-- ffi.load(package.searchpath('migrate.xlog.libtarantoolrpl', package.cpath), true)
ffi.load(package.searchpath('migrate.xlog.internal', package.cpath), true)

local function man_gc(object)
    log.debug("GC'ed %s", tostring(object))
end

ffi.cdef[[
struct tnt_iter;

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
    int def;
    int throw;
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

enum tnt_log_error {
    TNT_LOG_EOK,
    TNT_LOG_EFAIL,
    TNT_LOG_EMEMORY,
    TNT_LOG_ETYPE,
    TNT_LOG_EVERSION,
    TNT_LOG_ECORRUPT,
    TNT_LOG_ESYSTEM,
    TNT_LOG_LAST
};

enum tnt_log_type {
    TNT_LOG_NONE,
    TNT_LOG_XLOG,
    TNT_LOG_SNAPSHOT
};

struct tnt_stream;

enum tnt_log_type tnt_log_guess(const char *file);

struct tnt_stream *tnt_snapshot(struct tnt_stream *s);
int tnt_snapshot_open(struct tnt_stream *s, const char *file);
enum tnt_log_error tnt_snapshot_error(struct tnt_stream *s);
char *tnt_snapshot_strerror(struct tnt_stream *s);
int tnt_snapshot_errno(struct tnt_stream *s);
int tnt_snapshot_reset(struct tnt_stream *s);

struct tnt_stream *tnt_xlog(struct tnt_stream *s);
int tnt_xlog_open(struct tnt_stream *s, const char *file);
enum tnt_log_error tnt_xlog_error(struct tnt_stream *s);
char *tnt_xlog_strerror(struct tnt_stream *s);
int tnt_xlog_errno(struct tnt_stream *s);
int tnt_xlog_reset(struct tnt_stream *s);

void tnt_stream_free(struct tnt_stream *s);

/* Iterator */
struct tnt_iter;
struct tnt_iter *tnt_iter_request(struct tnt_iter *i, struct tnt_stream *s);
struct tnt_iter *tnt_iter_storage(struct tnt_iter *i, struct tnt_stream *s);
void tnt_iter_free(struct tnt_iter *i);

void *malloc(size_t size);

]]

local internal = require('migrate.xlog.internal')

local iter_helper_t = ffi.typeof('struct iter_helper [1]')
local space_def_t = ffi.typeof('struct space_def [1]')
local int_arr_t = ffi.typeof('int [?]')

local function field_convert(fld)
    if fld == nil or fld == 'str' or fld == 'STR' then
        return ffi.C.F_FLD_STR
    elseif fld == 'num' or fld == 'NUM' then
        return ffi.C.F_FLD_NUM
    else
        error("bad schema field value, expected 'num'/'str', got '%s'", fld)
    end
end

local function fields_convert(schema)
    return iter(schema):map(function (fld) return field_convert(fld) end)
end

hold = {}

local function checkt_spaces_table_xc(spaces, ext, convert, throw)
    local rv = {}
    for id, v in pairs(spaces) do
        local space_def = ffi.gc(ffi.new(space_def_t), man_gc)
        table.insert(hold, space_def)
        log.debug("AL'ed " .. tostring(space_def))
        if not type(id) == 'number' then
            error("Bad 'space_id' type, expected 'number', got '%s' (%s)",
                  type(id), id)
        end
        space_def[0].def = ffi.C.F_FLD_STR
        if convert then
            checkt_xc(v, 'table', 'bad space description')
            checkt_xc(v.schema, 'table', 'bad schema field')
            space_def[0].throw = throw or false
            if ext == 'xlog' then
                checkt_xc(v.ischema, 'table', 'bad ischema field')
                local ischema = ffi.new(int_arr_t, #v.ischema)
                table.insert(hold, ischema)
                space_def[0].ischema = ffi.gc(ischema, man_gc)
                log.debug("AL'ed " .. tostring(space_def[0].ischema))
                fields_convert(v.ischema):enumerate():each(
                    function (k, v)
                        space_def[0].ischema[k - 1] = v
                    end
                )
                space_def[0].ischema_len = #v.ischema
            end
            local schema = ffi.new(int_arr_t, #v.schema)
            space_def[0].schema = ffi.gc(schema, man_gc)
            table.insert(hold, schema)
            log.debug("AL'ed " .. tostring(space_def[0].schema))
            fields_convert(v.schema):enumerate():each(
                function (k, v)
                    space_def[0].schema[k - 1] = v
                end
            )
            space_def[0].schema_len = #v.schema
            space_def[0].def = field_convert(v.default)
        end
        space_def[0].convert = convert
        space_def[0].space_no = id
        table.insert(rv, space_def)
    end
    return rv
end

--[[
config { -- table
    spaces = {
        [space_id1] = {
            -- for xlog
            ischema = {'num'/'str', ...} -- (NYI) or { xlog.NUM / xlog.STR, ...} or {1, ...},
            -- for xlog/snap
            schema = {'num'/'str', ...} (NYI) or { xlog.NUM / xlog.STR, ...},
            default = 'num'/'str' (NYI) or {xlog.NUM, xlog.STR, ...}
        },
        [space_id2] = ...
    } -- (NYI) - spaces to load
    -- OR
    -- spaces = {
    --     [space_id1] = true, [space_id2] = true, ...
    -- }
    -- for xlog/snap
    convert = true/false, -- (NYI) - convert tuple fields or not
    return_type = 'tuple'/'table', -- (NYI) - retval of tuples/keys.
    batch_count -- (NYI) - number of tuples to load at 1 batch
    throw = true/false/nil (NYI) - throw error if can't convert field
    -- for xlog
    lsn_from = (number)
    lsn_to   = (number)/
}
]]--

local function parse_cfg(cfg, ext, iter)
    cfg = cfg or {}
    checkt_xc(cfg, 'table', 'config')
    checkt_xc(cfg.convert, {'boolean', 'nil'}, 'config.convert')
    checkt_xc(cfg.spaces, {'table', 'nil'}, 'config.spaces')
    if cfg.convert and cfg.spaces == nil then
        error("'config.convert' is set, but 'config.spaces' is not")
    end
    checkt_xc(cfg.return_type, {'string', 'nil'}, 'config.return_type')
    checkt_xc(cfg.batch_count, {'number', 'nil'}, 'config.batch_count')
    checkt_xc(cfg.throw, {'boolean', 'nil'}, 'config.throw')
    checkt_xc(cfg.lsn_from, {'number', 'nil'}, 'config.lsn_from')
    checkt_xc(cfg.lsn_to, {'number', 'nil'}, 'config.lsn_to')

    local convert = cfg.convert or false
    local helper = iter_helper_t()
    helper[0].convert = convert
    helper[0].iter = iter
    helper[0].batch_count = cfg.batch_count or 1
    helper[0].lsn_from = cfg.lsn_from or 1
    helper[0].lsn_to = cfg.lsn_to or UINT64_MAX
    local return_type = cfg.return_type or 'table'
    if return_type == 'table' or return_type == 'TABLE' then
        helper[0].return_type = ffi.C.F_RET_TABLE
    elseif return_type == 'tuple' or return_type == 'TUPLE' then
        if type(box.cfg) == 'function' then
            error("Tuples are expected, but 'box.cfg' is not inited", 3)
        end
        helper[0].return_type = ffi.C.F_RET_TUPLE
    else
        error("bad 'config.return_type' value, expected 'table'/'tuple', got '%s'",
              return_type)
    end
    local space_def_arr = nil
    local last = nil
    if cfg.spaces then
        space_def_arr = checkt_spaces_table_xc(cfg.spaces, ext, convert, cfg.throw)
        for _, v in ipairs(space_def_arr) do
            table.insert(hold, v[0])
            table.insert(hold, v[0].schema)
            table.insert(hold, v[0].ischema)
            if last == nil then
                helper[0].spaces = v
            else
                last[0].next = v
            end
            last = v
        end
    end
    table.insert(hold, iter)
    local function gc_hold(obj)
        -- Temporary hack for gc (do not give GC to sweep space_def/iter)
        log.debug("hold object is destroyed")
        return hold
    end
    ffi.gc(helper, gc_hold)
    return helper
end

local function iterate(func)
    return function ()
        func()
    end
end

local function reader_open(name, cfg)
    checkt_xc(name, 'string', 'name')
    local ext = name:sub(-4, -1)
    if ext ~= 'xlog' and ext ~= 'snap' then
        error("bad extension name, expected 'snap'/'xlog', got '%s'", ext)
    end

    local log_type = ffi.C.tnt_log_guess(name)
    if log_type == ffi.C.TNT_LOG_SNAPSHOT then
        local log = ffi.C.tnt_snapshot(nil)
        if log == nil then
            error("Failed to allocate memory for snapshot")
        end
        ffi.gc(log, ffi.C.tnt_stream_free)
        if ffi.C.tnt_snapshot_open(log, name) == -1 then
            local errstr = ffi.string(ffi.C.tnt_snapshot_strerror(log))
            error("Cannot open snapshot '%s': %s", name, errstr)
        end
        local iter = ffi.C.tnt_iter_storage(nil, log)
        if iter == nil then
            error("failed to allocate memory for snap iterator")
        end
        local helper = parse_cfg(cfg, ext, iter)
        ffi.gc(iter, ffi.C.tnt_iter_free)
        -- return internal.snap_pairs, {log, helper}, 0
        return fun.wrap(internal.snap_pairs, {log, helper}, 0)
    elseif log_type == ffi.C.TNT_LOG_XLOG then
        local log = ffi.C.tnt_xlog(nil)
        if log == nil then
            error("Failed to allocate memory for xlog")
        end
        ffi.gc(log, ffi.C.tnt_stream_free)
        if ffi.C.tnt_xlog_open(log, name) == -1 then
            local errstr = ffi.string(ffi.C.tnt_xlog_strerror(log))
            error("Cannot open xlog '%s': %s", name, errstr)
        end
        local iter = ffi.C.tnt_iter_request(nil, log)
        if iter == nil then
            error("failed to allocate memory for xlog iterator")
        end
        local helper = parse_cfg(cfg, ext, iter)
        ffi.gc(iter, ffi.C.tnt_iter_free)
        return fun.wrap(internal.xlog_pairs, {log, helper}, 0)
    end
    error("can't detect filetype")
end

return {
    open = reader_open
}
