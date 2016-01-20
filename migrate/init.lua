local fun = require('fun')
local log = require('log')
local json = require('json')
local yaml = require('yaml')
local pickle = require('pickle')

require('strict').on()

local elog_mod = require('migrate.utils.elog')
local elog = elog_mod({
    path = './migrate.log',
    level = 20
})

local error = require('migrate.utils').error

local _ = nil

yaml.cfg{
    encode_invalid_numbers = true;
    encode_load_metatables = true;
    encode_use_tostring    = true;
    encode_invalid_as_nil  = true;
}

local iter, enumerate = fun.iter, fun.enumerate

local xlog = require('migrate.xlog')
local xdir = require('migrate.xdir')
local helper = require('migrate.utils.checktype')
local lazy_func = require('migrate.utils').lazy_func
local checkt_xc, checkt_table_xc = helper.checkt_xc, helper.checkt_table_xc

local box_add = 2
local box_replace = 4
local box_both = bit.bor(box_add, box_replace)

local function verify_space_def_advance(config)
    checkt_xc(config, 'table', 'config')
    checkt_xc(config.insert, 'function', 'config.insert')
    checkt_xc(config.delete, 'function', 'config.delete')
    checkt_xc(config.update, 'function', 'config.update')
end

local function verify_space_def_simple(config)
    checkt_xc(config, 'table', 'config')
    checkt_xc(config.new_id, {'number', 'string'}, 'config.new_id')
    checkt_table_xc(config.fields, 'string', 'config.fields')
    checkt_xc(config.default, 'string', 'config.default')
    checkt_xc(config.index, 'table', 'config.index')
    checkt_xc(config.index.new_id, {'number', 'string'}, 'config.index.new_id')
    checkt_table_xc(config.index.parts, 'number', 'config.index.parts')
end

local function convert_simple_config(cfg, throw)
    local sid = box.space[cfg.new_id]
    local iid = sid.index[cfg.index.new_id]
    local fields, default = cfg.fields, cfg.default

    local function get_type(i) return fields[i] or default end

    local ifields = iter(cfg.index.parts):map(get_type):totable()

    local function convert_field(field, ftype)
        if ftype == 'num' then
            if #field == 4 then
                return pickle.unpack('N', field)
            elseif #field == 8 then
                return pickle.unpack('Q', field)
            end
            if throw then
                error('failed to convert string to number')
            end
        end
        return field
    end

    local function convert_tuple(tuple)
        return enumerate(tuple):map(
            function (i, field) return convert_field(field, get_type(i)) end
        ):totable()
    end

    local function convert_key(key)
        return enumerate(key):map(
            function (i, field) return convert_field(field, ifields[i]) end
        ):totable()
    end

    local function convert_ops(ops)
        return iter(ops):map(
            function (op)
                op[#op] = convert_field(op[#op], get_type(op[2]))
                return op
            end
        ):totable()
    end

    local function xpcall_cb(err) elog:tb(elog_mod.ERROR); return err end

    return {
        insert = function (tuple, flags)
            local stat, val = xpcall(lazy_func(convert_tuple, tuple), xpcall_cb)
            if not stat then
                elog:error("Failed to convert tuple: %s", val)
                elog:error("Tuple: %s", tuple)
                elog:error("Space schema: %s (default '%s')", fields, default)
                error("Failed to convert tuple: %s", val)
            end
            tuple = val
            xpcall(lazy_func(sid.replace, sid, tuple), xpcall_cb)
        end,
        delete = function (key, flags)
            local stat, val = xpcall(lazy_func(convert_key, key), xpcall_cb)
            if not stat then
                elog:error("Failed to convert key: %s", val)
                elog:error("Key: %s", key)
                elog:error("Index schema: %s", ifields)
                error("Failed to convert key: %s", val)
            end
            key = val
            xpcall(lazy_func(sid.delete, sid, key), xpcall_cb)
        end,
        update = function (key, ops, flags)
            local val, stat = nil, nil

            stat, val = xpcall(lazy_func(convert_key, key), xpcall_cb)
            if not stat then
                elog:error("Failed to convert key: %s", val)
                elog:error("Key: %s", key)
                elog:error("Index schema: %s", ifields)
                error("Failed to convert key: %s", val)
            end
            key = val

            stat, val = xpcall(lazy_func(convert_ops, ops), xpcall_cb)
            if not stat then
                elog:error("Failed to convert ops: %s", val)
                elog:error("Ops: %s", ops)
                elog:error("Space schema: %s (default '%s')", fields, default)
                error("Failed to convert ops: %s", val)
            end
            ops = val

            xpcall(lazy_func(sid.delete, sid, key, ops), xpcall_cb)
        end
    }
end

local function verify_space_def(space_id, config)
    checkt_xc(space_id, 'number', 'space_id')
    if config.new_id then
        verify_space_def_simple(config)
        config = convert_simple_config(config)
    else
        verify_space_def_advance(config)
    end
    return config
end

local reader_mt = {
    resume = function (self)
        local files
        if self.lsn == 0 then
            _, files = xdir.xdir(self.snap_dir, self.xlog_dir)
        else
            _, files = xdir.xdir_lsn(self.xlog_dir, self.lsn)
        end
        local lsn = 0
        for _, file in pairs(files) do
            elog:info("opening %s", file);
            local fd = xlog.open(file)
            if file:sub(-4) == 'snap' then
                for k, v in fd:pairs() do
                    if k % 100000 == 0 then
                        elog:info('Processed %d tuples', k)
                    end
                    if self.spaces[v.space] then
                        self.spaces[v.space].insert(v.tuple)
                    end
                end
                lsn = xdir.xdir_lsn_from_filename(file)
            else
                for k, v in fd:pairs() do
                    if k % 100000 == 0 then
                        elog:info('Processed %d requests', k)
                    end
                    if self.spaces[v.space] then
                        if v.op == 'insert' then
                            self.spaces[v.space].insert(v.tuple, v.flags)
                        elseif v.op == 'delete' then
                            self.spaces[v.space].delete(v.key, v.flags)
                        elseif v.op == 'update' then
                            self.spaces[v.space].update(v.key, v.ops, v.flags)
                        end
                    end
                    lsn = v.lsn
                end
            end
        end
        self.lsn = lsn
    end
}

local function reader(cfg)
    checkt_xc(cfg, 'table', 'config')
    -- verify error flag
    cfg.error = cfg.error or true
    checkt_xc(cfg.error, 'boolean', 'error')
    -- verifying directory configuration
    local xlog_dir, snap_dir = nil, nil
    if type(cfg.dir) == 'table' then
        xlog_dir, snap_dir = cfg.dir.xlog, cfg.dir.snap
        if not xlog_dir or not snap_dir then
            error('"config" must have "snap_dir" and "xlog_dir"')
        end
    elseif type(cfg.dir) == 'string' then
        xlog_dir, snap_dir = cfg.dir, cfg.dir
    else
        error('"dir" must be present and must be table or string')
    end
    -- verifying space configuration
    checkt_xc(cfg.spaces, 'table', 'spaces')
    local space_def = {}
    for k, v in pairs(cfg.spaces) do
        space_def[k] = verify_space_def(k, v)
    end

    -- start work
    local self = setmetatable({
        lsn = 0,
        spaces = space_def,
        throw = cfg.error,
        xlog_dir = xlog_dir,
        snap_dir = snap_dir
    }, {
        __index = reader_mt
    })
    return self
end

return {
    reader = reader
}
