local fun = require('fun')
local log = require('log')
local json = require('json')
local yaml = require('yaml')
local pickle = require('pickle')

require('strict').on()

local elog_mod = require('migrate.utils.elog')
local elog = elog_mod()

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
local checkt_xc = helper.checkt_xc
local checkt_table_xc = helper.checkt_table_xc
local xpcall_tb = require('migrate.utils').xpcall_tb

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

    local function get_type(i)
        return fields[i] or default
    end

    return {
        defautls = cfg.default,
        schema = cfg.fields,
        ischema = iter(cfg.index.parts):map(get_type):totable(),

        insert = function (tuple, flags)
            xpcall_tb(elog, sid.replace, sid, tuple)
        end,
        delete = function (key, flags)
            xpcall_tb(elog, sid.delete, sid, key)
        end,
        update = function (key, ops, flags)
            xpcall_tb(elog, sid.update, sid, key, ops)
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
            if file:sub(-4) == 'snap' then
                local stat = {xpcall_tb(elog, xlog.open, file, {
                        spaces = self.spaces,
                        convert = true,
                        throws = true,
                        batch_count = 500,
                        return_value = 'tuple'
                })}
                table.remove(stat, 1)
                local floor = 0
                for _, rv in unpack(stat) do
                    box.begin()
                    if math.floor(_ / 100000) > floor then
                        floor = math.floor(_ / 100000)
                        elog:info('Processed %d tuples', floor * 100000)
                    end
                    for k, v in pairs(rv) do
                        self.spaces[v.space].insert(v.tuple)
                    end
                    box.commit()
                end
                lsn = xdir.xdir_lsn_from_filename(file)
            else
                local stat, val = {xpcall_tb(elog, xlog.open, file, {
                        spaces = self.spaces,
                        convert = true,
                        throws = true,
                        batch_count = 500,
                        return_value = 'tuple'
                })}
                if stat[1] == false then
                    error("failed to open '%s' for reading: %s", file, val)
                end
                for cnt, rv in val do
                    if math.floor(cnt / 100000) > floor then
                        floor = math.floor(cnt / 100000)
                        elog:info('Processed %d tuples', floor * 100000)
                    end
                    box.begin()
                    for k, v in pairs(rv) do
                        if v.op == 'insert' then
                            self.spaces[v.space].insert(v.tuple, v.flags)
                        elseif v.op == 'delete' then
                            self.spaces[v.space].delete(v.key, v.flags)
                        elseif v.op == 'update' then
                            self.spaces[v.space].update(v.key, v.ops, v.flags)
                        end
                    end
                    lsn = v.lsn
                    box.commit()
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
