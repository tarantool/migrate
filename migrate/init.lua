local fun = require('fun')
local log = require('log')
local json = require('json')
local yaml = require('yaml')
local pickle = require('pickle')

require('strict').on()

-- local elog_mod = require('migrate.utils.elog')
-- local elog = elog_mod()

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
            local stat, err = pcall(sid.replace, sid, tuple)
            if not stat then
                log.error("Error while replacing: %s", err)
                log.error("Tuple was: %s", yaml.encode(tuple))
                error(2, "Error while replacing: " .. err)
            end
        end,
        delete = function (key, flags)
            local stat, err = pcall(sid.delete, sid, key)
            if not stat then
                log.error("Error while deleting: %s", err)
                log.error("Key was: %s", yaml.encode(tuple))
                error(2, "Error while deleting: " .. err)
            end
        end,
        update = function (key, ops, flags)
            local stat, err = pcall(sid.update, sid, key, ops)
            if not stat then
                log.error("Error while updating: %s", err)
                log.error("Key was: %s", yaml.encode(key))
                log.error("Ops were: %s", yaml.encode(ops))
                error(2, "Error while updating: " .. err)
            end
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
        local files = nil
        if self.lsn == 0 then
            _, files = xdir.xdir(self.snap_dir, self.xlog_dir)
        else
            files = xdir.xdir_xlogs_after_lsn(self.xlog_dir, self.lsn)
        end
        local lsn = self.lsn
        for _, file in pairs(files) do
            local processed, floor = 0, 0
            log.info("opening '%s'", file)
            if file:sub(-4) == 'snap' then
                --log.info(yaml.encode(self.spaces))
                for _, rv in xlog.open(file, {
                        spaces = self.spaces,
                        convert = true,
                        throw = true,
                        batch_count = 500,
                        return_type = 'tuple'
                }) do
                    box.begin()
                    for k, v in pairs(rv) do
                        -- log.info("process tuple")
                        self.spaces[v.space].insert(v.tuple)
                    end
                    box.commit()
                    processed = processed + #rv
                    if math.floor(processed / 100000) > floor then
                        floor = math.floor(processed / 100000)
                        log.info("Processed %.1fM tuples", floor/10)
                    end
                end
                lsn = xdir.lsn_from_filename(file)
            else
                local floor = 0
                log.info("Starting from LSN " .. tostring(lsn + 1))
                --log.info(yaml.encode(self.spaces))
                for _, rv in xlog.open(file, {
                        spaces = self.spaces,
                        convert = true,
                        throw = true,
                        batch_count = 500,
                        return_type = 'tuple',
                        lsn_from = lsn + 1
                }) do
                    box.begin()
                    for k, v in pairs(rv) do
                        --log.info("process row")
                        if v.op == 'insert' then
                            self.spaces[v.space].insert(v.tuple, v.flags)
                        elseif v.op == 'delete' then
                            self.spaces[v.space].delete(v.key, v.flags)
                        elseif v.op == 'update' then
                            self.spaces[v.space].update(v.key, v.ops, v.flags)
                        end
                        lsn = v.lsn
                    end
                    box.commit()
                    processed = processed + #rv
                    if math.floor(processed / 100000) > floor then
                        floor = math.floor(processed / 100000)
                        log.info("Processed %.1fM row", floor/10)
                    end
                end
            end
            self.lsn = lsn
        end
    end
}

local function reader(cfg)
    checkt_xc(cfg, 'table', 'config')
    -- verify error flag
    cfg.throw = cfg.throw or true
    checkt_xc(cfg.throw, 'boolean', 'error')
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
        throw = cfg.throw,
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
