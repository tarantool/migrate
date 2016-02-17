local log = require('log')
local utils = require('migrate.utils')

box.cfg{
    logger = 'tarantool.log'
}

local elog_mod = require('migrate.utils.elog')
local elog = elog_mod({
    path = './migrate.log',
    level = 20
})

local s = box.schema.create_space('test_256', { if_not_exists = true })
local i = s:create_index('primary', {
    type = 'TREE',
    parts = {1, 'STR'},
    if_not_exists = true
})
s:truncate()

local migrate = require('migrate')

local console = require('console')
console.listen('3302')
local val = migrate.reader({
    -- or dir = {xlog = '.', snap = '.'}
    -- dir = '.',
    dir = {
        xlog = 'xlog',
        snap = 'snap'
    },
    spaces = {

--         [0] = {
--             new_id = s.name,
--             index = {
--                 new_id = i.name,
--                 parts = {1}
--             },
--             fields = {
--                 'num', 'str'
--             },
--             default = 'str'
--         },

        -- simple
        [1] = {
            new_id = s.name,
            index = {
                new_id = i.name,
                parts = {1}
            },
            fields = {
                'str', 'num', 'num'
            },
            default = 'str'
        },

--         -- original SID
--         [1] = { -- harder, with callbacks
--             -- callback for insert request/snap tuples
--             'insert' = function(tuple, flags)
--             end,
--             -- callback for delete request
--             'delete' = function(key, flags)
--             end,
--             -- callback for update request
--             'update' = function(key, ops, flags)
--             end,
--         },
--         -- ...
    }
})

local migration_object = val

-- load and convert records from all available xlogs/snaps
val:resume(val)
val:resume(val)
-- stop master here (downtime is going on)
-- load and convert from remaining xlogs
