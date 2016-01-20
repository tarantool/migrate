local log = require('log')
local migrate = require('migrate')

box.cfg{
--    wal_mode = 'none',
    logger = 'tarantool.log'
}
local s = box.schema.create_space('test_256', { if_not_exists = true })
local i = s:create_index('primary', {
    type = 'TREE',
    parts = {1, 'STR'},
    if_not_exists = true
})
s:truncate()

local console = require('console')
console.listen('3302')
-- get migrate object
migration_object = migrate.reader({
    -- or dir = {xlog = '.', snap = '.'}
    -- dir = '.',
    dir = {
        xlog = './xlogs',
        snap = './snaps'
    },
    spaces = {
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

-- load and convert records from all available xlogs/snaps
migration_object:resume()
-- stop master here (downtime is going on)
-- load and convert from remaining xlogs
migration_object:resume()
