local log = require('log')
local utils = require('migrate.utils')

box.cfg{
    wal_mode = 'none',
--    logger = 'tarantool.log'
    logger_nonblock = false
}

require('console').listen(3302)

local s = box.schema.create_space('test_256', { if_not_exists = true })
local i = s:create_index('primary', {
    type = 'TREE',
    parts = {1, 'NUM'},
    if_not_exists = true
})
s:truncate()

local migrate = require('migrate')

local val = migrate.reader({
    dir = {
        snap = './old/snap_1',
        xlog = './old/xlog_1'
    },
    spaces = {
        [0] = {
            new_id = s.name,
            index = {
                new_id = i.name,
                parts = {1}
            },
            fields = {
                'num', 'str'
            },
            default = 'str'
        },
    }
})

local migration_object = val

-- load and convert records from all available xlogs/snaps
val:resume()
val:resume()
-- stop master here (downtime is going on)
-- load and convert from remaining xlogs
box.snapshot()
os.exit(0)
