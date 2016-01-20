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

pcall(box.snapshot)

require('console').start()
