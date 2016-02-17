local console = require('console')
console.listen('3302')
local yaml = require('yaml')

box.cfg{
    wal_mode = 'none',
--    logger = 'tarantool.log'
}

local s_0 = box.schema.create_space('test_256', { if_not_exists = true })
local i_0 = s_0:create_index('primary', {
    type = 'TREE',
    parts = {1, 'STR'},
    if_not_exists = true
})
s_0:truncate()

local s_2 = box.schema.create_space('test_257', { if_not_exists = true })
local i_2 = s_2:create_index('primary', {
    type = 'TREE',
    parts = {1, 'NUM'},
    if_not_exists = true
})
s_2:truncate()

local migrate = require('migrate')

local val = migrate.reader({
    -- or dir = {xlog = '.', snap = '.'}
    -- dir = '.',
    dir = 'insert_test',
    spaces = {
        [0] = {
            new_id = s_0.name,
            index = {
                new_id = i_0.name,
                parts = {1}
            },
            fields = {'str', 'num', 'num'},
            default = 'str',
        },
        [2] = {
            new_id = s_2.name,
            index = {
                new_id = i_2.name,
                parts = {1}
            },
            fields = {'num', 'str', 'num'},
            default = 'str',
        },
    }
})

val:resume(val)
val:resume(val)

-- require('console').start()
os.exit(0)
