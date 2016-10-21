#!/usr/bin/env tarantool

local fun = require('fun')
local tap = require('tap')
local yaml = require('yaml')

local test = tap.test("xlog reader/converter")
test:plan(4)

local xdir = require('migrate.xdir')

local xlogs = {
    {
        lsn = 25,
        name = '00000000000000000025.xlog',
        ext = 'xlog'
    }, {
        lsn = 50,
        name = '00000000000000000050.xlog',
        ext = 'xlog'
    }, {
        lsn = 75,
        name = '00000000000000000075.xlog',
        ext = 'xlog'
    }, {
        lsn = 100,
        name = '00000000000000000100.xlog',
        ext = 'xlog'
    }
}

local snaps = {
    {
        lsn = 32,
        name = '00000000000000000032.snap',
        ext = 'snap'
    }
}

local xdir_xdir_yaml = {
[1] = yaml.decode[[
---
- 25
- - mock/test-1/0000000000000000025.snap
  - mock/test-1/0000000000000000025.xlog
  - mock/test-1/0000000000000000050.xlog
  - mock/test-1/0000000000000000075.xlog
  - mock/test-1/0000000000000000100.xlog
...
]],
[2] = yaml.decode[[
---
- 25
- - mock/test-2/0000000000000000025.snap
  - mock/test-2/0000000000000000026.xlog
  - mock/test-2/0000000000000000050.xlog
  - mock/test-2/0000000000000000075.xlog
  - mock/test-2/0000000000000000100.xlog
...
]],
[3] = yaml.decode[[
---
- 25
- - mock/test-3/0000000000000000025.snap
  - mock/test-3/0000000000000000024.xlog
  - mock/test-3/0000000000000000075.xlog
...
]],
[4] = yaml.decode[[
---
- 110
- - mock/test-4/0000000000000000110.snap
  - mock/test-4/0000000000000000100.xlog
...
]],
[5] = yaml.decode[[
---
- 110
- - mock/test-5/0000000000000000110.snap
...
]],
[6] = yaml.decode[[
---
- 110
- - mock/test-6/0000000000000000110.snap
  - mock/test-6/0000000000000000110.xlog
...
]]
}

local xdir_load_snap_yaml = {
    [1] = yaml.decode[[
---
- mock/test-1/0000000000000000025.snap
...
]], [2] = yaml.decode[[
---
- mock/test-2/0000000000000000001.snap
- mock/test-2/0000000000000000025.snap
...
]], [3] = yaml.decode[[
---
- mock/test-3/0000000000000000025.snap
...
]], [4] = yaml.decode[[
---
- mock/test-4/0000000000000000025.snap
- mock/test-4/0000000000000000110.snap
...
]], [5] = yaml.decode[[
---
- mock/test-5/0000000000000000110.snap
...
]], [6] = yaml.decode[[
---
- mock/test-6/0000000000000000110.snap
...
]]
}

local xdir_load_xlog_yaml = {
    [1] = yaml.decode[[
---
- mock/test-1/0000000000000000001.xlog
- mock/test-1/0000000000000000025.xlog
- mock/test-1/0000000000000000050.xlog
- mock/test-1/0000000000000000075.xlog
- mock/test-1/0000000000000000100.xlog
...
]], [2] = yaml.decode[[
---
- mock/test-2/0000000000000000015.xlog
- mock/test-2/0000000000000000026.xlog
- mock/test-2/0000000000000000050.xlog
- mock/test-2/0000000000000000075.xlog
- mock/test-2/0000000000000000100.xlog
...
]], [3] = yaml.decode[[
---
- mock/test-3/0000000000000000001.xlog
- mock/test-3/0000000000000000024.xlog
- mock/test-3/0000000000000000075.xlog
...
]], [4] = yaml.decode[[
---
- mock/test-4/0000000000000000001.xlog
- mock/test-4/0000000000000000024.xlog
- mock/test-4/0000000000000000050.xlog
- mock/test-4/0000000000000000075.xlog
- mock/test-4/0000000000000000100.xlog
...
]], [5] = yaml.decode[[
---
[ ]
...
]], [6] = yaml.decode[[
---
- mock/test-6/0000000000000000110.xlog
...
]]
}

test:test("xlog/snap name to lsn", function(test)
    test:plan(#xlogs + #snaps)
    fun.chain(xlogs, snaps):each(
        function(v)
            test:is(xdir.lsn_from_filename(v.name), v.lsn,
                    "check for " .. tostring(v.lsn) .. " + " .. v.ext)
        end
    )
end)

test:test("lsn to xlog/snap name", function(test)
    test:plan(#xlogs + #snaps)
    fun.chain(xlogs, snaps):each(
        function(v)
            test:is(xdir.filename_from_lsn(v.lsn, v.ext), v.name,
                    "check for " .. tostring(v.lsn) .. " + " .. v.ext)
        end
    )
end)

test:test("xdir_xdir", function(test)
    test:plan(6)
    fun.iter(xdir_xdir_yaml):enumerate():each(
        function(k, v)
            local path = 'mock/test-' .. tostring(k)
            local result = {xdir.xdir(path, path)}
            test:is_deeply(result, v, "check " .. path)
        end
    )
end)

test:test("xdir_load", function(test)
    test:plan(6 * 3)
    fun.iter(xdir_load_snap_yaml):enumerate():each(
        function(k, v)
            local path = 'mock/test-' .. tostring(k)
            local rv = xdir.xdir_load(path, '*.snap')
            test:is_deeply(rv, v, "Check snap loading")
        end
    )
    fun.iter(xdir_load_xlog_yaml):enumerate():each(
        function(k, v)
            local path = 'mock/test-' .. tostring(k)
            local rv = xdir.xdir_load(path, '*.xlog')
            test:is_deeply(rv, v, "Check xlog loading")
        end
    )
    fun.zip(xdir_load_snap_yaml, xdir_load_xlog_yaml):enumerate():each(
        function(k, v1, v2)
            local path = 'mock/test-' .. tostring(k)
            local tbl = fun.chain(v1, v2):totable()
            table.sort(tbl)
            local rv = xdir.xdir_load(path, {'*.xlog', '*.snap'})
            test:is_deeply(rv, tbl, "Check snap/xlog loading")
        end
    )
end)

os.exit(test:check() == true and 0 or -1)
