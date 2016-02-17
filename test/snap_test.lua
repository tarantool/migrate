#!/usr/bin/env tarantool

local fun = require('fun')
local tap = require('tap')
local xlog = require('migrate.xlog')

local common = require('common')

local snap_space0 = {
    space_no = 0,
    internal = {
        {'\x23\x6C\x25\x6B\x59\x6B\x31\x37\x73\x3D', 4,  24},
        {'\x28\x40\x73\x23\x5D\x64\x52\x2B\x2D\x23', 5,  20},
        {'\x2B\x25\x78\x60\x30\x78\x39\x40\x4B\x5E', 2,  22},
        {'\x2E\x66\x25\x69\x5F\x2E\x75\x69\x43\x68', 10, 20},
        {'\x30\x25\x35\x2E\x47\x57\x39\x6B\x3A\x33', 9,  24},
        {'\x4D\x5F\x4E\x6D\x2D\x3B\x6F\x65\x5F\x5B', 6,  21},
        {'\x54\x68\x49\x31\x5C\x34\x56\x2D\x6B\x55', 3,  23},
        {'\x68\x5F\x56\x64\x28\x5C\x3F\x63\x47\x54', 1,  21},
        {'\x6C\x21\x25\x66\x45\x77\x79\x43\x3B\x39', 7,  22},
        {'\x70\x77\x56\x65\x26\x41\x5A\x76\x64\x2A', 8,  23},
    },
    schema = {'str', 'num', 'num'}
}

local snap_space1 = {
    space_no = 1,
    internal = {
        {1,  '\'^Ph\'O_R)[\'2i^z]kyAA', 21, 1},
        {2,  'uvjn^[QpE%@")T\\hx?3;',   22, 2},
        {3,  'DdSkP#<;3\'wjg.:9Jh[9',   23, 3},
        {4,  'klBqgSh"Rp;OW>Eoq!7%',    24, 4},
        {5,  'ENu?t-R%;[jrTRuzh5>D',    20, 5},
        {6,  '@:24z#3L&NH3Q#s"_Nu1',    21, 6},
        {7,  'js[=*0!m49$bB\\[F]T$]',   22, 7},
        {8,  '\'_v+S3Vg;+=\\])Ld*:K!',  23, 8},
        {9,  ';CTWR>MF5Iy\'-^Y0vv(H',   24, 9},
        {10, 'J@\\u&uTE1Kj^3HA?2/?@',   20, 0}
    },
    schema = {'num', 'str', 'num', 'num'}
}

local snap_space2 = {
    space_no = 2,
    internal = {
        {1,  '\x76\x44\x4A\x76\x6F\x60\x30\x7A\x50\x61\x3B\x2F\x78\x6A\x57\x64\x38\x2A\x3C\x5F', 21},
        {2,  '\x26\x60\x71\x4E\x23\x4B\x46\x2C\x5C\x7A\x6F\x59\x4F\x53\x66\x5E\x61\x4C\x2D\x48', 22},
        {3,  '\x35\x6F\x6D\x48\x5A\x6B\x33\x35\x6A\x3B\x5C\x7A\x4E\x79\x63\x3D\x60\x74\x52\x62', 23},
        {4,  '\x40\x64\x61\x5A\x23\x6E\x37\x73\x6E\x54\x73\x50\x6E\x51\x4C\x74\x35\x31\x69\x68', 24},
        {5,  '\x62\x2D\x2B\x48\x79\x79\x52\x72\x62\x53\x5B\x76\x46\x39\x21\x26\x2D\x31\x5D\x2B', 20},
        {6,  '\x36\x67\x4E\x72\x38\x25\x63\x4A\x2A\x68\x6F\x72\x5E\x6A\x23\x52\x6B\x66\x26\x6D', 21},
        {7,  '\x51\x3E\x3C\x24\x5F\x69\x48\x70\x3A\x3B\x48\x4D\x2F\x41\x63\x48\x67\x45\x3C\x43', 22},
        {8,  '\x6A\x5C\x7A\x4E\x43\x3B\x79\x40\x45\x46\x52\x55\x5B\x51\x2A\x70\x63\x6F\x63\x51', 23},
        {9,  '\x6D\x76\x2D\x29\x3D\x41\x74\x75\x28\x2E\x46\x62\x41\x69\x42\x74\x3C\x58\x2D\x21', 24},
        {10, '\x79\x52\x29\x2E\x61\x50\x55\x69\x3C\x32\x57\x3E\x60\x42\x54\x2C\x40\x50\x2F\x59', 20},
        {11, '\x25\x78\x51\x4C\x6D\x39\x62\x30\x47\x30\x5F\x60\x2F\x3B\x65\x6C\x21\x31\x58\x2A', 21}
    },
    schema = {'num', 'str', 'num'}
}

local function verify_snap_table(sdef, bcount, return_type, convert)
    bcount = bcount or 1
    return_type = return_type or 'table'

    assert(return_type == 'table' or return_type == 'tuple')
    assert(type(bcount) == 'number' and bcount > 0)
    assert(type(convert) == 'boolean')

    return function(test)
        local tcount, def, cnt = 0, {}, {}

        tcount = fun.iter(sdef):map(
            function(s) return s.internal end
        ):map(fun.operator.len):reduce(fun.operator.add, 0)

        -- Count tests
        local testnum = math.ceil(tcount / bcount) + tcount * 2
        if not convert then
            testnum = testnum + tcount
        end
        test:plan(testnum)

        fun.iter(sdef):each(function(v)
            def[v.space_no] = true
            if convert then
                def[v.space_no] = {
                    schema = v.schema,
                    default = 'str'
                }
            end
            cnt[v.space_no] = v
            cnt[v.space_no].cnt = 1
        end)

        local batch_last = false
        for k, batch in xlog.open("insert_test/00000000000000000032.snap", {
            spaces = def,
            convert = convert,
            return_type = return_type,
            batch_count = bcount,
            throws = true
        }) do
            test:ok(#batch <= bcount and not batch_last, "Check batch length")
            if #batch < bcount then
                batch_last = true
            end
            for k1, t in pairs(batch) do
                local v = cnt[t.space]
                if return_type == 'tuple' then
                    test:iscdata(t.tuple, "const box_tuple_t&", "tuple is cdata")
                    if not convert then
                        test:ok(
                            t.tuple:pairs():map(
                                function(fld)
                                    return (type(fld) == 'string')
                                end
                            ):reduce(fun.operator.land, true),
                            "Check tuple #" .. tostring(bcount * k + k1) ..
                            " isn't converted"
                        )
                    end
                    test:ok(
                        common.tuple_cmp(
                            t.tuple, v.internal[v.cnt],
                            v.schema, 'str', convert
                        ),
                        "Check tuple #" .. tostring(bcount * k + k1)
                    )
                else
                    test:istable(t.tuple, "tuple is table")
                    if not convert then
                        test:ok(
                            fun.iter(t.tuple):map(
                                function(fld)
                                    return (type(fld) == 'string')
                                end
                            ):reduce(fun.operator.land, true),
                            "Check table #" .. tostring(bcount * k + k1) ..
                            " isn't converted"
                        )
                    end
                    test:is_deeply(
                        common.tuple_decode(t.tuple, v.schema, 'str', convert),
                        v.internal[v.cnt],
                        "Check table #" .. tostring(bcount * k + k1)
                    )
                end
                v.cnt = v.cnt + 1
            end
        end
    end
end

box.cfg{
    wal_mode = 'none',
    logger_nonblock = false
}

local test = tap.test("snapshot reader/converter")
test:plan(20)

for _, rtype in pairs({'table', 'tuple'}) do
    for _, ctype in pairs({false, true}) do
        test:test("snapshot, batch 1, space 0, " .. rtype ..
                    ", convert '" .. tostring(ctype) .. "'",
            verify_snap_table({
                snap_space0
            },  1, rtype, ctype)
        )

        test:test("snapshot, batch 3, space 1, " .. rtype ..
                    ", convert '" .. tostring(ctype) .. "'",
            verify_snap_table({
                snap_space1
            },  3, rtype, ctype)
        )

        test:test("snapshot, batch 5, space 0/2, " .. rtype ..
                    ", convert '" .. tostring(ctype) .. "'",
            verify_snap_table({
                snap_space0,
                snap_space2
            },  5, rtype, ctype)
        )

        test:test("snapshot, batch 10, space 0/1/2, " .. rtype ..
                    ", convert '" .. tostring(ctype) .. "'",
            verify_snap_table({
                snap_space0,
                snap_space1,
                snap_space2
            }, 10, rtype, ctype)
        )
        test:test("snapshot, batch 50, space 0/1/2, " .. rtype ..
                    ", convert '" .. tostring(ctype) .. "'",
            verify_snap_table({
                snap_space0,
                snap_space1,
                snap_space2
            }, 50, rtype, ctype)
        )
    end
end

os.exit(test:check() == true and 0 or -1)
