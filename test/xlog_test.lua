#!/usr/bin/env tarantool

local fun  = require('fun')
local fio  = require('fio')
local tap  = require('tap')
local yaml = require('yaml')

local xdir = require('migrate.xdir')
local xlog = require('migrate.xlog')

local common = require('common')

local space_schema = {
    [0] = {
        space_no = 0,
        schema = {'str', 'num', 'num'},
        ischema = {'str'},
        default = 'str'
    },
    [1] = {
        space_no = 1,
        schema = {'num', 'str', 'num', 'num'},
        ischema = {'num'},
        default = 'str'
    },
    [2] = {
        space_no = 2,
        schema = {'num', 'str', 'num'},
        ischema = {'num'},
        default = 'str'
    }
}

local xlog_list = {
    [25] = {
        lsn = {25, 49},
        name = '00000000000000000025.xlog',
        [25] = {
            op = 'insert',
            space = 2,
            tuple = {4, '\x40\x64\x61\x5A\x23\x6E\x37\x73\x6E\x54\x73\x50\x6E\x51\x4C\x74\x35\x31\x69\x68', 24}
        }, [26] = {
            op = 'insert',
            space = 2,
            tuple = {5, '\x62\x2D\x2B\x48\x79\x79\x52\x72\x62\x53\x5B\x76\x46\x39\x21\x26\x2D\x31\x5D\x2B', 20}
        }, [27] = {
            op = 'insert',
            space = 2,
            tuple = {6, '\x36\x67\x4E\x72\x38\x25\x63\x4A\x2A\x68\x6F\x72\x5E\x6A\x23\x52\x6B\x66\x26\x6D', 21}
        }, [28] = {
            op = 'insert',
            space = 2,
            tuple = {7, '\x51\x3E\x3C\x24\x5F\x69\x48\x70\x3A\x3B\x48\x4D\x2F\x41\x63\x48\x67\x45\x3C\x43', 22}
        }, [29] = {
            op = 'insert',
            space = 2,
            tuple = {8, '\x6A\x5C\x7A\x4E\x43\x3B\x79\x40\x45\x46\x52\x55\x5B\x51\x2A\x70\x63\x6F\x63\x51', 23}
        }, [30] = {
            op = 'insert',
            space = 2,
            tuple = {9, '\x6D\x76\x2D\x29\x3D\x41\x74\x75\x28\x2E\x46\x62\x41\x69\x42\x74\x3C\x58\x2D\x21', 24}
        }, [31] = {
            op = 'insert',
            space = 2,
            tuple = {10, '\x79\x52\x29\x2E\x61\x50\x55\x69\x3C\x32\x57\x3E\x60\x42\x54\x2C\x40\x50\x2F\x59', 20}
        }, [32] = {
            op = 'insert',
            space = 2,
            tuple = {11, '\x25\x78\x51\x4C\x6D\x39\x62\x30\x47\x30\x5F\x60\x2F\x3B\x65\x6C\x21\x31\x58\x2A', 21}
        }, [33] = {
            op = 'insert',
            space = 0,
            tuple = {'\x4B\x67\x53\x77\x41\x5C\x6A\x62\x4E\x44', 11, 21}
        }, [34] = {
            op = 'insert',
            space = 0,
            tuple = {'\x6A\x6C\x6F\x57\x35\x7A\x6A\x2A\x69\x31', 12, 22}
        }, [35] = {
            op = 'insert',
            space = 1,
            tuple = {11, '\x5A\x30\x4D\x65\x62\x64\x76\x31\x60\x22\x38\x6C\x69\x68\x39\x22\x5E\x6F\x71\x58', 21, 1}
        }, [36] = {
            op = 'insert',
            space = 1,
            tuple = {12, '\x53\x5D\x3B\x56\x38\x4D\x67\x22\x3D\x52\x74\x57\x45\x41\x3A\x29\x3D\x5D\x51\x47', 22, 2}
        }, [37] = {
            op = 'insert',
            space = 1,
            tuple = {13, '\x31\x79\x3E\x43\x71\x32\x21\x31\x71\x30\x31\x5C\x2D\x49\x50\x44\x29\x66\x78\x2A', 23, 3}
        }, [38] = {
            op = 'insert',
            space = 1,
            tuple = {14, '\x5B\x4F\x5D\x44\x2A\x5A\x69\x36\x6F\x66\x6A\x26\x2C\x65\x2F\x37\x66\x72\x70\x28', 24, 4}
        }, [39] = {
            op = 'insert',
            space = 1,
            tuple = {15, '\x6B\x71\x37\x27\x6E\x68\x37\x5D\x61\x2E\x67\x61\x5B\x2C\x38\x45\x33\x3F\x6E\x6F', 20, 5}
        }, [40] = {
            op = 'insert',
            space = 1,
            tuple = {16, '\x3D\x24\x75\x2D\x4E\x67\x40\x43\x2B\x4F\x49\x45\x31\x29\x7A\x5A\x68\x32\x6E\x4A', 21, 6}
        }, [41] = {
            op = 'insert',
            space = 1,
            tuple = {17, '\x55\x6C\x77\x41\x59\x77\x63\x31\x54\x4E\x59\x26\x30\x72\x6B\x44\x44\x41\x33\x4D', 22, 7}
        }, [42] = {
            op = 'insert',
            space = 1,
            tuple = {18, '\x4C\x71\x55\x41\x24\x4E\x43\x58\x5E\x57\x6E\x2F\x50\x34\x23\x2C\x5F\x58\x67\x36', 23, 8}
        }, [43] = {
            op = 'insert',
            space = 1,
            tuple = {19, '\x44\x6E\x30\x70\x47\x5A\x23\x70\x54\x5C\x47\x6E\x29\x3E\x26\x70\x33\x63\x6C\x39', 24, 9}
        }, [44] = {
            op = 'insert',
            space = 1,
            tuple = {20, '\x29\x68\x3C\x3C\x46\x6F\x75\x59\x6A\x2F\x57\x3B\x27\x50\x70\x24\x63\x28\x68\x48', 20, 0}
        }, [45] = {
            op = 'insert',
            space = 1,
            tuple = {21, '\x69\x58\x43\x78\x45\x42\x69\x38\x4E\x50\x2E\x77\x77\x51\x48\x47\x24\x29\x74\x6A', 21, 1}
        }, [46] = {
            op = 'insert',
            space = 1,
            tuple = {22, '\x5A\x4E\x39\x32\x77\x40\x2A\x48\x4D\x25\x32\x33\x21\x78\x3E\x37\x38\x22\x52\x72', 22, 2}
        }, [47] = {
            op = 'insert',
            space = 1,
            tuple = {23, '\x75\x42\x38\x73\x37\x45\x3D\x5A\x49\x35\x40\x33\x3E\x32\x2B\x2B\x50\x34\x3C\x55', 23, 3}
        }, [48] = {
            op = 'insert',
            space = 1,
            tuple = {24, '\x54\x46\x58\x5F\x4A\x39\x45\x58\x47\x58\x2C\x46\x25\x65\x23\x2D\x2A\x62\x59\x4D', 24, 4}
        }, [49] = {
            op = 'insert',
            space = 1,
            tuple = {25, '\x63\x49\x67\x4A\x4B\x36\x2C\x22\x2B\x78\x56\x25\x46\x55\x4A\x6A\x52\x42\x23\x3D', 20, 5}
        },
    },
    [50] = {
        lsn = {50, 74},
        name = '00000000000000000050.xlog',
        [50] = {
            op = 'insert',
            space = 1,
            tuple = {26, '\x76\x23\x72\x5A\x37\x42\x7A\x4D\x5A\x59\x53\x51\x29\x30\x33\x79\x49\x47\x2B\x3B', 21, 6}
        }, [51] = {
            op = 'insert',
            space = 1,
            tuple = {27, '\x73\x38\x5F\x22\x3B\x54\x56\x36\x6C\x7A\x43\x71\x37\x6E\x61\x25\x21\x51\x45\x23', 22, 7}
        }, [52] = {
            op = 'insert',
            space = 1,
            tuple = {28, '\x65\x60\x32\x6C\x36\x29\x25\x6E\x22\x3A\x37\x34\x39\x7A\x26\x29\x77\x21\x3C\x75', 23, 8}
        }, [53] = {
            op = 'insert',
            space = 1,
            tuple = {29, '\x6E\x78\x47\x22\x53\x62\x3C\x59\x6F\x47\x36\x6A\x79\x49\x5B\x72\x49\x22\x75\x21', 24, 9}
        }, [54] = {
            op = 'insert',
            space = 1,
            tuple = {30, '\x41\x25\x2C\x62\x60\x64\x2C\x75\x33\x37\x60\x5E\x68\x61\x57\x23\x5B\x24\x5E\x46', 20, 0}
        }, [55] = {
            op = 'insert',
            space = 2,
            tuple = {12, '\x45\x2F\x3B\x45\x3A\x53\x21\x40\x29\x3B\x5B\x69\x22\x31\x6B\x3C\x53\x38\x50\x67', 22}
        }, [56] = {
            op = 'insert',
            space = 2,
            tuple = {13, '\x4F\x2B\x2A\x52\x27\x75\x47\x3C\x46\x3F\x28\x66\x66\x2F\x2A\x3D\x74\x61\x35\x77', 23}
        }, [57] = {
            op = 'insert',
            space = 2,
            tuple = {14, '\x22\x71\x28\x61\x69\x5A\x3C\x72\x72\x52\x65\x73\x57\x6C\x3B\x52\x25\x65\x40\x25', 24}
        }, [58] = {
            op = 'insert',
            space = 2,
            tuple = {15, '\x28\x6D\x25\x6D\x30\x5D\x2B\x54\x50\x7A\x21\x60\x65\x22\x2D\x38\x4C\x63\x3D\x5A', 20}
        }, [59] = {
            op = 'insert',
            space = 2,
            tuple = {16, '\x53\x51\x60\x36\x36\x71\x5C\x4E\x30\x65\x3F\x28\x26\x41\x3F\x2F\x40\x4C\x71\x74', 21}
        }, [60] = {
            op = 'insert',
            space = 2,
            tuple = {17, '\x31\x3A\x4E\x39\x28\x28\x27\x63\x7A\x3E\x31\x27\x61\x77\x34\x38\x45\x7A\x6E\x72', 22}
        }, [61] = {
            op = 'insert',
            space = 2,
            tuple = {18, '\x4B\x76\x51\x71\x36\x74\x78\x6C\x71\x21\x70\x76\x64\x2A\x49\x2A\x3D\x42\x67\x2E', 23}
        }, [62] = {
            op = 'insert',
            space = 2,
            tuple = {19, '\x48\x62\x23\x35\x2B\x72\x72\x5A\x71\x34\x41\x75\x67\x55\x66\x6A\x55\x5C\x79\x5F', 24}
        }, [63] = {
            op = 'insert',
            space = 2,
            tuple = {20, '\x22\x3D\x58\x49\x44\x52\x31\x42\x71\x22\x52\x37\x78\x40\x7A\x77\x27\x37\x23\x51', 20}
        }, [64] = {
            op = 'insert',
            space = 2,
            tuple = {21, '\x3A\x58\x23\x42\x21\x3B\x31\x77\x3F\x54\x5D\x27\x2A\x69\x79\x6B\x38\x2D\x6B\x33', 21}
        }, [65] = {
            op = 'insert',
            space = 2,
            tuple = {22, '\x7A\x3B\x67\x5E\x6A\x2C\x43\x3B\x75\x2C\x37\x5E\x2A\x5B\x34\x79\x24\x73\x25\x39', 22}
        }, [66] = {
            op = 'insert',
            space = 2,
            tuple = {23, '\x54\x63\x39\x74\x5C\x6C\x5B\x71\x5B\x61\x6F\x2B\x6B\x60\x4D\x35\x4E\x42\x6C\x46', 23}
        }, [67] = {
            op = 'insert',
            space = 2,
            tuple = {24, '\x32\x2D\x3C\x3A\x6D\x4A\x59\x76\x42\x65\x2A\x70\x6B\x75\x33\x4A\x21\x4C\x3E\x72', 24}
        }, [68] = {
            op = 'insert',
            space = 2,
            tuple = {25, '\x41\x7A\x56\x5B\x49\x65\x49\x2F\x6B\x2E\x61\x6B\x76\x67\x56\x3B\x53\x74\x72\x5F', 20}
        }, [69] = {
            op = 'insert',
            space = 2,
            tuple = {26, '\x33\x76\x3B\x4D\x75\x21\x6F\x33\x58\x74\x35\x2F\x3D\x5F\x63\x73\x53\x3E\x61\x5D', 21}
        }, [70] = {
            op = 'insert',
            space = 2,
            tuple = {27, '\x63\x52\x5E\x3B\x48\x4F\x27\x28\x3C\x42\x3A\x3D\x57\x50\x37\x68\x5F\x59\x69\x3B', 22}
        }, [71] = {
            op = 'insert',
            space = 2,
            tuple = {28, '\x71\x61\x5A\x22\x3A\x7A\x4D\x24\x57\x71\x54\x67\x59\x31\x3F\x58\x29\x53\x6D\x22', 23}
        }, [72] = {
            op = 'insert',
            space = 2,
            tuple = {29, '\x48\x67\x43\x5A\x49\x35\x47\x39\x62\x33\x5C\x69\x70\x4F\x21\x39\x66\x38\x72\x4B', 24}
        }, [73] = {
            op = 'insert',
            space = 2,
            tuple = {30, '\x2F\x3D\x6A\x4D\x21\x67\x4B\x33\x4F\x24\x3F\x51\x4E\x53\x45\x23\x3C\x65\x38\x27', 20}
        }, [74] = {
            op = 'insert',
            space = 2,
            tuple = {31, '\x71\x69\x39\x2D\x28\x44\x33\x3B\x34\x63\x57\x25\x52\x30\x60\x30\x42\x36\x44\x3E', 21}
        }
    },
    [75] = {
        lsn = {75, 99},
        name = '00000000000000000075.xlog',
        [75] = {
            op = 'insert',
            space = 2,
            tuple = {32, '\x60\x6F\x6B\x56\x34\x72\x29\x4F\x53\x51\x69\x37\x38\x6E\x5A\x76\x27\x4C\x50\x51', 22}
        }, [76] = {
            op = 'insert',
            space = 2,
            tuple = {33, '\x79\x2B\x24\x52\x35\x49\x30\x5B\x56\x3E\x6A\x2E\x6D\x79\x72\x53\x36\x39\x4E\x51', 23}
        }, [77] = {
            op = 'insert',
            space = 2,
            tuple = {34, '\x60\x63\x29\x6F\x40\x48\x27\x56\x4E\x6F\x70\x4F\x62\x2C\x34\x4C\x39\x64\x2C\x50', 24}
        }, [78] = {
            op = 'insert',
            space = 2,
            tuple = {35, '\x58\x42\x54\x5B\x42\x2A\x50\x74\x4A\x6F\x60\x70\x6F\x3B\x49\x4B\x34\x61\x22\x37', 20}
        }, [79] = {
            op = 'insert',
            space = 2,
            tuple = {36, '\x30\x2F\x26\x4D\x67\x4D\x67\x26\x64\x2E\x6D\x6E\x25\x49\x58\x7A\x44\x60\x25\x37', 21}
        }, [80] = {
            op = 'insert',
            space = 2,
            tuple = {37, '\x45\x43\x5C\x23\x5F\x48\x69\x44\x71\x79\x5D\x51\x32\x6A\x3E\x6B\x55\x2A\x3E\x26', 22}
        }, [81] = {
            op = 'insert',
            space = 2,
            tuple = {38, '\x5D\x2A\x5D\x5A\x6A\x7A\x6E\x6C\x2B\x32\x5E\x31\x2A\x3D\x59\x32\x3F\x38\x3F\x6A', 23}
        }, [82] = {
            op = 'insert',
            space = 2,
            tuple = {39, '\x58\x29\x66\x38\x33\x56\x65\x43\x63\x28\x28\x6A\x55\x41\x46\x63\x56\x35\x3A\x4E', 24}
        }, [83] = {
            op = 'insert',
            space = 2,
            tuple = {40, '\x33\x72\x35\x71\x62\x51\x75\x5E\x51\x23\x3B\x31\x54\x32\x68\x2D\x7A\x74\x79\x4B', 20}
        }, [84] = {
            op = 'insert',
            space = 2,
            tuple = {41, '\x46\x35\x6F\x2B\x24\x39\x33\x6D\x7A\x5E\x39\x3B\x39\x26\x51\x50\x65\x5F\x31\x3D', 21}
        }, [85] = {
            op = 'insert',
            space = 2,
            tuple = {42, '\x49\x4E\x67\x29\x40\x2B\x3D\x4B\x3A\x64\x51\x33\x48\x5E\x42\x4A\x5A\x23\x41\x60', 22}
        }, [86] = {
            op = 'insert',
            space = 2,
            tuple = {43, '\x2C\x6A\x65\x6B\x2C\x7A\x71\x35\x6D\x78\x38\x4A\x25\x43\x77\x5A\x79\x4B\x70\x6B', 23}
        }, [87] = {
            op = 'insert',
            space = 2,
            tuple = {44, '\x37\x4D\x4B\x3A\x4B\x5F\x64\x37\x2E\x72\x5C\x79\x47\x57\x59\x23\x3D\x67\x79\x31', 24}
        }, [88] = {
            op = 'insert',
            space = 2,
            tuple = {45, '\x40\x59\x61\x39\x23\x4B\x5B\x2D\x4A\x62\x46\x72\x75\x45\x24\x45\x53\x69\x35\x5D', 20}
        }, [89] = {
            op = 'insert',
            space = 2,
            tuple = {46, '\x4B\x43\x2C\x22\x44\x2F\x2A\x38\x54\x24\x53\x49\x4D\x30\x24\x3A\x29\x25\x57\x65', 21}
        }, [90] = {
            op = 'insert',
            space = 2,
            tuple = {47, '\x3B\x3B\x44\x4F\x35\x52\x57\x30\x4B\x44\x41\x27\x3E\x4F\x74\x64\x78\x59\x58\x39', 22}
        }, [91] = {
            op = 'insert',
            space = 2,
            tuple = {48, '\x53\x6D\x68\x2A\x3A\x44\x58\x60\x5B\x2A\x2F\x5A\x58\x33\x64\x75\x36\x48\x6C\x34', 23}
        }, [92] = {
            op = 'insert',
            space = 2,
            tuple = {49, '\x5E\x25\x5D\x36\x70\x72\x72\x22\x4C\x74\x40\x55\x29\x59\x2E\x24\x45\x40\x2D\x27', 24}
        }, [93] = {
            op = 'insert',
            space = 2,
            tuple = {50, '\x5D\x6F\x5B\x51\x22\x28\x4B\x5A\x6A\x64\x6B\x68\x3E\x79\x37\x74\x38\x5A\x61\x26', 20}
        }, [94] = {
            op = 'insert',
            space = 2,
            tuple = {51, '\x58\x36\x26\x78\x76\x44\x42\x63\x2D\x51\x21\x60\x70\x63\x28\x31\x3C\x2B\x28\x25', 21}
        }, [95] = {
            op = 'insert',
            space = 2,
            tuple = {52, '\x72\x5F\x49\x76\x2C\x3B\x6A\x56\x3C\x7A\x4D\x6E\x6F\x3C\x26\x30\x30\x46\x57\x76', 22}
        }, [96] = {
            op = 'insert',
            space = 2,
            tuple = {53, '\x2B\x23\x48\x58\x54\x4E\x28\x5E\x5B\x3F\x24\x41\x2F\x3E\x54\x4B\x67\x5D\x3F\x47', 23}
        }, [97] = {
            op = 'insert',
            space = 2,
            tuple = {54, '\x4D\x60\x29\x70\x3B\x71\x39\x76\x45\x68\x5A\x61\x49\x47\x55\x3D\x2E\x5B\x63\x63', 24}
        }, [98] = {
            op = 'insert',
            space = 2,
            tuple = {55, '\x46\x70\x21\x55\x43\x31\x6E\x39\x50\x54\x4A\x4F\x42\x52\x2C\x46\x59\x76\x28\x46', 20}
        }, [99] = {
            op = 'insert',
            space = 2,
            tuple = {56, '\x41\x3D\x56\x67\x66\x37\x75\x45\x66\x47\x56\x79\x3B\x50\x47\x31\x62\x4A\x2F\x2D', 21}
        }
    },
    [100] = {
        lsn = {100, 114},
        name = '00000000000000000100.xlog',
        [100] = {
            op = 'insert',
            space = 0,
            tuple = {'\x79\x3E\x72\x62\x44\x73\x30\x33\x79\x62', 13, 23}
        }, [101] = {
            op = 'insert',
            space = 0,
            tuple = {'\x23\x25\x55\x2C\x5A\x6A\x2E\x62\x26\x78', 14, 24}
        }, [102] = {
            op = 'insert',
            space = 0,
            tuple = {'\x48\x38\x52\x5C\x55\x77\x62\x6D\x3A\x4F', 15, 20}
        }, [103] = {
            op = 'insert',
            space = 0,
            tuple = {'\x47\x67\x4D\x3E\x5C\x41\x24\x75\x27\x35', 16, 21}
        }, [104] = {
            op = 'insert',
            space = 0,
            tuple = {'\x2B\x31\x29\x53\x31\x78\x77\x4D\x4C\x2A', 17, 22}
        }, [105] = {
            op = 'insert',
            space = 0,
            tuple = {'\x35\x30\x56\x34\x25\x4E\x5C\x3E\x71\x59', 18, 23}
        }, [106] = {
            op = 'insert',
            space = 0,
            tuple = {'\x3A\x6D\x5B\x26\x57\x78\x44\x42\x5A\x57', 19, 24}
        }, [107] = {
            op = 'insert',
            space = 0,
            tuple = {'\x3F\x2F\x52\x32\x25\x2E\x53\x2A\x74\x2D', 20, 20}
        }, [108] = {
            op = 'insert',
            space = 0,
            tuple = {'\x6A\x6B\x61\x39\x2B\x2B\x46\x62\x50\x7A', 21, 21}
        }, [109] = {
            op = 'insert',
            space = 0,
            tuple = {'\x6A\x59\x2F\x26\x46\x25\x5F\x79\x4D\x25', 22, 22}
        }, [110] = {
            op = 'insert',
            space = 0,
            tuple = {'\x26\x70\x47\x21\x49\x42\x26\x30\x21\x73', 23, 23}
        }, [111] = {
            op = 'insert',
            space = 0,
            tuple = {'\x40\x24\x29\x24\x44\x5F\x3D\x65\x45\x62', 24, 24}
        }, [112] = {
            op = 'insert',
            space = 0,
            tuple = {'\x46\x60\x4E\x31\x71\x21\x48\x4D\x39\x50', 25, 20}
        }, [113] = {
            op = 'insert',
            space = 0,
            tuple = {'\x7A\x50\x52\x70\x34\x24\x76\x28\x24\x34', 26, 21}
        }, [114] = {
            op = 'insert',
            space = 0,
            tuple = {'\x45\x6D\x4A\x68\x76\x68\x29\x50\x43\x47', 27, 22}
        }
    }
}

box.cfg{
    wal_mode = 'none',
    logger_nonblock = false
}

local test = tap.test("xlog reader/converter")

function table_sort_key(t)
    local temp = {}
    for k, v in pairs(t) do table.insert(temp, k) end
    table.sort(temp)
    return fun.iter(temp):map(function(k) return k, t[k] end)
end

function xlog_filter(xlog_table, spaces, lsn_from, lsn_to)
    spaces = fun.iter(spaces):map(
        function(v) return v.space_no end
    ):totable()
    xlog_table = fun.iter(xlog_table):filter(
        function(k, v)
            if type(k) ~= 'number' or (k < lsn_from or k > lsn_to) then
                return false
            end
            for _, sno in pairs(spaces) do
                if sno == v.space then
                    return true
                end
            end
            return false
        end
    ):tomap()
    return xlog_table
end

local function verify_insert_op(test, log, spacedef, row, rtype, convert)
    test:is(row.op, "insert", "check that operation is insert")
    local v = space_schema[row.space]
    if rtype == 'tuple' then
        test:iscdata(row.tuple, "const box_tuple_t&", "tuple is cdata")
        if not convert then
            test:ok(
                row.tuple:pairs():map(
                    function(fld) return (type(fld) == 'string') end
                ):reduce(fun.operator.land, true),
                "Check tuple insert lsn#" .. tostring(row.lsn) ..
                " isn't converted"
            )
        end
        test:ok(
            common.tuple_cmp(
                row.tuple, log[row.lsn].tuple, v.schema, 'str', convert
            ),
            "Check tuple insert lsn#" .. tostring(row.lsn)
        )
    else
        test:istable(row.tuple, "tuple is table")
        if not convert then
            test:ok(
                fun.iter(row.tuple):map(
                    function(fld) return (type(fld) == 'string') end
                ):reduce(fun.operator.land, true),
                "Check table insert lsn#" .. tostring(row.lsn) ..
                " isn't converted"
            )
        end
        test:is_deeply(
            common.tuple_decode(row.tuple, v.schema, 'str', convert),
            log[row.lsn].tuple,
            "Check table insert lsn#" .. tostring(row.lsn)
        )
    end
end

function xlog_test(test, curlog, spacedefs, bcount, ctype, rtype, cut)
    cur = cur or false
    local def = {}

    local lsn_from = curlog.lsn[1]
    local lsn_to   = curlog.lsn[2]
    if cut then
        lsn_from = lsn_from + 5
        lsn_to = lsn_to + 5
    end

    local expected = xlog_filter(curlog, spacedefs, lsn_from, lsn_to)
    local ecount = 0
    for k, v in pairs(expected) do
        ecount = ecount + 1
    end

    local testnum = math.ceil(ecount / bcount)
    testnum = testnum + ecount * (ctype and 4 or 5) + 1
    test:plan(testnum)

    -- replace with map and totable
    local def = fun.iter(spacedefs):map(function(v)
        if ctype == true then
            return v.space_no, {
                schema = v.schema,
                ischema = v.ischema,
                default = 'str'
            }
        end
        return v.space_no, true
    end):tomap()

    local batch_last = false
    local lsn_path = fio.pathjoin('insert_test', curlog.name)
    for k, batch in xlog.open(lsn_path, {
        spaces = def,
        convert = ctype,
        return_type = rtype,
        batch_count = bcount,
        throws = true,
        lsn_from = lsn_from,
        lsn_to = lsn_to
    }) do
        test:ok(#batch <= bcount and not batch_last, "Check batch length")

        for _, t in pairs(batch) do
            if #batch < bcount then
                batch_last = true
            end
            if t.op == 'insert' then
                verify_insert_op(test, curlog, def[t.space], t, rtype, ctype)
            elseif t.op == 'delete' then
                verify_delete_op(test, curlog, def[t.space], t, rtype, ctype)
            elseif t.op == 'update' then
                verify_update_op(test, curlog, def[t.space], t, rtype, ctype)
            else
                error('la la la')
            end
            test:isnt(expected[t.lsn], nil, "expected row with LSN " ..
                      tostring(t.lsn))
            expected[t.lsn] = nil
        end
    end
    local endlen = 0
    for k, v in pairs(expected) do
        endlen = endlen + 1
    end
    test:is(endlen, 0, "read all OPS that we want")
end

local xcount = 0
for k, v in pairs(xlog_list) do
    xcount = xcount + 1
end

test:plan(xcount * 2 * 2 * 2 * 5)

local function construct_name(xlog_name, spaces, bcount, convert, return_type, cut)
    local spacenos = {}
    for _, v in pairs(spaces) do table.insert(spacenos, v.space_no) end
    return string.format("xlog '%s', batch %d, space %s, '%s', convert: '%s'," ..
                         " cut: '%s'", xlog_name, bcount,
                         table.concat(spacenos, '/'),
                         return_type, tostring(convert), tostring(cut))
end

for lsn, xlog_inst in pairs(xlog_list) do
    for _, rtype in pairs({'table', 'tuple'}) do
        for _, ctype in pairs({false, true}) do
            for _, cut in pairs({false, true}) do
                local s = nil
                s = construct_name(xlog_inst.name, {
                    space_schema[0]
                }, 1, ctype, rtype, cut)
                test:test(s, xlog_test, xlog_inst, {
                    space_schema[0],
                }, 1, ctype, rtype, cut)

                s = construct_name(xlog_inst.name, {
                    space_schema[1]
                }, 3, ctype, rtype, cut)
                test:test(s, xlog_test, xlog_inst, {
                    space_schema[1],
                }, 3, ctype, rtype, cut)

                s = construct_name(xlog_inst.name, {
                    space_schema[0],
                    space_schema[2]
                }, 5, ctype, rtype, cut)
                test:test(s, xlog_test, xlog_inst, {
                    space_schema[0],
                    space_schema[2]
                }, 5, ctype, rtype, cut)

                s = construct_name(xlog_inst.name, {
                    space_schema[0],
                    space_schema[1],
                    space_schema[2]
                }, 10, ctype, rtype, cut)
                test:test(s, xlog_test, xlog_inst, {
                    space_schema[0],
                    space_schema[1],
                    space_schema[2]
                }, 10, ctype, rtype, cut)

                s = construct_name(xlog_inst.name, {
                    space_schema[0],
                    space_schema[1],
                    space_schema[2]
                }, 50, ctype, rtype, cut)
                test:test(s, xlog_test, xlog_inst, {
                    space_schema[0],
                    space_schema[1],
                    space_schema[2]
                }, 50, ctype, rtype, cut)
            end
        end
    end
end

os.exit(test:check() == true and 0 or -1)
