#!/usr/bin/env tarantool

local fun = require('fun')
local tap = require('tap')

local test = tap.test("old connector-c test suite")
test:plan(0)

os.exit(test:check() == true and 0 or -1)
