# migrate - a [Tarantool][] rock for a migration from Tarantool 1.5 to 1.6

## Getting Started

### Prerequisites

Tarantool 1.6.5+ with heder files

	* tarantool && tarantool-dev packages on Ubuntu/Debian
	* tarantool && tarantool-devel package on Fedora/Centos
	* tarantool package on Mac OS X if you're using Homebrew

If building with LuaRocks:

	* libsmall && libsmall-dev/devel
	* msgpuck-dev/devel

### Installation

Clone repository and the build it using CMake:

``` bash
git clone https://github.com/bigbes/migrate.git
cd http && cmake . -DCMAKE_BUILD_TYPE=RelWithDebInfo
make
make install
```

You can also use LuaRocks:

``` bash
luarocks install https://raw.githubusercontent.com/bigbes/migrate/master/migrate-scm-1.rockspec
```

See [tarantool/rocks][TarantoolRocks] for LuaRocks configuration details.

### Tests

Run tests using ctest:

```
$ ctest
Test project <path>/xlog
    Start 1: snap_test
1/3 Test #1: snap_test ........................   Passed    0.05 sec
    Start 2: xlog_test
2/3 Test #2: xlog_test ........................   Passed    0.09 sec
    Start 3: xdir_test
3/3 Test #3: xdir_test ........................   Passed    0.02 sec

100% tests passed, 0 tests failed out of 3

Total Test time (real) =   0.15 sec
```

### Usage

``` lua
	box.cfg{
		wal_mode = 'none',
		logger = 'tarantool.log'
	}

	local s = box.schema.create_space('test_256', { if_not_exists = true })
	local i = s:create_index('primary', {
		type = 'TREE',
		parts = {1, 'STR'},
		if_not_exists = true
	})
	s:truncate()

	local migrate = require('migrate')

	local toolkit = migrate.reader({
		dir = {
			xlog = 'xlog' -- path to dir with xlogs
			snap = 'snap' -- path to dir with snapshots
		},
		spaces = {
			-- space id to look in snapshots/xlogs
			[0] = {
				new_id = s.name,
				index = {
					new_id = i.name,
					parts = {1}
				},
				fields = {'str', 'num', 'num'},
				default = 'str'
			}
		}
	})

	-- Load all available tuples/rows from snapshots/xlogs
	toolkit:resume()

	-- Do something to make service to know, that he works with new Tarantool
	-- ...

	-- Load the rest of rows from xlogs
	toolkit:resume()

	-- box.snapshot() -- save snapshot

	-- Now your Tarantool is ready to go!
```

## API for reader

``` lua
local migrate = require('migrate')
```

### \<table\> reader_object = migrate.reader(*cfg*)

Create migration object. This function returns new migration object

Configuration is a table object consists of:

* `dir = {xlog = 'xlog_dir', snap = 'snap_dir'}` or `dir = 'xlog_snap_dir'`.
	This field is required.
* `spaces` - table with space defintion, described later.
	This field is required.
* `throw` - boolean value to indicate ignore converting errors or not.
	It can happen, if you said that field must be number, but it's size is not or
	4 nor 8 OR if you can't insert/delete/update. Error messages are logged.
	`true` by default.
* `commit` - use `box.begin()`/`box.commit()` before and after batch processing.
	`true` by default.
* `batch_count` - count of tuples/rows to read from disk, parse and process at a
	time. `500` by default.
* `return_type` - type of objects to convert tuples to. May be one of `'table'`/`'tuple'`.
	`'tuple'` is the default value. Use `'table'` if you want to modify tuples and
	then push it into Tarantool (this way is slower).

`spaces` is a table that associates old space number and table with definitions:

* `new_id` - string or number with space `name`/`number`
* `index` - index definition table
	* `new_id` - new primary index name/number (`string` or `number`)
	* `parts` - table of positions of keys in primary index
* `fields` - table of key types ({'str'/'num', ..})
* `default` - default type of field (if it's not defined in fields) ('str'/'num')
* `insert` - custom insert function. `function(tuple, flags)`. If not defined,
	then function that inserts `tuple` into space `new_id`.
* `delete` - custom delete function. `function(key, flags)`. If not defined,
	then function thar deletes tuple with key `key` from space `new_id`
* `update` - custom update function. `function(key, ops, flags`. If not defined,
	then functiin that executes `update` in space `new_id` with operations `ops`
	on tuple with PK `key`.

### \<number\> processed = reader_object:resume()

Resume loading of xlogs/snapshots. It uses similar mechanism as Tarantool 1.5 to
find all snapshot and xlogs necessary to load (last snapshot and all xlogs, that
contain rows with LSN > lsn of snapshot)

* When you run this method first time - it loads snap + xlogs.
* When you run this method next times - it loads only xlogs with LSN > last
	processed lsn.

## API for xlog/snapshot reader

``` lua
local xlog = require('migrate.xlog')
```

### \<iterator\> it = xlog.open(*cfg*)

TODO: Document this

## API for xdir

``` lua
local xdir = require('xdir')
```

### \<number\> lsn, \<table\> files = xdir.xdir(*snap_path*[, *xlog_path*])

TODO: Document this

### \<table\> files = xdir.xdir_load(*ext*)

TODO: Document this

### \<table\> files = xdir.xdir_xlogs_after_lsn(*lsn*)

TODO: Document this

### \<string\> filename = xdir.filename_from_lsn(*lsn*[, *extension*])

TODO: Document this

### \<number\> lsn = xdir.filename_from_lsn(*filename*)

TODO: Document this

## See Also

* [Tarantool][]
* [Documentation][]
* [Tests][]

[Tarantool]: http://github.com/tarantool/tarantool
[Tests]: https://github.com/bigbes/migrate/tree/master/test
