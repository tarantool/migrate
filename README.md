<a href="http://tarantool.org">
  <img src="https://avatars2.githubusercontent.com/u/2344919?v=2&s=250" align="right">
</a>

# migrate - a [Tarantool][] rock for migration from Tarantool 1.5 to 1.6, 1.7, 1.8

## Getting Started

### Prerequisites

Tarantool 1.6.5+ with header files:

	* tarantool && tarantool-dev packages on Ubuntu/Debian
	* tarantool && tarantool-devel package on Fedora/Centos
	* tarantool package on Mac OS X if you are using Homebrew

If building with LuaRocks:

	* libsmall && libsmall-dev/devel
	* msgpuck-dev/devel

### Installation

Clone the repository and build it using CMake:

``` bash
git clone https://github.com/tarantool/migrate.git
cd http && cmake . -DCMAKE_BUILD_TYPE=RelWithDebInfo
make
make install
```

You can also use LuaRocks:

``` bash
luarocks install https://raw.githubusercontent.com/tarantool/migrate/master/migrate-scm-1.rockspec
```

See [tarantool/rocks][TarantoolRocks] for LuaRocks configuration details.

### Tests

Run tests using `ctest`:

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

	-- Do something to let the service know it works with the new Tarantool
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

Create a migration object. This function returns the new migration object.

Configuration is a table object that consists of:

* `dir = {xlog = 'xlog_dir', snap = 'snap_dir'}` or `dir = 'xlog_snap_dir'`.
	Required field.
* `spaces` - table with space definition, described later.
	Required field.
* `throw` - boolean value that indicates whether to ignore error conversion or not.
	Errors happen if you cannot insert/delete/update or if you said that some 
	field must be a number but its size is not 4 nor 8. Error messages are logged.
	`true` by default.
* `commit` - use `box.begin()`/`box.commit()` before and after batch processing.
	`true` by default.
* `batch_count` - count of tuples/rows to read from disk, parse, and process at a
	time. `500` by default.
* `return_type` - type of objects to convert tuples to. Can be `'table'` or 
    `'tuple'` (default). Use `'table'` if you want to modify tuples and
	then push them into Tarantool (this way is slower).

`spaces` is a table that associates old space number and table with definitions:

* `new_id` - string or number with space `name`/`number`.
* `index` - index definition table that includes:
	* `new_id` - new primary index name/number (`string` or `number`),
	* `parts` - table of key positions in primary index.
* `fields` - table of key types (`{'str'/'num', ..}`).
* `default` - default field type (`'str'`/`'num'`) if it is not defined in fields.
* `insert` - custom insert function `function(tuple, flags)`. If not defined, 
	a function that inserts a `tuple` into a space with `new_id`.
* `delete` - custom delete function `function(key, flags)`. If not defined, 
	a function that deletes a tuple with a `key` from a space with `new_id`.
* `update` - custom update function `function(key, ops, flags`. If not defined,
	a function that executes `update` with operations `ops` in a space with `new_id` 
	on a tuple with PK `key`.

### \<number\> processed = reader_object:resume()

Resume loading of xlogs/snapshots. It uses a mechanism similar to that of Tarantool 
1.5 to find all snapshots and xlogs necessary to load:

* When you run this method for the first time - it loads the last snapshot and xlogs.
* When you run this method next time and subsequently - it loads only the xlogs that
    contain rows with LSN greater than the last processed (the snapshot's one).

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
[Documentation]: http://tarantool.org/doc/
[Tests]: https://github.com/tarantool/migrate/tree/master/test
