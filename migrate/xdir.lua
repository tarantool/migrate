local fio = require('fio')
local fun = require('fun')

local chain, iter = fun.chain, fun.iter
local glob, pathjoin, basename = fio.glob, fio.pathjoin, fio.basename

local function filename_from_lsn(lsn, ext)
    ext = ext or 'snap'
    return string.format('%020d.%s', lsn, ext)
end

local function lsn_from_filename(filename)
    return tonumber64(basename(filename):sub(1, -6))
end

-- load all files with given extension (or xlog/snap), 
local function xdir_load(path, ext)
    ext = (type(ext) == 'table' and ext) or (ext and {ext}) or {'*.xlog', '*.snap'}
    local files = chain(
        unpack(
            iter(ext):map(
                function (x) return glob(pathjoin(path, x)) end
            ):totable()
        )
    ):filter(
        function(i) return basename(i):sub(1, -6):match('^%d+$') end
    ):totable()
    table.sort(files)
    return files
end

local function find_xlogs_after_lsn(path, lsn)
    local xlogs = xdir_load(path, '*.xlog')
    local idx = 1
    fun.iter(xlogs):enumerate():each(
        function(k, v)
            idx = lsn_from_filename(v) <= lsn + 1 and k or idx
        end
    )
    return idx == 0 and {} or iter(xlogs):drop_n(idx - 1):totable()
end

local function xdir(snap_path, xlog_path)
    if type(snap_path) ~= 'string' then
        error("Expected 'snap_path' to be string", 2)
    end
    if type(xlog_path) ~= 'string' then
        xlog_path = snap_path
    end
    local snap_last = xdir_load(snap_path, '*.snap')
    snap_last = #snap_last > 0 and snap_last[#snap_last] or nil
    local snap_lsn = snap_last and lsn_from_filename(snap_last) or 1
    local result = find_xlogs_after_lsn(xlog_path, snap_lsn)
    table.insert(result, 1, snap_last)
    return snap_lsn, result
end

return {
    xdir = xdir,
    xdir_load = xdir_load,
    xdir_xlogs_after_lsn = find_xlogs_after_lsn,
    filename_from_lsn = filename_from_lsn,
    lsn_from_filename = lsn_from_filename
}
