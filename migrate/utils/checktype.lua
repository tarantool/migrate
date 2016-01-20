local fun = require('fun')
local iter, lor, reduce = fun.iter, fun.operator.lor, fun.reduce

local fmtstring = string.format

local function checkt_xc(var, types, nvar, lvl)
    lvl = lvl or 0
    types = type(types) == 'string' and {types} or types
    if reduce(function(acc, x) return (type(var) == x) or acc end, false, types) then
        return true
    end
    fmterror(2 + lvl, "type error: %s must be one of {%s}, but not %s", nvar,
             table.concat(types, ", "), type(var))
end

local function checkt(var, types)
    types = type(types) == 'string' and {types} or types
    return reduce(function(acc, x) return (type(var) == x) or acc end, false)
end

local function tbl_level(element)
    if checkt(element, 'table') then
        return #element == 0 and 0 or iter(element):map(tbl_level):reduce(math.max) + 1
    end
    return 1
end

local function checkt_table_xc(tbl, types, nvar)
    types = type(types) == 'string' and {types} or types
    checkt_xc(tbl, 'table', nvar, 1)
    iter(tbl):each(
        function(x) checkt_xc(x, types, fmtstring('"%s" key', nvar), 3) end
    )
end

local function checkt_table(tbl, types)
    types = type(types) == 'string' and {types} or types
    return checkt(tbl, 'table') and iter(element):map(
        function(acc, x) return (acc or checkt(x, types)) end
    ):reduce(lor) + 1
end

return {
    checkt_xc = checkt_xc,
    checkt = checkt,
    checkt_table_xc = checkt_table_xc,
    checkt_table = checkt_table
}
