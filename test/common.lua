local fun = require('fun')
local pickle = require('pickle')

-- Decode field
-- If convert == true, then do nothing
local function field_decode(field, t, convert)
    assert(t == 'str' or t == 'STR' or t == 'num' or t == 'NUM')
    assert(type(convert) == 'boolean')
    if (t == 'num' or t == 'NUM') and not convert then
        if #field == 4 then
            return pickle.unpack('i', field)
        elseif #field == 8 then
            return pickle.unpack('l', field)
        end
    end
    return field
end

-- Decode tuple, that's stored in table
-- If convert == true, then do nothing
local function tuple_decode(t, tt, def, convert)
    def = def or 'str'
    convert = convert or false
    return fun.iter(t):enumerate():map(
        function(k, v) return field_decode(v, tt[k] or def, convert) end
    ):totable()
end

local function tuple_cmp(got, expected, tt, def, convert)
    def = def or 'str'
    convert = convert or false
    return got:pairs():enumerate():map(
        function(k, v)
            return field_decode(v, tt[k] or def, convert) == expected[k]
        end
    ):reduce(fun.operator.land, true)
end

return {
    field_decode = field_decode,
    tuple_decode = tuple_decode,
    tuple_cmp = tuple_cmp
}
