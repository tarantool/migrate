-- for k, v in require('xlog').open('../test/connector.xlog'):pairs() do
for k, v in require('xlog').open('../test/00000000000014000000.xlog'):pairs() do
    print(k)
    print(require('json').encode(v))
end
