for k, v in require('xlog').open('../test/connector.snap'):pairs() do
  print(k)
  print(require('yaml').encode(v))
end
