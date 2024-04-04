import jsonpickle
dic = {"lund" : 4, "lund2" : 5}
s = jsonpickle.encode(dic)
data = jsonpickle.decode(s)
print(data)
print(s)