import hashlib
hash_object = hashlib.sha1('madhuchaitanya'.encode("utf-8"))
hex_dig = hash_object.hexdigest()
print(hex_dig)
print(int(hex_dig,16))
hash_object2 = hashlib.sha1(str(23).encode("utf-8"))
hex_dig2 = hash_object2.hexdigest()
print(int(hex_dig2,16))
