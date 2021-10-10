import json
with open("C:/Users/mjoshi/PycharmProjects/pythonProject/config.json","r") as config:
    j = json.load(config)
    print(j["retail"]["cust"])