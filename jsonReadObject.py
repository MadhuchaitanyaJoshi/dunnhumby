import json
class dbProperties:
    def __init__(self,db,tabledetails,error_log):
        self.db = db
        self.tabledetails=tabledetails
        self.error_log = error_log
    def printer(self):
        print(self.db)
        print(self.tabledetails)
        try:
            print(0/0)
        except Exception as e:
            print(self.error_log)
with open("C:\\Users\\mjoshi\\PycharmProjects\\pythonProject\\config.json", "r") as config:
    conf = json.load(config)
ls = []
for db in conf:
    for table in conf[db]:
        print(db,table)
        ls.append(dbProperties(db,conf[db][table],conf[db][table]["error_log"]))
for i in ls:
    i.printer()