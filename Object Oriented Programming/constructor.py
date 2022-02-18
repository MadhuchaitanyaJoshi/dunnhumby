class tennis:
    def __init__(self,name,id):
        self.name = name+"123"
        self.id = id+50
        id = id-2
        print(self.name,self.id,name,id,self.id)
        return

a = tennis("madhu",100)
print(a.name,a.id)