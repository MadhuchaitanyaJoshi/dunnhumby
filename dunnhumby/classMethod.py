class Animals:
    def __init__(self,type,name):
        self.type = type
        self.name = name
        print(self.name,self.type,type,name)
        myplace = self.findPlace(type,name)
        print(myplace)
    def findPlace(self,type,name):
        print("inside 9")
        mypl = self.place(type,name)
        return mypl
    def place(self,type,name):
        if(type== 'mammal' and name == 'lion'):
            print("returning forest")
            return "Forest"

s = Animals("mammal","lion")