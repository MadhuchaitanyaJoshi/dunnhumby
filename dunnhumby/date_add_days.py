from datetime import datetime,timedelta

def dateConverter(rolling,post_end,post_end_2):
    if(rolling is not None):
        print(rolling)
    if(post_end is not None):
        print(post_end)
    if(post_end_2 is not None):
        print(post_end_2)

dt = '17-08-2021'
dtnew = datetime.strptime(dt,'%d-%m-%Y').date()
print(dtnew)
dtnew14 = dtnew+timedelta(days=21)
print(dtnew14.strftime("%d-%m-%Y"))
dateConverter(14,0,0)