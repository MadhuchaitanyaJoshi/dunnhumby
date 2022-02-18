# Python program to check special character

# import required package
import re

# take inputs
string = input('Enter any string: ')

# check string contains special characters or not
if(bool(re.match('^[a-zA-Z0-9]*$', string)) == False):
    print('The string contains special characters.')
else:
    print('String does not contain any special characters.')
