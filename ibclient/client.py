from models import IBWrapper, IBclient

#create client and wrapper
callback = IBWrapper()
client = IBclient(callback, 1)
