import sys, os

#.py is in the same dir than .wsgi
path=os.path.dirname(os.path.realpath(__file__))

sys.path.insert (0,path)
os.chdir(path)
from castor_wsgi_apis import app as application
