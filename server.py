import requests, time
from requests import Session



localhost = 'http://localhost:8001'
s = requests.Session()


print (s.get(localhost).text)
