import requests, time
from requests import Session
import xlrd
import requests
import json
import os


localhost = 'http://localhost:8001'
s = requests.Session()


print (s.get(localhost).text)


