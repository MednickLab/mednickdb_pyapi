import requests, time
from requests import Session

class MednickAPI:
	
	def __init__(self, getName):
		self.getName = getName


	def function(self):
		print("This is a message inside the class.")

	def get(self, getName):
		"""This one pass in the parameters for get"""
		if getName == 'Files':
			localhost = 'http://localhost:8001'
			s = requests.Session()
			return s.get(localhost+'/'+getName).text
		


Mednick = MednickAPI('Files');
print(Mednick.get('Files'))