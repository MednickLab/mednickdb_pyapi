import requests, time
from requests import Session

class MednickAPI:
	



	def function(self):
		print("This is a message inside the class.")

	def files(self, study,visit,session,doctype):
		"""This one pass in the parameters for get"""
		
		localhost = 'http://localhost:8001'
		s = requests.Session()
		return s.get(localhost+'/Files?'+'study='+study+'&visit='+visit+'&session='+session+'&doctype='+doctype).text

	def DeletedFiles(self):
		"""This one pass in the parameters for get"""
		
		localhost = 'http://localhost:8001'
		s = requests.Session()
		return s.get(localhost+'/DeletedFiles').text

	def file(self,id):
		"""This one pass in the parameters for get"""
		
		localhost = 'http://localhost:8001'
		s = requests.Session()
		return s.get(localhost+'/File?'+'id='+id).text

	def DownloadFile(self,id):
		"""This one pass in the parameters for get"""
		
		localhost = 'http://localhost:8001'
		s = requests.Session()
		return s.get(localhost+'/DownloadFile?'+'id='+id).text
	
	def TempFiles(self):
		"""This one pass in the parameters for get"""
		localhost = 'http://localhost:8001'
		s = requests.Session()
		return s.get(localhost+'/DeletedFiles').text

	def DocumentTypes(self):
		"""This one pass in the parameters for get"""
		localhost = 'http://localhost:8001'
		s = requests.Session()
		return s.get(localhost+'/DocumentTypes').text	

	def Sessions(self,study,visit):
		"""This one pass in the parameters for get"""
		localhost = 'http://localhost:8001'
		s = requests.Session()
		return s.get(localhost+'/Sessions?'+'study='+study+'&visit='+visit).text

	def Studies(self):
		"""This one pass in the parameters for get"""
		localhost = 'http://localhost:8001'
		s = requests.Session()
		return s.get(localhost+'/Studies').text	

	def Visits(self,study):
		"""This one pass in the parameters for get"""
		
		localhost = 'http://localhost:8001'
		s = requests.Session()
		return s.get(localhost+'/Visits?'+'study='+study).text

	def updateFile(self,id):
		"""This one pass in the parameters for get"""
		
		localhost = 'http://localhost:8001'
		s = requests.Session()
		s.post(localhost+'/UpdateFile/'+id)

	def UploadFile(self):
		"""This one pass in the parameters for get"""
		
		localhost = 'http://localhost:8001'
		s = requests.Session()
		s.post(localhost+'/FileUpload')

	def NewFileRecord(self):
		"""This one pass in the parameters for get"""
		
		localhost = 'http://localhost:8001'
		s = requests.Session()
		s.post(localhost+'/NewFileRecord')

	def TaskData(self):
		"""This one pass in the parameters for get"""
		
		localhost = 'http://localhost:8001'
		s = requests.Session()
		s.post(localhost+'/TaskData')

	def Screenings(self):
		"""This one pass in the parameters for get"""
		
		localhost = 'http://localhost:8001'
		s = requests.Session()
		s.post(localhost+'/Screenings')

	def UpdateParsedStatus(self):
		"""This one pass in the parameters for get"""
		
		localhost = 'http://localhost:8001'
		s = requests.Session()
		s.post(localhost+'/UpdateParsedStatus')


Mednick = MednickAPI();
print(Mednick.files('study4','visit1','session1','screening'))
print ('\n')
print(Mednick.DeletedFiles())
print ('\n')
print(Mednick.file('5977072b60950c2778cd2d33'))
print ('\n')
print(Mednick.Sessions('study1','visit1'))
print ('\n')
#s = requests.Session()
#print(s.get('http://localhost:8001/Files?study=study4&visit=visit1&session=session1&doctype=screening').text)